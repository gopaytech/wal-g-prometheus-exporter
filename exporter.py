import os
import signal
import subprocess
import json
import datetime
import re
import argparse
import logging
import time
from logging import warning, info, debug, error  # noqa: F401
from prometheus_client import start_http_server
from prometheus_client import Gauge
import psycopg2
from psycopg2.extras import DictCursor


# Configuration
# -------------

parser = argparse.ArgumentParser()
parser.version = "0.1.0"
parser.add_argument("--archive_dir",
                    help="pg_wal/archive_status/ Directory location", action="store", required=True)
parser.add_argument("--debug", help="enable debug log", action="store_true")
parser.add_argument("--version", help="show binary version", action="version")
parser.add_argument("--config", help="walg config file path", action="store")

args = parser.parse_args()
if args.debug:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

# Disable logging of libs
for key in logging.Logger.manager.loggerDict:
    if key != 'root':
        logging.getLogger(key).setLevel(logging.WARNING)

archive_dir = args.archive_dir
http_port = 9351
DONE_WAL_RE = re.compile(r"^[A-F0-9]{24}\.done$")
READY_WAL_RE = re.compile(r"^[A-F0-9]{24}\.ready$")

# TODO:
# * walg_last_basebackup_duration

# Base backup update
# ------------------


def format_date(bb):
    # fix date format to include timezone
    bb['date_fmt'] = bb['date_fmt'].replace('Z', '%z')
    bb['time'] = parse_date(bb['time'], bb['date_fmt'])
    bb['start_time'] = parse_date(bb['start_time'], bb['date_fmt'])
    bb['finish_time'] = parse_date(bb['finish_time'], bb['date_fmt'])
    return bb


def parse_date(date, fmt):
    fmt = fmt.replace('Z', '%z')
    try:
        return datetime.datetime.strptime(date, fmt)
    except ValueError:
        fmt = fmt.replace('.%f', '')
        return datetime.datetime.strptime(date, fmt)


def get_previous_wal(wal):
    timeline = wal[0:8]
    segment_low = int(wal[16:24], 16) - 1
    segment_high = int(wal[8:16], 16) + (segment_low // 0x100)
    segment_low = segment_low % 0x100
    return '%s%08X%08X' % (timeline, segment_high, segment_low)


def get_next_wal(wal):
    timeline = wal[0:8]
    segment_low = int(wal[16:24], 16) + 1
    segment_high = int(wal[8:16], 16) + (segment_low // 0x100)
    segment_low = segment_low % 0x100
    return '%s%08X%08X' % (timeline, segment_high, segment_low)


def is_before(a, b):
    timeline_a = a[0:8]
    timeline_b = b[0:8]
    if timeline_a != timeline_b:
        return False
    a_int = int(a[8:16], 16) * 0x100 + int(a[16:24], 16)
    b_int = int(b[8:16], 16) * 0x100 + int(b[16:24], 16)
    return a_int < b_int


def wal_diff(a, b):
    timeline_a = a[0:8]
    timeline_b = b[0:8]
    if timeline_a != timeline_b:
        return -1
    a_int = int(a[8:16], 16) * 0x100 + int(a[16:24], 16)
    b_int = int(b[8:16], 16) * 0x100 + int(b[16:24], 16)
    return a_int - b_int

class Exporter():

    def __init__(self):
        self.basebackup_exception = False
        self.xlog_exception = False
        self.bbs = []
        self.last_archive_check = None
        self.archive_status = None

        # Declare metrics
        self.basebackup = Gauge('walg_basebackup',
                                'Remote Basebackups',
                                ['start_wal_segment', 'start_lsn'])
        self.basebackup_count = Gauge('walg_basebackup_count',
                                      'Remote Basebackups count')
        self.basebackup_count.set_function(lambda: len(self.bbs))

        self.last_upload = Gauge('walg_last_upload',
                                 'Last upload of incremental or full backup',
                                 ['type'])
        self.last_upload.labels('xlog').set_function(
            self.last_xlog_upload_callback)
        self.last_upload.labels('basebackup').set_function(
            lambda: self.bbs[len(self.bbs) - 1]['time'].timestamp()
            if self.bbs else 0
        )
        self.oldest_basebackup = Gauge('walg_oldest_basebackup',
                                       'oldest full backup')
        self.oldest_basebackup.set_function(
            lambda: self.bbs[0]['time'].timestamp() if self.bbs else 0
        )

        self.xlog_ready = Gauge('walg_missing_remote_wal_segment_at_end',
                                'Xlog ready for upload')
        self.xlog_ready.set_function(self.xlog_ready_callback)

        self.exception = Gauge('walg_exception',
                               'Wal-g exception: 2 for basebackup error, '
                               '3 for xlog error and '
                               '5 for remote error')
        self.exception.set_function(
            lambda: (1 if self.basebackup_exception else 0 +
                     2 if self.xlog_exception else 0))

        self.xlog_since_last_bb = Gauge('walg_xlogs_since_basebackup',
                                        'Xlog uploaded since last base backup')
        self.xlog_since_last_bb.set_function(self.xlog_since_last_bb_callback)

        self.last_backup_duration = Gauge('walg_last_backup_duration',
                                          'Duration of the last full backup')
        self.last_backup_duration.set_function(
            lambda: ((self.bbs[len(self.bbs) - 1]['finish_time'] -
                      self.bbs[len(self.bbs) - 1]['start_time']).total_seconds()
                     if self.bbs else 0)
        )

        # Fetch remote base backups
        self.update_basebackup()

    def update_basebackup(self, *unused):
        """
            When this script receive a SIGHUP signal, it will call backup-list
            and update metrics about basebackups
        """

        info('Updating basebackups metrics...')
        try:
            # Fetch remote backup list
            command = ["wal-g", "backup-list",
                                  "--detail", "--json"]
            if args.config:
                command.extend(["--config", args.config])

            res = subprocess.run(command,
                                 capture_output=True, check=True)

            # Check if backup-list return an empty result
            if res.stdout.decode("utf-8") is "":
                new_bbs = []
            else:
                new_bbs = list(map(format_date, json.loads(res.stdout)))

            new_bbs.sort(key=lambda bb: bb['time'])
            new_bbs_name = [bb['backup_name'] for bb in new_bbs]
            old_bbs_name = [bb['backup_name'] for bb in self.bbs]
            bb_deleted = 0

            # Remove metrics for deleted backups
            for bb in self.bbs:
                if bb['backup_name'] not in new_bbs_name:
                    # Backup deleted
                    self.basebackup.remove(bb['wal_file_name'],
                                           bb['start_lsn'])
                    bb_deleted = bb_deleted + 1
            # Add metrics for new backups
            for bb in new_bbs:
                if bb['backup_name'] not in old_bbs_name:
                    (self.basebackup.labels(bb['wal_file_name'],
                                            bb['start_lsn'])
                     .set(bb['time'].timestamp()))

            if len(new_bbs) == 0:
                info("No basebackups found")
            else:
                # Update backup list
                self.bbs = new_bbs
                info("%s basebackups found (last: %s), %s deleted",
                     len(self.bbs),
                     self.bbs[len(self.bbs) - 1]['time'],
                     bb_deleted)

            self.basebackup_exception = False
        except subprocess.CalledProcessError as e:
            error(e)
            self.basebackup_exception = True

    def last_archive_status(self):
        if (self.last_archive_check is None or
                datetime.datetime.now().timestamp() -
                self.last_archive_check > 1):
            self.archive_status = self._last_archive_status()
            self.last_archive_check = datetime.datetime.now().timestamp()
        return self.archive_status

    def _last_archive_status(self):
        with psycopg2.connect(
            host=os.getenv('PGHOST', 'localhost'),
            port=os.getenv('PGPORT', '5432'),
            user=os.getenv('PGUSER', 'postgres'),
            password=os.getenv('PGPASSWORD'),
            dbname=os.getenv('PGDATABASE', 'postgres'),

        ) as db_connection:
            db_connection.autocommit = True
            with db_connection.cursor(cursor_factory=DictCursor) as c:
                c.execute('SELECT archived_count, failed_count, '
                          'last_archived_wal, '
                          'last_archived_time, '
                          'last_failed_wal, '
                          'last_failed_time '
                          'FROM pg_stat_archiver')
                res = c.fetchone()
                if not bool(result):
                    raise Exception("Cannot fetch archive status")
                return res

    def last_xlog_upload_callback(self):
        archive_status = self.last_archive_status()
        if archive_status['last_archived_time'] is None:
            raise Exception("There is no WAL archiver process running on this postgresql\nCheck with SELECT * FROM pg_stat_archiver;")
        else:
            return archive_status['last_archived_time'].timestamp()

    def xlog_ready_callback(self):
        res = 0
        try:
            for f in os.listdir(archive_dir):
                # search for xlog waiting for upload
                if READY_WAL_RE.match(f):
                    res += 1
            self.xlog_exception = 0
        except FileNotFoundError:
            self.xlog_exception = 1
        return res

    def xlog_since_last_bb_callback(self):
        # Compute xlog_since_last_basebackup
        if self.bbs:
            archive_status = self.last_archive_status()
            return wal_diff(archive_status['last_archived_wal'],
                            self.bbs[len(self.bbs) - 1]['wal_file_name'])
        else:
            return 0


if __name__ == '__main__':
    info("Startup...")
    info('My PID is: %s', os.getpid())

    # Start up the server to expose the metrics.
    start_http_server(http_port)
    # Test debug
    info("Webserver started on port %s", http_port)
    info("PGHOST %s", os.getenv('PGHOST', 'localhost'))
    info("PGUSER %s", os.getenv('PGUSER', 'postgres'))
    info("PGDATABASE %s", os.getenv('PGDATABASE', 'postgres'))
    info("WALG_GS_PREFIX %s", os.getenv('WALG_GS_PREFIX', ''))

    # Check if this is a master instance
    while True:
        try:
            with psycopg2.connect(
                host=os.getenv('PGHOST', 'localhost'),
                port=os.getenv('PGPORT', '5432'),
                user=os.getenv('PGUSER', 'postgres'),
                password=os.getenv('PGPASSWORD'),
                dbname=os.getenv('PGDATABASE', 'postgres'),

            ) as db_connection:
                db_connection.autocommit = True
                with db_connection.cursor() as c:
                    c.execute("SELECT pg_is_in_recovery();")
                    result = c.fetchone()
                    info("Is in recovery mode? %s", result[0])
                    break
        except Exception:
            error("Unable to connect postgres server, retrying in 60sec...")
            time.sleep(60)

    # Launch exporter
    exporter = Exporter()

    # listen to SIGHUP signal
    signal.signal(signal.SIGHUP, exporter.update_basebackup)

    while True:
        # Periodically update backup-list
        exporter.update_basebackup()
        time.sleep(30)
