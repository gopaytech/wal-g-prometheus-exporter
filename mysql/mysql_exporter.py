import os
import signal
import subprocess
import json
import datetime
import re
import argparse
import logging
import time
import math
from logging import info, error
from prometheus_client import start_http_server, Gauge
import pymysql  # CHANGED: Use pymysql instead of mysql.connector
from dotenv import load_dotenv
from pathlib import Path
import configparser

# ------------------
# CLI / Config
# ------------------
config_mysql = {}
config_exporter = {}
walg_binary_path = os.getenv("WALG_BINARY_PATH", "/usr/local/bin/wal-g")
parser = argparse.ArgumentParser()
parser.version = "0.3.1"
parser.add_argument("--archive_dir",
                    help="MySQL binlog directory (usually datadir)",
                    action="store", required=True)
parser.add_argument("--config", help="wal-g config file path", action="store")
parser.add_argument("--debug", help="enable debug log", action="store_true")
parser.add_argument("--version", help="show binary version", action="version")

args = parser.parse_args()
logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)

for key in logging.Logger.manager.loggerDict:
    if key != 'root':
        logging.getLogger(key).setLevel(logging.WARNING)

# Load config file if provided
if args.config:
    config = configparser.ConfigParser()
    config.read(args.config)
    if 'mysql' in config:
        config_mysql = dict(config['mysql'])
    if 'exporter' in config:
        config_exporter = dict(config['exporter'])

archive_dir = args.archive_dir
# Exporter port: config file > env > default
http_port = int(config_exporter.get('port', os.getenv('EXPORTER_PORT', 9351)))
BINLOG_RE = re.compile(r"^mysql-bin\.[0-9]{6}$")
terminate = False


def signal_handler(sig, frame):  # noqa: ARG001
    global terminate
    info('SIGTERM received, preparing to shutdown')
    terminate = True


def parse_backup_dates(bb):
    """Normalize wal-g MySQL backup list date fields to datetime objects.

    wal-g (mysql flavor) currently returns JSON keys like:
      - start_local_time
      - stop_local_time
      - modify_time
    whereas postgres flavor used start_time / finish_time. Map flexibly.
    """

    def _parse(value):
        if not value:
            return None
        # Trim trailing Z and keep fractional seconds (may be >6 digits)
        if value.endswith('Z'):
            value = value[:-1]
        # Split fractional part to microseconds precision for datetime
        if '.' in value:
            main, frac = value.split('.', 1)
            # Keep up to 6 digits for microseconds
            frac = ''.join(ch for ch in frac if ch.isdigit())
            frac = (frac + '000000')[:6]
            value_norm = f"{main}.{frac}"
            fmt = "%Y-%m-%dT%H:%M:%S.%f"
        else:
            value_norm = value
            fmt = "%Y-%m-%dT%H:%M:%S"
        try:
            return datetime.datetime.strptime(value_norm, fmt).replace(tzinfo=datetime.timezone.utc)
        except Exception:  # noqa: BLE001
            return None

    # Prefer explicit mysql keys, then generic ones
    start_raw = bb.get('start_time') or bb.get('start_local_time') or bb.get('time') or bb.get('modify_time')
    stop_raw = bb.get('finish_time') or bb.get('stop_local_time') or bb.get('stop_time') or bb.get('modify_time')
    bb['start_time'] = _parse(start_raw)
    bb['finish_time'] = _parse(stop_raw)
    return bb


def convert_size(size_bytes):
    try:
        size_bytes = int(size_bytes)
    except Exception:  # noqa: BLE001
        return str(size_bytes)
    if size_bytes == 0:
        return "0B"
    units = ("B", "KB", "MB", "GB", "TB", "PB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {units[i]}"


class MySQLExporter:
    def __init__(self):
        self.basebackup_exception = False
        self.binlog_exception = False
        self.bbs = []
        self.last_archive_check = None
        self.archive_status_cache = None

        # Base backup metrics (labels adapted for MySQL – fewer WAL specific fields)
        self.basebackup = Gauge('walg_mysql_basebackup', 'Remote MySQL basebackups',
                                ['backup_name', 'uncompressed_size', 'compressed_size',
                                 'start_time', 'finish_time'])
        self.basebackup_count = Gauge('walg_mysql_basebackup_count', 'Remote MySQL basebackup count')
        self.basebackup_count.set_function(lambda: len(self.bbs))

        self.last_upload = Gauge('walg_mysql_last_upload', 'Last upload of binlog or basebackup', ['type'])
        self.last_upload.labels('binlog').set_function(self.last_binlog_upload_callback)
        self.last_upload.labels('basebackup').set_function(
            lambda: (self.bbs[-1]['start_time'].timestamp() if self.bbs and self.bbs[-1]['start_time'] else 0)
        )
        self.oldest_basebackup = Gauge('walg_mysql_oldest_basebackup', 'Oldest MySQL basebackup')
        self.oldest_basebackup.set_function(
            lambda: (self.bbs[0]['start_time'].timestamp() if self.bbs and self.bbs[0]['start_time'] else 0)
        )

        self.binlog_ready = Gauge('walg_mysql_binlog_files_present', 'Binlog files present in archive_dir (not necessarily uploaded)')
        self.binlog_ready.set_function(self.binlog_ready_callback)

        self.exception = Gauge('walg_mysql_exception', 'Wal-g exception: 1 basebackup, 2 binlog, 3 both')
        self.exception.set_function(lambda: (1 if self.basebackup_exception else 0) + (2 if self.binlog_exception else 0))

        self.binlog_since_last_bb = Gauge('walg_mysql_binlogs_since_basebackup', 'Binlog index delta since last basebackup')
        self.binlog_since_last_bb.set_function(self.binlog_since_last_bb_callback)

        self.last_backup_duration = Gauge('walg_mysql_last_backup_duration', 'Duration seconds of last basebackup')
        self.last_backup_duration.set_function(
            lambda: ((self.bbs[-1]['finish_time'] - self.bbs[-1]['start_time']).total_seconds()
                     if self.bbs and self.bbs[-1]['finish_time'] and self.bbs[-1]['start_time'] else 0)
        )

        self.binlog_integrity_status = Gauge('walg_mysql_binlog_integrity_status', 'Overall binlog archive integrity status', ['status'])
        self.binlog_archive_count = Gauge('walg_mysql_binlog_archive_count', 'Total binlog entries counted in wal-g integrity report')
        self.binlog_archive_missing_count = Gauge('walg_mysql_binlog_archive_missing_count', 'Total missing binlog entries in wal-g report')

    # ------------- Binlog / Integrity -------------
    def update_binlog_status(self):
        """Call wal-g binlog-verify integrity if available, else graceful no-op."""
        if os.getenv('WALG_EXPORTER_ENABLE_BINLOG_VERIFY', 'false').lower() not in ('1', 'true', 'yes', 'on'):  # feature gate off -> fallback
            self._fallback_binlog_count()
            return
        try:
            command = [walg_binary_path, 'binlog-verify', 'integrity', '--json']
            if args.config:
                command.extend(["--config", args.config])
            res = subprocess.run(command, capture_output=True, check=True)
        except FileNotFoundError:
            logging.warning("wal-g binary not found; skipping binlog integrity")
            return
        except subprocess.CalledProcessError as e:  # noqa: PERF203
            error(e)
            self.binlog_exception = True
            # Fallback to simple binlog file count so metrics remain informative
            self._fallback_binlog_count()
            return

        stdout = res.stdout.decode('utf-8').strip()
        if not stdout:
            binlog_list = []
            integrity_status = None
        else:
            try:
                payload = json.loads(stdout)
                integrity = payload.get('integrity', {})
                binlog_list = integrity.get('details', []) or []
                integrity_status = integrity.get('status')
            except Exception as e:  # noqa: BLE001
                error(f"Cannot parse wal-g binlog-verify output: {e}")
                binlog_list = []
                integrity_status = None

        found = 0
        missing = 0
        for entry in binlog_list:
            if entry.get('status') == 'FOUND':
                found += entry.get('segments_count', 0)
            else:
                missing += entry.get('segments_count', 0)

        # Integrity status metrics
        if integrity_status == 'OK':
            self.binlog_integrity_status.labels('OK').set(1)
            self.binlog_integrity_status.labels('FAILURE').set(0)
        elif integrity_status:
            self.binlog_integrity_status.labels('OK').set(0)
            self.binlog_integrity_status.labels('FAILURE').set(1)

        self.binlog_archive_count.set(found)
        self.binlog_archive_missing_count.set(missing)
        # If wal-g returned nothing meaningful, also fallback to simple count
        if found == 0 and missing == 0:
            self._fallback_binlog_count()

    def _fallback_binlog_count(self):
        """Fallback when binlog-verify isn't available: count local binlog files."""
        try:
            cnt = 0
            for f in os.listdir(archive_dir):
                if BINLOG_RE.match(f):
                    cnt += 1
            self.binlog_archive_count.set(cnt)
            self.binlog_archive_missing_count.set(0)
            # Neutral integrity status (both 0) so alerts relying on explicit OK/FAILURE can detect absence
            self.binlog_integrity_status.labels('OK').set(0)
            self.binlog_integrity_status.labels('FAILURE').set(0)
        except Exception:  # noqa: BLE001
            # On error mark as binlog exception but avoid raising
            self.binlog_exception = True

    # ------------- Basebackup -------------
    def update_basebackup(self):
        info('Updating MySQL basebackup metrics...')
        try:
            cmd = [walg_binary_path, "backup-list", "--detail", "--json"]
            if args.config:
                cmd.extend(["--config", args.config])
            try:
                res = subprocess.run(cmd, capture_output=True, check=True)
                out = res.stdout.decode('utf-8').strip()
                if not out:
                    new_bbs = []
                else:
                    raw_bbs = json.loads(out)
                    new_bbs = [parse_backup_dates(bb) for bb in raw_bbs]
            except subprocess.CalledProcessError:
                # Fallback: plain text parsing (MySQL wal-g may not support --json)
                fallback_cmd = ["wal-g", "backup-list"]
                if args.config:
                    fallback_cmd.extend(["--config", args.config])
                res = subprocess.run(fallback_cmd, capture_output=True, check=True)
                lines = [l.strip() for l in res.stdout.decode('utf-8').splitlines() if l.strip()]
                new_bbs = []
                if lines and len(lines) > 1:
                    # Skip header line
                    for line in lines[1:]:
                        parts = line.split()
                        if len(parts) < 2:
                            continue
                        name = parts[0]
                        modified = parts[1]
                        # Attempt parse time
                        try:
                            dt = datetime.datetime.strptime(modified, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
                        except Exception:  # noqa: BLE001
                            dt = None
                        new_bbs.append({
                            'backup_name': name,
                            'start_time': dt,
                            'finish_time': dt,
                            'uncompressed_size': 0,
                            'compressed_size': 0
                        })
        except FileNotFoundError:
            logging.warning("wal-g binary not found; skipping basebackup metrics")
            return
        except subprocess.CalledProcessError as e:  # noqa: PERF203
            error(e)
            self.basebackup_exception = True
            return
        except Exception as e:  # noqa: BLE001
            error(f"Unexpected error reading backups: {e}")
            self.basebackup_exception = True
            return

        new_bbs.sort(key=lambda bb: bb.get('start_time') or datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc))
        existing_names = {bb['backup_name'] for bb in self.bbs if 'backup_name' in bb}
        new_names = {bb['backup_name'] for bb in new_bbs if 'backup_name' in bb}

        # Remove stale metrics
        for old in self.bbs:
            if old.get('backup_name') not in new_names:
                try:
                    self.basebackup.remove(old.get('backup_name'),
                                           convert_size(old.get('uncompressed_size', 0)),
                                           convert_size(old.get('compressed_size', 0)),
                                           old.get('start_time'),
                                           old.get('finish_time'))
                except Exception:  # noqa: BLE001
                    pass

        # Add new metrics
        for bb in new_bbs:
            if bb.get('backup_name') not in existing_names:
                start_dt = bb.get('start_time')
                finish_dt = bb.get('finish_time')
                # Format start/finish timestamps to ISO8601 Z form for label values (empty string if missing)
                start_label = (start_dt.isoformat().replace('+00:00', 'Z')
                               if isinstance(start_dt, datetime.datetime) else '')
                finish_label = (finish_dt.isoformat().replace('+00:00', 'Z')
                                if isinstance(finish_dt, datetime.datetime) else '')
                self.basebackup.labels(
                    bb.get('backup_name'),
                    convert_size(bb.get('uncompressed_size', 0)),
                    convert_size(bb.get('compressed_size', 0)),
                    start_label,
                    finish_label
                ).set(((finish_dt or start_dt) or datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)).timestamp())

        self.bbs = new_bbs
        self.basebackup_exception = False
        if self.bbs:
            info("%d basebackups found", len(self.bbs))
        else:
            info("No MySQL basebackups found")

    # ------------- Archive status -------------
    def last_archive_status(self):
        if (self.last_archive_check is None or time.time() - self.last_archive_check > 1):
            self.archive_status_cache = self._last_archive_status()
            self.last_archive_check = time.time()
        return self.archive_status_cache

    def _last_archive_status(self):
        # Map MySQL binlog info to expected fields
        try:
            conn = pymysql.connect(  # CHANGED: Use pymysql
                host=os.getenv('MYSQL_HOST', 'localhost'),
                port=int(os.getenv('MYSQL_PORT', '3306')),
                user=os.getenv('MYSQL_USER', 'root'),
                password=os.getenv('MYSQL_PASSWORD', ''),
                database=os.getenv('MYSQL_DATABASE', 'mysql'),
                charset='utf8mb4',
                connect_timeout=10
            )
            with conn:
                with conn.cursor(pymysql.cursors.DictCursor) as c:  # CHANGED: Use pymysql cursor
                    c.execute('SHOW MASTER STATUS')
                    row = c.fetchone()
                    if not row:
                        return {
                            'last_archived_file': None,
                            'last_archived_time': None
                        }
                    return {
                        'last_archived_file': row.get('File'),
                        # Using current time as proxy – MySQL does not track file archived time
                        'last_archived_time': datetime.datetime.now(datetime.timezone.utc)
                    }
        except Exception as e:
            error(f"Error getting archive status: {e}")
            return {
                'last_archived_file': None,
                'last_archived_time': None
            }

    # ------------- Metric callbacks -------------
    def last_binlog_upload_callback(self):
        status = self.last_archive_status()
        ts = status.get('last_archived_time')
        return ts.timestamp() if ts else 0

    def binlog_ready_callback(self):
        cnt = 0
        try:
            for f in os.listdir(archive_dir):
                if BINLOG_RE.match(f):
                    cnt += 1
            self.binlog_exception = False
        except FileNotFoundError:
            self.binlog_exception = True
        return cnt

    def binlog_since_last_bb_callback(self):
        if not self.bbs:
            return 0
        # Derive binlog numeric part vs last basebackup reference (not available -> 0)
        try:
            status = self.last_archive_status()
            last_file = status.get('last_archived_file')
            if not last_file:
                return 0
            # mysql-bin.000123
            num = int(last_file.split('.')[-1])
            return num  # simple representation; could subtract basebackup start binlog if known
        except Exception:  # noqa: BLE001
            return 0


if __name__ == '__main__':
    info("Startup MySQL WAL-G exporter...")
    info('My PID is: %s', os.getpid())
    signal.signal(signal.SIGTERM, signal_handler)

    dotenv_path = Path('/etc/default/walg.env')
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path)

    # MySQL connection params: config file > env > default
    # Always use config file values if present, else fallback to env/defaults
    dbhost = config_mysql.get('host') or os.getenv('MYSQL_HOST', 'localhost')
    dbport = config_mysql.get('port') or os.getenv('MYSQL_PORT', '3306')
    dbuser = config_mysql.get('user') or os.getenv('MYSQL_USER', 'root')
    dbpassword = config_mysql.get('password') or os.getenv('MYSQL_PASSWORD', '')
    dbname = config_mysql.get('database') or os.getenv('MYSQL_DATABASE', 'mysql')
    scrape_interval = int(config_exporter.get('walg_exporter_scrape_interval') or os.getenv('WALG_EXPORTER_SCRAPE_INTERVAL', 60))
    ssl_disabled = str(config_mysql.get('ssl_disabled', 'false')).lower() in ('1', 'true', 'yes', 'on')

    info(f"MySQL connection params: host={dbhost}, port={dbport}, user={dbuser}, db={dbname}, ssl_disabled={ssl_disabled}")

    start_http_server(http_port)
    info('Exporter listening on %d', http_port)

    # Connectivity warm-up - CHANGED: Use pymysql
    while True:
        if terminate:
            info('Terminating before initial connection')
            exit(0)
        try:
            conn_args = dict(
                host=dbhost,
                port=int(dbport),
                user=dbuser,
                password=dbpassword,
                database=dbname,
                charset='utf8mb4',
                connect_timeout=10
            )
            if ssl_disabled:
                conn_args['ssl'] = None
            conn = pymysql.connect(**conn_args)
            with conn:
                with conn.cursor() as c:
                    c.execute("SELECT 1")
                    c.fetchone()
                break
        except Exception as e:  # noqa: BLE001
            error(f"Unable to connect MySQL server: {e}; retrying in {scrape_interval}s...")
            time.sleep(scrape_interval)

    exporter = None
    first = True
    while True:
        if terminate:
            info('Shutdown requested')
            break
        try:
            enabled_flag = os.getenv('WALG_EXPORTER_ENABLE', 'true').lower() == 'true' or \
                os.path.isfile('/var/lib/mysql/walg_exporter.enable')
            if not enabled_flag:
                info('Exporter disabled - sleeping')
                time.sleep(scrape_interval)
                continue
            if first:
                exporter = MySQLExporter()
                first = False
            exporter.update_basebackup()
            exporter.update_binlog_status()
        except Exception as e:  # noqa: BLE001
            error(f"Loop error: {e}")
        time.sleep(scrape_interval)