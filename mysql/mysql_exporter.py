import os
import signal
import subprocess
import json
import datetime
import argparse
import logging
import time
from logging import info, error
from prometheus_client import start_http_server, Gauge
import pymysql
from dotenv import load_dotenv
from pathlib import Path
import configparser

# Metrics required:
# 1. basebackup (detailed gauge per backup)
# 2. basebackup_count
# 3. oldest_basebackup
# 4. latest binlog (SHOW MASTER STATUS)
# 5. latest binlog uploaded to storage (wal-g binlog-find)
# 6. last_backup_duration
# 7. basebackup exception flag

config_mysql = {}
config_exporter = {}
walg_binary_path = os.getenv("WALG_BINARY_PATH", "/usr/local/bin/wal-g")

parser = argparse.ArgumentParser()
parser.version = "0.3.1"
parser.add_argument("--archive_dir", required=True, help="MySQL binlog directory (usually datadir)")
parser.add_argument("--config", help="wal-g config file path")
parser.add_argument("--debug", action="store_true", help="Enable debug logging")
parser.add_argument("--once", action="store_true", help="Run collection once then exit (debug/testing)")
parser.add_argument("--port", type=int, help="HTTP listen port (override config/env)")
args = parser.parse_args()

logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)
for key in logging.Logger.manager.loggerDict:
    if key != 'root':
        logging.getLogger(key).setLevel(logging.WARNING)

if args.config:
    cfg = configparser.ConfigParser()
    cfg.read(args.config)
    if 'mysql' in cfg:
        config_mysql = dict(cfg['mysql'])
    if 'exporter' in cfg:
        config_exporter = dict(cfg['exporter'])

archive_dir = args.archive_dir
http_port = args.port or int(config_exporter.get('port', os.getenv('EXPORTER_PORT', 9352)))
scrape_interval = int(config_exporter.get('walg_exporter_scrape_interval', os.getenv('WALG_EXPORTER_SCRAPE_INTERVAL', 60)))

terminate = False

def signal_handler(sig, frame):  # noqa: ARG001
    global terminate
    terminate = True


def parse_backup_dates(bb):
    def _parse(value):
        if not value:
            return None
        if value.endswith('Z'):
            value = value[:-1]
        if '.' in value:
            main, frac = value.split('.', 1)
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

    start_raw = bb.get('start_time') or bb.get('start_local_time') or bb.get('time') or bb.get('modify_time')
    stop_raw = bb.get('finish_time') or bb.get('stop_local_time') or bb.get('stop_time') or bb.get('modify_time')
    bb['start_time'] = _parse(start_raw)
    bb['finish_time'] = _parse(stop_raw)
    return bb


class MySQLExporter:
    def __init__(self, conn_args):
        self.conn_args = conn_args
        self.basebackup_exception = False
        self.bbs = []
        self.latest_uploaded_binlog = None
        self.latest_active_binlog = None

        # Metrics
        self.basebackup = Gauge('walg_mysql_basebackup', 'Remote MySQL basebackups',
                                ['backup_name', 'uncompressed_size', 'compressed_size', 'start_time', 'finish_time'])
        self.basebackup_count = Gauge('walg_mysql_basebackup_count', 'Number of basebackups')
        self.basebackup_exception_flag = Gauge('walg_mysql_basebackup_exception', '1 if basebackup retrieval failed else 0')
        self.oldest_basebackup = Gauge('walg_mysql_oldest_basebackup', 'Oldest basebackup start time (unix seconds)')
        self.last_backup_duration = Gauge('walg_mysql_last_backup_duration', 'Duration seconds of last basebackup')
        self.latest_active_binlog_gauge = Gauge('walg_mysql_latest_active_binlog', 'Current active binlog file', ['file'])
        self.latest_uploaded_binlog_gauge = Gauge('walg_mysql_latest_uploaded_binlog', 'Latest uploaded binlog file (wal-g storage)', ['file'])

        self.basebackup_count.set_function(lambda: len(self.bbs))
        self.oldest_basebackup.set_function(self._oldest_bb_callback)
        self.last_backup_duration.set_function(self._last_backup_duration_callback)

    # ---- Basebackup ----
    def update_basebackups(self):
        try:
            cmd = [walg_binary_path, 'backup-list', '--detail', '--json']
            if args.config:
                cmd.extend(['--config', args.config])
            res = subprocess.run(cmd, capture_output=True, check=True)
            out = res.stdout.decode('utf-8').strip()
            if not out:
                new_bbs = []
            else:
                raw_bbs = json.loads(out)
                new_bbs = [parse_backup_dates(bb) for bb in raw_bbs]
        except subprocess.CalledProcessError:
            # Fallback plain list
            try:
                cmd = [walg_binary_path, 'backup-list']
                if args.config:
                    cmd.extend(['--config', args.config])
                res = subprocess.run(cmd, capture_output=True, check=True)
                lines = [l.strip() for l in res.stdout.decode('utf-8').splitlines() if l.strip()]
                new_bbs = []
                if lines and len(lines) > 1:
                    for line in lines[1:]:
                        parts = line.split()
                        if len(parts) < 2:
                            continue
                        name = parts[0]
                        modified = parts[1]
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
            except Exception as e:  # noqa: BLE001
                error(f"backup-list fallback failed: {e}")
                self.basebackup_exception = True
                self.bbs = []
                return
        except FileNotFoundError:
            error("wal-g binary not found for backup-list")
            self.basebackup_exception = True
            self.bbs = []
            return
        except Exception as e:  # noqa: BLE001
            error(f"Unexpected error listing backups: {e}")
            self.basebackup_exception = True
            self.bbs = []
            return

        # Update gauges
        existing_names = {bb['backup_name'] for bb in self.bbs if 'backup_name' in bb}
        new_names = {bb['backup_name'] for bb in new_bbs if 'backup_name' in bb}
        for old in self.bbs:
            if old.get('backup_name') not in new_names:
                try:
                    self.basebackup.remove(old.get('backup_name'),
                                           str(old.get('uncompressed_size', 0)),
                                           str(old.get('compressed_size', 0)),
                                           '', '')
                except Exception:  # noqa: BLE001
                    pass

        for bb in new_bbs:
            if bb.get('backup_name') not in existing_names:
                st = bb.get('start_time')
                ft = bb.get('finish_time')
                st_label = st.isoformat().replace('+00:00', 'Z') if isinstance(st, datetime.datetime) else ''
                ft_label = ft.isoformat().replace('+00:00', 'Z') if isinstance(ft, datetime.datetime) else ''
                self.basebackup.labels(
                    bb.get('backup_name'),
                    str(bb.get('uncompressed_size', 0)),
                    str(bb.get('compressed_size', 0)),
                    st_label,
                    ft_label
                ).set((ft or st or datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)).timestamp())

        new_bbs.sort(key=lambda x: x.get('start_time') or datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc))
        self.bbs = new_bbs
        self.basebackup_exception = False
        if self.bbs:
            info(f"{len(self.bbs)} basebackups found")
        else:
            info("No MySQL basebackups found")


    # ---- Binlogs ----
    def update_binlogs(self):
        # Latest uploaded via wal-g binlog-find (plain text, last match wins)
        try:
            cmd = [walg_binary_path, 'binlog-find']
            if args.config:
                cmd.extend(['--config', args.config])
            res = subprocess.run(cmd, capture_output=True, check=True)
            stdout = res.stdout.decode('utf-8', errors='replace')
            stderr = res.stderr.decode('utf-8', errors='replace')
            # wal-g often writes INFO/WARNING (and even the discovered binlog line) to stderr
            if args.debug:
                info(f"binlog-find stdout:\n{stdout}\n--- stderr ---\n{stderr}")
            combined = '\n'.join([stdout, stderr]).strip()
            latest = None
            for raw_line in combined.splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                # Try to extract filenames from any tokens in the line
                for token in line.split():
                    if token.startswith('mysql-bin.') or token.startswith('binlog.'):
                        latest = token
            if latest:
                self.latest_uploaded_binlog = latest
                self.latest_uploaded_binlog_gauge.labels(file=latest).set(1)
            else:
                if args.debug:
                    info('binlog-find produced no identifiable binlog filename')
        except subprocess.CalledProcessError as e:  # noqa: PERF203
            error(f"binlog-find failed: {e}")
        except FileNotFoundError:
            error("wal-g binary not found for binlog-find")
        except Exception as e:  # noqa: BLE001
            error(f"Unexpected binlog-find error: {e}")

        # Active binlog via SHOW MASTER STATUS
        try:
            conn = pymysql.connect(**self.conn_args)
            with conn:
                with conn.cursor(pymysql.cursors.DictCursor) as c:
                    c.execute('SHOW MASTER STATUS')
                    row = c.fetchone()
                    if row and row.get('File'):
                        self.latest_active_binlog = row['File']
                        self.latest_active_binlog_gauge.labels(file=row['File']).set(1)
        except Exception as e:  # noqa: BLE001
            error(f"SHOW MASTER STATUS failed: {e}")

    # ---- Metric callbacks ----
    def _oldest_bb_callback(self):
        if not self.bbs:
            return 0
        first = self.bbs[0]
        st = first.get('start_time')
        return st.timestamp() if isinstance(st, datetime.datetime) else 0

    def _last_backup_duration_callback(self):
        if not self.bbs:
            return 0
        last = self.bbs[-1]
        st = last.get('start_time')
        ft = last.get('finish_time')
        if st and ft:
            return (ft - st).total_seconds()
        return 0

    def basebackup_exception_status(self):
        return 1 if self.basebackup_exception else 0


def main():
    info("Startup MySQL WAL-G exporter")
    signal.signal(signal.SIGTERM, signal_handler)

    dotenv_path = Path('/etc/default/walg.env')
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path)

    # Connection params (env > defaults) - config file overrides handled earlier if desired
    dbhost = config_mysql.get('host') or os.getenv('MYSQL_HOST', 'localhost')
    dbport = int(config_mysql.get('port') or os.getenv('MYSQL_PORT', '3306'))
    dbuser = config_mysql.get('user') or os.getenv('MYSQL_USER', 'root')
    dbpassword = config_mysql.get('password') or os.getenv('MYSQL_PASSWORD', '')
    dbname = config_mysql.get('database') or os.getenv('MYSQL_DATABASE', 'mysql')
    ssl_disabled = str(config_mysql.get('ssl_disabled', 'false')).lower() in ('1', 'true', 'yes', 'on')

    conn_args = dict(host=dbhost, port=dbport, user=dbuser, password=dbpassword, database=dbname, charset='utf8mb4', connect_timeout=10)
    if ssl_disabled:
        conn_args['ssl'] = None

    exporter = MySQLExporter(conn_args)
    # Add dynamic exception gauge (value provided via function)
    exporter.basebackup_exception_flag.set_function(exporter.basebackup_exception_status)

    start_http_server(http_port)
    info(f'Exporter listening on {http_port}')

    # Warm-up DB connectivity
    while True:
        try:
            conn = pymysql.connect(**conn_args)
            with conn:
                with conn.cursor() as c:
                    c.execute('SELECT 1')
                    c.fetchone()
            break
        except Exception as e:  # noqa: BLE001
            error(f"Initial DB connect failed: {e}")
            if args.once:
                return
            time.sleep(5)

    # Main loop
    while True:
        if terminate:
            info('Shutdown requested')
            break
        try:
            exporter.update_basebackups()
            exporter.update_binlogs()
        except Exception as e:  # noqa: BLE001
            error(f"Loop error: {e}")
        if args.once:
            break
        time.sleep(scrape_interval)


if __name__ == '__main__':
    main()
