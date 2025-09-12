# wal-g-prometheus-exporter

Exporter for WAL-G that runs in PostgreSQL and MySQL instances.

## Requirements

1. Python 3.7+ (For `make build-binary`).
2. WAL-G binary must be installed.
3. This exporter require a flag file called `/var/lib/postgresql/walg_exporter.enable`
  The exporter will keep trying every `WALG_EXPORTER_SCRAPE_INTERVAL` seconds to see if the flag exists or not. 
  This flag file feature can be used in case when there is a failover event happens, for example if the exporter supposed to be run only in master node.

## Build
For PostgreSQL 
1. For Linux, you can use the binary from `make build`. It should run on most Linux distro.
2. For specific OS or to run in your own machine, you can get the binary with `make build-binary` and use the binary output.

For MySQL
1. For specific OS or to run in your own machine, you can get the binary with `make build-binary-mysql` and use the binary output.

## Usage

```
usage: wal-g-exporter [-h] --archive_dir=ARCHIVE_DIR [--debug] [--config=CONFIG_FILE_PATH] [--version]

optional arguments:
  -h, --help            show this help message and exit
  --archive_dir ARCHIVE_DIR
                        pg_wal/archive_status/ Directory location
  --config CONFIG_FILE_PATH
                        file path for wal-g config
  --debug               enable debug log
  --version             show binary version
```

## Exposed Metrics for PostgreSQL

```
# HELP walg_basebackup Remote Basebackups
# TYPE walg_basebackup gauge
walg_basebackup{compressed_size="31.15 GB",finish_lsn="154846241808",finish_time="2024-02-14 08:11:02.501703+00:00",is_permanent="False",start_lsn="154464435792",start_time="2024-02-14 08:01:01.469195+00:00",start_wal_segment="0000000800000023000000F6",uncompressed_size="59.95 GB"} 1.707897661469195e+09
# HELP walg_basebackup_count Remote Basebackups count
# TYPE walg_basebackup_count gauge
walg_basebackup_count 3.0
# HELP walg_last_upload Last upload of incremental or full backup
# TYPE walg_last_upload gauge
walg_last_upload{type="xlog"} 1.707988922995336e+09
walg_last_upload{type="basebackup"} 1.707980547275256e+09
walg_last_upload{type="wal"} 1.707988922995336e+09
# HELP walg_oldest_basebackup Oldest full backup
# TYPE walg_oldest_basebackup gauge
walg_oldest_basebackup 1.707897661469195e+09
# HELP walg_missing_remote_wal_segment_at_end Xlog ready for upload
# TYPE walg_missing_remote_wal_segment_at_end gauge
walg_missing_remote_wal_segment_at_end 0.0
# HELP walg_exception Wal-g exception: 1 for basebackup error, 2 for xlog error and 3 for both errors
# TYPE walg_exception gauge
walg_exception 0.0
# HELP walg_xlogs_since_basebackup Xlog uploaded since last base backup
# TYPE walg_xlogs_since_basebackup gauge
walg_xlogs_since_basebackup 159.0
# HELP walg_last_backup_duration Duration of the last full backup
# TYPE walg_last_backup_duration gauge
walg_last_backup_duration 675.399887
# HELP walg_wal_integrity_status Overall WAL archive integrity status
# TYPE walg_wal_integrity_status gauge
walg_wal_integrity_status{status="OK"} 1.0
walg_wal_integrity_status{status="FAILURE"} 0.0
# HELP walg_wal_archive_count Total WAL archived count from oldest to latest full backup
# TYPE walg_wal_archive_count gauge
walg_wal_archive_count 2717.0
# HELP walg_wal_archive_missing_count Total missing WAL count
# TYPE walg_wal_archive_missing_count gauge
walg_wal_archive_missing_count 0.0
```

## Exposed Metrics for MySQL
```
# HELP walg_basebackup Remote basebackups
# TYPE walg_basebackup gauge
walg_basebackup{backup_name="stream_20250912T155554Z",compressed_size="2543052",finish_time="2025-09-12T15:56:02.824003Z",start_time="2025-09-12T15:55:54.141199Z",uncompressed_size="74101944"} 1.757692562824003e+09
walg_basebackup{backup_name="stream_20250912T155658Z",compressed_size="2543055",finish_time="2025-09-12T15:57:07.979960Z",start_time="2025-09-12T15:56:58.811089Z",uncompressed_size="74101945"} 1.75769262797996e+09
walg_basebackup{backup_name="stream_20250912T160308Z",compressed_size="2542950",finish_time="2025-09-12T16:03:17.477113Z",start_time="2025-09-12T16:03:08.255359Z",uncompressed_size="74101945"} 1.757692997477113e+09
# HELP walg_basebackup_count Number of basebackups
# TYPE walg_basebackup_count gauge
walg_basebackup_count 3.0
# HELP walg_basebackup_exception 1 if basebackup retrieval failed else 0
# TYPE walg_basebackup_exception gauge
walg_basebackup_exception 0.0
# HELP walg_oldest_basebackup Oldest basebackup start time (unix seconds)
# TYPE walg_oldest_basebackup gauge
walg_oldest_basebackup 1.757692554141199e+09
# HELP walg_last_backup_duration Duration seconds of last basebackup
# TYPE walg_last_backup_duration gauge
walg_last_backup_duration 9.221754
# HELP walg_binlog_latest_active Current active binlog file
# TYPE walg_binlog_latest_active gauge
walg_binlog_latest_active{file="mysql-bin.000005"} 1.0
# HELP walg_binlog_latest_uploaded Latest uploaded binlog file (wal-g storage)
# TYPE walg_binlog_latest_uploaded gauge
walg_binlog_latest_uploaded{file="mysql-bin.000004"} 1.0
```
