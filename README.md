# wal-g-prometheus-exporter

## Requirements

1. Python 3.7+ (For `make build-binary`).
2. This exporter must run on Postgres instance that do the WAL archiving or you can check with `SELECT * FROM pg_stat_archiver;`.
If there is no output from the query, then this exporter won't run and give an error message `There is no WAL archiver process running on this postgresql\nCheck with SELECT * FROM pg_stat_archiver;`
3. WAL-G must be installed.

## Build

1. For Linux, you can use the binary from `make build`. It should run on most Linux distro.
2. For specific OS or to run in your own machine, you can get the binary with `make build-binary` and use the binary output.

## Usage

```
usage: wal-g-exporter [-h] --archive_dir ARCHIVE_DIR [--debug] [--config] CONFIG_FILE_PATH [--version]

optional arguments:
  -h, --help            show this help message and exit
  --archive_dir ARCHIVE_DIR
                        pg_wal/archive_status/ Directory location
  --config CONFIG_FILE_PATH
                        file path for wal-g config
  --debug               enable debug log
  --version             show binary version
```

## Exposed Metrics

```
# HELP walg_basebackup Remote Basebackups
# TYPE walg_basebackup gauge
walg_basebackup{start_lsn="154100834568",start_wal_segment="0000000800000023000000E1"} 1.707896489927616e+09
# HELP walg_basebackup_count Remote Basebackups count
# TYPE walg_basebackup_count gauge
walg_basebackup_count 4.0
# HELP walg_last_upload Last upload of incremental or full backup
# TYPE walg_last_upload gauge
walg_last_upload{type="xlog"} 1.707984782346683e+09
walg_last_upload{type="basebackup"} 1.707980547275256e+09
walg_last_upload{type="wal"} 1.707984782346683e+09
# HELP walg_oldest_basebackup Oldest full backup
# TYPE walg_oldest_basebackup gauge
walg_oldest_basebackup 1.707896489927616e+09
# HELP walg_missing_remote_wal_segment_at_end Xlog ready for upload
# TYPE walg_missing_remote_wal_segment_at_end gauge
walg_missing_remote_wal_segment_at_end 0.0
# HELP walg_exception Wal-g exception: 2 for basebackup error, 3 for xlog error and 5 for remote error
# TYPE walg_exception gauge
walg_exception 0.0
# HELP walg_xlogs_since_basebackup Xlog uploaded since last base backup
# TYPE walg_xlogs_since_basebackup gauge
walg_xlogs_since_basebackup 84.0
# HELP walg_last_backup_duration Duration of the last full backup
# TYPE walg_last_backup_duration gauge
walg_last_backup_duration 675.399887
# HELP walg_wal_integrity_status Overall WAL archive integrity status
# TYPE walg_wal_integrity_status gauge
walg_wal_integrity_status{status="OK"} 1.0
walg_wal_integrity_status{status="FAILURE"} 0.0
# HELP walg_wal_archive_count Total WAL archived count from oldest to latest full backup
# TYPE walg_wal_archive_count gauge
walg_wal_archive_count 2663.0
# HELP walg_wal_archive_missing_count Total missing WAL count
# TYPE walg_wal_archive_missing_count gauge
walg_wal_archive_missing_count 0.0
```
