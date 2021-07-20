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
  --config              file path for wal-g config
  --debug               enable debug log
  --version             show binary version
```
