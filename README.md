# wal-g-prometheus-exporter

## Requirements

1. Python 3.7+ (For `make build-binary`).

## Build

1. For Linux, you can build with `make build` and use the binary output.
2. For specific OS or to run in your own machine, you can get the binary with `make build-binary` and use the binary output.

## Usage

```
usage: wal-g-prometheus-exporter [-h] [--debug] archive_dir

positional arguments:
  archive_dir  pg_wal/archive_status/ Directory location

optional arguments:
  -h, --help   show this help message and exit
  --debug      enable debug log
```
