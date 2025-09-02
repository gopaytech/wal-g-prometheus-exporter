.PHONY: clean

clean:
	rm -fr build/ dist/ __pycache__

build:
	docker build -t wal-g-prometheus-exporter .
	docker run --entrypoint="" --name wal-g-prometheus-exporter wal-g-prometheus-exporter bash
	docker cp wal-g-prometheus-exporter:/usr/bin/wal-g-prometheus-exporter ./wal-g-exporter
	docker rm wal-g-prometheus-exporter

build-binary:
	pip3 install -r requirements.txt
	pyinstaller --onefile exporter.py && \
	  mv dist/exporter wal-g-exporter

build-binary-mysql:
	grep -v "psycopg2" requirements.txt | pip3 install -r /dev/stdin
	pyinstaller --onefile \
		--hidden-import=prometheus_client \
		--hidden-import=dotenv \
		--hidden-import=mysql.connector \
		mysql/mysql_exporter.py
	mv dist/mysql_exporter wal-g-exporter-mysql

compress:
	tar -zcvf wal-g-exporter.linux-amd64.tar.gz wal-g-exporter

#include Makefile.mysql