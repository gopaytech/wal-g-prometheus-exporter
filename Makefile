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
	pip3 install -r requirements.txt
	pyinstaller --onefile mysql/mysql_exporter.py && \
	  mv dist/exporter wal-g-exporter

compress:
	tar -zcvf wal-g-exporter.linux-amd64.tar.gz wal-g-exporter

