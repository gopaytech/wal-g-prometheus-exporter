.PHONY: clean

clean:
	rm -fr build/ dist/ __pycache__

build:
	docker build -t wal-g-prometheus-exporter .
	docker run --entrypoint="" --name wal-g-prometheus-exporter wal-g-prometheus-exporter bash
	docker cp wal-g-prometheus-exporter:/usr/bin/wal-g-prometheus-exporter ./wal-g-prometheus-exporter
	docker rm wal-g-prometheus-exporter

build-binary:
	pip3 install -r requirements.txt
	pyinstaller --onefile exporter.py && \
	mv dist/exporter wal-g-prometheus-exporter
