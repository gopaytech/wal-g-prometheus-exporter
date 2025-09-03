############################
# Ubuntu 22.04 + MySQL Server + wal-g (mysql flavor)
# NOTE: Ubuntu's mysql-server package sets root to use unix socket auth.
# If MYSQL_ROOT_PASSWORD is provided, an init script will set it on first start.
############################

ARG WAL_G_VERSION=master

#############################
# Stage 1: fetch wal-g mysql prebuilt binary
#############################
FROM ubuntu:22.04 AS walgbinary
ARG WAL_G_VERSION=v3.0.7
ARG TARGETARCH
ENV DEBIAN_FRONTEND=noninteractive TZ=UTC
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl jq bash coreutils && rm -rf /var/lib/apt/lists/*
WORKDIR /tmp
RUN bash -lc 'set -euo pipefail; \
  arch="${TARGETARCH:-amd64}"; \
  case "$arch" in aarch64|arm64) BIN=wal-g-mysql-ubuntu-22.04-arm64 ;; amd64|x86_64|*) BIN=wal-g-mysql-ubuntu-22.04-amd64 ;; esac; \
  echo "Downloading wal-g mysql binary ${WAL_G_VERSION} ($BIN)"; \
  base="https://github.com/wal-g/wal-g/releases/download/${WAL_G_VERSION}"; \
  curl -sSL -o ${BIN} ${base}/${BIN}; \
  curl -sSL -o ${BIN}.sha256 ${base}/${BIN}.sha256; \
  sha256sum -c ${BIN}.sha256; \
  install -m 0755 ${BIN} /usr/local/bin/wal-g; \
  /usr/local/bin/wal-g --help | head -20 || true'

############################
# Stage 2: final runtime on Ubuntu 22.04
############################
FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive TZ=UTC
RUN apt-get update && apt-get install -y --no-install-recommends \
  mysql-server mysql-client jq curl ca-certificates gnupg lsb-release bash apt-transport-https \
    && rm -rf /var/lib/apt/lists/*

# Install Percona XtraBackup (for physical MySQL backups via wal-g)
RUN bash -lc 'set -euo pipefail; \
  echo "Installing Percona XtraBackup"; \
  curl -fsSL https://repo.percona.com/apt/percona-release_latest.jammy_all.deb -o /tmp/percona-release.deb; \
  dpkg -i /tmp/percona-release.deb; \
  percona-release setup ps80; \
  apt-get update; \
  apt-get install -y --no-install-recommends percona-xtrabackup-80 qpress lz4; \
  rm -rf /var/lib/apt/lists/* /tmp/percona-release.deb; \
  xtrabackup --version | head -1 || true'

# Copy wal-g binary (ensure it's executable ELF, not archive)
COPY --from=walgbinary /usr/local/bin/wal-g /usr/local/bin/wal-g
RUN chmod +x /usr/local/bin/wal-g || true

# Binlog config for wal-g
RUN bash -c 'echo -e "[mysqld]\nserver_id=1\nlog_bin=mysql-bin\nbinlog_format=ROW\nbinlog_expire_logs_seconds=3600\n" > /etc/mysql/mysql.conf.d/99-binlog-walg.cnf'

# Network override: listen on all interfaces for exporter/container access
RUN bash -c 'echo -e "[mysqld]\nbind-address=0.0.0.0\nmysqlx-bind-address=0.0.0.0\n" > /etc/mysql/mysql.conf.d/10-network.cnf'
RUN sed -i "s/^bind-address/# bind-address/" /etc/mysql/mysql.conf.d/mysqld.cnf || true && \
  sed -i "s/^mysqlx-bind-address/# mysqlx-bind-address/" /etc/mysql/mysql.conf.d/mysqld.cnf || true

# Local file backend directory (optional)
ENV WALG_FILE_PREFIX=/var/lib/walg
RUN mkdir -p /var/lib/walg && chmod 750 /var/lib/walg
VOLUME ["/var/lib/mysql","/var/lib/walg","/etc/wal-g"]

# Init script to set root password if provided
ADD <<'EOF' /usr/local/bin/docker-entrypoint.sh
#!/usr/bin/env bash
set -euo pipefail
MYSQL_ROOT_PASSWORD=${MYSQL_ROOT_PASSWORD:-}
if [ ! -d /var/lib/mysql/mysql ]; then
  echo "Initializing MySQL datadir" >&2
  mysqld --initialize-insecure --datadir=/var/lib/mysql --user=mysql
fi
echo "Starting MySQL (temporary - skip networking)" >&2
mysqld --skip-networking --datadir=/var/lib/mysql --user=mysql &
pid=$!
tries=60
until mysqladmin ping >/dev/null 2>&1 || [ $tries -le 0 ]; do
  sleep 1; tries=$((tries-1))
done
if [ -n "$MYSQL_ROOT_PASSWORD" ]; then
  echo "Setting root password (caching_sha2_password)" >&2
  mysql -uroot -e "ALTER USER 'root'@'localhost' IDENTIFIED WITH caching_sha2_password BY '${MYSQL_ROOT_PASSWORD}'; FLUSH PRIVILEGES;" || true
fi
mysqladmin -uroot${MYSQL_ROOT_PASSWORD:+ -p$MYSQL_ROOT_PASSWORD} shutdown || kill $pid || true
exec mysqld --datadir=/var/lib/mysql --user=mysql
EOF
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 3306
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD []