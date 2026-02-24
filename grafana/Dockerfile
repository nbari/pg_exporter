FROM grafana/grafana:latest

ARG PROM_VERSION=3.5.0
ENV EXPORTER_TARGET=host.docker.internal:9432

USER root

# Install dependencies (cached layer)
RUN set -eux; \
    if command -v apt-get >/dev/null; then \
      apt-get update; \
      apt-get install -y --no-install-recommends curl ca-certificates; \
      rm -rf /var/lib/apt/lists/*; \
    elif command -v apk >/dev/null; then \
      apk add --no-cache curl ca-certificates; \
    elif command -v microdnf >/dev/null; then \
      microdnf install -y curl ca-certificates tar gzip && microdnf clean all; \
    else \
      echo "No supported package manager found in base image" >&2; \
      exit 1; \
    fi

# Download and install Prometheus (cached layer - only rebuilds when PROM_VERSION changes)
RUN set -eux; \
    curl -L "https://github.com/prometheus/prometheus/releases/download/v${PROM_VERSION}/prometheus-${PROM_VERSION}.linux-amd64.tar.gz" \
      | tar -xz --strip-components=1 -C /opt; \
    mkdir -p /var/lib/prometheus; \
    chown -R 472:0 /opt /var/lib/prometheus

# Copy static config files (cached layer - only rebuilds when these files change)
COPY prometheus.yml.tmpl /opt/prometheus.yml.tmpl
COPY stack-entrypoint.sh /usr/local/bin/stack-entrypoint.sh
COPY provisioning/datasources /etc/grafana/provisioning/datasources
COPY provisioning/dashboards /etc/grafana/provisioning/dashboards

RUN chmod +x /usr/local/bin/stack-entrypoint.sh

# Copy dashboard.json last (separate layer - rebuilds only when dashboard changes)
COPY dashboard.json /etc/grafana/provisioning/dashboards/pg_exporter.json
RUN chmod 0644 /etc/grafana/provisioning/dashboards/pg_exporter.json

# Drop back to the grafana user (uid 472, gid 0/root in the base image)
USER 472

EXPOSE 3000 9090

ENTRYPOINT ["/usr/local/bin/stack-entrypoint.sh"]
