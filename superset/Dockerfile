FROM apache/superset:6.0.0

USER root

RUN /usr/local/bin/pip install --no-cache-dir \
    --target=/app/.venv/lib/python3.10/site-packages \
    clickhouse-connect

USER superset