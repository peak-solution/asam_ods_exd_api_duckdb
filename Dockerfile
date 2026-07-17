# GitHub: ghcr.io/<repository_owner>/asam-ods-exd-api-duckdb:latest
# docker build -t ghcr.io/peak-solution/asam-ods-exd-api-duckdb:latest .
# docker run --rm -it -v "$(pwd)/data":"$(pwd)/data" -p 50051:50051 ghcr.io/peak-solution/asam-ods-exd-api-duckdb:latest

FROM python:3.14-slim
LABEL org.opencontainers.image.source=https://github.com/totonga/asam-ods-exd-api-duckdb
LABEL org.opencontainers.image.description="ASAM ODS External Data API for DuckDB files (*.duckdb)"
LABEL org.opencontainers.image.licenses=MIT
WORKDIR /app
# Create a non-root user and change ownership of /app
RUN useradd -ms /bin/bash appuser && chown -R appuser /app
# Copy source code first (needed for pip install)
COPY pyproject.toml .
# Install required packages
RUN pip3 install --upgrade pip && pip3 install .
# should be copied at the end to avoid unnecessary rebuilds
COPY external_data_file.py ./
COPY exd_duckdb_server.py ./
COPY values_append_service.py ./
COPY protobuf_generated/ ./protobuf_generated/
USER appuser
# Start dual server (ExternalDataReader + ValuesAppender)
CMD [ "python3", "exd_duckdb_server.py"]