# Phronexus Core service image. Runs any entrypoint via command override:
#   python -m phronexus.api.server            (default: REST API)
#   python -m phronexus.statemachine.runner   (Kafka state-machine runner)
#   python -m phronexus.retention.main        (Iceberg retention)
#   python -m phronexus.cli ...               (admin CLI)
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# build-essential/libssl are a fallback in case a wheel isn't available for the
# platform (e.g. the aerospike client on some arches); curl is for healthchecks.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential libssl-dev zlib1g-dev curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies first for better layer caching.
COPY pyproject.toml README.md ./
COPY phronexus ./phronexus
RUN pip install --upgrade pip && pip install '.[api,aerospike,kafka,oidc,iceberg]'

# Contracts + examples for ingestion and the smoke test.
COPY contracts_examples ./contracts_examples
COPY examples ./examples
COPY config ./config

EXPOSE 8080
CMD ["python", "-m", "phronexus.api.server"]
