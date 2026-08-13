#!/usr/bin/env bash

set -euo pipefail

REPOSITORY_DIR=$(
    unset CDPATH
    cd -- "$(dirname -- "$0")"
    pwd -P
)
cd "$REPOSITORY_DIR"

INFRA_COMPOSE=(docker compose -f docker-compose.infra.yml)
LOCAL_COMPOSE=(
    docker compose
    -f docker-compose.polymarket.yml
    -f docker-compose.local.yml
)
APPLICATION_SERVICES=(
    obdata-polymarket-active-markets
    obdata-polymarket-orderbook-rust-pubsub
    obdata-polymarket-orderbook-rust-from-pubsub
    obdata-polymarket-r2-archive-exporter
)
LOCAL_EXPORT_HOST_DIR="$REPOSITORY_DIR/.data/parquet"

usage() {
    cat <<'EOF'
Usage: ./run_local.sh [up|logs|status|down]

  up      Build and start the complete local collection and export pipeline.
  logs    Follow application logs.
  status  Show infrastructure and application container status.
  down    Stop the local stack without deleting collected data.

The default action is "up". Parquet files are written under .data/parquet.
EOF
}

require_docker() {
    if ! command -v docker >/dev/null 2>&1; then
        echo "docker is required but was not found in PATH" >&2
        exit 1
    fi
    if ! docker compose version >/dev/null 2>&1; then
        echo "the Docker Compose plugin is required" >&2
        exit 1
    fi
    if ! docker info >/dev/null 2>&1; then
        echo "the Docker daemon is not available" >&2
        exit 1
    fi
}

ensure_environment() {
    if [[ ! -f .env ]]; then
        cp .env.example .env
        echo "Created .env from .env.example"
    fi
    mkdir -p "$LOCAL_EXPORT_HOST_DIR"
    export LOCAL_RUN_UID
    export LOCAL_RUN_GID
    LOCAL_RUN_UID=$(id -u)
    LOCAL_RUN_GID=$(id -g)
}

wait_for_service() {
    local description=$1
    shift

    for ((attempt = 1; attempt <= 60; attempt++)); do
        if "$@" >/dev/null 2>&1; then
            echo "$description is ready"
            return 0
        fi
        sleep 1
    done

    echo "$description did not become ready within 60 seconds" >&2
    return 1
}

start_stack() {
    "${INFRA_COMPOSE[@]}" up -d obdata-redis obdata-clickhouse
    wait_for_service Redis docker exec obdata-redis redis-cli ping
    # Expand the credentials inside the container rather than in this shell.
    # shellcheck disable=SC2016
    wait_for_service ClickHouse docker exec obdata-clickhouse sh -ec \
        'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SELECT 1"'

    "${LOCAL_COMPOSE[@]}" up -d --build "${APPLICATION_SERVICES[@]}"

    cat <<EOF

The local collector and exporter are running.
Parquet output: $LOCAL_EXPORT_HOST_DIR

An hour is exported after it is complete and the next hour has reached
ClickHouse. Depending on when collection starts, the first files can therefore
take a little over one hour to appear.

Follow logs with: ./run_local.sh logs
Stop services with: ./run_local.sh down
EOF
}

action=${1:-up}
case "$action" in
    up)
        require_docker
        ensure_environment
        start_stack
        ;;
    logs)
        require_docker
        ensure_environment
        "${LOCAL_COMPOSE[@]}" logs -f "${APPLICATION_SERVICES[@]}"
        ;;
    status)
        require_docker
        ensure_environment
        "${INFRA_COMPOSE[@]}" ps
        "${LOCAL_COMPOSE[@]}" ps
        ;;
    down)
        require_docker
        ensure_environment
        "${LOCAL_COMPOSE[@]}" down
        "${INFRA_COMPOSE[@]}" down
        echo "Stopped services; retained data under $REPOSITORY_DIR/.data"
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac
