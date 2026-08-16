#!/usr/bin/env bash

set -euo pipefail

REPOSITORY_DIR=$(
    unset CDPATH
    cd -- "$(dirname -- "$0")"
    pwd -P
)
cd "$REPOSITORY_DIR"

CONTAINER_COMMAND=()
COMPOSE_COMMAND=()
INFRA_COMPOSE=()
LOCAL_COMPOSE=()
APPLICATION_SERVICES=(
    obdata-polymarket-collector
    obdata-polymarket-clickhouse-writer
    obdata-polymarket-archive-exporter
)
LOCAL_EXPORT_HOST_DIR="$REPOSITORY_DIR/.data/parquet"

usage() {
    cat <<'EOF'
Usage: ./run_local.sh [up|redeploy|logs|status|down]

  up        Build and start the stack without recreating unchanged services.
  redeploy  Rebuild and recreate every application service.
  logs      Follow application logs.
  status    Show infrastructure and application container status.
  down      Stop the local stack without deleting collected data.

The default action is "up". Parquet files are written under .data/parquet.
Docker Compose and Podman Compose are both supported. Set CONTAINER_RUNTIME to
"docker" or "podman" to override automatic selection. After changing source
code, use "redeploy", especially with Podman Compose.
EOF
}

configure_runtime() {
    local runtime=$1

    command -v "$runtime" >/dev/null 2>&1 || return 1
    "$runtime" info >/dev/null 2>&1 || return 1

    if [[ $runtime == docker ]]; then
        docker compose version >/dev/null 2>&1 || return 1
        COMPOSE_COMMAND=(docker compose)
    elif podman compose version >/dev/null 2>&1; then
        COMPOSE_COMMAND=(podman compose)
    elif command -v podman-compose >/dev/null 2>&1 \
        && podman-compose --version >/dev/null 2>&1; then
        COMPOSE_COMMAND=(podman-compose)
    else
        return 1
    fi

    CONTAINER_COMMAND=("$runtime")
    INFRA_COMPOSE=("${COMPOSE_COMMAND[@]}" -f docker-compose.infra.yml)
    LOCAL_COMPOSE=(
        "${COMPOSE_COMMAND[@]}"
        -f docker-compose.polymarket.yml
        -f docker-compose.local.yml
    )
}

select_runtime() {
    local requested=${CONTAINER_RUNTIME:-}
    if [[ -n $requested ]]; then
        if [[ $requested != docker && $requested != podman ]]; then
            echo "CONTAINER_RUNTIME must be 'docker' or 'podman'" >&2
            exit 1
        fi
        if ! configure_runtime "$requested"; then
            echo "$requested and its Compose provider are not available" >&2
            exit 1
        fi
    elif configure_runtime docker; then
        :
    elif configure_runtime podman; then
        :
    else
        echo "no working Docker Compose or Podman Compose runtime was found" >&2
        exit 1
    fi

    echo "Using ${CONTAINER_COMMAND[0]} with ${COMPOSE_COMMAND[*]}"
}

runtime_is_rootless() {
    if [[ ${CONTAINER_COMMAND[0]} == podman ]]; then
        [[ $(podman info --format '{{.Host.Security.Rootless}}') == true ]]
    else
        docker info --format '{{json .SecurityOptions}}' | grep -q rootless
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
    if runtime_is_rootless; then
        # Container root maps to the invoking user under a rootless runtime.
        LOCAL_RUN_UID=0
        LOCAL_RUN_GID=0
        export CONTAINER_NOFILE_LIMIT
        CONTAINER_NOFILE_LIMIT=$(ulimit -Hn)
        if [[ $CONTAINER_NOFILE_LIMIT == unlimited ]]; then
            CONTAINER_NOFILE_LIMIT=-1
        fi
        echo "Capping container nofile limits at $CONTAINER_NOFILE_LIMIT"
    else
        LOCAL_RUN_UID=$(id -u)
        LOCAL_RUN_GID=$(id -g)
    fi
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
    local force_recreate=${1:-false}

    "${INFRA_COMPOSE[@]}" up -d obdata-redis obdata-clickhouse
    wait_for_service Redis "${CONTAINER_COMMAND[@]}" exec \
        obdata-redis redis-cli ping
    # Expand the credentials inside the container rather than in this shell.
    # shellcheck disable=SC2016
    wait_for_service ClickHouse "${CONTAINER_COMMAND[@]}" exec \
        obdata-clickhouse sh -ec \
        'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SELECT 1"'

    local application_up=(up -d --build)
    if [[ $force_recreate == true ]]; then
        # podman-compose does not consistently replace containers after rebuilding
        # their images, so redeployment makes replacement explicit.
        application_up+=(--force-recreate)
    fi
    application_up+=(--remove-orphans "${APPLICATION_SERVICES[@]}")
    "${LOCAL_COMPOSE[@]}" "${application_up[@]}"

    cat <<EOF

The local collector, ClickHouse writer, and archive exporter are running.
Parquet output: $LOCAL_EXPORT_HOST_DIR

An hour is exported after it is complete and the next hour has reached
ClickHouse. Depending on when collection starts, the first files can therefore
take a little over one hour to appear.

Follow logs with: ./run_local.sh logs
Redeploy code with: ./run_local.sh redeploy
Stop services with: ./run_local.sh down
EOF
}

action=${1:-up}
case "$action" in
    up)
        select_runtime
        ensure_environment
        start_stack false
        ;;
    redeploy)
        select_runtime
        ensure_environment
        start_stack true
        ;;
    logs)
        select_runtime
        ensure_environment
        "${LOCAL_COMPOSE[@]}" logs -f "${APPLICATION_SERVICES[@]}"
        ;;
    status)
        select_runtime
        ensure_environment
        "${INFRA_COMPOSE[@]}" ps
        "${LOCAL_COMPOSE[@]}" ps
        ;;
    down)
        select_runtime
        ensure_environment
        "${LOCAL_COMPOSE[@]}" down --remove-orphans
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
