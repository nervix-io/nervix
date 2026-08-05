#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage: benches/kafka_e2e/run.sh [options]

Options:
  --duration-seconds N|auto
                         Timed producer interval (default: auto; at least 30s
                         and 12 cycles of the slower flush interval)
  --partitions N         Partitions on both fresh topics (default: 16)
  --value-bytes N        Bytes in the JSON string value (default: 128)
  --max-backlog-messages N
                         Bound accepted input minus observed output (default: 4194304)
  --ingestor-flush-each D
                         Ingestor flush interval (default: 10ms)
  --ingestor-max-batch-size B
                         Ingestor logical batch limit (default: 8MiB)
  --emitter-flush-each D
                         Emitter flush interval (default: 10ms)
  --emitter-max-batch-size B
                         Emitter logical batch limit (default: 8MiB)
  --wait-timeout-seconds N
                         Readiness and drain timeout (default: 120)
  --skip-build           Reuse existing release binaries
  --keep-topics          Keep the two run-specific Kafka topics after success
  -h, --help             Show this help
USAGE
}

fail() {
    echo "benchmark error: $*" >&2
    exit 1
}

require_positive_integer() {
    local name="$1"
    local value="$2"
    if [[ ! "${value}" =~ ^[0-9]+$ ]] || (( value < 1 )); then
        fail "${name} must be a positive integer, got '${value}'"
    fi
}

duration_seconds="${NERVIX_BENCH_DURATION_SECONDS:-auto}"
partitions="${NERVIX_BENCH_PARTITIONS:-16}"
value_bytes="${NERVIX_BENCH_VALUE_BYTES:-128}"
max_backlog_messages="${NERVIX_BENCH_MAX_BACKLOG_MESSAGES:-4194304}"
ingestor_flush_each="${NERVIX_BENCH_INGESTOR_FLUSH_EACH:-10ms}"
ingestor_max_batch_size="${NERVIX_BENCH_INGESTOR_MAX_BATCH_SIZE:-8MiB}"
emitter_flush_each="${NERVIX_BENCH_EMITTER_FLUSH_EACH:-10ms}"
emitter_max_batch_size="${NERVIX_BENCH_EMITTER_MAX_BATCH_SIZE:-8MiB}"
wait_timeout_seconds="${NERVIX_BENCH_WAIT_TIMEOUT_SECONDS:-120}"
skip_build="${NERVIX_BENCH_SKIP_BUILD:-0}"
keep_topics="${NERVIX_BENCH_KEEP_TOPICS:-0}"

while (( $# > 0 )); do
    case "$1" in
        --duration-seconds)
            [[ $# -ge 2 ]] || fail "--duration-seconds requires a value"
            duration_seconds="$2"
            shift 2
            ;;
        --partitions)
            [[ $# -ge 2 ]] || fail "--partitions requires a value"
            partitions="$2"
            shift 2
            ;;
        --value-bytes)
            [[ $# -ge 2 ]] || fail "--value-bytes requires a value"
            value_bytes="$2"
            shift 2
            ;;
        --max-backlog-messages)
            [[ $# -ge 2 ]] || fail "--max-backlog-messages requires a value"
            max_backlog_messages="$2"
            shift 2
            ;;
        --ingestor-flush-each)
            [[ $# -ge 2 ]] || fail "--ingestor-flush-each requires a value"
            ingestor_flush_each="$2"
            shift 2
            ;;
        --ingestor-max-batch-size)
            [[ $# -ge 2 ]] || fail "--ingestor-max-batch-size requires a value"
            ingestor_max_batch_size="$2"
            shift 2
            ;;
        --emitter-flush-each)
            [[ $# -ge 2 ]] || fail "--emitter-flush-each requires a value"
            emitter_flush_each="$2"
            shift 2
            ;;
        --emitter-max-batch-size)
            [[ $# -ge 2 ]] || fail "--emitter-max-batch-size requires a value"
            emitter_max_batch_size="$2"
            shift 2
            ;;
        --wait-timeout-seconds)
            [[ $# -ge 2 ]] || fail "--wait-timeout-seconds requires a value"
            wait_timeout_seconds="$2"
            shift 2
            ;;
        --skip-build)
            skip_build=1
            shift
            ;;
        --keep-topics)
            keep_topics=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            fail "unknown option '$1'"
            ;;
    esac
done

require_positive_integer "partition count" "${partitions}"
require_positive_integer "value byte count" "${value_bytes}"
require_positive_integer "maximum backlog" "${max_backlog_messages}"
require_positive_integer "wait timeout" "${wait_timeout_seconds}"
for flush_setting in \
    "ingestor:${ingestor_flush_each}" \
    "emitter:${emitter_flush_each}"
do
    flush_owner="${flush_setting%%:*}"
    flush_interval="${flush_setting#*:}"
    if [[ ! "${flush_interval}" =~ ^[1-9][0-9]*(ns|us|ms|s|m|h)$ ]]; then
        fail "${flush_owner} flush interval must be a positive NSPL duration, got '${flush_interval}'"
    fi
done

flush_cycles_seconds() {
    local flush_interval="$1"
    local flush_value="${flush_interval%%[a-z]*}"
    local flush_unit="${flush_interval#"${flush_value}"}"
    flush_value=$(( 10#${flush_value} ))
    case "${flush_unit}" in
        ns)
            echo $(( (flush_value * 12 + 999999999) / 1000000000 ))
            ;;
        us)
            echo $(( (flush_value * 12 + 999999) / 1000000 ))
            ;;
        ms)
            echo $(( (flush_value * 12 + 999) / 1000 ))
            ;;
        s)
            echo $(( flush_value * 12 ))
            ;;
        m)
            echo $(( flush_value * 60 * 12 ))
            ;;
        h)
            echo $(( flush_value * 60 * 60 * 12 ))
            ;;
    esac
}

duration_selection="explicit"
if [[ "${duration_seconds}" == "auto" ]]; then
    ingestor_flush_cycles_seconds="$(flush_cycles_seconds "${ingestor_flush_each}")"
    emitter_flush_cycles_seconds="$(flush_cycles_seconds "${emitter_flush_each}")"
    slower_flush_cycles_seconds="${ingestor_flush_cycles_seconds}"
    if (( emitter_flush_cycles_seconds > slower_flush_cycles_seconds )); then
        slower_flush_cycles_seconds="${emitter_flush_cycles_seconds}"
    fi
    duration_seconds=30
    if (( slower_flush_cycles_seconds > duration_seconds )); then
        duration_seconds="${slower_flush_cycles_seconds}"
    fi
    duration_selection="auto (max 30s, 12 cycles of slower flush interval)"
else
    require_positive_integer "duration" "${duration_seconds}"
fi
for batch_setting in \
    "ingestor:${ingestor_max_batch_size}" \
    "emitter:${emitter_max_batch_size}"
do
    batch_owner="${batch_setting%%:*}"
    batch_size="${batch_setting#*:}"
    if [[ ! "${batch_size}" =~ ^[1-9][0-9]*(B|KiB|MiB|GiB)$ ]]; then
        fail "${batch_owner} maximum batch size must be a positive binary byte size, got '${batch_size}'"
    fi
done
if (( value_bytes > 1048576 )); then
    fail "value byte count must not exceed 1048576"
fi
if [[ "${skip_build}" != "0" && "${skip_build}" != "1" ]]; then
    fail "NERVIX_BENCH_SKIP_BUILD must be 0 or 1"
fi
if [[ "${keep_topics}" != "0" && "${keep_topics}" != "1" ]]; then
    fail "NERVIX_BENCH_KEEP_TOPICS must be 0 or 1"
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
cd "${repo_root}"

for command in awk cargo curl docker git getconf just sha256sum uname; do
    command -v "${command}" >/dev/null 2>&1 || fail "required command '${command}' is unavailable"
done
[[ "$(uname -s)" == "Linux" ]] || fail "process CPU and RSS collection requires Linux /proc"
docker compose version >/dev/null 2>&1 || fail "Docker Compose v2 is unavailable"

target_root="${CARGO_TARGET_DIR:-${repo_root}/target}"
case "${target_root}" in
    /*) ;;
    *) target_root="${repo_root}/${target_root}" ;;
esac
server_bin="${target_root}/release/nervix-server"
cli_bin="${target_root}/release/nervix-cli"
driver_bin="${target_root}/release/nervix-kafka-e2e-driver"

if [[ "${skip_build}" == "0" ]]; then
    echo "Building the web console, release server, CLI, and Kafka benchmark driver..."
    just build-web-console
    cargo build --release --package nervix-server --features benchmarks \
        --bin nervix-server --bin nervix-kafka-e2e-driver
    cargo build --release --package nervix-cli --bin nervix-cli
fi
for binary in "${server_bin}" "${cli_bin}" "${driver_bin}"; do
    [[ -x "${binary}" ]] || fail "expected executable '${binary}'; rerun without --skip-build"
done
server_sha256="$(sha256sum "${server_bin}" | awk '{print $1}')"
cli_sha256="$(sha256sum "${cli_bin}" | awk '{print $1}')"
driver_sha256="$(sha256sum "${driver_bin}" | awk '{print $1}')"

run_stamp="$(date -u +%Y%m%d%H%M%S)"
run_id="${run_stamp}_$$"
run_dir="${NERVIX_BENCH_OUTPUT_DIR:-${target_root}/benchmarks/kafka-e2e/${run_id}}"
[[ ! -e "${run_dir}" ]] || fail "output directory already exists: ${run_dir}"
mkdir -p "${run_dir}"
state_dir="$(mktemp -d -t nervix-kafka-e2e.XXXXXX)"

input_topic="nervix_bench_in_${run_id}"
output_topic="nervix_bench_out_${run_id}"
consumer_group="nervix_bench_group_${run_id}"
domain="nervix_bench_${run_id}"
kafka_bootstrap_servers="127.0.0.1:9092"
kafka_admin_bootstrap_servers="localhost:9092"
password="nervix-benchmark"

grpc_addr="${NERVIX_BENCH_GRPC_ADDR:-127.0.0.1:47391}"
http_addr="${NERVIX_BENCH_HTTP_ADDR:-127.0.0.1:48080}"
https_addr="${NERVIX_BENCH_HTTPS_ADDR:-127.0.0.1:48443}"
observability_addr="${NERVIX_BENCH_OBSERVABILITY_ADDR:-127.0.0.1:49090}"
web_console_addr="${NERVIX_BENCH_WEB_CONSOLE_ADDR:-127.0.0.1:47420}"
cluster_api_addr="${NERVIX_BENCH_CLUSTER_API_ADDR:-127.0.0.1:47393}"
server_url="http://${grpc_addr}"
observability_url="http://${observability_addr}"

server_pid=""
driver_pid=""
monitor_pid=""
kafka_started_by_benchmark=0
input_topic_created=0
output_topic_created=0
benchmark_succeeded=0

process_is_live() {
    local pid="$1"
    [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1
}

stop_server() {
    local pid="$1"
    local state=""
    if ! process_is_live "${pid}"; then
        wait "${pid}" >/dev/null 2>&1 || true
        return
    fi
    kill -INT "${pid}" >/dev/null 2>&1 || true
    for _ in $(seq 1 100); do
        if ! process_is_live "${pid}"; then
            break
        fi
        state="$(awk '{print $3}' "/proc/${pid}/stat" 2>/dev/null || true)"
        [[ "${state}" == "Z" ]] && break
        sleep 0.1
    done
    if process_is_live "${pid}" && [[ "${state}" != "Z" ]]; then
        kill -TERM "${pid}" >/dev/null 2>&1 || true
    fi
    wait "${pid}" >/dev/null 2>&1 || true
}

kafka_exec() {
    docker compose exec -T kafka "$@"
}

cleanup() {
    local status=$?
    set +e
    if [[ -n "${monitor_pid}" ]]; then
        rm -f "${state_dir}/monitor-active"
        wait "${monitor_pid}" >/dev/null 2>&1 || true
    fi
    if process_is_live "${driver_pid}"; then
        kill -TERM "${driver_pid}" >/dev/null 2>&1 || true
        wait "${driver_pid}" >/dev/null 2>&1 || true
    fi
    if [[ -n "${server_pid}" ]]; then
        stop_server "${server_pid}"
    fi
    if [[ "${benchmark_succeeded}" == "1" && "${keep_topics}" == "0" ]]; then
        if [[ "${input_topic_created}" == "1" ]]; then
            kafka_exec /opt/kafka/bin/kafka-topics.sh \
                --bootstrap-server "${kafka_admin_bootstrap_servers}" \
                --delete --if-exists --topic "${input_topic}" >/dev/null 2>&1 || true
        fi
        if [[ "${output_topic_created}" == "1" ]]; then
            kafka_exec /opt/kafka/bin/kafka-topics.sh \
                --bootstrap-server "${kafka_admin_bootstrap_servers}" \
                --delete --if-exists --topic "${output_topic}" >/dev/null 2>&1 || true
        fi
    elif [[ "${input_topic_created}" == "1" || "${output_topic_created}" == "1" ]]; then
        echo "Kafka topics retained for inspection: ${input_topic}, ${output_topic}" >&2
    fi
    if [[ "${kafka_started_by_benchmark}" == "1" ]]; then
        docker compose stop kafka >/dev/null 2>&1 || true
    fi
    if [[ -n "${state_dir}" && -d "${state_dir}" ]]; then
        rm -r -- "${state_dir}"
    fi
    if (( status != 0 )); then
        echo "Benchmark failed; artifacts are in ${run_dir}" >&2
    fi
}
trap cleanup EXIT

echo "Preparing the repository Kafka broker..."
just generate-dev-tls
if [[ -z "$(docker compose ps --status running -q kafka)" ]]; then
    kafka_started_by_benchmark=1
fi
docker compose up -d kafka

kafka_ready=0
deadline=$((SECONDS + wait_timeout_seconds))
while (( SECONDS < deadline )); do
    if kafka_exec /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "${kafka_admin_bootstrap_servers}" --list >/dev/null 2>&1
    then
        kafka_ready=1
        break
    fi
    sleep 0.2
done
[[ "${kafka_ready}" == "1" ]] || fail "Kafka was not ready within ${wait_timeout_seconds}s"

kafka_exec /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${kafka_admin_bootstrap_servers}" \
    --create --topic "${input_topic}" --partitions "${partitions}" --replication-factor 1
input_topic_created=1
kafka_exec /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${kafka_admin_bootstrap_servers}" \
    --create --topic "${output_topic}" --partitions "${partitions}" --replication-factor 1
output_topic_created=1

echo "Starting a fresh single-node Nervix server..."
NERVIX_INIT_DEFAULT_USER_PASSWORD="${password}" \
NERVIX_DB_PATH="${state_dir}/db" \
RUST_LOG="${NERVIX_BENCH_RUST_LOG:-info}" \
"${server_bin}" \
    --node-id "bench-node-${run_id}" \
    --cluster-id "bench-cluster-${run_id}" \
    --addr "${grpc_addr}" \
    --http-listen-addr "${http_addr}" \
    --https-listen-addr "${https_addr}" \
    --observability-listen-addr "${observability_addr}" \
    --web-console-listen-addr "${web_console_addr}" \
    --cluster-api-listen-addr "${cluster_api_addr}" \
    --cluster-api-advertise-addr "${cluster_api_addr}" \
    --allow-bootstrap >"${run_dir}/server.log" 2>&1 &
server_pid=$!

server_ready=0
deadline=$((SECONDS + wait_timeout_seconds))
while (( SECONDS < deadline )); do
    process_is_live "${server_pid}" || fail "nervix-server exited before readiness; see server.log"
    if curl -fsS "${observability_url}/readyz" >/dev/null 2>&1; then
        server_ready=1
        break
    fi
    sleep 0.2
done
[[ "${server_ready}" == "1" ]] || fail "nervix-server was not ready within ${wait_timeout_seconds}s"

cli_once() {
    local selected_domain="$1"
    local query="$2"
    local output_file="$3"
    NO_COLOR=1 NERVIX_PASSWORD="${password}" "${cli_bin}" \
        --server "${server_url}" --domain "${selected_domain}" --command "${query}" \
        >"${output_file}" 2>&1
}

cli_checked() {
    local selected_domain="$1"
    local query="$2"
    local output_file="$3"
    if ! cli_once "${selected_domain}" "${query}" "${output_file}"; then
        echo "CLI command failed:" >&2
        sed -n '1,160p' "${output_file}" >&2
        return 1
    fi
    if grep -Eq '^[[:space:]]*error:' "${output_file}"; then
        echo "CLI command returned an error:" >&2
        sed -n '1,160p' "${output_file}" >&2
        return 1
    fi
}

authenticated=0
deadline=$((SECONDS + wait_timeout_seconds))
while (( SECONDS < deadline )); do
    if cli_once default "SHOW CLUSTER STATUS;" "${state_dir}/cluster-status.txt" \
        && ! grep -Eq '^[[:space:]]*error:' "${state_dir}/cluster-status.txt"
    then
        authenticated=1
        break
    fi
    process_is_live "${server_pid}" || fail "nervix-server exited before authentication was ready"
    sleep 0.2
done
[[ "${authenticated}" == "1" ]] || fail "the Nervix control plane was not ready within ${wait_timeout_seconds}s"
cp "${state_dir}/cluster-status.txt" "${run_dir}/cluster-status.txt"

echo "Loading the benchmark domain and Kafka ingestor-to-emitter graph..."
cli_checked default "CREATE UNPACED DOMAIN ${domain};" "${run_dir}/create-domain.txt"
relay_definitions=""
ingestor_definitions=""
emitter_definitions=""
for (( lane = 0; lane < partitions; lane++ )); do
    printf -v relay_definitions \
        '%sCREATE RELAY benchmark_records_%d SCHEMA benchmark_record UNBRANCHED;\n' \
        "${relay_definitions}" "${lane}"
    printf -v ingestor_definitions \
        '%sCREATE INGESTOR kafka_in_%d\n  FROM KAFKA kafka_local\n  TOPIC %s\n  OFFSET BY CONSUMER GROUP %s\n  INSTANCES 1\n  MODE NO_ACK PARALLEL\n  DECODE USING benchmark_codec\n  TO benchmark_records_%d\n    INHERIT ALL\n    UNBRANCHED\n    FLUSH EACH %s MAX BATCH SIZE %s\n    ON MESSAGE ERROR LOG\n  ON GENERAL ERROR LOG;\n' \
        "${ingestor_definitions}" "${lane}" "${input_topic}" "${consumer_group}" "${lane}" \
        "${ingestor_flush_each}" "${ingestor_max_batch_size}"
    printf -v emitter_definitions \
        '%sCREATE EMITTER kafka_out_%d\n  FROM benchmark_records_%d\n  TO KAFKA kafka_local TOPIC %s ENCODE USING benchmark_codec\n  INHERIT ALL\n  FLUSH EACH %s MAX BATCH SIZE %s\n  ON MESSAGE ERROR LOG\n  ON GENERAL ERROR LOG;\n' \
        "${emitter_definitions}" "${lane}" "${lane}" "${output_topic}" \
        "${emitter_flush_each}" "${emitter_max_batch_size}"
done
graph="$(<"${script_dir}/graph.nspl")"
graph="${graph//__KAFKA_BOOTSTRAP_SERVERS__/${kafka_bootstrap_servers}}"
graph="${graph//__RELAY_DEFINITIONS__/${relay_definitions}}"
graph="${graph//__INGESTOR_DEFINITIONS__/${ingestor_definitions}}"
graph="${graph//__EMITTER_DEFINITIONS__/${emitter_definitions}}"
[[ "${graph}" != *"__"* ]] || fail "the rendered NSPL graph contains an unresolved placeholder"
printf '%s\n' "${graph}" >"${run_dir}/graph.nspl"
cli_checked "${domain}" "${graph}" "${run_dir}/create-graph.txt"
cli_checked "${domain}" "START;" "${run_dir}/start-domain.txt"

graph_nodes_ready() {
    local output_file="$1"
    local lane
    local node_output="${state_dir}/node-readiness.txt"
    : >"${output_file}"
    for (( lane = 0; lane < partitions; lane++ )); do
        if ! cli_once "${domain}" "DESCRIBE INGESTOR kafka_in_${lane};" "${node_output}"; then
            return 1
        fi
        {
            echo "INGESTOR kafka_in_${lane}"
            sed 's/^/  /' "${node_output}"
        } >>"${output_file}"
        if grep -Eq '^[[:space:]]*error:' "${node_output}" \
            || ! grep -Fxq "status: running" "${node_output}" \
            || ! grep -Fxq "ready: true" "${node_output}"
        then
            return 1
        fi

        if ! cli_once "${domain}" "DESCRIBE EMITTER kafka_out_${lane};" "${node_output}"; then
            return 1
        fi
        {
            echo "EMITTER kafka_out_${lane}"
            sed 's/^/  /' "${node_output}"
        } >>"${output_file}"
        if grep -Eq '^[[:space:]]*error:' "${node_output}" \
            || ! grep -Eq '^owner: .+' "${node_output}" \
            || grep -Fq "owner: -" "${node_output}" \
            || ! grep -Fq "transient error:" "${node_output}"
        then
            return 1
        fi
    done
}

nodes_ready=0
deadline=$((SECONDS + wait_timeout_seconds))
while (( SECONDS < deadline )); do
    if graph_nodes_ready "${run_dir}/describe-node-readiness.txt"; then
        nodes_ready=1
        break
    fi
    process_is_live "${server_pid}" || fail "nervix-server exited while graph nodes were starting"
    sleep 0.2
done
if [[ "${nodes_ready}" != "1" ]]; then
    fail "benchmark graph nodes were not ready within ${wait_timeout_seconds}s"
fi

ready_file="${state_dir}/driver-ready"
go_file="${state_dir}/driver-go"
echo "Warming the full path before the timed interval..."
"${driver_bin}" \
    --bootstrap-servers "${kafka_bootstrap_servers}" \
    --input-topic "${input_topic}" \
    --output-topic "${output_topic}" \
    --duration-seconds "${duration_seconds}" \
    --value-bytes "${value_bytes}" \
    --max-backlog-messages "${max_backlog_messages}" \
    --wait-timeout-seconds "${wait_timeout_seconds}" \
    --ready-file "${ready_file}" \
    --go-file "${go_file}" \
    >"${run_dir}/driver-results.txt" 2>"${run_dir}/driver.log" &
driver_pid=$!

driver_ready=0
deadline=$((SECONDS + wait_timeout_seconds))
while (( SECONDS < deadline )); do
    if [[ -f "${ready_file}" ]]; then
        driver_ready=1
        break
    fi
    if ! process_is_live "${driver_pid}"; then
        wait "${driver_pid}" || true
        driver_pid=""
        sed -n '1,160p' "${run_dir}/driver.log" >&2
        fail "Kafka benchmark driver exited during warm-up"
    fi
    sleep 0.1
done
[[ "${driver_ready}" == "1" ]] || fail "the end-to-end warm-up did not finish within ${wait_timeout_seconds}s"

curl -fsS "${observability_url}/metrics" >"${run_dir}/metrics-before.prom"

read_process_ticks() {
    local pid="$1"
    awk '{print $14 + $15}' "/proc/${pid}/stat"
}

monitor_peak_rss() {
    local pid="$1"
    local active_file="$2"
    local output_file="$3"
    local peak_kib=0
    local current_kib=0
    while [[ -e "${active_file}" ]] && process_is_live "${pid}"; do
        current_kib="$(awk '/^VmRSS:/ {print $2}' "/proc/${pid}/status" 2>/dev/null || true)"
        if [[ "${current_kib}" =~ ^[0-9]+$ ]] && (( current_kib > peak_kib )); then
            peak_kib="${current_kib}"
        fi
        sleep 0.1
    done
    printf '%s\n' "${peak_kib}" >"${output_file}"
}

cpu_ticks_before="$(read_process_ticks "${server_pid}")"
touch "${state_dir}/monitor-active"
monitor_peak_rss "${server_pid}" "${state_dir}/monitor-active" "${state_dir}/peak-rss-kib" &
monitor_pid=$!

echo "Generating broker-acknowledged load for ${duration_seconds}s (${duration_selection})..."
touch "${go_file}"
if ! wait "${driver_pid}"; then
    driver_pid=""
    rm -f "${state_dir}/monitor-active"
    wait "${monitor_pid}" >/dev/null 2>&1 || true
    monitor_pid=""
    sed -n '1,200p' "${run_dir}/driver.log" >&2
    fail "Kafka benchmark driver failed"
fi
driver_pid=""
cpu_ticks_after="$(read_process_ticks "${server_pid}")"
rm -f "${state_dir}/monitor-active"
wait "${monitor_pid}"
monitor_pid=""

curl -fsS "${observability_url}/metrics" >"${run_dir}/metrics-after.prom"
cli_checked "${domain}" "DESCRIBE DOMAIN;" "${run_dir}/describe-domain.txt"
: >"${run_dir}/describe-ingestors.txt"
: >"${run_dir}/describe-emitters.txt"
for (( lane = 0; lane < partitions; lane++ )); do
    cli_checked "${domain}" "DESCRIBE INGESTOR kafka_in_${lane};" \
        "${state_dir}/describe-ingestor.txt"
    {
        echo "INGESTOR kafka_in_${lane}"
        sed 's/^/  /' "${state_dir}/describe-ingestor.txt"
    } >>"${run_dir}/describe-ingestors.txt"
    cli_checked "${domain}" "DESCRIBE EMITTER kafka_out_${lane};" \
        "${state_dir}/describe-emitter.txt"
    {
        echo "EMITTER kafka_out_${lane}"
        sed 's/^/  /' "${state_dir}/describe-emitter.txt"
    } >>"${run_dir}/describe-emitters.txt"
done

result_value() {
    local key="$1"
    local value
    value="$(awk -F= -v key="${key}" '$1 == key {print substr($0, index($0, "=") + 1); exit}' "${run_dir}/driver-results.txt")"
    [[ -n "${value}" ]] || fail "driver result '${key}' is missing"
    printf '%s\n' "${value}"
}

prom_sum() {
    local file="$1"
    local metric="$2"
    local target_kind="$3"
    local target="$4"
    local direction="$5"
    local relay="$6"
    local value
    if ! value="$(awk \
        -v prefix="${metric}{" \
        -v domain="${domain}" \
        -v target_kind="${target_kind}" \
        -v target="${target}" \
        -v direction="${direction}" \
        -v relay="${relay}" \
        'index($1, prefix) == 1 \
            && index($1, "domain=\"" domain "\"") \
            && index($1, "target_kind=\"" target_kind "\"") \
            && index($1, "target=\"" target "\"") \
            && index($1, "direction=\"" direction "\"") \
            && index($1, "relay=\"" relay "\"") {matched = 1; sum += $2} \
         END {if (!matched) exit 1; printf "%.0f\n", sum}' \
        "${file}")"
    then
        fail "missing Prometheus series ${metric} for ${target_kind} ${target} (${direction} ${relay}) in ${file}"
    fi
    printf '%s\n' "${value}"
}

prom_sum_lanes() {
    local file="$1"
    local metric="$2"
    local target_kind="$3"
    local target_prefix="$4"
    local direction="$5"
    local relay_prefix="$6"
    local lane
    local lane_value
    local total=0
    for (( lane = 0; lane < partitions; lane++ )); do
        lane_value="$(prom_sum "${file}" "${metric}" "${target_kind}" \
            "${target_prefix}_${lane}" "${direction}" "${relay_prefix}_${lane}")"
        total="$((total + lane_value))"
    done
    printf '%s\n' "${total}"
}

counter_delta() {
    local before="$1"
    local after="$2"
    awk -v before="${before}" -v after="${after}" 'BEGIN {printf "%.0f\n", after - before}'
}

average() {
    local numerator="$1"
    local denominator="$2"
    awk -v numerator="${numerator}" -v denominator="${denominator}" \
        'BEGIN {if (denominator == 0) print "-"; else printf "%.2f\n", numerator / denominator}'
}

metric_scalar() {
    local file="$1"
    local metric="$2"
    awk -v metric="${metric}" '$1 == metric {print $2; exit}' "${file}"
}

optional_mib() {
    local bytes="$1"
    if [[ -z "${bytes}" ]]; then
        printf '%s\n' "-"
    else
        awk -v bytes="${bytes}" 'BEGIN {printf "%.1f\n", bytes / 1048576}'
    fi
}

input_messages="$(result_value input_messages)"
output_messages="$(result_value output_messages)"
generation_seconds="$(result_value generation_seconds)"
producer_flush_seconds="$(result_value producer_flush_seconds)"
drain_seconds="$(result_value drain_seconds)"
end_to_end_seconds="$(result_value end_to_end_seconds)"
parity_stability_seconds="$(result_value parity_stability_seconds)"
wire_bytes_per_message="$(result_value wire_bytes_per_message)"
observed_partitions="$(result_value partitions)"
warmup_messages="$(result_value warmup_messages)"
configured_max_backlog_messages="$(result_value max_backlog_messages)"
peak_backlog_messages="$(result_value peak_backlog_messages)"
output_messages_at_flush="$(result_value output_messages_at_flush)"
output_messages_at_generation_end="$(result_value output_messages_at_generation_end)"
backlog_messages_at_generation_end="$(result_value backlog_messages_at_generation_end)"
backlog_messages_at_flush="$(result_value backlog_messages_at_flush)"
input_rate="$(result_value input_messages_per_second)"
output_rate_during_generation="$(result_value output_messages_per_second_during_generation)"
end_to_end_rate="$(result_value end_to_end_messages_per_second)"
input_mib_rate="$(result_value input_payload_mib_per_second)"
end_to_end_mib_rate="$(result_value end_to_end_payload_mib_per_second)"
[[ "${input_messages}" == "${output_messages}" ]] \
    || fail "input/output mismatch after driver success: ${input_messages} != ${output_messages}"
wire_payload_bytes="$((input_messages * wire_bytes_per_message))"

ingestor_messages_before="$(prom_sum_lanes "${run_dir}/metrics-before.prom" nervix_messages_total INGESTOR kafka_in sent benchmark_records)"
ingestor_messages_after="$(prom_sum_lanes "${run_dir}/metrics-after.prom" nervix_messages_total INGESTOR kafka_in sent benchmark_records)"
ingestor_batches_before="$(prom_sum_lanes "${run_dir}/metrics-before.prom" nervix_batches_total INGESTOR kafka_in sent benchmark_records)"
ingestor_batches_after="$(prom_sum_lanes "${run_dir}/metrics-after.prom" nervix_batches_total INGESTOR kafka_in sent benchmark_records)"
ingestor_bytes_before="$(prom_sum_lanes "${run_dir}/metrics-before.prom" nervix_bytes_total INGESTOR kafka_in sent benchmark_records)"
ingestor_bytes_after="$(prom_sum_lanes "${run_dir}/metrics-after.prom" nervix_bytes_total INGESTOR kafka_in sent benchmark_records)"
emitter_messages_before="$(prom_sum_lanes "${run_dir}/metrics-before.prom" nervix_messages_total EMITTER kafka_out sent benchmark_records)"
emitter_messages_after="$(prom_sum_lanes "${run_dir}/metrics-after.prom" nervix_messages_total EMITTER kafka_out sent benchmark_records)"
emitter_batches_before="$(prom_sum_lanes "${run_dir}/metrics-before.prom" nervix_batches_total EMITTER kafka_out sent benchmark_records)"
emitter_batches_after="$(prom_sum_lanes "${run_dir}/metrics-after.prom" nervix_batches_total EMITTER kafka_out sent benchmark_records)"
emitter_bytes_before="$(prom_sum_lanes "${run_dir}/metrics-before.prom" nervix_bytes_total EMITTER kafka_out sent benchmark_records)"
emitter_bytes_after="$(prom_sum_lanes "${run_dir}/metrics-after.prom" nervix_bytes_total EMITTER kafka_out sent benchmark_records)"

ingestor_messages="$(counter_delta "${ingestor_messages_before}" "${ingestor_messages_after}")"
ingestor_batches="$(counter_delta "${ingestor_batches_before}" "${ingestor_batches_after}")"
ingestor_bytes="$(counter_delta "${ingestor_bytes_before}" "${ingestor_bytes_after}")"
emitter_messages="$(counter_delta "${emitter_messages_before}" "${emitter_messages_after}")"
emitter_batches="$(counter_delta "${emitter_batches_before}" "${emitter_batches_after}")"
emitter_bytes="$(counter_delta "${emitter_bytes_before}" "${emitter_bytes_after}")"
[[ "${ingestor_messages}" == "${input_messages}" ]] \
    || fail "ingestor Prometheus count ${ingestor_messages} != broker input count ${input_messages}"
[[ "${emitter_messages}" == "${output_messages}" ]] \
    || fail "emitter Prometheus count ${emitter_messages} != Kafka output count ${output_messages}"
ingestor_messages_per_batch="$(average "${ingestor_messages}" "${ingestor_batches}")"
emitter_messages_per_batch="$(average "${emitter_messages}" "${emitter_batches}")"

clock_ticks="$(getconf CLK_TCK)"
cpu_ticks="$((cpu_ticks_after - cpu_ticks_before))"
cpu_seconds="$(awk -v ticks="${cpu_ticks}" -v hz="${clock_ticks}" 'BEGIN {printf "%.3f\n", ticks / hz}')"
observation_seconds="$(awk -v end_to_end="${end_to_end_seconds}" -v stability="${parity_stability_seconds}" \
    'BEGIN {printf "%.6f\n", end_to_end + stability}')"
cpu_percent="$(awk -v cpu="${cpu_seconds}" -v wall="${observation_seconds}" 'BEGIN {printf "%.1f\n", cpu / wall * 100}')"
peak_rss_kib="$(<"${state_dir}/peak-rss-kib")"
peak_rss_mib="$(awk -v kib="${peak_rss_kib}" 'BEGIN {printf "%.1f\n", kib / 1024}')"
jemalloc_allocated_bytes="$(metric_scalar "${run_dir}/metrics-after.prom" nervix_jemalloc_allocated_bytes)"
jemalloc_resident_bytes="$(metric_scalar "${run_dir}/metrics-after.prom" nervix_jemalloc_resident_bytes)"
jemalloc_allocated_mib="$(optional_mib "${jemalloc_allocated_bytes}")"
jemalloc_resident_mib="$(optional_mib "${jemalloc_resident_bytes}")"

commit="$(git rev-parse --short HEAD)"
if [[ -n "$(git status --porcelain)" ]]; then
    commit="${commit}-dirty"
fi
machine="$(uname -srmo)"
logical_cpus="$(getconf _NPROCESSORS_ONLN)"

report_file="${run_dir}/report.txt"
{
    echo "Nervix Kafka end-to-end benchmark"
    echo
    echo "status: PASS (stable exact Kafka input/output count parity)"
    echo "commit: ${commit}"
    echo "build_skipped: ${skip_build}"
    echo "server_sha256: ${server_sha256}"
    echo "cli_sha256: ${cli_sha256}"
    echo "driver_sha256: ${driver_sha256}"
    echo "machine: ${machine}"
    echo "logical_cpus: ${logical_cpus}"
    echo "domain: ${domain}"
    echo "partitions: ${partitions}"
    echo "observed_partitions: ${observed_partitions}"
    echo "parallel_lanes: ${partitions}"
    echo "ingestor_instances_per_lane: 1"
    echo "warmup_messages_excluded: ${warmup_messages}"
    echo "wire_bytes_per_message: ${wire_bytes_per_message}"
    echo "configured_max_backlog_messages: ${configured_max_backlog_messages}"
    echo "peak_backlog_messages: ${peak_backlog_messages}"
    echo "ack_mode: NO_ACK PARALLEL"
    echo "ingestor_flush_each: ${ingestor_flush_each}"
    echo "ingestor_max_batch_size: ${ingestor_max_batch_size}"
    echo "emitter_flush_each: ${emitter_flush_each}"
    echo "emitter_max_batch_size: ${emitter_max_batch_size}"
    echo "load_duration_selection: ${duration_selection}"
    echo
    echo "Load and completion"
    echo "  requested_load_seconds: ${duration_seconds}"
    echo "  actual_load_seconds: ${generation_seconds}"
    echo "  broker_acknowledged_input_messages: ${input_messages}"
    echo "  kafka_output_messages: ${output_messages}"
    echo "  kafka_input_output_count_difference: 0"
    echo "  parity_stability_seconds: ${parity_stability_seconds}"
    echo "  kafka_wire_payload_bytes: ${wire_payload_bytes}"
    echo "  producer_rate_messages_per_second: ${input_rate}"
    echo "  producer_payload_mib_per_second: ${input_mib_rate}"
    echo "  output_messages_at_generation_end: ${output_messages_at_generation_end}"
    echo "  backlog_messages_at_generation_end: ${backlog_messages_at_generation_end}"
    echo "  output_rate_during_generation_messages_per_second: ${output_rate_during_generation}"
    echo "  output_messages_at_producer_flush: ${output_messages_at_flush}"
    echo "  backlog_messages_at_producer_flush: ${backlog_messages_at_flush}"
    echo "  producer_flush_seconds: ${producer_flush_seconds}"
    echo "  post_flush_drain_seconds: ${drain_seconds}"
    echo "  end_to_end_seconds: ${end_to_end_seconds}"
    echo "  end_to_end_messages_per_second: ${end_to_end_rate}"
    echo "  end_to_end_payload_mib_per_second: ${end_to_end_mib_rate}"
    echo
    echo "Nervix metrics across load, drain, and parity confirmation"
    echo "  observation_seconds: ${observation_seconds}"
    echo "  ingestor_messages: ${ingestor_messages}"
    echo "  ingestor_batches: ${ingestor_batches}"
    echo "  ingestor_messages_per_batch: ${ingestor_messages_per_batch}"
    echo "  ingestor_arrow_bytes: ${ingestor_bytes}"
    echo "  emitter_messages: ${emitter_messages}"
    echo "  emitter_batches: ${emitter_batches}"
    echo "  emitter_messages_per_batch: ${emitter_messages_per_batch}"
    echo "  emitter_arrow_bytes: ${emitter_bytes}"
    echo "  server_cpu_seconds: ${cpu_seconds}"
    echo "  server_average_cpu_percent_over_observation: ${cpu_percent}"
    echo "  server_peak_rss_mib: ${peak_rss_mib}"
    echo "  jemalloc_allocated_mib_at_end: ${jemalloc_allocated_mib}"
    echo "  jemalloc_resident_mib_at_end: ${jemalloc_resident_mib}"
    echo
    echo "DESCRIBE histogram summaries"
    echo "  ingestor:"
    if grep -Eh 'messages_per_batch|delivery_latency|relay_buffer_len' \
        "${run_dir}/describe-ingestors.txt" | sed 's/^/    /'
    then
        true
    else
        echo "    none"
    fi
    echo "  emitter:"
    if grep -Eh 'messages_per_batch|delivery_latency|relay_buffer_len' \
        "${run_dir}/describe-emitters.txt" | sed 's/^/    /'
    then
        true
    else
        echo "    none"
    fi
    echo
    echo "Artifacts: ${run_dir}"
} | tee "${report_file}"

benchmark_succeeded=1
