#!/usr/bin/env bash
set -euo pipefail

script_path="${1:?expected a rendered Flink SQL script path}"

/opt/flink/bin/start-cluster.sh
sql_output="$(/opt/flink/bin/sql-client.sh --file "${script_path}" 2>&1)" || {
    printf '%s\n' "${sql_output}" >&2
    exit 1
}
printf '%s\n' "${sql_output}"
if [[ "${sql_output}" == *'[ERROR]'* ]]; then
    echo 'Flink SQL client rejected the benchmark job' >&2
    exit 1
fi

for attempt in $(seq 1 120); do
    if curl --fail --silent --show-error http://localhost:8081/jobs/overview 2>/dev/null \
        | grep -q '"state":"RUNNING"'; then
        echo 'BENCHMARK_FLINK_READY'
        exec tail -F /opt/flink/log/*.log
    fi
    sleep 1
done

echo 'Flink benchmark job did not reach RUNNING within 120 seconds' >&2
exit 1
