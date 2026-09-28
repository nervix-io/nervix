#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${script_dir}/run-baseline.sh" --scenario partition-recovery --records 1000 --timeout 3000 "$@"
