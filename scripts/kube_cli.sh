#!/usr/bin/env bash
set -euo pipefail

server="http://$(minikube ip):31390"

exec cargo run -q -p nervix-cli -- --server "${server}" "$@"
