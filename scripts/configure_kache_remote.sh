#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <config-path>" >&2
    exit 2
fi

config_path="$1"
bucket="${KACHE_S3_BUCKET:?KACHE_S3_BUCKET is required}"
region="${KACHE_S3_REGION:?KACHE_S3_REGION is required}"
: "${KACHE_S3_ENDPOINT:?KACHE_S3_ENDPOINT is required}"
: "${KACHE_S3_ACCESS_KEY:?KACHE_S3_ACCESS_KEY is required}"
: "${KACHE_S3_SECRET_KEY:?KACHE_S3_SECRET_KEY is required}"

if [[ ! "${bucket}" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]]; then
    echo "KACHE_S3_BUCKET is not a valid S3 bucket name" >&2
    exit 2
fi
if [[ ! "${region}" =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
    echo "KACHE_S3_REGION is not a valid S3 region" >&2
    exit 2
fi

endpoint_toml="$(python3 - <<'PY'
import json
import os
from urllib.parse import urlsplit

endpoint = os.environ["KACHE_S3_ENDPOINT"]
try:
    parsed = urlsplit(endpoint)
    parsed.port
    valid = (
        parsed.scheme in ("http", "https")
        and bool(parsed.hostname)
        and parsed.username is None
        and parsed.password is None
        and parsed.path in ("", "/")
        and not parsed.query
        and not parsed.fragment
        and not any(character.isspace() or ord(character) < 32 for character in endpoint)
    )
except ValueError:
    valid = False
if not valid:
    raise SystemExit("KACHE_S3_ENDPOINT must be an HTTP(S) service URL without credentials, a bucket path, query, or fragment")
print(json.dumps(endpoint.rstrip("/")))
PY
)"

mkdir -p "$(dirname -- "${config_path}")"
umask 077
printf \
    '[cache.remote]\ntype = "s3"\nbucket = "%s"\nregion = "%s"\nendpoint = %s\n' \
    "${bucket}" \
    "${region}" \
    "${endpoint_toml}" \
    > "${config_path}"
