#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <config-path>" >&2
    exit 2
fi

config_path="$1"
bucket="${KACHE_S3_BUCKET:?KACHE_S3_BUCKET is required}"
region="${KACHE_S3_REGION:?KACHE_S3_REGION is required}"
: "${KACHE_S3_ACCESS_KEY:?KACHE_S3_ACCESS_KEY is required}"
: "${KACHE_S3_SECRET_KEY:?KACHE_S3_SECRET_KEY is required}"

if [[ ! "${bucket}" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]]; then
    echo "KACHE_S3_BUCKET is not a valid S3 bucket name" >&2
    exit 2
fi
if [[ ! "${region}" =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
    echo "KACHE_S3_REGION is not a valid AWS region" >&2
    exit 2
fi

mkdir -p "$(dirname -- "${config_path}")"
umask 077
printf \
    '[cache.remote]\ntype = "s3"\nbucket = "%s"\nregion = "%s"\n' \
    "${bucket}" \
    "${region}" \
    > "${config_path}"
