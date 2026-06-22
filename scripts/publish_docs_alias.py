#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import subprocess
import tempfile
from pathlib import Path


def run(args: list[str]) -> None:
    subprocess.run(args, check=True)


def upload_tree(bucket: str, target: str, source: Path) -> None:
    run(
        [
            "python",
            "scripts/upload_book_to_r2.py",
            "--bucket",
            bucket,
            "--prefix",
            target,
            "--source",
            str(source),
        ]
    )


def update_alias(bucket: str, alias: str, target: str) -> None:
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False) as tmp:
        tmp.write(f"{target.rstrip('/')}\n")
        temp_name = tmp.name
    try:
        run(
            [
                "npx",
                "--yes",
                "wrangler",
                "r2",
                "object",
                "put",
                "--remote",
                f"{bucket}/meta/{alias}.txt",
                "--file",
                temp_name,
                "--content-type",
                "text/plain; charset=utf-8",
            ]
        )
    finally:
        os.unlink(temp_name)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Upload a rendered docs directory to an immutable R2 prefix, repoint an alias "
            "such as snapshot, and purge Cloudflare cache."
        )
    )
    parser.add_argument("--source", required=True, help="Directory to upload")
    parser.add_argument("--target", required=True, help="Immutable prefix to upload into")
    parser.add_argument("--alias", default="snapshot", help="Alias prefix to repoint")
    parser.add_argument("--bucket", default="nervix-docs", help="R2 bucket name")
    parser.add_argument("--zone-id", required=True, help="Cloudflare zone id to purge")
    args = parser.parse_args()

    api_token = os.environ.get("CLOUDFLARE_API_TOKEN")
    if api_token is None:
        raise SystemExit("CLOUDFLARE_API_TOKEN must be set")

    source = Path(args.source)
    if not source.is_dir():
        raise SystemExit(f"source directory does not exist: {source}")

    upload_tree(args.bucket, args.target, source)
    update_alias(args.bucket, args.alias, args.target)
    run(
        [
            "python",
            "scripts/purge_cloudflare_cache.py",
            "--zone-id",
            args.zone_id,
        ]
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
