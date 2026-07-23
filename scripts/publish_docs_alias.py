#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
from pathlib import Path

if __package__:
    from . import purge_cloudflare_cache, upload_book_to_r2
else:
    import purge_cloudflare_cache
    import upload_book_to_r2


def upload_tree(
    client: upload_book_to_r2.S3Client,
    bucket: str,
    target: str,
    source: Path,
) -> None:
    entries = upload_book_to_r2.collect_upload_entries(source, target)
    print(
        f"Uploading {len(entries)} files to R2 with "
        f"{upload_book_to_r2.PARALLEL_UPLOADS} parallel requests"
    )
    upload_book_to_r2.upload_directory(client, bucket, entries)


def update_alias(
    client: upload_book_to_r2.S3Client,
    bucket: str,
    alias: str,
    target: str,
) -> None:
    upload_book_to_r2.put_object(
        client=client,
        bucket=bucket,
        object_key=f"meta/{alias}.txt",
        payload=f"{target.rstrip('/')}\n".encode(),
        content_type="text/plain; charset=utf-8",
    )


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
    account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    if account_id is None:
        raise SystemExit("CLOUDFLARE_ACCOUNT_ID must be set")

    source = Path(args.source)
    if not source.is_dir():
        raise SystemExit(f"source directory does not exist: {source}")

    try:
        credentials = upload_book_to_r2.r2_credentials(
            upload_book_to_r2.cloudflare_token_id(account_id, api_token),
            api_token,
        )
        client = upload_book_to_r2.create_r2_client(account_id, credentials)
        upload_tree(client, args.bucket, args.target, source)
        update_alias(
            client,
            args.bucket,
            args.alias,
            args.target,
        )
    except RuntimeError as error:
        raise SystemExit(str(error)) from error

    purge_cloudflare_cache.purge_everything(args.zone_id, api_token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
