#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import urllib.error
import urllib.request
from pathlib import Path

PARALLEL_TRANSFERS = 16

def upload_command(bucket: str, prefix: str, root: Path) -> list[str]:
    target = f"r2:{bucket}/{prefix.strip('/')}"
    return [
        "rclone",
        "copy",
        str(root),
        target,
        "--transfers",
        str(PARALLEL_TRANSFERS),
        "--checkers",
        str(PARALLEL_TRANSFERS),
        "--no-check-dest",
        "--s3-no-check-bucket",
        "--stats",
        "10s",
        "--stats-one-line",
    ]


def r2_environment(account_id: str, token_id: str, api_token: str) -> dict[str, str]:
    # Cloudflare defines an R2 S3 access key as the API token ID and its secret
    # as the SHA-256 digest of the token value.
    secret_access_key = hashlib.sha256(api_token.encode()).hexdigest()
    return {
        "RCLONE_CONFIG_R2_TYPE": "s3",
        "RCLONE_CONFIG_R2_PROVIDER": "Cloudflare",
        "RCLONE_CONFIG_R2_ACCESS_KEY_ID": token_id,
        "RCLONE_CONFIG_R2_SECRET_ACCESS_KEY": secret_access_key,
        "RCLONE_CONFIG_R2_ENDPOINT": (
            f"https://{account_id}.r2.cloudflarestorage.com"
        ),
        "RCLONE_CONFIG_R2_NO_CHECK_BUCKET": "true",
    }


def cloudflare_token_id(account_id: str, api_token: str) -> str:
    endpoints = [
        f"https://api.cloudflare.com/client/v4/accounts/{account_id}/tokens/verify",
        "https://api.cloudflare.com/client/v4/user/tokens/verify",
    ]
    failures: list[str] = []
    for endpoint in endpoints:
        request = urllib.request.Request(
            endpoint,
            headers={"Authorization": f"Bearer {api_token}"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = json.load(response)
        except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError) as error:
            failures.append(str(error))
            continue

        result = payload.get("result")
        if (
            payload.get("success") is True
            and isinstance(result, dict)
            and result.get("status") == "active"
            and isinstance(result.get("id"), str)
        ):
            return result["id"]
        failures.append("Cloudflare did not return an active token")

    details = "; ".join(failures)
    raise RuntimeError(f"failed to resolve Cloudflare API token ID: {details}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Upload the rendered mdBook output to a Cloudflare R2 prefix."
    )
    parser.add_argument("--bucket", required=True, help="R2 bucket name")
    parser.add_argument("--prefix", required=True, help="Target object prefix")
    parser.add_argument(
        "--source",
        default="docs/book",
        help="Rendered book directory to upload",
    )
    args = parser.parse_args()

    api_token = os.environ.get("CLOUDFLARE_API_TOKEN")
    if api_token is None:
        raise SystemExit("CLOUDFLARE_API_TOKEN must be set for remote uploads")
    account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
    if account_id is None:
        raise SystemExit("CLOUDFLARE_ACCOUNT_ID must be set for remote uploads")
    if shutil.which("rclone") is None:
        raise SystemExit("rclone must be installed for remote uploads")

    root = Path(args.source)
    if not root.is_dir():
        raise SystemExit(f"book output directory does not exist: {root}")

    file_count = sum(1 for candidate in root.rglob("*") if candidate.is_file())
    print(
        f"Uploading {file_count} files to R2 with {PARALLEL_TRANSFERS} parallel transfers"
    )
    try:
        token_id = cloudflare_token_id(account_id, api_token)
    except RuntimeError as error:
        raise SystemExit(str(error)) from error

    environment = os.environ.copy()
    environment.update(r2_environment(account_id, token_id, api_token))
    subprocess.run(
        upload_command(args.bucket, args.prefix, root),
        check=True,
        env=environment,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
