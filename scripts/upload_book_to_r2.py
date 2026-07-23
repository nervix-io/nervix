#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

PARALLEL_UPLOADS = 16
MAX_UPLOAD_ATTEMPTS = 4


@dataclass(frozen=True)
class R2Credentials:
    access_key_id: str
    secret_access_key: str


@dataclass(frozen=True)
class UploadEntry:
    path: Path
    object_key: str
    content_type: str


class UploadError(RuntimeError):
    pass


class S3Client(Protocol):
    def put_object(self, **kwargs: object) -> object: ...


def r2_credentials(token_id: str, api_token: str) -> R2Credentials:
    # Cloudflare defines an R2 S3 access key as the API token ID and its secret
    # as the SHA-256 digest of the token value.
    return R2Credentials(
        access_key_id=token_id,
        secret_access_key=hashlib.sha256(api_token.encode()).hexdigest(),
    )


def content_type_for(path: Path) -> str:
    explicit_types = {
        ".css": "text/css",
        ".js": "application/javascript",
        ".json": "application/json",
        ".svg": "image/svg+xml",
        ".woff": "font/woff",
        ".woff2": "font/woff2",
        ".eot": "application/vnd.ms-fontobject",
        ".ttf": "font/ttf",
        ".otf": "font/otf",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".ico": "image/x-icon",
        ".md": "text/markdown; charset=utf-8",
        ".txt": "text/plain; charset=utf-8",
        ".html": "text/html; charset=utf-8",
        ".xml": "application/xml",
    }
    if path.suffix.lower() in explicit_types:
        return explicit_types[path.suffix.lower()]
    guessed, _ = mimetypes.guess_type(str(path))
    if guessed is None:
        return "application/octet-stream"
    if guessed.startswith("text/"):
        return f"{guessed}; charset=utf-8"
    return guessed


def create_r2_client(
    account_id: str,
    credentials: R2Credentials,
) -> S3Client:
    return boto3.client(
        service_name="s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=credentials.access_key_id,
        aws_secret_access_key=credentials.secret_access_key,
        region_name="auto",
        config=Config(
            max_pool_connections=PARALLEL_UPLOADS,
            retries={
                "mode": "standard",
                "total_max_attempts": MAX_UPLOAD_ATTEMPTS,
            },
        ),
    )


def put_object(
    client: S3Client,
    bucket: str,
    object_key: str,
    payload: bytes,
    content_type: str,
) -> None:
    try:
        client.put_object(
            Bucket=bucket,
            Key=object_key,
            Body=payload,
            ContentType=content_type,
        )
    except (BotoCoreError, ClientError) as error:
        raise UploadError(f"{object_key}: R2 upload failed: {error}") from error


def upload_entry(
    client: S3Client,
    bucket: str,
    entry: UploadEntry,
) -> None:
    put_object(
        client=client,
        bucket=bucket,
        object_key=entry.object_key,
        payload=entry.path.read_bytes(),
        content_type=entry.content_type,
    )


def collect_upload_entries(root: Path, prefix: str) -> list[UploadEntry]:
    normalized_prefix = prefix.strip("/")
    entries = []
    for path in sorted(
        candidate for candidate in root.rglob("*") if candidate.is_file()
    ):
        relative_path = path.relative_to(root).as_posix()
        object_key = (
            f"{normalized_prefix}/{relative_path}"
            if normalized_prefix
            else relative_path
        )
        entries.append(
            UploadEntry(
                path=path,
                object_key=object_key,
                content_type=content_type_for(path),
            )
        )
    return entries


def upload_directory(
    client: S3Client,
    bucket: str,
    entries: list[UploadEntry],
) -> None:
    failures = []
    with ThreadPoolExecutor(max_workers=PARALLEL_UPLOADS) as executor:
        pending = {
            executor.submit(
                upload_entry,
                client,
                bucket,
                entry,
            ): entry.object_key
            for entry in entries
        }
        for future in as_completed(pending):
            try:
                future.result()
            except (OSError, UploadError) as error:
                failures.append(str(error))

    if failures:
        raise UploadError(
            f"{len(failures)} object upload(s) failed; first error: {failures[0]}"
        )


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
    root = Path(args.source)
    if not root.is_dir():
        raise SystemExit(f"book output directory does not exist: {root}")

    entries = collect_upload_entries(root, args.prefix)
    print(
        f"Uploading {len(entries)} files to R2 with {PARALLEL_UPLOADS} parallel requests"
    )
    try:
        token_id = cloudflare_token_id(account_id, api_token)
    except RuntimeError as error:
        raise SystemExit(str(error)) from error

    client = create_r2_client(
        account_id,
        r2_credentials(token_id, api_token),
    )
    try:
        upload_directory(
            client=client,
            bucket=args.bucket,
            entries=entries,
        )
    except UploadError as error:
        raise SystemExit(str(error)) from error

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
