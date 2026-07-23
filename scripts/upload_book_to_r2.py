#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import mimetypes
import os
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

PARALLEL_UPLOADS = 16
MAX_UPLOAD_ATTEMPTS = 4
RETRYABLE_HTTP_STATUSES = {408, 409, 425, 429, 500, 502, 503, 504}


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


def build_put_request(
    account_id: str,
    bucket: str,
    object_key: str,
    payload: bytes,
    content_type: str,
    credentials: R2Credentials,
    now: datetime,
) -> urllib.request.Request:
    host = f"{account_id}.r2.cloudflarestorage.com"
    canonical_uri = (
        f"/{quote(bucket, safe='-_.~')}/{quote(object_key, safe='/-_.~')}"
    )
    request_time = now.astimezone(UTC)
    date_stamp = request_time.strftime("%Y%m%d")
    amz_date = request_time.strftime("%Y%m%dT%H%M%SZ")
    payload_hash = hashlib.sha256(payload).hexdigest()
    signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date"
    canonical_headers = (
        f"content-type:{content_type}\n"
        f"host:{host}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    canonical_request = "\n".join(
        [
            "PUT",
            canonical_uri,
            "",
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )
    credential_scope = f"{date_stamp}/auto/s3/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )
    date_key = hmac.new(
        f"AWS4{credentials.secret_access_key}".encode(),
        date_stamp.encode(),
        hashlib.sha256,
    ).digest()
    region_key = hmac.new(date_key, b"auto", hashlib.sha256).digest()
    service_key = hmac.new(region_key, b"s3", hashlib.sha256).digest()
    signing_key = hmac.new(service_key, b"aws4_request", hashlib.sha256).digest()
    signature = hmac.new(
        signing_key, string_to_sign.encode(), hashlib.sha256
    ).hexdigest()
    authorization = (
        "AWS4-HMAC-SHA256 "
        f"Credential={credentials.access_key_id}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    return urllib.request.Request(
        f"https://{host}{canonical_uri}",
        data=payload,
        headers={
            "Authorization": authorization,
            "Content-Type": content_type,
            "Host": host,
            "X-Amz-Content-Sha256": payload_hash,
            "X-Amz-Date": amz_date,
        },
        method="PUT",
    )


def put_object(
    account_id: str,
    bucket: str,
    object_key: str,
    payload: bytes,
    content_type: str,
    credentials: R2Credentials,
    *,
    open_request: Callable[..., object] = urllib.request.urlopen,
    wait: Callable[[float], None] = time.sleep,
) -> None:
    for attempt in range(MAX_UPLOAD_ATTEMPTS):
        request = build_put_request(
            account_id=account_id,
            bucket=bucket,
            object_key=object_key,
            payload=payload,
            content_type=content_type,
            credentials=credentials,
            now=datetime.now(UTC),
        )
        try:
            with open_request(request, timeout=60) as response:
                if 200 <= response.status < 300:
                    return
                raise UploadError(f"{object_key}: R2 returned HTTP {response.status}")
        except urllib.error.HTTPError as error:
            error.close()
            if (
                error.code not in RETRYABLE_HTTP_STATUSES
                or attempt + 1 == MAX_UPLOAD_ATTEMPTS
            ):
                raise UploadError(
                    f"{object_key}: R2 returned HTTP {error.code} {error.reason}"
                ) from error
        except (urllib.error.URLError, TimeoutError) as error:
            if attempt + 1 == MAX_UPLOAD_ATTEMPTS:
                reason = getattr(error, "reason", error)
                raise UploadError(
                    f"{object_key}: R2 upload request failed: {reason}"
                ) from error
        wait(2**attempt)


def upload_entry(
    account_id: str,
    bucket: str,
    entry: UploadEntry,
    credentials: R2Credentials,
    *,
    open_request: Callable[..., object] = urllib.request.urlopen,
    wait: Callable[[float], None] = time.sleep,
) -> None:
    put_object(
        account_id=account_id,
        bucket=bucket,
        object_key=entry.object_key,
        payload=entry.path.read_bytes(),
        content_type=entry.content_type,
        credentials=credentials,
        open_request=open_request,
        wait=wait,
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
    account_id: str,
    bucket: str,
    entries: list[UploadEntry],
    credentials: R2Credentials,
) -> None:
    failures = []
    with ThreadPoolExecutor(max_workers=PARALLEL_UPLOADS) as executor:
        pending = {
            executor.submit(
                upload_entry,
                account_id,
                bucket,
                entry,
                credentials,
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

    try:
        upload_directory(
            account_id=account_id,
            bucket=args.bucket,
            entries=entries,
            credentials=r2_credentials(token_id, api_token),
        )
    except UploadError as error:
        raise SystemExit(str(error)) from error

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
