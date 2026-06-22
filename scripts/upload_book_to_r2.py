#!/usr/bin/env python3

from __future__ import annotations

import argparse
import mimetypes
import os
import subprocess
from pathlib import Path


def content_type_for(path: Path) -> str:
    # Rely on explicit overrides instead of host defaults so browsers get
    # correct asset types from R2.
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
        ".txt": "text/plain; charset=utf-8",
        ".html": "text/html; charset=utf-8",
        ".xml": "application/xml",
    }
    if path.suffix in explicit_types:
        return explicit_types[path.suffix]
    guessed, _ = mimetypes.guess_type(str(path))
    if guessed is None:
        return "application/octet-stream"
    if guessed.startswith("text/"):
        return f"{guessed}; charset=utf-8"
    return guessed


def upload_file(bucket: str, prefix: str, file_path: Path, root: Path) -> None:
    rel = file_path.relative_to(root).as_posix()
    object_key = f"{bucket}/{prefix.rstrip('/')}/{rel}"
    content_type = content_type_for(file_path)
    subprocess.run(
        [
            "npx",
            "--yes",
            "wrangler",
            "r2",
            "object",
            "put",
            "--remote",
            object_key,
            "--file",
            str(file_path),
            "--content-type",
            content_type,
        ],
        check=True,
    )


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

    if "CLOUDFLARE_API_TOKEN" not in os.environ:
        raise SystemExit("CLOUDFLARE_API_TOKEN must be set for remote uploads")

    root = Path(args.source)
    if not root.is_dir():
        raise SystemExit(f"book output directory does not exist: {root}")

    for path in sorted(candidate for candidate in root.rglob("*") if candidate.is_file()):
        upload_file(args.bucket, args.prefix, path, root)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
