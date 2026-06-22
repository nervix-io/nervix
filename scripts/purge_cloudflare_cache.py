#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import urllib.request


def purge_everything(zone_id: str, api_token: str) -> None:
    req = urllib.request.Request(
        f"https://api.cloudflare.com/client/v4/zones/{zone_id}/purge_cache",
        method="POST",
        headers={
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        },
        data=json.dumps({"purge_everything": True}).encode("utf-8"),
    )
    with urllib.request.urlopen(req) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not payload.get("success"):
        raise SystemExit(f"cache purge failed: {payload}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Purge Cloudflare zone cache.")
    parser.add_argument("--zone-id", required=True, help="Cloudflare zone id")
    args = parser.parse_args()

    api_token = os.environ.get("CLOUDFLARE_API_TOKEN")
    if api_token is None:
        raise SystemExit("CLOUDFLARE_API_TOKEN must be set")

    purge_everything(args.zone_id, api_token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
