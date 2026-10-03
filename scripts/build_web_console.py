"""Build real console assets and publish their current complete output set."""

from __future__ import annotations

import os
import pathlib
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]


def publish_assets(source: pathlib.Path, destination: pathlib.Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    unreferenced = {path.relative_to(destination)
                    for path in destination.rglob("*") if path.is_file()}
    for asset in sorted(source.rglob("*")):
        if not asset.is_file():
            continue
        relative = asset.relative_to(source)
        unreferenced.discard(relative)
        published = destination / relative
        if published.is_file() and published.read_bytes() == asset.read_bytes():
            continue
        published.parent.mkdir(parents=True, exist_ok=True)
        asset.replace(published)
    for relative in unreferenced:
        (destination / relative).unlink()


def main() -> None:
    target = ROOT / "target"
    target.mkdir(exist_ok=True)
    console = ROOT / "crates/web-console"
    with tempfile.TemporaryDirectory(prefix="web-console-", dir=target) as directory:
        staging = pathlib.Path(directory) / "dist"
        env = os.environ.copy()
        env.pop("NO_COLOR", None)
        subprocess.run(
            ["trunk", "build", "--release", "--dist", str(staging)],
            cwd=console, env=env, check=True,
        )
        publish_assets(staging, console / "dist")


if __name__ == "__main__":
    main()
