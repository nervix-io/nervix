"""Build real console assets and publish their current complete output set.

trunk runs offline with the wasm-bindgen CLI `scripts/wasm_bindgen.py` installs first on PATH, so the
build never downloads a tool and a missing or mismatched one fails at once.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import tempfile

from scripts import wasm_bindgen

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
    tools = wasm_bindgen.install(ROOT, wasm_bindgen.default_target())
    with tempfile.TemporaryDirectory(prefix="web-console-", dir=target) as directory:
        staging = pathlib.Path(directory) / "dist"
        env = os.environ.copy()
        env.pop("NO_COLOR", None)
        env["PATH"] = f"{tools}{os.pathsep}{env.get('PATH', '')}"
        subprocess.run(
            ["trunk", "build", "--release", "--offline", "--dist", str(staging)],
            cwd=console, env=env, check=True,
        )
        publish_assets(staging, console / "dist")


if __name__ == "__main__":
    try:
        main()
    except wasm_bindgen.ToolError as error:
        sys.exit(f"error: {error}")
