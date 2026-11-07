"""Build real console assets and publish their current complete output set.

trunk runs offline with the wasm-bindgen CLI `scripts/wasm_bindgen.py` installs first on PATH, so the
build never downloads a tool and a missing or mismatched one fails at once.
"""

from __future__ import annotations

import argparse
import contextlib
from collections.abc import Iterator
import os
import pathlib
import subprocess
import sys
import tempfile

from scripts import wasm_bindgen

ROOT = pathlib.Path(__file__).resolve().parents[1]


@contextlib.contextmanager
def development_index(console: pathlib.Path) -> Iterator[pathlib.Path]:
    """Keep development symbols mangled instead of expanding generic Rust type names."""
    source = (console / "index.html").read_text()
    if source.count("</head>") != 1:
        raise wasm_bindgen.ToolError("console index must contain one closing head element")
    source = source.replace(
        "</head>", '<link data-trunk rel="rust" data-no-demangle />\n  </head>', 1,
    )
    with tempfile.NamedTemporaryFile(
        mode="w", prefix=".trunk-dev-", suffix=".html", dir=console, delete=False,
    ) as output:
        output.write(source)
        index = pathlib.Path(output.name)
    try:
        yield index
    finally:
        index.unlink()


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


def main(*, release: bool = True) -> None:
    target = ROOT / "target"
    target.mkdir(exist_ok=True)
    console = ROOT / "crates/web-console"
    tools = wasm_bindgen.install(ROOT, wasm_bindgen.default_target())
    with tempfile.TemporaryDirectory(prefix="web-console-", dir=target) as directory:
        staging = pathlib.Path(directory) / "dist"
        env = os.environ.copy()
        env.pop("NO_COLOR", None)
        env["PATH"] = f"{tools}{os.pathsep}{env.get('PATH', '')}"
        if not release:
            cargo_target = wasm_bindgen.default_target()
            if not cargo_target.is_absolute():
                cargo_target = ROOT / cargo_target
            env["CARGO_TARGET_DIR"] = str(cargo_target / "web-console-dev")
        command = ["trunk", "build"]
        if release:
            command.append("--release")
        command.extend(["--offline", "--dist", str(staging)])
        if release:
            subprocess.run(command, cwd=console, env=env, check=True)
        else:
            with development_index(console) as index:
                command.extend(["--html-output", "index.html", str(index)])
                subprocess.run(command, cwd=console, env=env, check=True)
        publish_assets(staging, console / "dist")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dev", action="store_true",
                        help="build development assets for focused local verification")
    arguments = parser.parse_args()
    try:
        main(release=not arguments.dev)
    except wasm_bindgen.ToolError as error:
        sys.exit(f"error: {error}")
