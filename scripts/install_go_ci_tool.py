#!/usr/bin/env python3
"""Install a pinned Go CI tool and retain every public download transport failure."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import time


TOOLS = {
    "dockerfmt": "github.com/reteps/dockerfmt@v0.5.4",
    "ghokin": "github.com/antham/ghokin/v3@v3.9.0",
}
MAX_ATTEMPTS = 3
TOTAL_BUDGET_SECONDS = 480
ATTEMPT_TIMEOUT_SECONDS = 300
RETRY_DELAYS_SECONDS = (2, 5)
TRANSPORT_ERRORS = (
    "stream error",
    "internal_error",
    "connection reset",
    "connection refused",
    "unexpected eof",
    "i/o timeout",
    "tls handshake timeout",
    "service unavailable",
    " 502 ",
    " 503 ",
    " 504 ",
)


def retryable_public_transport(output: str) -> bool:
    """Retry failed reads from Go's public module and checksum services."""
    lowered = output.lower()
    if "checksum mismatch" in lowered or "security error" in lowered:
        return False
    return any(
        any(
            source in line
            for source in (
                "reading https://sum.golang.org/",
                "reading https://proxy.golang.org/",
                'read "https://proxy.golang.org/',
            )
        )
        and any(error in line for error in TRANSPORT_ERRORS)
        for line in lowered.splitlines()
    )


def install(tool: str, destination: Path) -> int:
    if tool not in TOOLS:
        print(f"unsupported Go CI tool: {tool}", file=sys.stderr)
        return 2

    tool_directory = destination / tool
    tool_directory.mkdir(parents=True, exist_ok=True)
    command = ["go", "install", TOOLS[tool]]
    environment = os.environ.copy()
    # These are public tools. Keep the public checksum database active even if a
    # runner carries a local Go override intended for a different project.
    environment.update(GOSUMDB="sum.golang.org", GONOSUMDB="", GOPRIVATE="", GOTOOLCHAIN="local")
    started = time.monotonic()

    for attempt in range(1, MAX_ATTEMPTS + 1):
        remaining = TOTAL_BUDGET_SECONDS - (time.monotonic() - started)
        if remaining <= 0:
            print(f"{tool}: installation budget exhausted", file=sys.stderr)
            return 124

        attempt_started = time.monotonic()
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=min(ATTEMPT_TIMEOUT_SECONDS, remaining),
                check=False,
                env=environment,
            )
            status = result.returncode
            output = result.stdout + result.stderr
        except subprocess.TimeoutExpired as error:
            status = 124
            stdout = error.stdout or b""
            stderr = error.stderr or b""
            output = (stdout if isinstance(stdout, str) else stdout.decode(errors="replace"))
            output += stderr if isinstance(stderr, str) else stderr.decode(errors="replace")
            output += "\nGo installation attempt timed out.\n"
        except OSError as error:
            status = 127
            output = f"Could not launch go: {error}\n"

        duration = time.monotonic() - attempt_started
        retryable = status not in (0, 124, 127) and retryable_public_transport(output)
        log = tool_directory / f"attempt-{attempt:02d}.log"
        log.write_text(
            f"Command: {' '.join(command)}\n"
            f"GOSUMDB: {environment['GOSUMDB']}\n"
            f"Exit status: {status}\n"
            f"Duration: {duration:.1f}s\n"
            f"Retryable public download transport error: {retryable}\n\n{output}"
        )
        print(f"{tool}: attempt {attempt}/{MAX_ATTEMPTS} exited {status}; log: {log}", flush=True)
        if status == 0:
            return 0

        print(output, file=sys.stderr, flush=True)
        if not retryable or attempt == MAX_ATTEMPTS:
            return status or 1

        delay = RETRY_DELAYS_SECONDS[attempt - 1]
        if time.monotonic() - started + delay >= TOTAL_BUDGET_SECONDS:
            print(f"{tool}: no time remains for a verified retry", file=sys.stderr)
            return 124
        print(f"{tool}: public download transport failed; retrying in {delay}s", flush=True)
        time.sleep(delay)

    raise AssertionError("the installation loop must return")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: install_go_ci_tool.py <dockerfmt|ghokin> <log-directory>", file=sys.stderr)
        raise SystemExit(2)
    raise SystemExit(install(sys.argv[1], Path(sys.argv[2])))
