#!/usr/bin/env python3
"""Compile a workspace crate with the native coverage collector's instrumentation flags.

Cargo runs this as its workspace compiler wrapper, beneath the configured kache, for a producer
that instruments only workspace crates, so dependencies are compiled without instrumentation. It
adds the collector's flags to the compilation of a named crate and leaves Cargo's queries of the
compiler, which name the placeholder crate `___` or none, as they are.
"""

import os
import sys

FLAGS_VARIABLE = "NERVIX_NATIVE_COVERAGE_WORKSPACE_RUSTFLAGS"
FLAG_SEPARATOR = "\x1f"
PLACEHOLDER_CRATE = "___"


def compiled_crate(arguments: list[str]) -> str | None:
    """The crate a compiler invocation compiles, or None for a query of the compiler."""

    for index, argument in enumerate(arguments):
        if argument == "--crate-name" and index + 1 < len(arguments):
            name = arguments[index + 1]
            if name == PLACEHOLDER_CRATE:
                return None
            return name
        if argument.startswith("--crate-name="):
            name = argument.removeprefix("--crate-name=")
            if name == PLACEHOLDER_CRATE:
                return None
            return name
    return None


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit("coverage_workspace_wrapper.py: Cargo passes the compiler as the first argument")
    compiler = sys.argv[1]
    arguments = sys.argv[2:]
    if compiled_crate(arguments) is not None:
        encoded = os.environ.get(FLAGS_VARIABLE)
        if encoded is None:
            sys.exit(f"coverage_workspace_wrapper.py: {FLAGS_VARIABLE} is not set")
        flags = [flag for flag in encoded.split(FLAG_SEPARATOR) if flag]
        arguments = [*arguments, *flags]
    os.execv(compiler, [compiler, *arguments])


if __name__ == "__main__":
    main()
