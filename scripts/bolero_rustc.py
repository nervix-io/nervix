#!/usr/bin/env python3
"""Preserve the source parser's production span contract in sanitizer builds.

Cargo-bolero sets `cfg(fuzzing)` on dependencies too. Proc-macro2 uses that cfg to discard
source locations even when span-locations is enabled. Select its ordinary location behavior
while retaining every sanitizer, coverage and libFuzzer flag. Cargo still invokes this compiler
adapter through the configured kache wrapper.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Sequence


def compiler_arguments(arguments: Sequence[str]) -> list[str]:
    pairs = set(zip(arguments, arguments[1:]))
    location_parser = ("--crate-name", "proc_macro2") in pairs and (
        ("--cfg", 'feature="span-locations"') in pairs
        or '--cfg=feature="span-locations"' in arguments
    )
    if not location_parser:
        return list(arguments)
    result = []
    iterator = iter(arguments)
    for argument in iterator:
        if argument == "--cfg":
            value = next(iterator, None)
            if value is None:
                result.append(argument)
            elif value != "fuzzing":
                result.extend((argument, value))
        elif argument != "--cfg=fuzzing":
            result.append(argument)
    return result


def main() -> None:
    compiler = os.environ["NERVIX_BOLERO_RUSTC"]
    os.execv(compiler, [compiler, *compiler_arguments(sys.argv[1:])])


if __name__ == "__main__":
    main()
