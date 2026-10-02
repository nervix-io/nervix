#!/usr/bin/env bash
# Keep native CI linker selection independent of flags added by test recipes and tooling.
set -euo pipefail

exec clang-23 --ld-path=wild "$@"
