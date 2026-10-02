#!/usr/bin/env python3
"""Forward Cargo's workspace compiler to the driver, recording native coverage when selected."""

import os
import pathlib
import sys

driver = os.environ["NERVIX_LINT_DRIVER"]
arguments = [driver, *sys.argv[1:]]
if os.environ.get("NERVIX_NATIVE_COVERAGE_ATTEMPT"):
    recorder = pathlib.Path(__file__).with_name("native_coverage.py")
    os.execv(sys.executable, [sys.executable, str(recorder), "exec", *arguments])
os.execv(driver, arguments)
