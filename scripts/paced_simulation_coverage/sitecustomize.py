"""Start coverage in the published Python driver processes spawned by Cucumber."""

import os
import sys

if sys.argv[0].endswith("paced_simulation.py") and os.environ.get("COVERAGE_PROCESS_START"):
    import coverage

    coverage.process_startup()
