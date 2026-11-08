#!/usr/bin/env bash
set -euo pipefail

# The JVM host suite uses JUnit 5 without a Maven or Gradle build. Always verify the pinned jar,
# including a cached copy, before any compiler or runner reads it.
artifacts="${1:?expected the Java artifacts directory}"
version="1.13.4"
digest="3fdfc37e29744a9a67dd5365e81467e26fbde0b7aa204e6f8bbe79eeaa7ae892"
name="junit-platform-console-standalone-${version}.jar"
jar="${artifacts}/junit-platform-console-standalone.jar"
mkdir -p "${artifacts}"
verify() {
    python3 - "${digest}" "$1" <<'PY'
import hashlib
import pathlib
import sys
path = pathlib.Path(sys.argv[2])
with path.open("rb") as source:
    actual = hashlib.file_digest(source, "sha256").hexdigest()
if actual != sys.argv[1]:
    sys.exit(f"JUnit SHA-256 mismatch: {path}")
PY
}
if [[ ! -f "${jar}" ]]; then
    pending="$(mktemp "${artifacts}/.junit.XXXXXXXX")"
    trap 'rm -f "${pending}"' EXIT
    curl --fail --location --proto '=https' --tlsv1.2 --retry 5 --retry-all-errors \
        --silent --show-error \
        "https://repo.maven.apache.org/maven2/org/junit/platform/junit-platform-console-standalone/${version}/${name}" \
        --output "${pending}"
    verify "${pending}"
    mv "${pending}" "${jar}"
fi
verify "${jar}"
printf '%s\n' "${jar}"
