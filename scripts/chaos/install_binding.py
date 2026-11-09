"""Install one prepared wheel offline into the external chaos client's private interpreter."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import venv


def install(wheels, destination, revision):
    if re.fullmatch(r"[a-f0-9]{40}(?:-dirty)?", revision) is None:
        raise ValueError("the client image needs its exact source revision")
    archives = list(Path(wheels).glob("nervix_client-*.whl"))
    if len(archives) != 1:
        raise ValueError("the client image needs exactly one prepared Nervix wheel")
    archive = archives[0]
    destination = Path(destination)
    venv.EnvBuilder(with_pip=True).create(destination)
    subprocess.run([str(destination / "bin/python"), "-m", "pip", "install", "--no-index",
                    "--no-deps", str(archive)], check=True)
    subprocess.run([str(destination / "bin/python"), "-c", "import nervix_client"], check=True)
    with archive.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    provenance = {"revision": revision, "wheel": archive.name, "wheel_sha256": digest}
    (destination / "provenance.json").write_text(json.dumps(provenance) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheels", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--revision", required=True)
    args = parser.parse_args()
    install(args.wheels, args.destination, args.revision)
