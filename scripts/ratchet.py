#!/usr/bin/env python3

"""Count the architecture debt the layered rework retires, and hold it down.

Every count in `debt-baseline.json` is a ratchet: a change may lower it and never raise it. The
script prints each count next to its baseline and exits non-zero when any count is above it,
naming the offending counts. A count that has fallen is reported too, with the reminder that
`--update` rewrites the baseline in the same change.

Semantic rules are compiler-resolved through the pinned tooling workspace. The six disjoint
counts cover physical file length, Rust cast expressions, testing-gated struct fields,
functions above a cyclomatic complexity, hidden numeric policy and cryptography outside AWS-LC.
A stable Rust syntax parser visits authored casts, fields and cryptography paths, including
inactive configurations, while skipping test-only items. It parses the original source so Unicode
token locations remain accurate; comments and embedded fixture text never become syntax findings.
The pinned cargo-crap analyzer measures complexity over the same discovered files. Cargo's
resolved graph of the product supplies the cryptography the product links.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

from scripts import cargo_crap
from scripts.rust_source import (
    TEST_DIRECTORIES, RustFile, Site, load_files, product_files, repository_root,
)

BASELINE_FILE = "debt-baseline.json"

LARGE_FILE_LINES = 3000

COMPLEXITY_COUNT = "functions_over_complexity_30"
COMPLEXITY_LIMIT = 30

# The count of cryptography outside AWS-LC. Its authored sites come from the source parser and its
# graph sites from the product's dependency graph; docs/src/cryptography.md owns the rule.
CRYPTOGRAPHY_COUNT = "non_aws_lc_crypto"

# The package the release image builds, and the targets it builds it for (Dockerfile.debian).
PRODUCT_PACKAGE = "nervix-server"
PRODUCT_TARGETS = ("x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu")
LOCKFILE = "Cargo.lock"

# Crates that implement cryptography other than AWS-LC. Each dependency edge onto one of them in
# the product's normal and build graph is one site, so a further dependent of a crate the product
# already links still counts. Crates that only define traits or encodings (`digest`, `cipher`,
# `signature`, `der`, `pkcs8`), entropy interfaces (`getrandom`, `rand_core`) and
# non-cryptographic checksums (`crc`, `xxhash-rust`) implement no algorithm and are not listed.
NON_AWS_LC_CRYPTOGRAPHY_CRATES = frozenset({
    # Digests.
    "blake2", "blake3", "md-5", "md4", "md5", "ripemd", "sha1", "sha1_smol", "sha2", "sha3", "sm3",
    # Message authentication, key derivation and password hashing.
    "argon2", "bcrypt", "cmac", "hkdf", "hmac", "pbkdf2", "scrypt",
    # Ciphers, their modes and authenticated encryption.
    "aes", "aes-gcm", "aes-gcm-siv", "aes-siv", "blowfish", "cbc", "ccm", "cfb-mode", "chacha20",
    "chacha20poly1305", "ctr", "des", "eax", "ghash", "ocb3", "ofb", "poly1305", "polyval", "rc2",
    "rc4", "salsa20", "xsalsa20poly1305",
    # Public-key algorithms.
    "curve25519-dalek", "dsa", "ecdsa", "ed25519-dalek", "k256", "p256", "p384", "p521", "rsa",
    "x25519-dalek",
    # Random generators.
    "rand_chacha",
    # Complete cryptography libraries.
    "boring", "boring-sys", "libsodium-sys", "mbedtls", "openssl", "openssl-sys", "ring",
    "wolfssl",
})

def count_files_over_3000_lines(files: Sequence[RustFile]) -> list[Site]:
    return [
        Site(file.path, 1, f"{file.lines} lines")
        for file in files
        if file.lines > LARGE_FILE_LINES
    ]


def count_functions_over_complexity_30(root: Path, files: Sequence[RustFile], target: Path) -> list[Site]:
    """Functions the pinned analyzer scores above the limit, over exactly the discovered files.

    The analyzer reads the same eligible files as file size, so in-crate test files count. It
    reports no `#[test]` function and nothing inside a `#[cfg(test)]` module, and it cannot see a
    function inside a macro, a trait's provided method or a function nested in another's body.
    """

    unreachable = cargo_crap.unreachable_sources(root, [file.path for file in files])
    if unreachable:
        raise SystemExit(
            "error: the complexity analyzer cannot read these discovered Rust files, so the count "
            "would miss their functions:\n" + "\n".join(unreachable)
        )
    excludes = [f"**/{directory}/**" for directory in sorted(TEST_DIRECTORIES)]
    try:
        functions = cargo_crap.analyze(root, target, excludes)
    except cargo_crap.AnalyzerError as error:
        raise SystemExit(f"error: complexity analysis failed: {error}") from error
    discovered = {file.path for file in files}
    outside = sorted({function.path for function in functions} - discovered)
    if outside:
        raise SystemExit(
            "error: the complexity analysis measured Rust files that source discovery does not "
            "govern:\n" + "\n".join(outside)
        )
    sites = []
    for function in functions:
        if function.complexity > COMPLEXITY_LIMIT:
            detail = f"{function.name} has cyclomatic complexity {function.complexity:g}"
            sites.append(Site(function.path, function.line, detail))
    return sorted(sites, key=lambda site: (site.path, site.line))


@dataclass(frozen=True, order=True)
class Package:
    """One package of Cargo's resolved graph, named as Cargo.lock names it."""

    name: str
    version: str

    def render(self) -> str:
        return f"{self.name} {self.version}"


@dataclass(frozen=True)
class Edge:
    """A dependency one package of the product's graph declares on another."""

    dependent: Package
    dependency: Package


# One line of `cargo tree --prefix depth --format {p}`: the depth, the package name, its version,
# then any notes such as a source, `(proc-macro)` or the `(*)` of a repeated package.
TREE_LINE = re.compile(r"^(?P<depth>\d+)(?P<name>\S+) v(?P<version>\S+)(?: .*)?$")
LOCK_FIELD = re.compile(r'^(?P<key>name|version) = "(?P<value>[^"]*)"$')


def read_product_tree(root: Path) -> str:
    """Cargo's resolved normal and build graph of the product for every release target.

    `cargo tree` resolves features for the product alone, as the release build does, where
    `cargo metadata` unifies the features every workspace member enables.
    """

    command = [
        "cargo", "tree", "--locked",
        "--manifest-path", str(root / "Cargo.toml"),
        "--package", PRODUCT_PACKAGE,
        "--edges", "normal,build",
        "--prefix", "depth",
        "--format", "{p}",
    ]
    for target in PRODUCT_TARGETS:
        command.extend(["--target", target])
    completed = subprocess.run(command, capture_output=True, text=True)
    if completed.returncode:
        raise SystemExit("error: cannot read the product dependency graph\n" + completed.stderr)
    return completed.stdout


def dependency_edges(tree: str) -> set[Edge]:
    """Every edge of the depth-prefixed product trees, one tree for each release target.

    A repeated package, marked `(*)`, lists its own dependencies once, under its first
    occurrence, and each occurrence still records the edge onto it.
    """

    edges: set[Edge] = set()
    ancestors: list[Package] = []
    trees = 0
    for line in tree.splitlines():
        if not line.strip():
            continue
        match = TREE_LINE.match(line)
        if match is None:
            raise SystemExit(f"error: unrecognized product dependency graph line: {line!r}")
        depth = int(match.group("depth"))
        package = Package(match.group("name"), match.group("version"))
        if depth == 0:
            if package.name != PRODUCT_PACKAGE:
                raise SystemExit(f"error: a product dependency tree starts at {package.render()}, not {PRODUCT_PACKAGE}")
            trees += 1
        elif depth > len(ancestors):
            raise SystemExit(f"error: product dependency graph line skips a level: {line!r}")
        del ancestors[depth:]
        if depth:
            edges.add(Edge(ancestors[depth - 1], package))
        ancestors.append(package)
    if trees != len(PRODUCT_TARGETS):
        raise SystemExit(
            f"error: the product dependency graph has {trees} trees, one for each of the "
            f"{len(PRODUCT_TARGETS)} release targets was expected"
        )
    return edges


def lockfile_entries(lockfile: str) -> dict[Package, int]:
    """The line of each package's `[[package]]` entry in Cargo.lock."""

    entries: dict[Package, int] = {}
    start: int | None = None
    name: str | None = None
    for number, line in enumerate(lockfile.splitlines(), start=1):
        if line == "[[package]]":
            start = number
            name = None
            continue
        field = LOCK_FIELD.match(line)
        if start is None or field is None:
            continue
        if field.group("key") == "name":
            name = field.group("value")
        elif name is not None:
            entries.setdefault(Package(name, field.group("value")), start)
    return entries


def cryptography_sites(tree: str, lockfile: str) -> list[Site]:
    """One site for each edge of the product graph onto a crate implementing other cryptography.

    A site points at the dependent's Cargo.lock entry, which records the dependency.
    """

    entries = lockfile_entries(lockfile)
    edges = sorted(dependency_edges(tree), key=lambda edge: (edge.dependency, edge.dependent))
    sites = []
    for edge in edges:
        if edge.dependency.name not in NON_AWS_LC_CRYPTOGRAPHY_CRATES:
            continue
        line = entries.get(edge.dependent)
        if line is None:
            raise SystemExit(
                f"error: {LOCKFILE} has no entry for {edge.dependent.render()}, which the product "
                "dependency graph names"
            )
        detail = f"{CRYPTOGRAPHY_COUNT} (graph): {edge.dependent.render()} -> {edge.dependency.render()}"
        sites.append(Site(LOCKFILE, line, detail))
    return sites


def product_graph_sites(root: Path) -> list[Site]:
    lockfile = (root / LOCKFILE).read_text(encoding="utf-8")
    return cryptography_sites(read_product_tree(root), lockfile)


@dataclass(frozen=True)
class Count:
    name: str
    description: str
    collect: Callable[[Sequence[RustFile]], list[Site]] | None


COUNTS: tuple[Count, ...] = (
    Count(
        "files_over_3000_lines",
        f"Rust files longer than {LARGE_FILE_LINES} lines",
        count_files_over_3000_lines,
    ),
    Count("as_casts", "Rust cast expressions", None),
    Count("hidden_tunables", "numeric policy outside class-typed settings or fixed contracts", None),
    Count(
        "testing_feature_struct_fields",
        'struct fields gated on `feature = "testing"`',
        None,
    ),
    Count(
        COMPLEXITY_COUNT,
        f"functions whose cyclomatic complexity exceeds {COMPLEXITY_LIMIT}",
        None,
    ),
    Count(
        CRYPTOGRAPHY_COUNT,
        "authored cryptography and product dependencies outside AWS-LC",
        None,
    ),
)


def measure_sources(root: Path) -> dict[str, list[Site]]:
    files = load_files(root)
    sites = {count.name: count.collect(files) if count.collect is not None else [] for count in COUNTS}
    target = Path(os.environ.get("CARGO_TARGET_DIR", Path(__file__).resolve().parents[1] / "target"))
    parser = target / "typed-ratchet/source/debug/nervix-lint-report"
    if not parser.is_file():
        raise SystemExit("error: missing Rust source parser; run just typed-ratchet-source-build")
    sources = [{"path": file.path, "source": file.source} for file in product_files(files)]
    completed = subprocess.run([str(parser), "--source"], input=json.dumps(sources), text=True, capture_output=True)
    if completed.returncode:
        raise SystemExit("error: Rust source analysis failed\n" + completed.stderr)
    by_path = {file.path: file for file in files}
    for site in json.loads(completed.stdout):
        detail = site["rule"]
        if site["rule"] == CRYPTOGRAPHY_COUNT:
            # The authored line names the crate or item, which the rule name alone does not.
            text = by_path[site["path"]].source.splitlines()[site["line"] - 1]
            detail = f"{CRYPTOGRAPHY_COUNT} (authored): {' '.join(text.split())}"
        sites[site["rule"]].append(Site(site["path"], site["line"], detail))
    sites[COMPLEXITY_COUNT] = count_functions_over_complexity_30(root, files, target)
    sites[CRYPTOGRAPHY_COUNT].extend(product_graph_sites(root))
    return sites


def check_synchronization(root: Path) -> None:
    from scripts import typed_ratchet

    target = Path(os.environ.get("CARGO_TARGET_DIR", root / "target"))
    destination = target / "typed-ratchet/gate.json"
    if typed_ratchet.main(["--root", str(root), "--target-dir", str(target), "--output", str(destination)]):
        raise SystemExit("error: compiler synchronization diagnostics failed")


def measure(root: Path) -> dict[str, list[Site]]:
    from scripts.check_layer_provenance import check

    provenance = check(root)
    if provenance:
        raise SystemExit("error: language import provenance failed\n" + "\n".join(provenance))
    check_synchronization(root)
    return measure_sources(root)


def read_baseline(path: Path) -> dict[str, int]:
    if not path.exists():
        raise SystemExit(
            f"error: no baseline at {path}. Run `just ratchet --update` to write one."
        )
    baseline = json.loads(path.read_text(encoding="utf-8"))
    expected = {count.name for count in COUNTS}
    missing = sorted(expected - baseline.keys())
    unknown = sorted(baseline.keys() - expected)
    if missing or unknown:
        raise SystemExit(
            f"error: {path.name} does not match the counts this script measures"
            + (f"; missing {', '.join(missing)}" if missing else "")
            + (f"; unknown {', '.join(unknown)}" if unknown else "")
            + ". Run `just ratchet --update` to rewrite it."
        )
    return baseline


def write_baseline(path: Path, counts: dict[str, int]) -> None:
    body = json.dumps({name: counts[name] for name in sorted(counts)}, indent=2)
    path.write_text(f"{body}\n", encoding="utf-8")


def report(counts: dict[str, int], baseline: dict[str, int]) -> None:
    width = max(len(count.name) for count in COUNTS)
    print("Debt ratchet — every count may fall and none may rise.\n")
    for count in COUNTS:
        current, before = counts[count.name], baseline[count.name]
        change = current - before
        marker = "  " if change == 0 else ("^^" if change > 0 else "vv")
        print(
            f"  {marker} {count.name:<{width}}  {current:>6}"
            f"  baseline {before:>6}  {change:+6}   {count.description}"
        )
    print()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Count the architecture debt named in AGENTS.md and fail when any count is above "
            "its checked-in baseline."
        )
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="rewrite the baseline from the current counts, for the change that lowered them",
    )
    parser.add_argument(
        "--show",
        metavar="COUNT",
        help="list the sites behind one count instead of comparing against the baseline",
    )
    parser.add_argument(
        "--root",
        type=Path,
        help="repository to measure; defaults to the repository holding this script",
    )
    arguments = parser.parse_args(argv)

    root = arguments.root if arguments.root is not None else repository_root()
    baseline_path = root / BASELINE_FILE
    sites = measure(root)

    if arguments.show is not None:
        if arguments.show not in sites:
            known = ", ".join(count.name for count in COUNTS)
            print(
                f"error: unknown count {arguments.show!r}. Known counts: {known}",
                file=sys.stderr,
            )
            return 2
        try:
            for site in sites[arguments.show]:
                print(site.render())
        except BrokenPipeError:
            # A listing is read through `head` and `grep`; closing the pipe early is not an error.
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        return 0

    counts = {name: len(found) for name, found in sites.items()}

    baseline = read_baseline(baseline_path)
    report(counts, baseline)

    risen = [count.name for count in COUNTS if counts[count.name] > baseline[count.name]]
    fallen = [count.name for count in COUNTS if counts[count.name] < baseline[count.name]]

    if fallen:
        print("These counts fell. Run `just ratchet --update` and commit the baseline with them:")
        for name in fallen:
            print(f"  {name}: {baseline[name]} -> {counts[name]}")
        print()

    if risen:
        # The whole run is one report on stdout, because a CI log reads stdout and stderr as
        # independent pipes and would otherwise print this above the table it refers to. The exit
        # code, not the stream, is what says the run failed.
        print("error: debt rose. These counts are above their baseline:")
        for name in risen:
            print(
                f"  {name}: {baseline[name]} -> {counts[name]}"
                f" (+{counts[name] - baseline[name]});"
                f" `just ratchet --show {name}` lists the sites"
            )
        # The baseline holds counts, not sites, so the gate names every site of a risen count and
        # the one a change added is among them.
        for name in risen:
            print(f"\n{name} sites:")
            for site in sites[name]:
                print(f"  {site.render()}")
        return 1

    if arguments.update:
        write_baseline(baseline_path, counts)
        print(f"Wrote {baseline_path.name} from the current counts.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
