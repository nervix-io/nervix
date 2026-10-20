"""Name analysis compilations the way the typed ratchet's runner and workspace wrapper share."""

from __future__ import annotations

LIBRARY_KINDS = {"lib", "rlib", "cdylib", "staticlib", "proc-macro"}


def target_kind(kinds: list[str]) -> str:
    """Cargo's target kinds as the analysis names them: every library form is one `lib`."""
    if set(kinds) & LIBRARY_KINDS:
        return "lib"
    return kinds[0]
