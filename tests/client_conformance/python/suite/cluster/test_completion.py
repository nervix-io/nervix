"""Completing a statement: the cluster suggests what may follow the cursor, with the edit of the
input that applies each candidate, one page at a time."""

from __future__ import annotations

import nervix_client as nx


def test_a_statement_is_completed_by_the_cluster(session: nx.Session) -> None:
    # ANCHOR: suggest
    text = "CREATE SCH"
    page = session.suggest(text, len(text), 20)
    assert page.status == nx.CompletionStatus.READY
    schema = next(item for item in page.suggestions if item.value == "SCHEMA")
    assert schema.kind == nx.CompletionKind.TEXT
    start, end = schema.span
    completed_text = text[:start] + schema.replacement + text[end:]
    # ANCHOR_END: suggest
    assert end == len(text)
    assert completed_text.startswith("CREATE SCHEMA")


def test_the_cluster_suggests_the_names_it_holds(session: nx.Session) -> None:
    text = "SHOW CREATE RELAY "
    page = session.suggest(text, len(text), 50)
    assert page.status == nx.CompletionStatus.READY
    values = {suggestion.value for suggestion in page.suggestions}
    assert {"typed_rows", "watched_rows"} <= values, values


def test_a_page_continues_with_its_token_until_the_last_page(session: nx.Session) -> None:
    text = "CREATE "
    # ANCHOR: suggest_pages
    candidates: list[str] = []
    continuation: str | None = None
    while True:
        page = session.suggest(text, len(text), 2, continuation)
        candidates.extend(suggestion.value for suggestion in page.suggestions)
        continuation = page.continuation
        if continuation is None:
            break
    # ANCHOR_END: suggest_pages
    assert len(candidates) > 2
    assert len(set(candidates)) == len(candidates)
