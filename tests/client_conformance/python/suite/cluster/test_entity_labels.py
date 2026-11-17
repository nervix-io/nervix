"""Typed, exact domain metadata and independently retained revision views."""

from __future__ import annotations

import gc

import nervix_client as nx
from support import Cluster, completed, execute


def test_entity_labels_keep_exact_values_revisions_and_staged_views(
    cluster: Cluster, session: nx.Session, unique: str
) -> None:
    name = f"labelled_{unique}"
    completed(session, f"CREATE SCHEMA {name} ( value U32 ) LABELS {{ 'empty' = '', 'owner' = ' Δ ' }};")
    # ANCHOR: entity_labels
    outcome = execute(session, f"SHOW LABELS ON SCHEMA {name} FORMAT TEXT;")
    labels = outcome.entity_labels
    assert isinstance(labels, nx.EntityLabelsInspection)
    assert (labels.domain, labels.entity_kind, labels.entity_name) == (cluster.domain, "SCHEMA", name)
    assert labels.labels == {"empty": "", "owner": " Δ "}
    assert labels.view == nx.EntityLabelViewKind.COMMITTED
    revision = labels.revision
    assert revision is not None and revision >= 0
    assert labels.transaction_id is None and labels.position is None and labels.planning_basis is None
    assert labels.policy_revision > 0
    assert (labels.max_count, labels.max_key_bytes, labels.max_value_bytes, labels.max_bytes) == (64, 128, 1024, 16384)
    snapshot = labels.labels
    snapshot["owner"] = "only the returned dictionary changed"
    assert labels.labels["owner"] == " Δ "
    completed(session, "BEGIN;")
    completed(session, f"SET LABELS ON SCHEMA {name} {{ 'owner' = 'Platform', 'team/path' = 'east' }};")
    staged = execute(session, f"SHOW LABELS ON SCHEMA {name};").entity_labels
    assert isinstance(staged, nx.EntityLabelsInspection)
    assert staged.view == nx.EntityLabelViewKind.STAGED and staged.revision is None
    assert staged.transaction_id and staged.position == 1
    basis = staged.planning_basis
    assert basis is not None and len(basis) == 32
    assert staged.labels == {"empty": "", "owner": "Platform", "team/path": "east"}
    completed(session, "REVERT;")
    committed = execute(session, f"SHOW LABELS ON SCHEMA {name} FORMAT JSON;").entity_labels
    assert isinstance(committed, nx.EntityLabelsInspection)
    assert committed.labels == {"empty": "", "owner": " Δ "}
    assert committed.view == nx.EntityLabelViewKind.COMMITTED
    assert committed.revision is not None and committed.revision >= revision
    del outcome, committed
    gc.collect()
    assert labels.labels == {"empty": "", "owner": " Δ "}
    assert staged.labels == {"empty": "", "owner": "Platform", "team/path": "east"}
    assert staged.transaction_id and staged.position == 1
    # ANCHOR_END: entity_labels
