"""Backing up and restoring: a backup writes its archive to a local file and reports its size and
digest, a dry-run restore plans the archive and changes nothing, a restore recreates the domain
under another name, and a backup whose wait ended is recovered by executing it again."""

from __future__ import annotations

import pytest

import nervix_client as nx
from support import Cluster, completed, deadline, execute


def test_a_backup_writes_its_archive_and_a_restore_reads_it(
    cluster: Cluster, session: nx.Session, unique: str
) -> None:
    archive = cluster.backups / f"domain-{unique}.nvxb"
    copy = f"{cluster.domain}_copy_{unique}"
    domain = cluster.domain
    # ANCHOR: backup
    backed_up = session.execute(session.prepare(f"BACKUP DOMAIN {domain} TO '{archive}';"))
    assert backed_up.disposition == nx.Disposition.COMPLETED
    summary = backed_up.backup
    assert summary is not None
    assert summary.total_bytes == archive.stat().st_size
    # The BLAKE3 digest the client checked every downloaded byte against.
    assert len(summary.digest) == 32
    # ANCHOR_END: backup
    # ANCHOR: restore
    planned = session.execute(
        session.prepare(f"RESTORE DOMAIN {domain} AS {copy} FROM '{archive}' DRY RUN;")
    )
    report = planned.restore
    assert report is not None and report.dry_run and not report.failed
    print(report.domains, report.models, report.resource_versions)
    restored = session.execute(session.prepare(f"RESTORE DOMAIN {domain} AS {copy} FROM '{archive}';"))
    report = restored.restore
    assert report is not None and not report.dry_run and not report.failed
    # ANCHOR_END: restore
    assert report.domains == 1
    assert report.models >= 1
    assert report.resource_versions == 0
    assert copy in execute(session, "LIST DOMAINS;").message


def test_a_backup_whose_wait_ended_is_recovered_by_executing_it_again(
    cluster: Cluster, session: nx.Session, unique: str
) -> None:
    archive = cluster.backups / f"recovered-{unique}.nvxb"
    execution = session.prepare(f"BACKUP DOMAIN {cluster.domain} TO '{archive}';")
    cancel = nx.Cancel()
    cancel.trigger()
    with pytest.raises(nx.CancelledError) as cancelled:
        session.execute(execution, cancel=cancel)
    assert cancelled.value.execution_reference == execution.reference
    recovered = session.execute(execution, cancel=deadline())
    assert recovered.disposition == nx.Disposition.COMPLETED
    summary = recovered.backup
    assert summary is not None and summary.total_bytes == archive.stat().st_size
    completed(session, "LIST DOMAINS;")
