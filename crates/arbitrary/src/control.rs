//! What an administrative command records about itself: the restore it applies and the steps that
//! restore applies, the report a restore answers with, the archive a backup assembles, and the
//! admission and lifecycle a transaction command reports.
//!
//! A restore applies its steps in their fixed order: the users of a cluster restore, then each
//! domain's creation, resource versions and models, under the name the domain is restored under.
//! Its report records what became of every step the way a run leaves them: a dry run plans each,
//! and a restore that applies applies them in order, completely or until one fails, after which
//! none is attempted. A backup of one domain holds that domain and no users, and a cluster backup
//! holds its users beside its domains in name order.

use std::num::NonZeroUsize;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ArchiveDigest, BackupArchiveSummary, BackupCut, BackupDomainSummary, BackupQuiesceCounters,
    BackupResources, DomainName, DomainStatus, Restore, RestoreArchive, RestoreMode, RestoreReport,
    RestoreScope, RestoreStep, RestoreStepOutcome, RestoreStepReport, RestoredDomain,
    RestoredUsers, Statement, TransactionLifecycle, TransactionOperationAdmission,
    TransactionOperationNumber, TransactionOperationRange, TransactionPosition,
    TransactionPreviewIdentity,
};

use crate::{Arbitrary, StatementVariant};

/// The most domains a generated cluster restore or cluster backup holds.
const DOMAINS: usize = 3;

impl Arbitrary<'_> {
    /// A `RESTORE` of any scope, source, runtime state and lifecycle that `mode` applies or plans.
    pub fn restore_of(&mut self, mode: RestoreMode) -> Restore {
        let statement = self.statement_of(StatementVariant::Restore);
        let restore = match statement {
            Statement::Restore(restore) => Some(restore),
            _ => None,
        };
        let restore = restore.assured("statement_of builds the statement form it is asked for");
        Restore { mode, ..restore }
    }

    /// The BLAKE3 digest of an archive file. A digest is accepted for its archive whatever its
    /// bytes.
    pub fn archive_digest(&mut self) -> ArchiveDigest {
        ArchiveDigest::from_bytes(self.digest())
    }

    /// The archive a restore reads, as its client declared it: any positive size and any digest.
    pub fn restore_archive(&mut self) -> RestoreArchive {
        let total_bytes = self.positive_u64();
        let digest = self.archive_digest();
        RestoreArchive {
            total_bytes,
            digest,
        }
    }

    /// The domains a restore of `scope` recreates, as the archive names them: up to three distinct
    /// domains of a cluster archive, or the one domain a domain restore names.
    fn archived_domains(&mut self, scope: &RestoreScope) -> Vec<DomainName> {
        match scope {
            RestoreScope::Cluster { .. } => self.distinct_names(0, DOMAINS),
            RestoreScope::Domain { domain, .. } => vec![domain.clone()],
        }
    }

    /// Every step restoring the domains `archived` names under `scope`, in the order a restore
    /// applies them.
    fn steps_restoring(scope: &RestoreScope, archived: &[DomainName]) -> Vec<RestoreStep> {
        let mut steps = Vec::new();
        if let RestoreScope::Cluster { .. } = scope {
            steps.push(RestoreStep::Users);
        }
        for source in archived {
            let domain = scope.target_of(source);
            steps.push(RestoreStep::CreateDomain(domain.clone()));
            steps.push(RestoreStep::ImportResources(domain.clone()));
            steps.push(RestoreStep::ApplyModels(domain.clone()));
        }
        steps
    }

    /// Every step a restore of `scope` applies, in the order it applies them, for the domains it
    /// recreates.
    pub fn restore_steps(&mut self, scope: &RestoreScope) -> Vec<RestoreStep> {
        let archived = self.archived_domains(scope);
        Self::steps_restoring(scope, &archived)
    }

    /// Any one step of a restore, of any domain.
    pub fn restore_step(&mut self) -> RestoreStep {
        match self.entropy.byte() % 4 {
            0 => RestoreStep::Users,
            1 => RestoreStep::CreateDomain(self.rule_name()),
            2 => RestoreStep::ImportResources(self.rule_name()),
            _ => RestoreStep::ApplyModels(self.rule_name()),
        }
    }

    /// What a cluster restore did with the archived users: any count created, skipped and
    /// replaced.
    pub fn restored_users(&mut self) -> RestoredUsers {
        let created = self.entropy.any_u64();
        let skipped = self.entropy.any_u64();
        let replaced = self.entropy.any_u64();
        RestoredUsers {
            created,
            skipped,
            replaced,
        }
    }

    /// The outcome of each of `planned`, in order, as a run of `mode` leaves them: a dry run plans
    /// every step, and a restore that applies applies them in order, either all of them or every
    /// step before one that failed, attempting none after it.
    fn step_reports(
        &mut self,
        planned: Vec<RestoreStep>,
        mode: RestoreMode,
    ) -> Vec<RestoreStepReport> {
        let steps =
            u64::try_from(planned.len()).assured("supported targets address at most 64 bits");
        let applied = self.entropy.boundary_biased(0..=steps);
        let applied = usize::try_from(applied).verified("the draw is at most the step count");
        let mut reports = Vec::with_capacity(planned.len());
        for (index, step) in planned.into_iter().enumerate() {
            let outcome = match mode {
                RestoreMode::DryRun => RestoreStepOutcome::Planned,
                RestoreMode::Apply if index < applied => RestoreStepOutcome::Applied,
                RestoreMode::Apply if index == applied => RestoreStepOutcome::Failed,
                RestoreMode::Apply => RestoreStepOutcome::NotAttempted,
            };
            reports.push(RestoreStepReport { step, outcome });
        }
        reports
    }

    /// One domain a restore of `mode` recreates from the archived domain `source` under the name
    /// `domain`, with any counts, lifecycle and start generation. Only a dry run carries the
    /// planner's report on the domain's models, and only when the domain holds models to plan.
    fn restored_domain(
        &mut self,
        source: DomainName,
        domain: DomainName,
        mode: RestoreMode,
    ) -> RestoredDomain {
        let resource_versions = self.entropy.any_u64();
        let models = self.entropy.any_u64();
        let status = self.entropy.pick([
            DomainStatus::Stopped,
            DomainStatus::Running,
            DomainStatus::Paused,
        ]);
        let start_version = self.entropy.any_u64();
        let plans_models = match mode {
            RestoreMode::Apply => false,
            RestoreMode::DryRun => self.entropy.flag(),
        };
        let planned_models = if plans_models {
            Some(self.admitted_impact_report(domain.clone()))
        } else {
            None
        };
        RestoredDomain {
            source,
            domain,
            resource_versions,
            models,
            status,
            start_version,
            planned_models,
        }
    }

    /// The report `restore` answers with after reading `archive`: the domains it recreates, and
    /// what became of each of its steps as a run of its mode leaves them. Only a cluster restore
    /// imports users, which a restore that applies reports once its users step applied.
    pub fn restore_report_of(
        &mut self,
        restore: &Restore,
        archive: RestoreArchive,
    ) -> RestoreReport {
        let archived = self.archived_domains(&restore.scope);
        let captured_at = self.timestamp();
        let mut domains = Vec::with_capacity(archived.len());
        for source in &archived {
            let domain = restore.scope.target_of(source).clone();
            domains.push(self.restored_domain(source.clone(), domain, restore.mode));
        }
        let planned = Self::steps_restoring(&restore.scope, &archived);
        let steps = self.step_reports(planned, restore.mode);
        let mut imported_users = false;
        for report in &steps {
            let reached = report.outcome == RestoreStepOutcome::Applied
                || report.outcome == RestoreStepOutcome::Planned;
            if report.step == RestoreStep::Users && reached {
                imported_users = true;
            }
        }
        let users = if imported_users {
            Some(self.restored_users())
        } else {
            None
        };
        RestoreReport {
            mode: restore.mode,
            archive,
            captured_at,
            users,
            domains,
            steps,
        }
    }

    /// What ingestion did during one freeze: any count of buffered, dropped and rejected records.
    fn backup_quiesce_counters(&mut self) -> BackupQuiesceCounters {
        let buffered_records = self.entropy.any_u64();
        let buffered_bytes = self.entropy.any_u64();
        let dropped_records = self.entropy.any_u64();
        let rejected_records = self.entropy.any_u64();
        BackupQuiesceCounters {
            buffered_records,
            buffered_bytes,
            dropped_records,
            rejected_records,
        }
    }

    /// The consistency boundary a domain's runtime state was captured at: quiesced between two
    /// instants with any freeze counters, live, stopped, or configuration only.
    fn backup_cut(&mut self) -> BackupCut {
        match self.entropy.byte() % 4 {
            0 => {
                let engaged_at = self.timestamp();
                let released_at = self.timestamp();
                let quiesce = self.backup_quiesce_counters();
                BackupCut::Quiesced {
                    engaged_at,
                    released_at,
                    quiesce,
                }
            }
            1 => BackupCut::Live,
            2 => BackupCut::Stopped,
            _ => BackupCut::ConfigurationOnly,
        }
    }

    /// One domain of a completed backup: the revision its configuration was read at, the cut its
    /// state was captured at, and any count of sections and their bytes.
    fn backup_domain_summary(&mut self, domain: DomainName) -> BackupDomainSummary {
        let revision = self.entropy.any_u64();
        let cut = self.backup_cut();
        let sections = self.entropy.any_u64();
        let section_bytes = self.entropy.any_u64();
        BackupDomainSummary {
            domain,
            revision,
            cut,
            sections,
            section_bytes,
        }
    }

    /// What a completed backup assembled: any size, digest and instants, its resources included or
    /// omitted, and the domains it holds in name order. A cluster backup holds up to three domains
    /// beside any count of users, and a domain backup holds its one domain and no users section.
    pub fn backup_archive_summary(&mut self) -> BackupArchiveSummary {
        let total_bytes = self.positive_u64();
        let digest = self.archive_digest();
        let captured_at = self.timestamp();
        let retained_until = self.timestamp();
        let resources = self
            .entropy
            .pick([BackupResources::Included, BackupResources::Omitted]);
        let cluster = self.entropy.flag();
        let mut names = if cluster {
            self.distinct_names::<DomainName>(0, DOMAINS)
        } else {
            vec![self.name::<DomainName>()]
        };
        names.sort();
        let users = if cluster {
            Some(self.entropy.any_u64())
        } else {
            None
        };
        let mut domains = Vec::with_capacity(names.len());
        for domain in names {
            domains.push(self.backup_domain_summary(domain));
        }
        BackupArchiveSummary {
            total_bytes,
            digest,
            captured_at,
            retained_until,
            resources,
            users,
            domains,
        }
    }

    /// An operation from the first through `last`, landing on either end as often as between them.
    fn operation_through(&mut self, last: NonZeroUsize) -> TransactionOperationNumber {
        let first = TransactionOperationNumber::new(NonZeroUsize::MIN);
        let last = TransactionOperationNumber::new(last);
        let operations = TransactionOperationRange::new(first, last)
            .assured("the first operation number is at or before every other");
        self.operation_within(operations)
    }

    /// Any operation number, landing on the first and on the last a `usize` holds as often as
    /// elsewhere.
    pub fn transaction_operation_number(&mut self) -> TransactionOperationNumber {
        self.operation_through(NonZeroUsize::MAX)
    }

    /// The admission of operation `operation` of the transaction `transaction_id`: the preview its
    /// append left, of every operation through it, with any planning basis.
    pub fn transaction_operation_admission_of(
        &mut self,
        transaction_id: String,
        operation: TransactionOperationNumber,
    ) -> TransactionOperationAdmission {
        let position = TransactionPosition::new(operation.get());
        let planning_basis = self.impact_planning_basis();
        TransactionOperationAdmission {
            operation,
            preview: TransactionPreviewIdentity {
                transaction_id,
                position,
                planning_basis,
            },
        }
    }

    /// The admission of any operation of any transaction, as
    /// [`Self::transaction_operation_admission_of`] builds it.
    pub fn transaction_operation_admission(&mut self) -> TransactionOperationAdmission {
        let transaction_id = self.transaction_id();
        let operation = self.transaction_operation_number();
        self.transaction_operation_admission_of(transaction_id, operation)
    }

    /// Where a transaction that accepted `accepted` operations is in its lifecycle: open,
    /// committing, committed, reverted or expired, or, once it accepted an operation, failed at
    /// one of them with any error.
    pub fn transaction_lifecycle_of(
        &mut self,
        accepted: TransactionPosition,
    ) -> TransactionLifecycle {
        let Some(last) = NonZeroUsize::new(accepted.accepted_operations()) else {
            return self.entropy.pick([
                TransactionLifecycle::Open,
                TransactionLifecycle::Committing,
                TransactionLifecycle::Committed,
                TransactionLifecycle::Reverted,
                TransactionLifecycle::Expired,
            ]);
        };
        match self.entropy.byte() % 6 {
            0 => TransactionLifecycle::Open,
            1 => TransactionLifecycle::Committing,
            2 => TransactionLifecycle::Committed,
            3 => TransactionLifecycle::Reverted,
            4 => TransactionLifecycle::Expired,
            _ => {
                let failing_operation = self.operation_through(last);
                let error = self.string();
                TransactionLifecycle::Failed {
                    failing_operation,
                    error,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use nervix_models::{
        RestoreMode, RestoreScope, RestoreStep, RestoreStepOutcome, TransactionLifecycle,
        TransactionPosition,
    };

    use crate::{Arbitrary, Domain};

    /// How many seeds each check draws its values from.
    const SEEDS: u64 = 256;

    /// How many bytes each seed's values are drawn from.
    const BYTES: usize = 4096;

    /// Bytes that differ from seed to seed. SplitMix64 spreads them, so the choices a value reads
    /// late vary between seeds as much as the first ones.
    fn seeded_bytes(seed: u64) -> Vec<u8> {
        let mut state = seed;
        let mut bytes = Vec::with_capacity(BYTES);
        while bytes.len() < BYTES {
            // SplitMix64 is defined over wrapping arithmetic: wrapping is the mixer's meaning.
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut mixed = state;
            mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            mixed ^= mixed >> 31;
            bytes.extend_from_slice(&mixed.to_le_bytes());
        }
        bytes
    }

    /// The steps a restore of `scope` applies for `domains`, each restored under its target name.
    fn fixed_order(
        scope: &RestoreScope,
        domains: &[nervix_models::RestoredDomain],
    ) -> Vec<RestoreStep> {
        let mut steps = Vec::new();
        if let RestoreScope::Cluster { .. } = scope {
            steps.push(RestoreStep::Users);
        }
        for domain in domains {
            steps.push(RestoreStep::CreateDomain(domain.domain.clone()));
            steps.push(RestoreStep::ImportResources(domain.domain.clone()));
            steps.push(RestoreStep::ApplyModels(domain.domain.clone()));
        }
        steps
    }

    #[test]
    fn restore_reports_record_every_step_in_order_as_a_run_leaves_it() {
        let mut seen = BTreeSet::new();
        for seed in 0..SEEDS {
            let bytes = seeded_bytes(seed);
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
            for mode in [RestoreMode::Apply, RestoreMode::DryRun] {
                let restore = arbitrary.restore_of(mode);
                assert_eq!(restore.mode, mode);
                let planned = arbitrary.restore_steps(&restore.scope);
                if let RestoreScope::Domain { .. } = restore.scope {
                    assert_eq!(planned.len(), 3, "{planned:?}");
                }
                let archive = arbitrary.restore_archive();
                let report = arbitrary.restore_report_of(&restore, archive);
                assert_eq!(report.mode, mode);
                assert_eq!(report.archive, archive);
                for domain in &report.domains {
                    assert_eq!(&domain.domain, restore.scope.target_of(&domain.source));
                    if domain.planned_models.is_some() {
                        assert_eq!(mode, RestoreMode::DryRun);
                        seen.insert("planned_models");
                    }
                }
                let mut steps = Vec::new();
                let mut outcomes = Vec::new();
                let mut reached_users = false;
                for step in &report.steps {
                    steps.push(step.step.clone());
                    outcomes.push(step.outcome);
                    let reached = step.outcome == RestoreStepOutcome::Applied
                        || step.outcome == RestoreStepOutcome::Planned;
                    if step.step == RestoreStep::Users && reached {
                        reached_users = true;
                    }
                }
                assert_eq!(steps, fixed_order(&restore.scope, &report.domains));
                assert_eq!(report.users.is_some(), reached_users);
                if reached_users {
                    seen.insert("users");
                }
                match mode {
                    RestoreMode::DryRun => {
                        for outcome in &outcomes {
                            assert_eq!(*outcome, RestoreStepOutcome::Planned);
                        }
                    }
                    RestoreMode::Apply => {
                        let mut remaining = outcomes.into_iter();
                        let mut after_applied = None;
                        for outcome in remaining.by_ref() {
                            if outcome != RestoreStepOutcome::Applied {
                                after_applied = Some(outcome);
                                break;
                            }
                        }
                        match after_applied {
                            None => {
                                seen.insert("applied");
                            }
                            Some(RestoreStepOutcome::Failed) => {
                                seen.insert("failed");
                                for outcome in remaining {
                                    assert_eq!(outcome, RestoreStepOutcome::NotAttempted);
                                }
                            }
                            Some(outcome) => panic!("{outcome:?} follows the applied steps"),
                        }
                    }
                }
            }
        }
        assert_eq!(
            seen,
            BTreeSet::from(["applied", "failed", "planned_models", "users"])
        );
    }

    #[test]
    fn a_backup_holds_its_domains_in_name_order_and_users_only_beside_a_cluster() {
        let mut seen = BTreeSet::new();
        for seed in 0..SEEDS {
            let bytes = seeded_bytes(seed);
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
            let summary = arbitrary.backup_archive_summary();
            assert!(
                summary
                    .domains
                    .is_sorted_by(|left, right| left.domain < right.domain)
            );
            match summary.users {
                Some(_) => {
                    seen.insert("cluster");
                }
                None => {
                    seen.insert("domain");
                    assert_eq!(summary.domains.len(), 1);
                }
            }
        }
        assert_eq!(seen, BTreeSet::from(["cluster", "domain"]));
    }

    #[test]
    fn a_transaction_fails_only_at_an_operation_it_accepted() {
        let mut failed = false;
        for seed in 0..SEEDS {
            let bytes = seeded_bytes(seed);
            let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
            let admission = arbitrary.transaction_operation_admission();
            assert_eq!(
                admission.preview.position.accepted_operations(),
                admission.operation.get()
            );
            for accepted in [0, 1, 3, usize::MAX] {
                let lifecycle =
                    arbitrary.transaction_lifecycle_of(TransactionPosition::new(accepted));
                if let TransactionLifecycle::Failed {
                    failing_operation, ..
                } = lifecycle
                {
                    failed = true;
                    assert!(failing_operation.get() <= accepted, "{failing_operation}");
                }
            }
        }
        assert!(failed, "no lifecycle failed at an accepted operation");
    }
}
