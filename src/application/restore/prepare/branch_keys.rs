//! Checking every archived branch key a restore installs against the branching its restored entity
//! declares.
//!
//! Layer: control plane.
//! - **Owns.** Resolving, from a restored schedule, the branch declarations an archived key of one
//!   entity may belong to, and admitting each archived lifecycle entry, descriptor and record
//!   identity key as the runtime's typed key only when it is a key of one of them, naming the
//!   archive section, the place in it and the field of the first key that is not.
//! - **Depends on.** The verified archive description, the vocabulary's schedule and resolved
//!   branch declarations, and the runtime's typed branch key.
//! - **Must not know.** Checkpoint encoding, staging, placement or publication.

use std::{
    collections::{BTreeMap, hash_map::Entry},
    fmt,
};

use ahash::HashMap;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_backup::{DescribedRuntimeState, DescribedSection, StateField};
use nervix_execution::{Cancellation, CpuClass, MemoryClass};
use nervix_models::{
    BranchName, CreateSchema, DomainName, DomainSchedule, Model, ModelKind, ModelName, NodeRef,
    ProcessorOutput, ResolvedBranching, ScheduledNode,
};
use nervix_primitives::sync::Arc;
use thiserror::Error;

use super::{RestoreRefusal, VerifiedArchive};
use crate::{
    application::restore::steps::remote_branch_key,
    runtime::{BranchKey, Runtime},
};

/// The branch declarations the archived branch keys of one restored entity may belong to.
///
/// A processor runs every branch under its one declaration and a relay carries one. An ingestor
/// or a reingestor keeps one branch lifecycle for all its routes, so a key it archived belongs to
/// the branching of the relay one of its routes writes. A resolution that finds no declaration is
/// refused, so a value always admits an unbranched key, a concrete one, or both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) struct RestoredBranchDeclarations {
    /// Whether an archived key may be absent: the entity runs unbranched, or one of its routes
    /// writes an unbranched relay.
    unbranched: bool,
    /// Every branch a concrete archived key may belong to, with the schema of its keys.
    branches: BTreeMap<BranchName, CreateSchema>,
}

/// Why the restored schedule gives an entity no branching its archived keys could belong to.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(in crate::application) enum RestoredBranchDeclarationError {
    #[error("the restored schedule holds no {} '{entity}'", .kind.as_str())]
    NotScheduled { kind: ModelKind, entity: ModelName },
    #[error("{} entities keep no archived branch keys", .kind.as_str())]
    NotBranchKeyed { kind: ModelKind },
    #[error("the restored schedule resolves no branching for {} '{entity}'", .kind.as_str())]
    Unresolved { kind: ModelKind, entity: ModelName },
}

/// Why one archived branch key does not belong to the branching of its restored entity. No variant
/// carries a key value, which a branch schema may declare sensitive.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(in crate::application) enum ArchivedBranchKeyError {
    #[error("it is not a valid typed branch key")]
    NotABranchKey,
    #[error("it is unbranched, where the entity runs in {branches}")]
    Unbranched { branches: BranchNames },
    #[error("it names a concrete branch, where the entity runs unbranched")]
    Concrete,
    #[error("it is not a key of branch '{}'", .branch.as_str())]
    NotOfBranch { branch: BranchName },
    #[error("it is a key of none of {branches}")]
    NotOfAnyBranch { branches: BranchNames },
}

/// The branches an archived key could have belonged to, as a refusal names them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::application) struct BranchNames(Vec<BranchName>);

impl fmt::Display for BranchNames {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.len() == 1 {
            formatter.write_str("branch ")?;
        } else {
            formatter.write_str("branches ")?;
        }
        let mut separator = "";
        for branch in &self.0 {
            write!(formatter, "{separator}'{}'", branch.as_str())?;
            separator = ", ";
        }
        Ok(())
    }
}

/// Where an archived branch key sits in its archive section.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application) enum ArchivedKeyPlace {
    /// The branch a WASM, deduplicator or window descriptor names.
    Descriptor,
    /// One entry of a branch lifecycle, numbered from 1 in the order the section lists them.
    LifecycleEntry(usize),
}

impl fmt::Display for ArchivedKeyPlace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Descriptor => formatter.write_str("the branch key its descriptor names"),
            Self::LifecycleEntry(entry) => {
                write!(formatter, "the branch key of lifecycle entry {entry}")
            }
        }
    }
}

impl RestoredBranchDeclarations {
    /// The declarations of `entity` in `schedule`: the branching a processor runs in or a relay
    /// carries, or the branching of every relay the routes of an ingestor or reingestor write.
    pub(in crate::application) fn of(
        schedule: &DomainSchedule,
        entity: &NodeRef,
    ) -> Result<Self, Report<RestoredBranchDeclarationError>> {
        let node = Self::scheduled(schedule, entity)?;
        let mut declarations = Self {
            unbranched: false,
            branches: BTreeMap::new(),
        };
        match node.config.as_ref() {
            Model::Ingestor(ingestor) => {
                declarations.add_routes(schedule, ingestor.output_routes.outputs())?;
            }
            Model::Reingestor(reingestor) => {
                declarations.add_routes(schedule, reingestor.output_routes.outputs())?;
            }
            _ if entity.kind == ModelKind::Relay || entity.kind.is_processor() => {
                declarations.add(entity, node)?;
            }
            _ => {
                return Err(Report::new(
                    RestoredBranchDeclarationError::NotBranchKeyed { kind: entity.kind },
                ));
            }
        }
        Ok(declarations)
    }

    fn scheduled<'schedule>(
        schedule: &'schedule DomainSchedule,
        entity: &NodeRef,
    ) -> Result<&'schedule ScheduledNode, Report<RestoredBranchDeclarationError>> {
        let Some(node) = schedule.nodes.get(entity) else {
            return Err(Report::new(RestoredBranchDeclarationError::NotScheduled {
                kind: entity.kind,
                entity: entity.identifier.clone(),
            }));
        };
        Ok(node)
    }

    /// Admits the keys of the branching the registry resolved for `node`.
    fn add(
        &mut self,
        entity: &NodeRef,
        node: &ScheduledNode,
    ) -> Result<(), Report<RestoredBranchDeclarationError>> {
        let Some(branching) = &node.resolved_branching else {
            return Err(Report::new(RestoredBranchDeclarationError::Unresolved {
                kind: entity.kind,
                entity: entity.identifier.clone(),
            }));
        };
        match branching {
            ResolvedBranching::Unbranched => self.unbranched = true,
            ResolvedBranching::Branched { branch, schema } => {
                self.branches.insert(branch.clone(), schema.clone());
            }
        }
        Ok(())
    }

    /// Admits the keys of every relay `routes` write. A route writes exactly the branching of the
    /// relay it targets, which the registry checked when it validated the route.
    fn add_routes<'route>(
        &mut self,
        schedule: &DomainSchedule,
        routes: impl Iterator<Item = &'route ProcessorOutput>,
    ) -> Result<(), Report<RestoredBranchDeclarationError>> {
        for route in routes {
            let relay = NodeRef::new(ModelKind::Relay, route.relay.clone());
            let node = Self::scheduled(schedule, &relay)?;
            self.add(&relay, node)?;
        }
        Ok(())
    }

    /// The typed key `archived` names, when it is a key one of these declarations admits: absent
    /// where the entity runs unbranched, or holding exactly the fields of one of its branches,
    /// each with a value of the field's declared type.
    pub(in crate::application) fn admit(
        &self,
        archived: Option<&Vec<StateField>>,
    ) -> Result<Option<BranchKey>, Report<ArchivedBranchKeyError>> {
        let key = BranchKey::from_remote_key(remote_branch_key(archived))
            .change_context(ArchivedBranchKeyError::NotABranchKey)?;
        let Some(concrete) = &key else {
            if self.unbranched {
                return Ok(None);
            }
            return Err(Report::new(ArchivedBranchKeyError::Unbranched {
                branches: self.branch_names(),
            }));
        };
        let mut branches = self.branches.iter();
        let Some((branch, schema)) = branches.next() else {
            return Err(Report::new(ArchivedBranchKeyError::Concrete));
        };
        if branches.next().is_none() {
            concrete
                .conform_to(&schema.fields)
                .change_context_lazy(|| ArchivedBranchKeyError::NotOfBranch {
                    branch: branch.clone(),
                })?;
            return Ok(key);
        }
        // The routes of an ingestor or reingestor write several branches, and the key belongs to
        // whichever its route wrote. The walk is bounded by the distinct branches those routes
        // write, which the relays of one domain bound.
        for schema in self.branches.values() {
            if concrete.conform_to(&schema.fields).is_ok() {
                return Ok(key);
            }
        }
        Err(Report::new(ArchivedBranchKeyError::NotOfAnyBranch {
            branches: self.branch_names(),
        }))
    }

    fn branch_names(&self) -> BranchNames {
        BranchNames(self.branches.keys().cloned().collect())
    }
}

/// One archived state whose branch keys the restored schedule installs, with the declarations
/// they must belong to.
struct ArchivedKeyCheck {
    /// The state's position among its domain's archived states.
    index: usize,
    entity: NodeRef,
    /// Shared by every archived state of the same entity, one per branch for a descriptor.
    declarations: Arc<RestoredBranchDeclarations>,
}

impl ArchivedKeyCheck {
    /// Admits every branch key `archived`, the state this check names, holds.
    fn admit(
        &self,
        domain: &DomainName,
        archived: &DescribedRuntimeState,
        cancellation: &Cancellation,
    ) -> Result<(), Report<RestoreRefusal>> {
        match archived {
            DescribedRuntimeState::BranchLifecycle { lifecycle, record } => {
                for (index, entry) in lifecycle.branches.iter().enumerate() {
                    cancellation
                        .check()
                        .change_context(RestoreRefusal::Unreadable)?;
                    let entry_number = index
                        .checked_add(1)
                        .assured("an entry's index is below the length of the list that holds it");
                    self.declarations
                        .admit(entry.key.as_ref())
                        .change_context_lazy(|| {
                            self.refusal(
                                domain,
                                record,
                                ArchivedKeyPlace::LifecycleEntry(entry_number),
                            )
                        })?;
                }
                Ok(())
            }
            DescribedRuntimeState::Wasm {
                descriptor, record, ..
            } => self.admit_descriptor(domain, descriptor.branch.as_ref(), record),
            DescribedRuntimeState::Deduplicator {
                descriptor, record, ..
            } => self.admit_descriptor(domain, descriptor.branch.as_ref(), record),
            DescribedRuntimeState::Window {
                descriptor, record, ..
            } => self.admit_descriptor(domain, descriptor.branch.as_ref(), record),
            // Kafka offsets carry no branch key, and materialized record identities are admitted
            // by the conversion that reads them from their own sections.
            DescribedRuntimeState::KafkaOffsets { .. }
            | DescribedRuntimeState::Materialized { .. } => Ok(()),
        }
    }

    fn admit_descriptor(
        &self,
        domain: &DomainName,
        branch: Option<&Vec<StateField>>,
        record: &DescribedSection,
    ) -> Result<(), Report<RestoreRefusal>> {
        self.declarations
            .admit(branch)
            .change_context_lazy(|| self.refusal(domain, record, ArchivedKeyPlace::Descriptor))?;
        Ok(())
    }

    fn refusal(
        &self,
        domain: &DomainName,
        record: &DescribedSection,
        place: ArchivedKeyPlace,
    ) -> RestoreRefusal {
        RestoreRefusal::BranchKey {
            domain: domain.clone(),
            kind: self.entity.kind,
            entity: self.entity.identifier.clone(),
            section: record.path.clone(),
            place,
        }
    }
}

impl VerifiedArchive {
    /// Checks every branch key of archived domain `domain` that `schedule` installs against the
    /// branching its restored entity declares: each branch lifecycle entry and the branch every
    /// WASM, deduplicator and window descriptor names. State whose entity the schedule does not hold,
    /// or holds under another schema fingerprint, is skipped by installation and is not checked.
    ///
    /// The keys are checked as one admitted job off the async workers. The description they belong
    /// to, and each key's conversion, are already charged to the archive's preparation reservation.
    pub(in crate::application) async fn validate_branch_keys(
        &self,
        runtime: &Runtime,
        domain: &DomainName,
        schedule: &DomainSchedule,
    ) -> Result<(), Report<RestoreRefusal>> {
        // An archive holds each domain once, and a cluster archive at most as many domains as one
        // cluster has, which is what bounds this walk.
        let Some(domain_index) = self
            .data
            .contents
            .description
            .domains
            .iter()
            .position(|described| &described.capture.domain == domain)
        else {
            return Ok(());
        };
        let mut checks = Vec::new();
        let mut resolved = HashMap::<NodeRef, Arc<RestoredBranchDeclarations>>::default();
        let states = &self.data.contents.description.domains[domain_index].state;
        for (index, archived) in states.iter().enumerate() {
            nervix_primitives::task::consume_budget().await;
            let (entity, schema) = match archived {
                DescribedRuntimeState::BranchLifecycle { lifecycle, .. } => (
                    NodeRef::new(lifecycle.owner_kind, lifecycle.entity.clone()),
                    lifecycle.schema,
                ),
                DescribedRuntimeState::Wasm { descriptor, .. } => (
                    NodeRef::new(ModelKind::WasmProcessor, descriptor.entity.clone()),
                    descriptor.schema,
                ),
                DescribedRuntimeState::Deduplicator { descriptor, .. } => (
                    NodeRef::new(ModelKind::Deduplicator, descriptor.entity.clone()),
                    descriptor.schema,
                ),
                DescribedRuntimeState::Window { descriptor, .. } => (
                    NodeRef::new(ModelKind::WindowProcessor, descriptor.entity.clone()),
                    descriptor.schema,
                ),
                DescribedRuntimeState::KafkaOffsets { .. }
                | DescribedRuntimeState::Materialized { .. } => continue,
            };
            let Some(node) = schedule.nodes.get(&entity) else {
                continue;
            };
            if node.schema_fingerprint != schema {
                continue;
            }
            let declarations = match resolved.entry(entity.clone()) {
                Entry::Occupied(occupied) => occupied.get().clone(),
                Entry::Vacant(vacant) => {
                    let declarations = RestoredBranchDeclarations::of(schedule, &entity)
                        .change_context_lazy(|| RestoreRefusal::UndeclaredBranching {
                            domain: domain.clone(),
                            kind: entity.kind,
                            entity: entity.identifier.clone(),
                        })?;
                    vacant.insert(Arc::new(declarations)).clone()
                }
            };
            checks.push(ArchivedKeyCheck {
                index,
                entity,
                declarations,
            });
        }
        if checks.is_empty() {
            return Ok(());
        }
        let data = self.data.clone();
        let domain = domain.clone();
        let executor = runtime.executor();
        let charge = executor
            .reserve(MemoryClass::Bulk, 1)
            .await
            .change_context(RestoreRefusal::MetadataAdmission)?;
        executor
            .run_cpu(CpuClass::Bulk, charge, move |_charge, cancellation| {
                let states = &data.contents.description.domains[domain_index].state;
                for check in &checks {
                    check.admit(&domain, &states[check.index], cancellation)?;
                }
                Ok::<_, Report<RestoreRefusal>>(())
            })
            .await
            .change_context(RestoreRefusal::Unreadable)?
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_backup::StateValue;
    use nervix_models::{FieldName, ParseAsType, SchemaField, SchemaFingerprint};

    use super::*;
    use crate::runtime::BranchKeyShapeError;

    fn domain() -> DomainName {
        DomainName::parse("orders").assured("the test domain is a valid literal name")
    }

    fn branch(name: &str) -> BranchName {
        BranchName::parse(name).assured("the test branch is a valid literal name")
    }

    fn field(name: &str) -> FieldName {
        FieldName::parse(name).assured("the test field is a valid literal name")
    }

    fn key_schema(name: &str, fields: &[(&str, ParseAsType)]) -> CreateSchema {
        CreateSchema {
            name: nervix_models::SchemaName::parse(name)
                .assured("the test schema is a valid literal name"),
            fields: fields
                .iter()
                .map(|(name, ty)| SchemaField {
                    name: field(name),
                    ty: ty.clone(),
                    optional: false,
                    sensitive: false,
                })
                .collect(),
        }
    }

    fn tenant_shard() -> ResolvedBranching {
        ResolvedBranching::branched(
            branch("by_tenant_shard"),
            key_schema(
                "tenant_shard",
                &[("tenant", ParseAsType::String), ("shard", ParseAsType::I32)],
            ),
        )
    }

    fn region() -> ResolvedBranching {
        ResolvedBranching::branched(
            branch("by_region"),
            key_schema("region_key", &[("region", ParseAsType::String)]),
        )
    }

    /// The schedule of the models `text` creates, each carrying the branching `branchings` names
    /// for it, as the registry resolves it for every relay and processor.
    fn schedule(text: &str, branchings: &[(&str, ResolvedBranching)]) -> DomainSchedule {
        let models = super::super::parse_models(&domain(), text)
            .assured("the test models are canonical NSPL");
        let mut nodes = Vec::with_capacity(models.len());
        for model in models {
            let model = model
                .try_map_resource_versions(|_, _| Err::<u64, ()>(()))
                .assured("the test models bind no resource");
            let mut resolved = None;
            for (name, branching) in branchings {
                if model.name().as_str() == *name {
                    resolved = Some(branching.clone());
                }
            }
            nodes.push(
                ScheduledNode::new(model, SchemaFingerprint::from_digest([7; 32]))
                    .with_resolved_branching(resolved),
            );
        }
        DomainSchedule::new(domain(), nodes, Vec::new())
    }

    fn declarations(
        schedule: &DomainSchedule,
        kind: ModelKind,
        name: &str,
    ) -> RestoredBranchDeclarations {
        let entity = NodeRef::new(
            kind,
            ModelName::parse(name).assured("the test entity is a valid literal name"),
        );
        RestoredBranchDeclarations::of(schedule, &entity)
            .assured("the test schedule declares the entity's branching")
    }

    fn archived(fields: &[(&str, StateValue)]) -> Vec<StateField> {
        fields
            .iter()
            .map(|(name, value)| StateField {
                name: (*name).to_string(),
                value: value.clone(),
            })
            .collect()
    }

    fn tenant(shard: StateValue) -> Vec<StateField> {
        archived(&[
            ("shard", shard),
            ("tenant", StateValue::String("north".to_string())),
        ])
    }

    const JUNCTION: &str =
        "CREATE RELAY orders SCHEMA order_event BRANCHED BY by_tenant_shard;\nCREATE RELAY routed \
         SCHEMA order_event BRANCHED BY by_tenant_shard;\nCREATE JUNCTION router FROM orders \
         BRANCHED BY by_tenant_shard TO routed INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;\n";

    /// A processor runs every branch under its one declaration, so an archived key must hold
    /// exactly that branch's fields, each with its declared type, and cannot be absent.
    #[test]
    fn a_processor_admits_only_keys_of_the_branch_it_runs_in() {
        let schedule = schedule(
            JUNCTION,
            &[
                ("orders", tenant_shard()),
                ("routed", tenant_shard()),
                ("router", tenant_shard()),
            ],
        );
        let router = declarations(&schedule, ModelKind::Junction, "router");

        let admitted = router
            .admit(Some(&tenant(StateValue::I32(4))))
            .assured("a key of exactly the declared fields and types is admitted")
            .assured("a concrete archived key stays concrete");
        assert_eq!(admitted.as_str(), r#"{"shard":4,"tenant":"north"}"#);

        let unbranched = router
            .admit(None)
            .expect_err("a branched processor holds no unbranched key");
        assert_eq!(
            unbranched.to_string(),
            "it is unbranched, where the entity runs in branch 'by_tenant_shard'"
        );

        let missing = router
            .admit(Some(&archived(&[(
                "tenant",
                StateValue::String("north".to_string()),
            )])))
            .expect_err("a key without a declared field is refused");
        assert_eq!(
            missing.current_context(),
            &ArchivedBranchKeyError::NotOfBranch {
                branch: branch("by_tenant_shard"),
            }
        );
        assert_eq!(
            missing.downcast_ref::<BranchKeyShapeError>(),
            Some(&BranchKeyShapeError::MissingField {
                field: field("shard"),
            })
        );

        let retyped = router
            .admit(Some(&tenant(StateValue::String("4".to_string()))))
            .expect_err("a key whose field holds another type is refused");
        assert_eq!(
            format!("{retyped:#}"),
            "it is not a key of branch 'by_tenant_shard': field 'shard' does not hold a value of \
             its declared type: expected I32, found STRING"
        );

        let not_a_key = router
            .admit(Some(&tenant(StateValue::F64Bits(f64::NAN.to_bits()))))
            .expect_err("a key holding a non-finite float is no typed branch key");
        assert_eq!(
            not_a_key.current_context(),
            &ArchivedBranchKeyError::NotABranchKey
        );
    }

    /// An unbranched processor runs one unbranched execution, so only an absent key is admitted.
    #[test]
    fn an_unbranched_processor_admits_only_an_absent_key() {
        let text = "CREATE RELAY orders SCHEMA order_event UNBRANCHED;\nCREATE RELAY routed \
                    SCHEMA order_event UNBRANCHED;\nCREATE JUNCTION router FROM orders UNBRANCHED \
                    TO routed INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;\n";
        let schedule = schedule(
            text,
            &[
                ("orders", ResolvedBranching::unbranched()),
                ("routed", ResolvedBranching::unbranched()),
                ("router", ResolvedBranching::unbranched()),
            ],
        );
        let router = declarations(&schedule, ModelKind::Junction, "router");

        assert_eq!(
            router
                .admit(None)
                .assured("an unbranched processor holds the absent key"),
            None
        );
        let concrete = router
            .admit(Some(&tenant(StateValue::I32(4))))
            .expect_err("an unbranched processor holds no concrete key");
        assert_eq!(
            concrete.current_context(),
            &ArchivedBranchKeyError::Concrete
        );
    }

    /// An ingestor keeps one lifecycle for all its routes, so a key it archived belongs to the
    /// branching of the relay one of its routes writes, including an unbranched one.
    #[test]
    fn an_ingestor_admits_keys_of_every_branching_its_routes_write() {
        let text = "CREATE RELAY orders SCHEMA order_event BRANCHED BY by_tenant_shard;\nCREATE \
                    RELAY regions SCHEMA order_event BRANCHED BY by_region;\nCREATE RELAY audit \
                    SCHEMA order_event UNBRANCHED;\nCREATE INGESTOR source FROM ENDPOINT ingress \
                    MODE NO_ACK SEQUENTIAL ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING \
                    order_codec TO orders INHERIT ALL BRANCHED BY by_tenant_shard SET tenant = \
                    message.tenant, shard = message.shard FLUSH IMMEDIATE ON MESSAGE ERROR LOG TO \
                    regions INHERIT ALL BRANCHED BY by_region SET region = message.tenant FLUSH \
                    IMMEDIATE ON MESSAGE ERROR LOG TO audit INHERIT ALL UNBRANCHED FLUSH \
                    IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;\n";
        let schedule = schedule(
            text,
            &[
                ("orders", tenant_shard()),
                ("regions", region()),
                ("audit", ResolvedBranching::unbranched()),
            ],
        );
        let source = declarations(&schedule, ModelKind::Ingestor, "source");

        source
            .admit(Some(&tenant(StateValue::I32(4))))
            .assured("a key of the first route's branch is admitted");
        source
            .admit(Some(&archived(&[(
                "region",
                StateValue::String("west".to_string()),
            )])))
            .assured("a key of the second route's branch is admitted");
        source
            .admit(None)
            .assured("the unbranched route's absent key is admitted");
        let foreign = source
            .admit(Some(&archived(&[("zone", StateValue::I32(1))])))
            .expect_err("a key of no route's branch is refused");
        assert_eq!(
            foreign.to_string(),
            "it is a key of none of branches 'by_region', 'by_tenant_shard'"
        );
    }

    /// Only an entity the schedule holds, whose kind keeps archived branch keys and whose
    /// branching the registry resolved, declares a branching archived keys can belong to.
    #[test]
    fn an_entity_without_a_resolved_branching_declares_none() {
        let schedule = schedule(JUNCTION, &[("routed", tenant_shard())]);
        let refusal = |kind: ModelKind, name: &str| {
            let entity = NodeRef::new(
                kind,
                ModelName::parse(name).assured("the test entity is a valid literal name"),
            );
            match RestoredBranchDeclarations::of(&schedule, &entity) {
                Ok(declarations) => panic!("{name} declares {declarations:?}"),
                Err(refusal) => refusal.current_context().clone(),
            }
        };

        assert_eq!(
            refusal(ModelKind::Junction, "absent"),
            RestoredBranchDeclarationError::NotScheduled {
                kind: ModelKind::Junction,
                entity: ModelName::parse("absent").assured("a valid literal name"),
            }
        );
        assert_eq!(
            refusal(ModelKind::Junction, "router"),
            RestoredBranchDeclarationError::Unresolved {
                kind: ModelKind::Junction,
                entity: ModelName::parse("router").assured("a valid literal name"),
            }
        );
        let schema_only = schedule_of_schema();
        let entity = NodeRef::new(
            ModelKind::Schema,
            ModelName::parse("order_event").assured("a valid literal name"),
        );
        let not_keyed = RestoredBranchDeclarations::of(&schema_only, &entity)
            .expect_err("a schema keeps no archived branch keys");
        assert_eq!(
            not_keyed.current_context(),
            &RestoredBranchDeclarationError::NotBranchKeyed {
                kind: ModelKind::Schema,
            }
        );
    }

    fn schedule_of_schema() -> DomainSchedule {
        schedule("CREATE SCHEMA order_event ( tenant STRING );\n", &[])
    }
}
