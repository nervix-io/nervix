//! Canonical explicit starts for independently owned Kafka intake units.
//!
//! Layer: data plane.
//! - **Owns.** Selecting one durable starting cut, bounded candidate offers, and transferring
//!   pending positions from instance zero to the unit that will commit them.
//! - **Depends on.** Installed execution identities, assignment-qualified native offsets and bulk
//!   checkpoint transfer.
//! - **Must not know.** Kafka drivers, Models, parsing or placement decisions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns distributed Kafka start coordination")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "only source initialization and its bounded requests reach this owner"
    )
)]

use error_stack::ResultExt as _;
use nervix_execution::{CpuClass, MemoryClass};
use nervix_interconnect::{
    IngestorUnitSelection, KafkaStartAction, KafkaStartRequest, KafkaStartResponse,
    StateCheckpointRead, StateSnapshotEnvelope,
};
use nervix_primitives::publication::ArcSwapOption;

use super::{
    kafka_offset_state::{
        KafkaOffsetStateRead, KafkaStartApplication, decode_start_positions, encode_start_candidate,
    },
    state_replication::StateReplicationError,
    *,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DurableKafkaStart {
    generation: u64,
    assignment: StateAssignmentToken,
}

pub(super) struct KafkaStartCapture<'a> {
    _guard: nervix_primitives::sync::MutexGuard<'a, Option<DurableKafkaStart>>,
}

#[derive(Debug)]
struct KafkaStartOffer {
    assignment: StateAssignmentToken,
    descriptor: StateSnapshotEnvelope,
    bytes: ChargedBytes,
}

/// The mutex serializes cold start selection and its durability boundary. Recorded-partition
/// commits use neither this mutex nor the offer publication. At most one candidate is retained
/// per initializing source; an admitted stream retains its own charged reference.
#[derive(Debug)]
pub(super) struct KafkaStartCoordination {
    durable: Mutex<Option<DurableKafkaStart>>,
    next_offer: AtomicU64,
    offer: ArcSwapOption<KafkaStartOffer>,
}

impl Default for KafkaStartCoordination {
    fn default() -> Self {
        Self {
            durable: Mutex::new(None),
            next_offer: AtomicU64::new(0),
            offer: ArcSwapOption::empty(),
        }
    }
}

impl KafkaStartCoordination {
    async fn admit(
        &self,
        freeze: &entity_gate::OwnershipHandoffFreezeState,
    ) -> error_stack::Result<
        nervix_primitives::sync::MutexGuard<'_, Option<DurableKafkaStart>>,
        KafkaStartError,
    > {
        let guard = self.durable.lock().await;
        if freeze.is_frozen() {
            return Err(Report::new(KafkaStartError::Frozen));
        }
        Ok(guard)
    }

    /// A capture waits for an admitted initializer; later initializers see the published freeze
    /// under this same mutex and cannot change the cut after it is captured.
    pub(super) async fn capture_guard(&self) -> KafkaStartCapture<'_> {
        KafkaStartCapture {
            _guard: self.durable.lock().await,
        }
    }

    fn next_offer_id(&self) -> error_stack::Result<u64, KafkaStartError> {
        let mut current = self.next_offer.load(Ordering::Relaxed);
        loop {
            let next = current
                .checked_add(1)
                .ok_or_else(|| Report::new(KafkaStartError::IdentityExhausted))?;
            match self.next_offer.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(current),
                Err(observed) => current = observed,
            }
        }
    }
}

struct KafkaStartOfferLease {
    state: KafkaOffsetStateRead,
    id: u64,
}

impl Drop for KafkaStartOfferLease {
    fn drop(&mut self) {
        self.state
            .start_coordination()
            .offer
            .rcu(|current| match current {
                Some(offer) if offer.descriptor.lsm == self.id => None,
                _ => current.clone(),
            });
    }
}

#[derive(Debug, Error)]
enum KafkaStartError {
    #[error("the Kafka intake unit is frozen for ownership capture")]
    Frozen,
    #[error("the Kafka start candidate identity space is exhausted")]
    IdentityExhausted,
    #[error("the Kafka start candidate exceeds the admitted allocation range")]
    Bounds,
    #[error("the selected Kafka start candidate is no longer offered")]
    OfferEnded,
    #[error("the Kafka start reply does not identify a current canonical cut")]
    Reply,
}

struct KafkaStartAdmission {
    state: KafkaOffsetStateOriginator,
    selection: IngestorUnitSelection,
    explicit: bool,
}

struct KafkaStartExchange {
    coordinator: ClusterNodeName,
    response: KafkaStartResponse,
}

impl Runtime {
    fn verify_kafka_start_recipient(
        &self,
        state: &KafkaOffsetStateOriginator,
        generation: u64,
    ) -> error_stack::Result<(), StateReplicationError> {
        let stale = || {
            Report::new(StateReplicationError::Superseded {
                placement: state.placement().clone(),
            })
        };
        let lifecycle = self
            .domain_clock_lifecycle(&state.placement().domain)
            .change_context(StateReplicationError::Capture {
                placement: state.placement().clone(),
            })?;
        if !lifecycle.task_state().is_some_and(|current| {
            current.generation == generation
                && current.status == nervix_models::DomainStatus::Running
        }) {
            return Err(stale());
        }
        let installed = self
            .inner
            .replicated_kafka_offset_states
            .get(state.placement())
            .ok_or_else(stale)?;
        let current =
            ReplicatedKafkaOffsetState::current_originator(installed.value()).ok_or_else(stale)?;
        if !current.same_assignment(state) {
            return Err(stale());
        }
        Ok(())
    }
    fn kafka_start_freeze(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Arc<entity_gate::OwnershipHandoffFreezeState> {
        let key = DomainNodeUnitRef::new(
            DomainNodeRef::node_in(
                placement.domain.clone(),
                placement.kind,
                placement.identifier.clone(),
            ),
            placement.unit,
        );
        self.inner
            .frozen_ownership_handoff_entities
            .entry(key)
            .or_default()
            .clone()
    }
    fn kafka_start_admission(
        &self,
        request: &KafkaStartRequest,
    ) -> error_stack::Result<KafkaStartAdmission, StateReplicationError> {
        let placement = RuntimeStatePlacement::from_remote(request.placement.clone())
            .change_context(StateReplicationError::KafkaStart {
                domain: request.placement.domain.clone(),
                ingestor: request.placement.identifier.clone(),
                unit: request.placement.unit,
            })?;
        let stale = || {
            Report::new(StateReplicationError::Superseded {
                placement: placement.clone(),
            })
        };
        if placement.state != RuntimeState::KafkaOffset
            || placement.kind != ModelKind::Ingestor
            || placement.branch_key.is_some()
        {
            return Err(stale());
        }
        let dispatcher = self.inner.remote_dispatcher.load_full().ok_or_else(stale)?;
        if !dispatcher.is_live_node_identity(&request.source) {
            return Err(stale());
        }
        let execution = self
            .inner
            .executions
            .get(&placement.domain)
            .ok_or_else(stale)?;
        if execution.start_version != request.generation
            || execution.revision.ownership_handoff_fingerprint != request.ownership_fingerprint
        {
            return Err(stale());
        }
        let node = execution
            .revision
            .nodes
            .get(&NodeRef::new(placement.kind, placement.identifier.clone()))
            .ok_or_else(stale)?;
        if !node
            .ownership_state_components
            .contains(&OwnershipStateComponent::KafkaOffsets)
        {
            return Err(stale());
        }
        let Some(nervix_models::UnitKind::Instance(index)) = node
            .units
            .get_key_value(&placement.unit)
            .map(|(unit, _)| unit.kind())
        else {
            return Err(stale());
        };
        let source = node.units.get(&placement.unit).ok_or_else(stale)?;
        let root = node.units.get(&Unit::FIRST_INSTANCE).ok_or_else(stale)?;
        if source.primary_node.as_ref() != Some(request.source.node_id())
            || root.primary_node.as_ref() != Some(dispatcher.local_node_id())
        {
            return Err(stale());
        }
        let count = NonZeroU64::new(u64::try_from(node.units.len()).map_err(|_| stale())?)
            .ok_or_else(stale)?;
        if index >= count.get() {
            return Err(stale());
        }
        drop(execution);
        let domain_state = self
            .domain_clock_lifecycle(&placement.domain)
            .change_context(StateReplicationError::Capture {
                placement: placement.clone(),
            })?
            .task_state()
            .ok_or_else(stale)?;
        let explicit = matches!(
            domain_state.last_start,
            nervix_models::DomainStartPoint::At { .. }
        );
        if domain_state.status != nervix_models::DomainStatus::Running {
            return Err(stale());
        }
        let mut root_placement = placement.clone();
        root_placement.unit = Unit::FIRST_INSTANCE;
        let state = self
            .inner
            .replicated_kafka_offset_states
            .get(&root_placement)
            .ok_or_else(stale)?;
        let originator =
            ReplicatedKafkaOffsetState::current_originator(state.value()).ok_or_else(stale)?;
        Ok(KafkaStartAdmission {
            state: originator,
            selection: IngestorUnitSelection {
                unit: placement.unit,
                count,
            },
            explicit,
        })
    }

    fn kafka_start_still_admitted(
        &self,
        request: &KafkaStartRequest,
        admission: &KafkaStartAdmission,
    ) -> error_stack::Result<(), StateReplicationError> {
        let current = self.kafka_start_admission(request)?;
        if !admission.state.same_assignment(&current.state) {
            return Err(Report::new(StateReplicationError::Superseded {
                placement: admission.state.placement().clone(),
            }));
        }
        Ok(())
    }

    /// An explicit start is selected once on unit zero. Pending seeds stay in that unit's native
    /// checkpoint until each recipient has persisted its own positions and acknowledged them.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "one bounded start request admits a driver before its recurring polls"
        )
    )]
    pub(crate) async fn answer_kafka_start(
        &self,
        request: KafkaStartRequest,
    ) -> error_stack::Result<KafkaStartResponse, StateReplicationError> {
        let timeout = StateReplicationError::KafkaStartDeadline {
            domain: request.placement.domain.clone(),
            ingestor: request.placement.identifier.clone(),
            unit: request.placement.unit,
        };
        nervix_primitives::time::timeout(
            <KafkaStartRequest as nervix_interconnect::InterconnectRequest>::DEADLINE.resolve(
                self.inner.interconnect_request_timeout,
                nervix_interconnect::InterconnectLiveness::default(),
            ),
            self.answer_kafka_start_within_deadline(request),
        )
        .await
        .change_context(timeout)?
    }

    async fn answer_kafka_start_within_deadline(
        &self,
        request: KafkaStartRequest,
    ) -> error_stack::Result<KafkaStartResponse, StateReplicationError> {
        let admission = self.kafka_start_admission(&request)?;
        let state = &admission.state;
        let freeze = self.kafka_start_freeze(state.placement());
        let mut durable = state
            .read()
            .start_coordination()
            .admit(&freeze)
            .await
            .change_context(StateReplicationError::KafkaStartFrozen {
                placement: state.placement().clone(),
            })?;
        self.kafka_start_still_admitted(&request, &admission)?;
        match &request.action {
            KafkaStartAction::Propose { snapshot } => {
                if !admission.explicit {
                    return Err(Report::new(StateReplicationError::Superseded {
                        placement: state.placement().clone(),
                    }));
                }
                if state.read().applied_start_generation() != Some(request.generation) {
                    let offered_placement =
                        RuntimeStatePlacement::from_remote(request.placement.clone())
                            .change_context(StateReplicationError::Capture {
                                placement: state.placement().clone(),
                            })?;
                    let (candidate, charge) = self
                        .fetch_state_checkpoint_charged(
                            request.source.node_id(),
                            &offered_placement,
                            snapshot,
                            StateCheckpointRead::KafkaStartOffer,
                        )
                        .await?;
                    let positions = self
                        .decode_kafka_start_positions(
                            &offered_placement,
                            candidate,
                            request.generation,
                        )
                        .await?;
                    self.kafka_start_still_admitted(&request, &admission)?;
                    self.reset_domain_kafka_offsets(
                        state,
                        request.generation,
                        positions,
                        KafkaStartApplication::Canonical,
                    )
                    .await?;
                    drop(charge);
                    *durable = Some(DurableKafkaStart {
                        generation: request.generation,
                        assignment: state.assignment_token(),
                    });
                }
            }
            KafkaStartAction::Acknowledge { start_generation } => {
                let _publication = self.backup_publication(&state.placement().domain);
                state
                    .acknowledge_start_seed(*start_generation, admission.selection)
                    .change_context(StateReplicationError::ReplaceKafkaOffsets {
                        placement: state.placement().clone(),
                    })?;
            }
            KafkaStartAction::Read => {}
        }
        let Some(generation) = state.read().applied_start_generation() else {
            return Ok(KafkaStartResponse::Unresolved);
        };
        if admission.explicit && generation != request.generation {
            return Ok(KafkaStartResponse::Unresolved);
        }
        let expected = DurableKafkaStart {
            generation,
            assignment: state.assignment_token(),
        };
        if *durable != Some(expected) {
            let checkpoint = self
                .capture_kafka_start_checkpoint(state.read(), None)
                .await?;
            self.persist_kafka_offset_snapshot(
                &state.persistence(),
                checkpoint.lsm,
                &checkpoint.payload,
            )
            .await?;
            *durable = Some(expected);
        }
        self.kafka_start_still_admitted(&request, &admission)?;
        if admission.selection.unit == Unit::FIRST_INSTANCE
            || matches!(&request.action, KafkaStartAction::Acknowledge { .. })
        {
            return Ok(KafkaStartResponse::Current { generation });
        }
        let checkpoint = self
            .capture_kafka_start_checkpoint(state.read(), Some((generation, admission.selection)))
            .await?;
        let snapshot = self
            .describe_state_checkpoint(state.placement(), checkpoint)
            .await?;
        self.kafka_start_still_admitted(&request, &admission)?;
        Ok(KafkaStartResponse::Seed {
            generation,
            snapshot,
        })
    }

    async fn capture_kafka_start_checkpoint(
        &self,
        state: &KafkaOffsetStateRead,
        seed: Option<(u64, IngestorUnitSelection)>,
    ) -> error_stack::Result<PersistedRuntimeStateEntry, StateReplicationError> {
        let failure = || StateReplicationError::Capture {
            placement: state.placement().clone(),
        };
        let captured = state
            .capture_after(None)
            .verified("an unfiltered capture always retains the table");
        let (encoded, scratch) = captured.bounds().change_context_lazy(failure)?;
        let encoded_capacity = encoded
            .checked_mul(3)
            .ok_or_else(|| Report::new(KafkaStartError::Bounds).change_context(failure()))?;
        let capacity = encoded_capacity
            .checked_add(scratch)
            .ok_or_else(|| Report::new(KafkaStartError::Bounds).change_context(failure()))?;
        let reservation = self
            .inner
            .executor
            .reserve(MemoryClass::RestoreMetadata, capacity)
            .await
            .change_context_lazy(failure)?;
        self.inner
            .executor
            .run_cpu(CpuClass::Bulk, reservation, move |_charge, cancellation| {
                cancellation
                    .check()
                    .change_context(RuntimePersistenceError::Cancelled)?;
                match seed {
                    Some((generation, selection)) => {
                        captured.start_seed(generation, selection, || {
                            cancellation
                                .check()
                                .change_context(RuntimePersistenceError::Cancelled)
                        })
                    }
                    None => captured.snapshot(|| {
                        cancellation
                            .check()
                            .change_context(RuntimePersistenceError::Cancelled)
                    }),
                }
            })
            .await
            .change_context_lazy(failure)?
            .change_context_lazy(failure)
    }

    async fn decode_kafka_start_positions(
        &self,
        placement: &RuntimeStatePlacement,
        snapshot: PersistedRuntimeStateEntry,
        generation: u64,
    ) -> error_stack::Result<Vec<KafkaOffsetPosition>, StateReplicationError> {
        let length =
            u64::try_from(snapshot.payload.len()).assured("a native payload length fits u64");
        let failure = || StateReplicationError::Capture {
            placement: placement.clone(),
        };
        let capacity = length
            .checked_mul(3)
            .ok_or_else(|| Report::new(KafkaStartError::Bounds).change_context(failure()))?;
        let reservation = self
            .inner
            .executor
            .reserve(MemoryClass::RestoreMetadata, capacity.max(1))
            .await
            .change_context_lazy(failure)?;
        self.inner
            .executor
            .run_cpu(CpuClass::Bulk, reservation, move |_charge, cancellation| {
                decode_start_positions(&snapshot.payload, generation, cancellation)
            })
            .await
            .change_context_lazy(failure)?
            .change_context_lazy(failure)
    }

    async fn exchange_kafka_start(
        &self,
        state: &KafkaOffsetStateOriginator,
        generation: u64,
        action: KafkaStartAction,
    ) -> error_stack::Result<KafkaStartExchange, StateReplicationError> {
        let placement = state.placement();
        let stale = || {
            Report::new(StateReplicationError::Superseded {
                placement: placement.clone(),
            })
        };
        let dispatcher = self.inner.remote_dispatcher.load_full().ok_or_else(stale)?;
        let execution = self
            .inner
            .executions
            .get(&placement.domain)
            .ok_or_else(stale)?;
        let node = execution
            .revision
            .nodes
            .get(&NodeRef::new(placement.kind, placement.identifier.clone()))
            .ok_or_else(stale)?;
        let coordinator_unit = node.units.get(&Unit::FIRST_INSTANCE).ok_or_else(stale)?;
        let coordinator = coordinator_unit.primary_node.clone().ok_or_else(stale)?;
        let request = KafkaStartRequest {
            placement: placement.to_remote(),
            source: ClusterNodeIdentity::new(
                dispatcher.local_node_id().clone(),
                dispatcher.local_node_incarnation(),
            ),
            generation,
            ownership_fingerprint: execution.revision.ownership_handoff_fingerprint,
            action,
        };
        drop(execution);
        let response = if &coordinator == dispatcher.local_node_id() {
            self.answer_kafka_start(request).await?
        } else {
            dispatcher
                .request_with_timeout(
                    &coordinator,
                    request,
                    dispatcher.request_deadline::<KafkaStartRequest>(),
                )
                .await
                .change_context(StateReplicationError::Request {
                    target: coordinator.clone(),
                    placement: placement.clone(),
                })?
                .map_err(|failure| {
                    Report::new(StateReplicationError::RemoteFailure {
                        target: coordinator.clone(),
                        placement: placement.clone(),
                        failure,
                    })
                })?
        };
        Ok(KafkaStartExchange {
            coordinator,
            response,
        })
    }

    async fn adopt_kafka_start(
        &self,
        state: &KafkaOffsetStateOriginator,
        generation: u64,
        exchange: KafkaStartExchange,
    ) -> error_stack::Result<(), StateReplicationError> {
        if let KafkaStartResponse::Seed {
            generation: start_generation,
            snapshot,
        } = exchange.response
        {
            let execution = self
                .inner
                .executions
                .get(&state.placement().domain)
                .ok_or_else(|| {
                    Report::new(StateReplicationError::Superseded {
                        placement: state.placement().clone(),
                    })
                })?;
            let node = execution
                .revision
                .nodes
                .get(&NodeRef::new(
                    state.placement().kind,
                    state.placement().identifier.clone(),
                ))
                .ok_or_else(|| {
                    Report::new(StateReplicationError::Superseded {
                        placement: state.placement().clone(),
                    })
                })?;
            let count = NonZeroU64::new(
                u64::try_from(node.units.len()).assured("the unit inventory fits u64"),
            )
            .verified("an originating ingestor has at least its own unit");
            drop(execution);
            let mut root = state.placement().clone();
            root.unit = Unit::FIRST_INSTANCE;
            let selection = IngestorUnitSelection {
                unit: state.placement().unit,
                count,
            };
            let (seed, charge) = self
                .fetch_state_checkpoint_charged(
                    &exchange.coordinator,
                    &root,
                    &snapshot,
                    StateCheckpointRead::KafkaStartSeed {
                        generation: start_generation,
                        selection,
                    },
                )
                .await?;
            let positions = self
                .decode_kafka_start_positions(&root, seed, start_generation)
                .await?;
            let has_seeds = !positions.is_empty();
            let needs_positions = state.read().applied_start_generation() != Some(start_generation)
                || positions.iter().any(|position| {
                    state
                        .read()
                        .next_offset(&position.topic, position.partition)
                        .is_none()
                });
            let expected = DurableKafkaStart {
                generation: start_generation,
                assignment: state.assignment_token(),
            };
            let freeze = self.kafka_start_freeze(state.placement());
            let mut durable = state
                .read()
                .start_coordination()
                .admit(&freeze)
                .await
                .change_context(StateReplicationError::KafkaStartFrozen {
                    placement: state.placement().clone(),
                })?;
            self.verify_kafka_start_recipient(state, generation)?;
            if needs_positions || *durable != Some(expected) {
                self.reset_domain_kafka_offsets(
                    state,
                    start_generation,
                    positions,
                    KafkaStartApplication::Unit,
                )
                .await?;
                *durable = Some(expected);
            }
            drop(durable);
            drop(charge);
            if has_seeds {
                self.exchange_kafka_start(
                    state,
                    generation,
                    KafkaStartAction::Acknowledge { start_generation },
                )
                .await?;
            }
        }
        Ok(())
    }

    pub(super) async fn synchronize_kafka_start(
        &self,
        state: &KafkaOffsetStateOriginator,
        generation: u64,
    ) -> error_stack::Result<(), StateReplicationError> {
        let exchange = self
            .exchange_kafka_start(state, generation, KafkaStartAction::Read)
            .await?;
        self.adopt_kafka_start(state, generation, exchange).await
    }

    pub(super) async fn propose_kafka_start(
        &self,
        state: &KafkaOffsetStateOriginator,
        generation: u64,
        positions: Vec<KafkaOffsetPosition>,
    ) -> error_stack::Result<(), StateReplicationError> {
        let failure = || StateReplicationError::Capture {
            placement: state.placement().clone(),
        };
        let mut capacity = 64 * 1024_u64;
        for position in &positions {
            let topic = u64::try_from(position.topic.len()).assured("a topic length fits u64");
            let entry_length = topic
                .checked_add(128)
                .ok_or_else(|| Report::new(KafkaStartError::Bounds).change_context(failure()))?;
            let entry_capacity = entry_length
                .checked_mul(3)
                .ok_or_else(|| Report::new(KafkaStartError::Bounds).change_context(failure()))?;
            capacity = capacity
                .checked_add(entry_capacity)
                .ok_or_else(|| Report::new(KafkaStartError::Bounds).change_context(failure()))?;
        }
        let reservation = self
            .inner
            .executor
            .reserve(MemoryClass::RestoreMetadata, capacity)
            .await
            .change_context_lazy(failure)?;
        let payload = self
            .inner
            .executor
            .run_cpu(CpuClass::Bulk, reservation, move |_charge, cancellation| {
                cancellation
                    .check()
                    .change_context(RuntimePersistenceError::Cancelled)?;
                encode_start_candidate(generation, positions)
            })
            .await
            .change_context_lazy(failure)?
            .change_context_lazy(failure)?;
        let coordination = state.read().start_coordination();
        let id = coordination.next_offer_id().change_context_lazy(failure)?;
        let described = self
            .describe_retained_state_checkpoint(
                state.placement(),
                PersistedRuntimeStateEntry { lsm: id, payload },
            )
            .await?;
        let snapshot = described.descriptor;
        coordination.offer.store(Some(StdArc::new(KafkaStartOffer {
            assignment: state.assignment_token(),
            descriptor: snapshot.clone(),
            bytes: ChargedBytes::from_owned(described.snapshot.payload, described.reservation),
        })));
        let _lease = KafkaStartOfferLease {
            state: state.read().clone(),
            id,
        };
        let exchange = self
            .exchange_kafka_start(state, generation, KafkaStartAction::Propose { snapshot })
            .await?;
        if matches!(exchange.response, KafkaStartResponse::Unresolved) {
            return Err(Report::new(KafkaStartError::Reply).change_context(failure()));
        }
        self.adopt_kafka_start(state, generation, exchange).await
    }

    /// A one-unit source already owns the complete cut. Retain its native reset and durability
    /// path without encoding an offer, hashing it or transferring it back to the same owner.
    pub(super) async fn start_single_kafka_unit(
        &self,
        state: &KafkaOffsetStateOriginator,
        generation: u64,
        positions: Vec<KafkaOffsetPosition>,
    ) -> error_stack::Result<(), StateReplicationError> {
        let freeze = self.kafka_start_freeze(state.placement());
        let _initializing = state
            .read()
            .start_coordination()
            .admit(&freeze)
            .await
            .change_context(StateReplicationError::KafkaStartFrozen {
                placement: state.placement().clone(),
            })?;
        self.verify_kafka_start_recipient(state, generation)?;
        if state.placement().unit != Unit::FIRST_INSTANCE {
            return Err(Report::new(StateReplicationError::Superseded {
                placement: state.placement().clone(),
            }));
        }
        self.reset_domain_kafka_offsets(
            state,
            generation,
            positions,
            KafkaStartApplication::Canonical,
        )
        .await
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            key = "one ingestor instance and its retained startup offer revision",
            bound = "one synchronous native-state lookup at stream admission; no guard across an \
                     await; the stream retains its admitted charged bytes",
            reason = "start-offer admission resolves its retained body once; stream chunks retain \
                      only the admitted charged bytes"
        )
    )]
    pub(super) fn offered_kafka_start_bytes(
        &self,
        placement: &RuntimeStatePlacement,
        lsm: u64,
    ) -> error_stack::Result<ChargedBytes, StateReplicationError> {
        let failure = || StateReplicationError::Capture {
            placement: placement.clone(),
        };
        let state = self
            .inner
            .replicated_kafka_offset_states
            .get(placement)
            .ok_or_else(|| Report::new(KafkaStartError::OfferEnded).change_context(failure()))?;
        let read = ReplicatedKafkaOffsetState::read(state.value());
        let offer =
            read.start_coordination().offer.load_full().ok_or_else(|| {
                Report::new(KafkaStartError::OfferEnded).change_context(failure())
            })?;
        if offer.descriptor.lsm != lsm {
            return Err(Report::new(KafkaStartError::OfferEnded).change_context(failure()));
        }
        read.authorize_start_offer(offer.assignment)
            .change_context_lazy(failure)?;
        Ok(offer.bytes.clone())
    }

    pub(super) async fn kafka_start_seed_checkpoint(
        &self,
        placement: &RuntimeStatePlacement,
        generation: u64,
        selection: IngestorUnitSelection,
    ) -> error_stack::Result<PersistedRuntimeStateEntry, StateReplicationError> {
        let state = self.kafka_start_seed_originator(placement, selection)?;
        let checkpoint = self
            .capture_kafka_start_checkpoint(state.read(), Some((generation, selection)))
            .await?;
        let current = self.kafka_start_seed_originator(placement, selection)?;
        if !state.same_assignment(&current) {
            return Err(Report::new(StateReplicationError::Superseded {
                placement: placement.clone(),
            }));
        }
        Ok(checkpoint)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            key = "one ingestor instance's canonical generation and selected layout",
            bound = "two synchronous lookups before and after one admitted checkpoint capture; no \
                     guard across an await; existing snapshot size and progress bounds",
            reason = "one pending-seed admission validates the current layout and native owner \
                      before and after capture; recurring stream chunks retain admitted bytes"
        )
    )]
    fn kafka_start_seed_originator(
        &self,
        placement: &RuntimeStatePlacement,
        selection: IngestorUnitSelection,
    ) -> error_stack::Result<KafkaOffsetStateOriginator, StateReplicationError> {
        let stale = || {
            Report::new(StateReplicationError::Superseded {
                placement: placement.clone(),
            })
        };
        if placement.state != RuntimeState::KafkaOffset
            || placement.kind != ModelKind::Ingestor
            || placement.unit != Unit::FIRST_INSTANCE
            || placement.branch_key.is_some()
        {
            return Err(stale());
        }
        let execution = self
            .inner
            .executions
            .get(&placement.domain)
            .ok_or_else(stale)?;
        let node = execution
            .revision
            .nodes
            .get(&NodeRef::new(placement.kind, placement.identifier.clone()))
            .ok_or_else(stale)?;
        if u64::try_from(node.units.len()).assured("the unit inventory fits u64")
            != selection.count.get()
            || !node.units.contains_key(&selection.unit)
        {
            return Err(stale());
        }
        let state = self
            .inner
            .replicated_kafka_offset_states
            .get(placement)
            .ok_or_else(stale)?;
        ReplicatedKafkaOffsetState::current_originator(state.value()).ok_or_else(stale)
    }
}

#[cfg(test)]
#[path = "kafka_start_tests.rs"]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
#[cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "checks startup admission against the production ownership freeze"
    )
)]
mod shuttle_checks {
    use nervix_model_harness::shuttle::{check_pct, check_random};

    use super::*;

    fn start_racing_capture() {
        shuttle::future::block_on(async {
            let placement = RuntimeStatePlacement {
                domain: DomainName::parse("starts").assured("a fixed valid domain"),
                state: RuntimeState::KafkaOffset,
                kind: ModelKind::Ingestor,
                identifier: ModelName::parse("source").assured("a fixed valid source"),
                unit: Unit::FIRST_INSTANCE,
                branch_key: None,
            };
            let state = Arc::new(
                ReplicatedKafkaOffsetState::new(placement, None).assured("empty state initializes"),
            );
            let originator = ReplicatedKafkaOffsetState::bind(
                &state,
                StateReplicationRoles::owned_by(None),
                None,
                2,
            )
            .originator
            .assured("the local assignment originates");
            let freeze = Arc::new(entity_gate::OwnershipHandoffFreezeState::default());
            let initializing = nervix_primitives::task::spawn({
                let originator = originator.clone();
                let freeze = freeze.clone();
                async move {
                    match originator.read().start_coordination().admit(&freeze).await {
                        Ok(_admitted) => {
                            originator
                                .apply_canonical_start(
                                    7,
                                    vec![KafkaOffsetPosition {
                                        topic: "events".to_owned(),
                                        partition: 0,
                                        offset: 31,
                                    }],
                                )
                                .assured(
                                    "an admitted initializer publishes its canonical positions",
                                );
                        }
                        Err(error) => {
                            assert!(matches!(error.current_context(), KafkaStartError::Frozen))
                        }
                    }
                }
            });
            let capturing = nervix_primitives::task::spawn({
                let read = originator.read().clone();
                let freeze = freeze.clone();
                async move {
                    freeze.insert(CoordinationIdentity::new(
                        ClusterNodeName::parse("coordinator").assured("a fixed valid coordinator"),
                        1,
                        1,
                    ));
                    let _capture = read.start_coordination().capture_guard().await;
                    read.latest_snapshot()
                        .assured("the frozen cut captures completely")
                }
            });
            let captured = capturing
                .await
                .assured("a failing model participant fails the entire execution");
            initializing
                .await
                .assured("a failing model participant fails the entire execution");
            let final_state = originator
                .read()
                .latest_snapshot()
                .assured("the final native positions encode");
            assert_eq!(captured.lsm, final_state.lsm);
            assert_eq!(
                captured.payload, final_state.payload,
                "an initializer cannot publish beyond an already captured ownership cut"
            );
            assert!(
                originator
                    .read()
                    .start_coordination()
                    .admit(&freeze)
                    .await
                    .is_err()
            );
        });
    }

    #[test]
    fn shuttle_a_start_racing_ownership_capture_finishes_before_the_cut_or_is_refused() {
        check_random(start_racing_capture, 500);
        check_pct(start_racing_capture, 500, 3);
    }
}
