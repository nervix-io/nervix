//! MQTT ingestor runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the MQTT connector plan, with one rendered client configuration per
//!   instance, the node resolver its connections resolve the broker through, the declared
//!   acknowledgement policy, and host-owned intake whose quiesce either disconnects the session or
//!   buffers and drops what stays subscribed, retiring inputs through their connector plans, in a
//!   swap only once it can no longer be undone, and the retirement each instance of a replacement
//!   owes a broker session its preceding input left with another subscription: kept on the node
//!   beyond any one domain execution, and settled there before the instance runs elsewhere.
//! - **Depends on.** The connector ingestor contract, the MQTT connector, and pre-resolved runtime
//!   execution handles.
//! - **Must not know.** The MQTT driver, session protocol, Plexus parsing, registry validation, or
//!   placement computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::ResultExt as _;
use nervix_connector::{ClientResourceMounts, IngestorConnector as _};
use nervix_connector_mqtt::{
    MqttSessionRetirement, MqttTopicIngestor, MqttTopicIngestorError, MqttTopicIngestorPlan,
    MqttTopicIngestorSessions, MqttTopicIngestorSettings,
};

use super::{
    super::*,
    IngestorConnectorStartError, IngestorStartError,
    connector::{BrokerIngestorStart, IngestorStart},
};

/// The resolved connector plan and the resource mounts kept alive while it is used.
struct ResolvedMqttIngestor {
    connector: MqttTopicIngestorPlan,
    client_mounts: Arc<[Option<Arc<ClientResourceMounts>>]>,
}

/// Immutable per-instance client material rendered once for a logical ingestor on this node.
struct RenderedMqttClient {
    connector: MqttTopicIngestorPlan,
    mounts: Arc<[Option<Arc<ClientResourceMounts>>]>,
}

pub(in crate::runtime) struct PreparedMqttIngestorClient {
    declaration: MqttTopicIngestorStartPlan,
    rendered: Arc<RenderedMqttClient>,
}

impl PreparedMqttIngestorClient {
    pub(in crate::runtime) fn agrees_with(
        &self,
        name: &IngestorName,
        revision: &ExecutionRevision,
    ) -> bool {
        revision.entrypoints.ingestor(name, Unit::FIRST_INSTANCE).is_some_and(|plan|
            matches!(&plan.input, IngestorInputPlan::Transport(input)
                if matches!(&input.source, IngestorConnectorPlan::Mqtt(MqttIngestorStartPlan::Topic(entity)) if entity == &self.declaration)))
    }
}

/// The retirements the instances of one MQTT ingestor owe their broker sessions on this node, and
/// the input they owe them under. Each session may still hold a subscription a preceding input of
/// the ingestor declared, so the instance that starts next here retires it before it subscribes.
/// The node keeps them beyond any one domain execution, so a stop, a start or a rebuild hands them
/// to that instance.
pub(in crate::runtime) struct OwedMqttSessionRetirements {
    /// The input whose sessions the retirements are owed for.
    declaration: MqttTopicIngestorStartPlan,
    /// The plan of one of the ingestor's instances on this node, through which this node resolves
    /// the input when it retires a session itself.
    ingestor: IngestorSpec,
    /// One for each instance, until an instance has retired its session and subscribed, or this
    /// node retired the session itself.
    instances: BTreeMap<u64, MqttSessionRetirement>,
}

const _: () = assert!(matches!(
    MqttTopicIngestor::KIND,
    nervix_models::IngestorEntityKind::Mqtt(nervix_models::MqttIngestorEntity::Topic)
));

/// What retiring some of an input's instances achieved.
struct InstanceRetirements {
    /// The instances whose broker session is gone.
    retired: BTreeSet<u64>,
    /// Why each of the other instances kept its session.
    failures: Vec<Report<MqttTopicIngestorError>>,
}

impl ResolvedMqttIngestor {
    /// Runs at most the declared instance count of cleanup connections concurrently. The resource
    /// mounts remain borrowed until every attempt finishes; every typed failure is returned beside
    /// the instances it did not stop.
    async fn retire_instances(
        &self,
        instances: impl IntoIterator<Item = u64>,
    ) -> InstanceRetirements {
        let mut cleanups = FuturesUnordered::new();
        for instance in instances {
            cleanups.push(async move { (instance, self.connector.retire(instance).await) });
        }
        let mut retirements = InstanceRetirements {
            retired: BTreeSet::new(),
            failures: Vec::new(),
        };
        while let Some((instance, result)) = cleanups.next().await {
            nervix_primitives::task::consume_budget().await;
            match result {
                Ok(()) => {
                    retirements.retired.insert(instance);
                }
                Err(error) => retirements.failures.push(error),
            }
        }
        retirements
    }
}

impl MqttTopicIngestorStartPlan {
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<IngestorStart, IngestorStartError> {
        let ResolvedMqttIngestor {
            connector,
            client_mounts,
        } = self
            .resolve(runtime, ingestor)
            .await
            .change_context_lazy(|| ingestor.initialize_failure())?;
        let retirement = runtime.outstanding_mqtt_session_retirement(
            &ingestor.domain,
            &ingestor.name,
            ingestor.instance_index(),
        );
        let connector = match retirement {
            Some(retirement) => connector.with_first_connection_retirement(retirement),
            None => connector,
        };
        BrokerIngestorStart {
            connector,
            instances: self.instances,
            acknowledgement: self.mode.acknowledgement(),
            buffered_intake: true,
            flush_each_intake: self.mode.is_ack(),
            client_mounts: client_mounts
                .get(
                    usize::try_from(ingestor.instance_index())
                        .assured("an ingestor unit index fits usize"),
                )
                .assured("rendering retains one mount slot per global instance")
                .clone()
                .into_iter()
                .collect(),
        }
        .open::<MqttTopicIngestor>(runtime, ingestor)
        .await
    }

    async fn resolve(
        &self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<ResolvedMqttIngestor, IngestorConnectorStartError> {
        let dispatcher = runtime.inner.remote_dispatcher.load();
        let read_ahead_overflow = runtime.inner.metrics.ingestor_read_ahead_dropped_payloads(
            &ingestor.domain,
            &ingestor.name,
            ingestor.unit,
            dispatcher.as_deref().map(RemoteDispatcher::local_node_id),
            ingestor.declared_input.entity_kind(),
        );
        let cached = match runtime.inner.executions.get(&ingestor.domain) {
            Some(execution) => match execution.mqtt_ingestor_clients.get(&ingestor.name) {
                Some(prepared) if prepared.declaration == *self => Some(prepared.rendered.clone()),
                _ => None,
            },
            None => None,
        };
        let rendered = match cached {
            Some(rendered) => rendered,
            None => {
                let mut entries = Vec::with_capacity(self.instances.get().arch_into());
                let mut mounts = Vec::with_capacity(self.instances.get().arch_into());
                for index in 0..self.instances.get() {
                    nervix_primitives::task::consume_budget().await;
                    let resolved = runtime
                        .resolve_client_config_with_instance(
                            &ingestor.domain,
                            self.client.mount.as_ref(),
                            &self.client.config,
                            index,
                        )
                        .change_context(IngestorConnectorStartError::ClientConfiguration)?;
                    entries.push(resolved.entries);
                    mounts.push(resolved.mounts);
                }
                let Some(dns) = runtime.dns() else {
                    return Err(Report::new(IngestorConnectorStartError::NodeDnsUnavailable));
                };
                let default_client_id =
                    format!("{}~{}", ingestor.domain.as_str(), ingestor.name.as_str());
                let connector = MqttTopicIngestorPlan::new(MqttTopicIngestorSettings {
                    instances: entries.into(),
                    share_group: default_client_id.clone(),
                    default_client_id,
                    topic: self.topic.as_str().to_string(),
                    session: self.session,
                    qos: self.qos,
                    acknowledgement_window: self.mode.acknowledgement_window(),
                    dns: dns.clone(),
                    read_ahead_overflow: read_ahead_overflow.clone(),
                });
                let rendered = Arc::new(RenderedMqttClient {
                    connector,
                    mounts: mounts.into(),
                });
                if let Some(mut execution) = runtime.inner.executions.get_mut(&ingestor.domain) {
                    execution.mqtt_ingestor_clients.insert(
                        ingestor.name.clone(),
                        PreparedMqttIngestorClient {
                            declaration: self.clone(),
                            rendered: rendered.clone(),
                        },
                    );
                }
                rendered
            }
        };
        let connector = rendered
            .connector
            .clone()
            .with_read_ahead_overflow(read_ahead_overflow);
        Ok(ResolvedMqttIngestor {
            connector,
            client_mounts: rendered.mounts.clone(),
        })
    }

    /// Retires this replaced input's broker state in one step where nothing puts the previous
    /// input back, as a domain rebuild: every obsolete session of this input best-effort, then every
    /// session the replacement changes, whose first failure fails the step.
    pub(super) async fn retire_before(
        &self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
        units: &[Unit],
        replacement: Option<&Self>,
    ) -> error_stack::Result<(), RuntimeError> {
        let ending = MqttInputTransition::instances_of(units);
        let transition =
            MqttInputTransition::resolve(runtime, ingestor, ending, self, replacement).await?;
        transition.report_previous_failure(runtime);
        transition.retire_previous(runtime).await;
        transition
            .prepare_replacement(runtime, ReplacementSessions::Changed)
            .await
    }

    /// Prepares the replacement of this input while the swap that replaces it can still be undone.
    /// It cleans only the replacement's changed sessions at a broker session this input never
    /// connects, so everything this input keeps at its broker stays for a restored previous
    /// ingestor. The first failure fails the swap. A configuration of this input that no longer
    /// resolves leaves every changed session to clean; the retirement that follows the swap
    /// reports it.
    pub(super) async fn prepare_replacement(
        &self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
        units: &[Unit],
        replacement: Option<&Self>,
    ) -> error_stack::Result<(), RuntimeError> {
        let ending = MqttInputTransition::instances_of(units);
        let transition =
            MqttInputTransition::resolve(runtime, ingestor, ending, self, replacement).await?;
        transition
            .prepare_replacement(runtime, ReplacementSessions::Unshared)
            .await
    }

    /// Retires this replaced input's broker sessions once the swap that replaced it can no longer
    /// be undone, before the replacement starts, and reports every failure. Returns the
    /// replacement's instances whose broker session now holds no subscription the replacement does
    /// not declare.
    pub(super) async fn retire_after_swap(
        &self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
        units: &[Unit],
        replacement: Option<&Self>,
    ) -> BTreeSet<u64> {
        let ending = MqttInputTransition::instances_of(units);
        let resolved =
            MqttInputTransition::resolve(runtime, ingestor, ending, self, replacement).await;
        let transition = match resolved {
            Ok(transition) => transition,
            Err(error) => {
                runtime.events().report_error(format!("{error:#}"));
                return BTreeSet::new();
            }
        };
        transition.report_previous_failure(runtime);
        let retired = transition.retire_previous(runtime).await;
        transition.clean_replacement_instances(&retired)
    }

    /// Retires, while the change that replaces this input can still fail, the broker sessions of
    /// `instances` that `replacement` keeps with the same subscription. Each may still hold a
    /// subscription a preceding input declared, which an instance that resumes it would read.
    /// Returns the instances whose session is gone; the first failure fails the change.
    async fn retire_kept_sessions(
        &self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
        instances: BTreeSet<u64>,
        replacement: &Self,
    ) -> error_stack::Result<BTreeSet<u64>, RuntimeError> {
        let transition =
            MqttInputTransition::resolve(runtime, ingestor, instances, self, Some(replacement))
                .await?;
        transition.retire_kept_sessions(runtime).await
    }

    /// The MQTT topic input `revision` declares for the ingestor `name`, if it reads one.
    fn declared_in<'revision>(
        revision: &'revision ExecutionRevision,
        name: &IngestorName,
    ) -> Option<&'revision Self> {
        let plan = revision.entrypoints.ingestor(name, Unit::FIRST_INSTANCE)?;
        let IngestorInputPlan::Transport(input) = &plan.input else {
            return None;
        };
        let IngestorConnectorPlan::Mqtt(MqttIngestorStartPlan::Topic(declaration)) = &input.source
        else {
            return None;
        };
        Some(declaration)
    }

    /// Whether a broker session this input's instances connect may still hold a subscription the
    /// `previous` input declared and this one does not: both keep their sessions, and the topic or
    /// the quality of service changed.
    pub(super) fn may_find_subscription_of(&self, previous: &Self) -> bool {
        let persistent = self.session == nervix_models::MqttSession::Persistent
            && previous.session == nervix_models::MqttSession::Persistent;
        persistent && (self.topic != previous.topic || self.qos != previous.qos)
    }
}

/// Which of a replacement's changed sessions its preparation cleans.
#[derive(Debug, Clone, Copy)]
enum ReplacementSessions {
    /// Every session the replacement changes, where nothing puts the previous input back.
    Changed,
    /// Only those at a broker session the previous input never connects, which a swap cleans while
    /// it can still be undone.
    Unshared,
}

/// One MQTT input's replacement on this node: both inputs resolved, and the replaced input's
/// instances the transition concerns here.
struct MqttInputTransition {
    domain: DomainName,
    ingestor: IngestorName,
    /// The replaced input with validated sessions, or why its configuration no longer serves.
    previous: error_stack::Result<ResolvedMqttIngestor, RuntimeError>,
    /// The replacement with validated sessions, absent when the input is removed.
    replacement: Option<ResolvedMqttIngestor>,
    ending: BTreeSet<u64>,
}

impl MqttInputTransition {
    /// The instance indices of an MQTT input's `units`.
    fn instances_of(units: &[Unit]) -> BTreeSet<u64> {
        let mut instances = BTreeSet::new();
        for unit in units {
            let index = match unit.kind() {
                nervix_models::UnitKind::Instance(index) => index,
                _ => unreachable!("MQTT plans declare instance units"),
            };
            instances.insert(index);
        }
        instances
    }

    /// Resolves both inputs and validates their sessions, for the replaced input's `ending`
    /// instances. The replacement must resolve; the replaced input's failure is kept for the step
    /// that reports it.
    async fn resolve(
        runtime: &Runtime,
        ingestor: &IngestorSpec,
        ending: BTreeSet<u64>,
        previous: &MqttTopicIngestorStartPlan,
        replacement: Option<&MqttTopicIngestorStartPlan>,
    ) -> error_stack::Result<Self, RuntimeError> {
        let context = || RuntimeError::RetireIngestorEntity {
            domain: ingestor.domain.clone(),
            ingestor: ingestor.name.clone(),
        };
        let replacement = match replacement {
            Some(replacement) => {
                let resolved = replacement
                    .resolve(runtime, ingestor)
                    .await
                    .change_context_lazy(context)?;
                resolved.connector.sessions().change_context_lazy(context)?;
                Some(resolved)
            }
            None => None,
        };
        // An invalid or unavailable previous configuration remains a reported cleanup failure.
        // The explicitly supplied replacement is prepared independently at its own endpoint.
        let previous = match previous.resolve(runtime, ingestor).await {
            Ok(resolved) => {
                let validation = resolved.connector.sessions().map(|_sessions| ());
                match validation {
                    Ok(()) => Ok(resolved),
                    Err(error) => Err(error.change_context(context())),
                }
            }
            Err(error) => Err(error.change_context(context())),
        };
        Ok(Self {
            domain: ingestor.domain.clone(),
            ingestor: ingestor.name.clone(),
            previous,
            replacement,
            ending,
        })
    }

    fn context(&self) -> RuntimeError {
        RuntimeError::RetireIngestorEntity {
            domain: self.domain.clone(),
            ingestor: self.ingestor.clone(),
        }
    }

    /// Reports why the replaced input's configuration no longer serves, when it does not.
    fn report_previous_failure(&self, runtime: &Runtime) {
        if let Err(error) = &self.previous {
            runtime.events().report_error(format!("{error:#}"));
        }
    }

    /// The replaced input's sessions, absent when its configuration no longer serves.
    fn previous_sessions(&self) -> Option<MqttTopicIngestorSessions<'_>> {
        let Ok(previous) = &self.previous else {
            return None;
        };
        let sessions = previous
            .connector
            .sessions()
            .verified("resolving the transition validated the replaced input's sessions");
        Some(sessions)
    }

    /// The replacement's sessions, absent when the input is removed.
    fn replacement_sessions(&self) -> Option<MqttTopicIngestorSessions<'_>> {
        let replacement = self.replacement.as_ref()?;
        let sessions = replacement
            .connector
            .sessions()
            .verified("resolving the transition validated the replacement's sessions");
        Some(sessions)
    }

    /// Retires every session of the replaced input's ending instances that the replacement does
    /// not keep with the same subscription, best-effort, and reports every failure. Returns the
    /// instances whose session is gone.
    async fn retire_previous(&self, runtime: &Runtime) -> BTreeSet<u64> {
        let Ok(previous) = &self.previous else {
            return BTreeSet::new();
        };
        let Some(sessions) = self.previous_sessions() else {
            return BTreeSet::new();
        };
        let replacement_sessions = self.replacement_sessions();
        let mut instances = BTreeSet::new();
        for instance in sessions.retiring_instances(replacement_sessions.as_ref()) {
            if self.ending.contains(&instance) {
                instances.insert(instance);
            }
        }
        let retirements = previous.retire_instances(instances).await;
        for error in retirements.failures {
            nervix_primitives::task::consume_budget().await;
            let error = error.change_context(self.context());
            runtime.events().report_error(format!("{error:#}"));
        }
        retirements.retired
    }

    /// The replacement's changed sessions of the ending instances that `reach` selects. Without
    /// the replaced input's sessions every changed session counts as unshared.
    fn changed_replacement_instances(&self, reach: ReplacementSessions) -> BTreeSet<u64> {
        let Some(sessions) = self.replacement_sessions() else {
            return BTreeSet::new();
        };
        let previous_sessions = self.previous_sessions();
        let unshared = sessions.unshared_instances(previous_sessions.as_ref());
        let mut instances = BTreeSet::new();
        for instance in sessions.retiring_instances(previous_sessions.as_ref()) {
            if !self.ending.contains(&instance) {
                continue;
            }
            if let ReplacementSessions::Unshared = reach
                && !unshared.contains(&instance)
            {
                continue;
            }
            instances.insert(instance);
        }
        instances
    }

    /// Cleans the replacement's changed sessions that `reach` selects, so the replacement starts
    /// from a clean session at its endpoint even when cleanup at the preceding endpoint failed.
    /// Sessions unchanged by the transition stay. The first failure is returned and the others are
    /// reported.
    async fn prepare_replacement(
        &self,
        runtime: &Runtime,
        reach: ReplacementSessions,
    ) -> error_stack::Result<(), RuntimeError> {
        let Some(replacement) = self.replacement.as_ref() else {
            return Ok(());
        };
        let instances = self.changed_replacement_instances(reach);
        let retirements = replacement.retire_instances(instances).await;
        self.fail_on_first(runtime, retirements.failures).await
    }

    /// Retires the sessions of the ending instances that the replacement keeps with the same
    /// subscription, through the replaced input, and returns the instances whose session is gone.
    /// The first failure is returned and the others are reported. Without the replaced input's
    /// sessions it retires nothing: the replacement's own preparation then counts every one of its
    /// sessions as changed and cleans it.
    async fn retire_kept_sessions(
        &self,
        runtime: &Runtime,
    ) -> error_stack::Result<BTreeSet<u64>, RuntimeError> {
        let Ok(previous) = &self.previous else {
            return Ok(BTreeSet::new());
        };
        let Some(sessions) = self.previous_sessions() else {
            return Ok(BTreeSet::new());
        };
        let replacement_sessions = self.replacement_sessions();
        let mut changed = BTreeSet::new();
        for instance in sessions.retiring_instances(replacement_sessions.as_ref()) {
            changed.insert(instance);
        }
        let mut kept = BTreeSet::new();
        for instance in &self.ending {
            if !changed.contains(instance) {
                kept.insert(*instance);
            }
        }
        let retirements = previous.retire_instances(kept).await;
        self.fail_on_first(runtime, retirements.failures).await?;
        Ok(retirements.retired)
    }

    /// Fails with the first of `failures`, reporting the others.
    async fn fail_on_first(
        &self,
        runtime: &Runtime,
        failures: Vec<Report<MqttTopicIngestorError>>,
    ) -> error_stack::Result<(), RuntimeError> {
        let mut first_failure = None;
        for error in failures {
            nervix_primitives::task::consume_budget().await;
            let error = error.change_context(self.context());
            if first_failure.is_none() {
                first_failure = Some(error);
            } else {
                runtime.events().report_error(format!("{error:#}"));
            }
        }
        match first_failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// The replacement's instances whose broker session holds no subscription the replacement does
    /// not declare, once the swap prepared the replacement and `retired` the replaced input's
    /// sessions: the unshared sessions its preparation cleaned, and the sessions it shares with a
    /// retired instance. Without the replaced input's sessions it claims none.
    fn clean_replacement_instances(&self, retired: &BTreeSet<u64>) -> BTreeSet<u64> {
        let Some(sessions) = self.replacement_sessions() else {
            return BTreeSet::new();
        };
        let Some(previous_sessions) = self.previous_sessions() else {
            return BTreeSet::new();
        };
        let unshared = sessions.unshared_instances(Some(&previous_sessions));
        let mut clean = sessions.instances_sharing(&previous_sessions, retired);
        for instance in sessions.retiring_instances(Some(&previous_sessions)) {
            if self.ending.contains(&instance) && unshared.contains(&instance) {
                clean.insert(instance);
            }
        }
        clean
    }
}

impl Runtime {
    /// The key of the MQTT ingestor `name`'s owed retirements.
    fn owed_mqtt_session_retirements_key(
        domain: &DomainName,
        name: &IngestorName,
    ) -> DomainNodeRef {
        DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ModelName::from(name))
    }

    /// The units of the MQTT ingestor `name` that `revision` runs on this node, by instance index.
    fn local_mqtt_units(
        revision: &ExecutionRevision,
        name: &IngestorName,
        #[cfg_attr(
            nervix_lint,
            nervix::identity(
                node,
                reason = "identifies the node applying this execution revision"
            )
        )]
        local_node_id: Option<&ClusterNodeName>,
    ) -> BTreeMap<u64, Unit> {
        let mut units = BTreeMap::new();
        let identity = NodeRef::new(ModelKind::Ingestor, ModelName::from(name));
        let Some(scheduled) = revision.nodes.get(&identity) else {
            return units;
        };
        for unit in scheduled.units.keys() {
            if !Self::scheduled_unit_executes_locally(scheduled, *unit, local_node_id) {
                continue;
            }
            if let nervix_models::UnitKind::Instance(index) = unit.kind() {
                units.insert(index, *unit);
            }
        }
        units
    }

    /// The retirement an instance of an MQTT ingestor still owes its broker session here before it
    /// first connects, if any.
    pub(super) fn outstanding_mqtt_session_retirement(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
        instance: u64,
    ) -> Option<MqttSessionRetirement> {
        let key = Self::owed_mqtt_session_retirements_key(domain, ingestor);
        let owed = self.inner.mqtt_session_retirements.get(&key)?;
        let retirement = owed.instances.get(&instance)?;
        if !retirement.is_outstanding() {
            return None;
        }
        Some(retirement.clone())
    }

    /// Records, once the swap from `previous` to `desired` can no longer be undone, the retirement
    /// each instance of the MQTT ingestor `name` that `desired` places on this node owes its broker
    /// session before it first connects: the session may still hold the subscription `previous`
    /// declared. These replace whatever the instances owed before, which the new retirements
    /// include.
    pub(super) fn owe_mqtt_session_retirements(
        &self,
        domain: &DomainName,
        name: &IngestorName,
        previous: &ExecutionRevision,
        desired: &ExecutionRevision,
        #[cfg_attr(
            nervix_lint,
            nervix::identity(
                node,
                reason = "identifies the node applying this execution revision"
            )
        )]
        local_node_id: Option<&ClusterNodeName>,
    ) {
        let Some(replacement) = MqttTopicIngestorStartPlan::declared_in(desired, name) else {
            return;
        };
        let Some(preceding) = MqttTopicIngestorStartPlan::declared_in(previous, name) else {
            return;
        };
        if !replacement.may_find_subscription_of(preceding) {
            return;
        }
        let units = Self::local_mqtt_units(desired, name, local_node_id);
        let Some(unit) = units.values().next() else {
            return;
        };
        let Some(plan) = desired.entrypoints.ingestor(name, *unit) else {
            return;
        };
        let mut instances = BTreeMap::new();
        for instance in units.keys() {
            instances.insert(*instance, MqttSessionRetirement::outstanding());
        }
        self.inner.mqtt_session_retirements.insert(
            Self::owed_mqtt_session_retirements_key(domain, name),
            OwedMqttSessionRetirements {
                declaration: replacement.clone(),
                ingestor: plan.ingestor.clone(),
                instances,
            },
        );
    }

    /// Records that the broker sessions of `instances` of the MQTT ingestor `name` hold no
    /// subscription its declaration does not, so those instances owe them no retirement.
    pub(super) fn complete_mqtt_session_retirements(
        &self,
        domain: &DomainName,
        name: &IngestorName,
        instances: &BTreeSet<u64>,
    ) {
        let key = Self::owed_mqtt_session_retirements_key(domain, name);
        let Some(owed) = self.inner.mqtt_session_retirements.get(&key) else {
            return;
        };
        for instance in instances {
            if let Some(retirement) = owed.instances.get(instance) {
                retirement.complete();
            }
        }
    }

    /// Forgets the retirements owed here for MQTT ingestors of `domain` that `desired` leaves to no
    /// instance on this node: an instance it runs elsewhere had its session retired before it went,
    /// owes its next node a retirement of that node's own or uses another session, and an ingestor
    /// it no longer declares as an MQTT topic input resumes no session. Completed retirements go
    /// too. What stays is owed under `desired`'s input from now on: no instance has subscribed under
    /// it while the retirement is outstanding, so retiring the session through it loses nothing.
    pub(super) fn keep_mqtt_session_retirements_for(
        &self,
        domain: &DomainName,
        desired: &ExecutionRevision,
        #[cfg_attr(
            nervix_lint,
            nervix::identity(
                node,
                reason = "identifies the node applying this execution revision"
            )
        )]
        local_node_id: Option<&ClusterNodeName>,
    ) {
        self.inner.mqtt_session_retirements.retain(|key, owed| {
            if key.domain != *domain {
                return true;
            }
            let name = IngestorName::from(&key.node.identifier);
            let Some(declaration) = MqttTopicIngestorStartPlan::declared_in(desired, &name) else {
                return false;
            };
            let units = Self::local_mqtt_units(desired, &name, local_node_id);
            owed.instances.retain(|instance, retirement| {
                units.contains_key(instance) && retirement.is_outstanding()
            });
            let Some(instance) = owed.instances.keys().next().copied() else {
                return false;
            };
            let Some(unit) = units.get(&instance) else {
                return false;
            };
            let Some(plan) = desired.entrypoints.ingestor(&name, *unit) else {
                return false;
            };
            if owed.declaration != *declaration {
                owed.declaration = declaration.clone();
            }
            if owed.ingestor != plan.ingestor {
                owed.ingestor = plan.ingestor.clone();
            }
            true
        });
    }

    /// Forgets every retirement owed here for the MQTT ingestors of `domain`, which no longer
    /// exists on this node.
    pub(in crate::runtime) fn clear_mqtt_session_retirements(&self, domain: &DomainName) {
        self.inner
            .mqtt_session_retirements
            .retain(|key, _| key.domain != *domain);
    }

    /// Retires, while the change to `desired` can still fail, the broker session of each instance
    /// of the MQTT ingestor `name` that still owes its session a retirement here, that `desired`
    /// no longer runs on this node and whose session `desired` keeps with the same subscription.
    /// The node that runs such an instance next cannot know that its session may still hold a
    /// subscription a preceding input declared, so this node retires the session before it lets
    /// the instance go. The first failure fails the change on this node, which holds a move until
    /// this node prepares the revision; a retired session's retirement is complete.
    pub(in crate::runtime) async fn settle_released_mqtt_session_retirements(
        &self,
        domain: &DomainName,
        name: &IngestorName,
        desired: &ExecutionRevision,
        #[cfg_attr(
            nervix_lint,
            nervix::identity(
                node,
                reason = "identifies the node applying this execution revision"
            )
        )]
        local_node_id: Option<&ClusterNodeName>,
    ) -> error_stack::Result<(), RuntimeError> {
        let units = Self::local_mqtt_units(desired, name, local_node_id);
        let key = Self::owed_mqtt_session_retirements_key(domain, name);
        let released = {
            let Some(owed) = self.inner.mqtt_session_retirements.get(&key) else {
                return Ok(());
            };
            let mut instances = BTreeMap::new();
            for (instance, retirement) in &owed.instances {
                if !units.contains_key(instance) && retirement.is_outstanding() {
                    instances.insert(*instance, retirement.clone());
                }
            }
            if instances.is_empty() {
                return Ok(());
            }
            OwedMqttSessionRetirements {
                declaration: owed.declaration.clone(),
                ingestor: owed.ingestor.clone(),
                instances,
            }
        };
        let Some(replacement) = MqttTopicIngestorStartPlan::declared_in(desired, name) else {
            return Ok(());
        };
        let instances = released.instances.keys().copied().collect();
        let retired = released
            .declaration
            .retire_kept_sessions(self, &released.ingestor, instances, replacement)
            .await?;
        for instance in retired {
            if let Some(retirement) = released.instances.get(&instance) {
                retirement.complete();
            }
        }
        Ok(())
    }

    /// Settles, before a rebuild of `domain` installs `desired`, the retirements MQTT ingestor
    /// instances owe their broker sessions here: it retires the sessions of instances `desired`
    /// runs elsewhere under the same subscription, then forgets what `desired` leaves to no
    /// instance here. What an instance it still runs here owes outlives the rebuild. The first
    /// failure fails the rebuild.
    pub(in crate::runtime) async fn settle_mqtt_session_retirements_for_rebuild(
        &self,
        domain: &DomainName,
        desired: Option<&ExecutionRevision>,
        #[cfg_attr(
            nervix_lint,
            nervix::identity(
                node,
                reason = "identifies the node applying this execution revision"
            )
        )]
        local_node_id: Option<&ClusterNodeName>,
    ) -> error_stack::Result<(), RuntimeError> {
        let Some(desired) = desired else {
            self.clear_mqtt_session_retirements(domain);
            return Ok(());
        };
        let mut names = Vec::new();
        for entry in self.inner.mqtt_session_retirements.iter() {
            if entry.key().domain == *domain {
                names.push(IngestorName::from(&entry.key().node.identifier));
            }
        }
        for name in names {
            nervix_primitives::task::consume_budget().await;
            Box::pin(self.settle_released_mqtt_session_retirements(
                domain,
                &name,
                desired,
                local_node_id,
            ))
            .await?;
        }
        self.keep_mqtt_session_retirements_for(domain, desired, local_node_id);
        Ok(())
    }
}
