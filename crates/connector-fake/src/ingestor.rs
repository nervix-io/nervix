//! Scripted broker and paced ingestor fixtures and contract-level observations.
//!
//! Layer: test harness.
//! - **Owns.** Queued and channel input, typed metadata, lifecycle scripts and transport observations.
//! - **Depends on.** The connector contract, vocabulary and selected execution primitives.
//! - **Must not know.** Runtime intake adapters, graph planning, real drivers or ACK trees.

use std::{
    collections::{BTreeMap, VecDeque},
    future,
};

use async_trait::async_trait;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::*;
use nervix_models::Timestamp;
use nervix_primitives::sync::{Arc, Notify, blocking::Mutex, mpsc, watch};

/// Which intake the loop asked the host for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordedIntake {
    Unacknowledged,
    Acknowledged,
}

#[derive(Default)]
pub struct IngestorObservations {
    /// Every resume, replay and poll in the order the loop performed them.
    pub sequence: Vec<&'static str>,
    /// Replays the host still has to deliver, as a quiesce buffer would.
    pub pending_replays: usize,
    pub requests: Vec<IngestorBatchRequest>,
    pub intake: Vec<(RecordedIntake, usize)>,
    pub acknowledged: Vec<Vec<u64>>,
    pub rejected: Vec<Vec<u64>>,
    pub rejection_notice: Option<Arc<Notify>>,
    pub unconfirmed_stops: Vec<usize>,
    pub poll_errors: Vec<String>,
    pub reported_errors: Vec<String>,
    pub ack_waits: usize,
    pub ack_outcomes: VecDeque<IngestorAcknowledgementOutcome>,
    pub resumes: usize,
    pub suspends: usize,
    pub closes: usize,
    pub flushes: usize,
    pub quiesce_waits: usize,
    pub suspension_waits: usize,
    pub transient_clears: usize,
    pub ready: usize,
    pub unready: usize,
}

/// An owned transport message, including the typed metadata its header scope exposes.
#[derive(Clone)]
pub struct FakeMessage {
    pub position: u64,
    pub payload: Vec<u8>,
    pub transport: MessageTransport,
}

#[derive(Clone)]
pub enum MessageTransport {
    Headers(RetainedIngestHeaders),
    Kafka {
        topic: String,
        partition: i32,
        offset: i64,
        headers: RetainedIngestHeaders,
    },
    Syslog {
        peer_addr: std::net::SocketAddr,
    },
    Websockets {
        frame_type: nervix_models::WebsocketsFrameType,
        headers: Arc<RetainedIngestHeaders>,
    },
}

impl FakeMessage {
    pub fn new(position: u64, payload: Vec<u8>) -> Self {
        Self {
            position,
            payload,
            transport: MessageTransport::Headers(RetainedIngestHeaders::none()),
        }
    }
}

impl IngestMessageHeaders for FakeMessage {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        match &self.transport {
            MessageTransport::Headers(headers) | MessageTransport::Kafka { headers, .. } => {
                headers.visit(visit)
            }
            MessageTransport::Websockets { headers, .. } => headers.visit(visit),
            MessageTransport::Syslog { .. } => {}
        }
    }
}

impl IngestorMessage for FakeMessage {
    type Position = u64;
    fn payload(&self) -> &[u8] {
        &self.payload
    }
    fn position(&self) -> &Self::Position {
        &self.position
    }
    fn headers(&self) -> &dyn IngestMessageHeaders {
        self
    }
    fn metadata(&self) -> IngestMetadataRow<'_> {
        match &self.transport {
            MessageTransport::Headers(_) => IngestMetadataRow::Headers { headers: self },
            MessageTransport::Kafka {
                topic,
                partition,
                offset,
                ..
            } => IngestMetadataRow::Kafka {
                topic,
                partition: *partition,
                offset: *offset,
                headers: self,
            },
            MessageTransport::Syslog { peer_addr } => IngestMetadataRow::Syslog {
                peer_addr: *peer_addr,
            },
            MessageTransport::Websockets {
                frame_type,
                headers,
            } => IngestMetadataRow::Websockets {
                frame_type: *frame_type,
                headers,
            },
        }
    }
}

pub struct FakeIngestor {
    pub input: IngestorInput,
    pub suspend_results: VecDeque<IngestorResult<()>>,
    pub close_results: VecDeque<IngestorResult<()>>,
    pub polls: VecDeque<IngestorResult<IngestorPoll>>,
    pub acknowledge_results: VecDeque<IngestorResult<()>>,
    pub stop_after_poll: Option<watch::Sender<bool>>,
    reading: bool,
    pub resume_required: bool,
    pub resume_results: VecDeque<IngestorResult<IngestorResume>>,
    pub reject_failures_left: usize,
    pub replay_rejected: bool,
    /// Replays each resume leaves pending, as a quiesce released during it would.
    pub replays_pending_after_resume: usize,
    pub resume_gate: Option<Arc<Notify>>,
    pub observations: Arc<Mutex<IngestorObservations>>,
}

#[async_trait]
impl IngestorConnector for FakeIngestor {
    const KIND: nervix_models::IngestorEntityKind =
        nervix_models::IngestorEntityKind::Kafka(nervix_models::KafkaIngestorEntity::Topic);

    type Plan = FakeIngestorPlan;

    async fn open(plan: &Self::Plan, instance_index: u64) -> IngestorResult<Self> {
        match plan.instances.lock().remove(&instance_index) {
            Some(result) => result,
            None => Err(Report::new(IngestorError::Open { connector: "fake" })),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "The test harness records the resume before entering its scripted readiness \
                      gate",
            key = "one scripted ingestor observation log",
            bound = "one synchronous resume observation; guards end before any await"
        )
    )]
    async fn resume(&mut self) -> IngestorResult<IngestorResume> {
        {
            let mut observations = self.observations.lock();
            observations.resumes += 1;
            observations.sequence.push("resume");
            observations.pending_replays = observations
                .pending_replays
                .checked_add(self.replays_pending_after_resume)
                .verified("the test leaves at most a few replays pending");
        }
        if let Some(started) = self.resume_gate.as_ref() {
            started.notify_one();
            future::pending::<()>().await;
        }
        match self.resume_results.pop_front() {
            Some(result) => result,
            None => Ok(IngestorResume::Ready),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "The test harness records the suspended instance after checking its read \
                      ownership",
            key = "one scripted ingestor observation log",
            bound = "one synchronous suspension-counter update; guards end before any await"
        )
    )]
    async fn suspend(&mut self) -> IngestorResult<()> {
        assert!(
            !self.reading,
            "the host suspended an ingestor after dropping its read in flight"
        );
        self.observations.lock().suspends += 1;
        match self.suspend_results.pop_front() {
            Some(result) => result,
            None => Ok(()),
        }
    }

    async fn close(&mut self) -> IngestorResult<()> {
        assert!(
            !self.reading,
            "the host closed an ingestor after dropping its read in flight"
        );
        self.observations.lock().closes += 1;
        match self.close_results.pop_front() {
            Some(result) => result,
            None => Ok(()),
        }
    }
}

#[async_trait]
impl BrokerIngestorConnector for FakeIngestor {
    type Message = FakeMessage;
    type Position = u64;

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "The test harness records the read request before waiting for input or \
                      cancellation",
            key = "one scripted ingestor observation log",
            bound = "one synchronous request observation; guards end before any await"
        )
    )]
    async fn next_batch(
        &mut self,
        request: IngestorBatchRequest,
        cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<IngestorBatch<Self::Message>> {
        {
            let mut observations = self.observations.lock();
            observations.requests.push(request);
            observations.sequence.push("next_batch");
        }
        if self.resume_required {
            self.resume_required = false;
            return Ok(IngestorBatch::ResumeRequired);
        }
        assert!(
            !self.reading,
            "the host dropped a read in flight before starting another"
        );
        self.reading = true;
        let batch = match &mut self.input {
            IngestorInput::Queued(messages) => {
                if messages.is_empty() {
                    IngestorBatch::Closed
                } else {
                    let count = messages.len().min(request.max_messages.get());
                    IngestorBatch::Messages(messages.drain(..count).collect())
                }
            }
            IngestorInput::Channel(messages) => {
                match cancellation.unless_cancelled(messages.recv()).await {
                    Some(Some(message)) => IngestorBatch::Messages(vec![message]),
                    Some(None) => IngestorBatch::Closed,
                    None => IngestorBatch::Interrupted {
                        gathered: Vec::new(),
                    },
                }
            }
        };
        self.reading = false;
        Ok(batch)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "The test harness records exactly the transport positions the host asks to \
                      acknowledge",
            key = "one scripted ingestor observation log",
            bound = "one synchronous copy of the caller-owned position slice; guards end before \
                     any await"
        )
    )]
    async fn acknowledge(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        self.observations
            .lock()
            .acknowledged
            .push(positions.to_vec());
        match self.acknowledge_results.pop_front() {
            Some(result) => result,
            None => Ok(()),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "The test harness records exactly the positions before its scripted \
                      rejection or replay",
            key = "one scripted ingestor observation log",
            bound = "one synchronous copy of the caller-owned position slice; guards end before \
                     any await"
        )
    )]
    async fn reject(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        let notice = {
            let mut record = self.observations.lock();
            record.rejected.push(positions.to_vec());
            record.rejection_notice.clone()
        };
        if let Some(notice) = notice {
            notice.notify_one();
        }
        if self.reject_failures_left > 0 {
            self.reject_failures_left -= 1;
            return Err(Report::new(IngestorError::Reject { connector: "fake" }));
        }
        if self.replay_rejected {
            for position in positions.iter().rev() {
                let IngestorInput::Queued(messages) = &mut self.input else {
                    panic!("replay is scripted only for a queued ingestor");
                };
                messages.push_front(FakeMessage::new(
                    *position,
                    vec![u8::try_from(*position).verified("the test positions are all below 256")],
                ));
            }
        }
        Ok(())
    }
}

/// A closed queue ends immediately; a channel remains open until its sender drops or the host cancels.
pub enum IngestorInput {
    Queued(VecDeque<FakeMessage>),
    Channel(mpsc::Receiver<FakeMessage>),
}

/// Each configured instance is opened exactly once, including a scripted open refusal.
pub struct FakeIngestorPlan {
    instances: Mutex<BTreeMap<u64, IngestorResult<FakeIngestor>>>,
}

impl FakeIngestorPlan {
    pub fn new(instances: BTreeMap<u64, IngestorResult<FakeIngestor>>) -> Self {
        Self {
            instances: Mutex::new(instances),
        }
    }
}

impl FakeIngestor {
    pub fn empty(observations: Arc<Mutex<IngestorObservations>>) -> Self {
        Self {
            input: IngestorInput::Queued(VecDeque::new()),
            suspend_results: VecDeque::new(),
            close_results: VecDeque::new(),
            polls: VecDeque::new(),
            acknowledge_results: VecDeque::new(),
            stop_after_poll: None,
            reading: false,
            resume_required: false,
            resume_results: VecDeque::new(),
            reject_failures_left: 0,
            replay_rejected: false,
            replays_pending_after_resume: 0,
            resume_gate: None,
            observations,
        }
    }

    pub fn new(messages: mpsc::Receiver<FakeMessage>) -> Self {
        let mut connector = Self::empty(Arc::new(Mutex::new(IngestorObservations::default())));
        connector.input = IngestorInput::Channel(messages);
        connector
    }

    pub fn queued(&mut self) -> &mut VecDeque<FakeMessage> {
        match &mut self.input {
            IngestorInput::Queued(messages) => messages,
            IngestorInput::Channel(_) => panic!("this fixture reads from a channel"),
        }
    }
}

#[async_trait]
impl PacedIngestorConnector for FakeIngestor {
    async fn poll(&mut self, scheduled_at: Timestamp) -> IngestorResult<IngestorPoll> {
        if let Some(shutdown) = &self.stop_after_poll {
            shutdown.send_replace(true);
        }
        if let Some(poll) = self.polls.pop_front() {
            return poll;
        }
        let message = match &mut self.input {
            IngestorInput::Queued(messages) => messages
                .pop_front()
                .assured("the fixture scripts every paced poll"),
            IngestorInput::Channel(messages) => messages
                .recv()
                .await
                .assured("the check keeps its payload sender until shutdown"),
        };
        let headers = RetainedIngestHeaders::capture(&message);
        Ok(IngestorPoll {
            messages: vec![IngestorPollMessage {
                payload: message.payload,
                headers,
                metadata: IngestorPollMetadata::Headers,
            }],
            failures: Vec::new(),
            observed_at: scheduled_at,
        })
    }
}
