//! A DNS authority on a loopback UDP port that answers from a zone the test controls.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One loopback UDP socket, the zone it answers from, a count of the questions it
//!   received for each name, and the bounded stop that ends it.
//! - **Depends on.** Tokio's UDP socket and Hickory's DNS message grammar.
//! - **Must not know.** Nervix, resolvers, or what the names it serves are used for.
//!
//! # Answers
//!
//! Every name in the zone answers with addresses and a TTL, as a name that does not exist, as a name
//! with no address, with a refusal, or not at all. The two negative answers carry an SOA record whose TTL is the
//! negative TTL, so a resolver caches them for exactly that long. A name outside the zone does not
//! exist, with a negative TTL of zero, which a resolver does not cache. A zone change applies to the
//! next question; a resolver that cached the previous answer keeps it until its TTL expires.
//!
//! # Bounds
//!
//! The authority counts questions for at most [`MAX_COUNTED_NAMES`] distinct names; questions for
//! any further name are counted only in the total. Stopping waits [`AUTHORITY_STOP_BUDGET`] for the
//! answer loop to end before it aborts and joins it.

use std::{
    collections::BTreeMap,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use hickory_proto::{
    op::{Message, ResponseCode},
    rr::{
        Name, RData, Record, RecordType,
        rdata::{A, AAAA, SOA},
    },
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_recovery::Discarded as _;
use parking_lot::Mutex;
use tokio::{net::UdpSocket, sync::oneshot, task::JoinHandle, time::timeout};

/// The distinct names whose questions the authority counts one by one.
pub const MAX_COUNTED_NAMES: usize = 1_024;
/// How long stopping waits for the answer loop to end before aborting it. The loop waits on nothing
/// but its socket and its stop signal, so this only waits for the scheduler to reach it.
pub const AUTHORITY_STOP_BUDGET: Duration = Duration::from_secs(1);
/// The largest question the authority reads. Resolvers ask with at most an EDNS payload of this size.
const MAX_MESSAGE_BYTES: usize = 4_096;
/// The zone every SOA record names as its apex.
const ZONE_APEX: &str = "authority.test.";

/// How the authority answers one name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsAnswer {
    /// Every address of the name, answered to the A or AAAA question of its family, with `ttl`.
    Addresses {
        addresses: Vec<IpAddr>,
        ttl: Duration,
    },
    /// The name does not exist (NXDOMAIN), cached for `negative_ttl`.
    NameNotFound { negative_ttl: Duration },
    /// The name exists with no address (NOERROR without answers), cached for `negative_ttl`.
    NoAddresses { negative_ttl: Duration },
    /// The name server refuses the question (REFUSED).
    Refused,
    /// Questions for the name are received and never answered.
    Silent,
}

/// A running authority. Stop it with [`DnsAuthority::stop`]; dropping it aborts the answer loop.
#[derive(Debug)]
pub struct DnsAuthority {
    address: SocketAddr,
    state: Arc<AuthorityState>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

#[derive(Debug, Default)]
struct AuthorityState {
    zone: Mutex<BTreeMap<Name, DnsAnswer>>,
    questions: Mutex<QuestionCounts>,
}

#[derive(Debug, Default)]
struct QuestionCounts {
    by_name: BTreeMap<Name, u64>,
    total: u64,
}

impl DnsAuthority {
    /// Start answering on the UDP address `bind`, a loopback address whose port may be zero.
    pub async fn start(bind: SocketAddr) -> io::Result<Self> {
        let socket = UdpSocket::bind(bind).await?;
        let address = socket.local_addr()?;
        let state = Arc::new(AuthorityState::default());
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(Self::answer(socket, Arc::clone(&state), stopped));
        Ok(Self {
            address,
            state,
            stop: Some(stop),
            task: Some(task),
        })
    }

    /// Start answering on an unused UDP port of `127.0.0.1`.
    pub async fn start_on_loopback() -> io::Result<Self> {
        Self::start(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await
    }

    /// The address a resolver sends its questions to.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Answer `name` with `answer` from the next question on.
    pub fn set(&self, name: &str, answer: DnsAnswer) {
        let name = Self::zone_name(name);
        self.state.zone.lock().insert(name, answer);
    }

    /// Questions received for `name`, of any record type.
    pub fn questions_for(&self, name: &str) -> u64 {
        let name = Self::zone_name(name);
        let questions = self.state.questions.lock();
        let Some(count) = questions.by_name.get(&name) else {
            return 0;
        };
        *count
    }

    /// Questions received for every name together.
    pub fn total_questions(&self) -> u64 {
        self.state.questions.lock().total
    }

    /// Stop answering and wait, within [`AUTHORITY_STOP_BUDGET`], for the answer loop to end.
    pub async fn stop(mut self) {
        if let Some(stop) = self.stop.take() {
            stop.send(())
                .discarded("the answer loop may already have ended with its socket");
        }
        let Some(mut task) = self.task.take() else {
            return;
        };
        if timeout(AUTHORITY_STOP_BUDGET, &mut task).await.is_err() {
            task.abort();
            task.await
                .discarded("an aborted answer loop ends with a cancellation");
        }
    }

    /// `name` as the zone keys it: lowercase and fully qualified.
    fn zone_name(name: &str) -> Name {
        let mut parsed = match Name::from_utf8(name) {
            Ok(parsed) => parsed,
            Err(error) => panic!("fixture name '{name}' is not a DNS name: {error}"),
        };
        parsed.set_fqdn(true);
        parsed.to_lowercase()
    }

    async fn answer(
        socket: UdpSocket,
        state: Arc<AuthorityState>,
        mut stopped: oneshot::Receiver<()>,
    ) {
        let mut buffer = vec![0_u8; MAX_MESSAGE_BYTES];
        loop {
            tokio::task::consume_budget().await;
            let received = tokio::select! {
                _ = &mut stopped => return,
                received = socket.recv_from(&mut buffer) => received,
            };
            let Ok((length, peer)) = received else {
                return;
            };
            let Some(question) = buffer.get(..length) else {
                continue;
            };
            let Some(response) = state.respond(question) else {
                continue;
            };
            if socket.send_to(&response, peer).await.is_err() {
                return;
            }
        }
    }
}

impl Drop for DnsAuthority {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl AuthorityState {
    /// The encoded response to one received message, or `None` when the message is not a question
    /// or its name is silent.
    fn respond(&self, bytes: &[u8]) -> Option<Vec<u8>> {
        let request = Message::from_vec(bytes).ok()?;
        let query = request.queries.first()?.clone();
        let mut name = query.name().clone();
        name.set_fqdn(true);
        let name = name.to_lowercase();
        self.count(&name);
        let answer = self.zone.lock().get(&name).cloned();
        let answer = match answer {
            Some(answer) => answer,
            None => DnsAnswer::NameNotFound {
                negative_ttl: Duration::ZERO,
            },
        };
        let mut response = Message::response(request.metadata.id, request.metadata.op_code);
        response.metadata.authoritative = true;
        response.metadata.recursion_desired = request.metadata.recursion_desired;
        response.metadata.recursion_available = true;
        response.queries = request.queries.clone();
        match answer {
            DnsAnswer::Silent => return None,
            DnsAnswer::Addresses { addresses, ttl } => {
                let ttl = seconds(ttl);
                for address in addresses {
                    let data = match (query.query_type(), address) {
                        (RecordType::A, IpAddr::V4(ip)) => RData::A(A(ip)),
                        (RecordType::AAAA, IpAddr::V6(ip)) => RData::AAAA(AAAA(ip)),
                        _ => continue,
                    };
                    response.add_answer(Record::from_rdata(query.name().clone(), ttl, data));
                }
            }
            DnsAnswer::NameNotFound { negative_ttl } => {
                response.metadata.response_code = ResponseCode::NXDomain;
                response.add_authority(start_of_authority(negative_ttl));
            }
            DnsAnswer::NoAddresses { negative_ttl } => {
                response.add_authority(start_of_authority(negative_ttl));
            }
            DnsAnswer::Refused => {
                response.metadata.response_code = ResponseCode::Refused;
            }
        }
        response.to_vec().ok()
    }

    fn count(&self, name: &Name) {
        let mut questions = self.questions.lock();
        questions.total = questions
            .total
            .checked_add(1)
            .assured("a test cannot ask 2^64 questions");
        let counted = questions.by_name.len();
        if let Some(count) = questions.by_name.get_mut(name) {
            *count = count
                .checked_add(1)
                .assured("a test cannot ask 2^64 questions");
        } else if counted < MAX_COUNTED_NAMES {
            questions.by_name.insert(name.clone(), 1);
        }
    }
}

/// An SOA record whose TTL and minimum are both `negative_ttl`, so a resolver caches the negative
/// answer it accompanies for exactly that long.
fn start_of_authority(negative_ttl: Duration) -> Record {
    let apex = Name::from_ascii(ZONE_APEX).assured("the zone apex is a DNS name");
    let ttl = seconds(negative_ttl);
    let soa = SOA::new(apex.clone(), apex.clone(), 1, 3_600, 600, 86_400, ttl);
    Record::from_rdata(apex, ttl, RData::SOA(soa))
}

/// `duration` as a DNS TTL, in whole seconds.
fn seconds(duration: Duration) -> u32 {
    u32::try_from(duration.as_secs()).assured("a fixture TTL fits a DNS TTL")
}
