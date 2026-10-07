//! An in-memory browser for the backup dialog's tests: scripted console connections, timers that
//! elapse at once, and archives, archive files and session storage held in memory.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Connections that answer from a script and record what was sent, timers that elapse
//!   at once and are recorded, archives kept and saved in memory, archive files held as bytes, and
//!   a storage map.
//! - **Depends on.** The browser traits the dialog's drivers and its pending backup record speak
//!   to.
//! - **Must not know.** What a download, a restore or a record decides.

use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    future::{Future, pending},
    rc::Rc,
    time::Duration,
};

use meticulous::OptionExt as _;

use super::{
    ArchiveFile, ArchiveSaveFailed, Browser, CallSocket, DownloadedArchive, Received,
    RecordStorage, SocketFailed,
};

/// One message a scripted server sends, once the connection has sent `after_sent` messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Reply {
    pub(crate) after_sent: usize,
    pub(crate) received: Received,
}

/// How one connection the in-memory browser opens behaves.
#[derive(Debug, Default)]
pub(crate) struct Script {
    /// What the server sends, in order. A server that has nothing left to send stays silent.
    pub(crate) replies: VecDeque<Reply>,
    /// How many messages the connection takes before a send fails; `None` takes every one.
    pub(crate) accepted: Option<usize>,
    /// What the connection reports queued, one reading per look; once the readings run out,
    /// nothing is queued.
    pub(crate) buffered: VecDeque<u32>,
}

impl Script {
    /// A connection whose server sends `replies` once the client sent its first message.
    pub(crate) fn answering(replies: impl IntoIterator<Item = Received>) -> Self {
        let replies = replies
            .into_iter()
            .map(|received| Reply {
                after_sent: 1,
                received,
            })
            .collect();
        Self {
            replies,
            ..Self::default()
        }
    }
}

/// An archive the in-memory browser saved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SavedArchive {
    pub(crate) file_name: String,
    pub(crate) bytes: Vec<u8>,
}

/// What the in-memory browser saw a download or a restore do.
#[derive(Debug, Default)]
pub(crate) struct Observed {
    /// The address of every connection the browser was asked to open, in order.
    pub(crate) opened: Vec<String>,
    /// Every message sent on any connection, in order.
    pub(crate) sent: Vec<Vec<u8>>,
    /// Every timer that ran out, in order.
    pub(crate) waits: Vec<Duration>,
    /// The archive the browser saved, once it saved one.
    pub(crate) saved: Option<SavedArchive>,
}

/// The browser the tests drive. Each connection it opens follows the next script, it refuses to
/// open one once the scripts run out, and every timer elapses at once.
#[derive(Default)]
pub(crate) struct InMemoryBrowser {
    scripts: RefCell<VecDeque<Script>>,
    observed: Rc<RefCell<Observed>>,
    /// Whether the browser refuses to save an archive, as one out of space does.
    refuses_saves: bool,
}

impl InMemoryBrowser {
    /// A browser whose connections follow `scripts`, one each.
    pub(crate) fn with_scripts(scripts: impl IntoIterator<Item = Script>) -> Self {
        Self {
            scripts: RefCell::new(scripts.into_iter().collect()),
            ..Self::default()
        }
    }

    /// The same browser, refusing to save an archive.
    pub(crate) fn refusing_saves(self) -> Self {
        Self {
            refuses_saves: true,
            ..self
        }
    }

    /// What the browser saw so far.
    pub(crate) fn observed(&self) -> std::cell::Ref<'_, Observed> {
        self.observed.borrow()
    }
}

impl Browser for InMemoryBrowser {
    type Socket = ScriptedSocket;
    type Archive = MemoryArchive;

    fn open(&self, url: &str) -> Option<ScriptedSocket> {
        self.observed.borrow_mut().opened.push(url.to_string());
        let script = self.scripts.borrow_mut().pop_front()?;
        Some(ScriptedSocket {
            replies: script.replies,
            accepted: script.accepted,
            buffered: RefCell::new(script.buffered),
            sent: 0,
            observed: Rc::clone(&self.observed),
        })
    }

    fn wait(&self, duration: Duration) -> impl Future<Output = ()> {
        let observed = Rc::clone(&self.observed);
        async move {
            // Recorded as it elapses: a timer the caller drops unpolled never ran out.
            observed.borrow_mut().waits.push(duration);
        }
    }

    fn new_archive(&self) -> MemoryArchive {
        MemoryArchive {
            bytes: Vec::new(),
            refuses_saves: self.refuses_saves,
            observed: Rc::clone(&self.observed),
        }
    }
}

/// A connection that follows its script.
pub(crate) struct ScriptedSocket {
    replies: VecDeque<Reply>,
    accepted: Option<usize>,
    buffered: RefCell<VecDeque<u32>>,
    /// Messages this connection sent.
    sent: usize,
    observed: Rc<RefCell<Observed>>,
}

impl ScriptedSocket {
    /// The next reply, when the server sends it after what this connection already sent.
    fn arrived(&mut self) -> Option<Received> {
        let next = self.replies.front()?;
        if next.after_sent > self.sent {
            return None;
        }
        let reply = self.replies.pop_front()?;
        Some(reply.received)
    }
}

impl CallSocket for ScriptedSocket {
    async fn send(&mut self, payload: Vec<u8>) -> Result<(), SocketFailed> {
        if let Some(accepted) = self.accepted {
            let Some(left) = accepted.checked_sub(1) else {
                return Err(SocketFailed);
            };
            self.accepted = Some(left);
        }
        self.observed.borrow_mut().sent.push(payload);
        self.sent = self
            .sent
            .checked_add(1)
            .assured("a test connection sends far fewer than usize::MAX messages");
        Ok(())
    }

    async fn receive(&mut self) -> Received {
        match self.arrived() {
            Some(received) => received,
            // A server with nothing to send stays silent; the caller's timer decides.
            None => pending().await,
        }
    }

    fn try_receive(&mut self) -> Option<Received> {
        self.arrived()
    }

    fn buffered_amount(&self) -> u32 {
        self.buffered.borrow_mut().pop_front().unwrap_or(0)
    }
}

/// A downloaded archive kept in memory.
pub(crate) struct MemoryArchive {
    bytes: Vec<u8>,
    refuses_saves: bool,
    observed: Rc<RefCell<Observed>>,
}

impl DownloadedArchive for MemoryArchive {
    fn keep(&mut self, chunk: &[u8]) -> Result<(), ArchiveSaveFailed> {
        self.bytes.extend_from_slice(chunk);
        Ok(())
    }

    fn save(self, file_name: &str) -> Result<(), ArchiveSaveFailed> {
        if self.refuses_saves {
            return Err(ArchiveSaveFailed);
        }
        self.observed.borrow_mut().saved = Some(SavedArchive {
            file_name: file_name.to_string(),
            bytes: self.bytes,
        });
        Ok(())
    }
}

/// An archive file held as bytes. An unreadable one fails every read.
pub(crate) struct MemoryFile {
    pub(crate) bytes: Vec<u8>,
    pub(crate) unreadable: bool,
}

impl ArchiveFile for MemoryFile {
    fn size(&self) -> Option<u64> {
        u64::try_from(self.bytes.len()).ok()
    }

    async fn read(&self, start: u64, end: u64) -> Option<Vec<u8>> {
        if self.unreadable {
            return None;
        }
        let start = usize::try_from(start).ok()?;
        let end = usize::try_from(end).ok()?;
        let slice = self.bytes.get(start..end)?;
        Some(slice.to_vec())
    }
}

/// A session storage held in memory. Its clones share their entries, so a test reads what the
/// dialog it gave a clone to recorded.
#[derive(Debug, Clone, Default)]
pub(crate) struct MemoryStorage {
    entries: Rc<RefCell<BTreeMap<String, String>>>,
}

impl MemoryStorage {
    /// Every entry, by key.
    pub(crate) fn entries(&self) -> BTreeMap<String, String> {
        self.entries.borrow().clone()
    }
}

impl RecordStorage for MemoryStorage {
    fn get(&self, key: &str) -> Option<String> {
        self.entries.borrow().get(key).cloned()
    }

    fn set(&self, key: &str, value: &str) {
        self.entries
            .borrow_mut()
            .insert(key.to_string(), value.to_string());
    }

    fn remove(&self, key: &str) {
        self.entries.borrow_mut().remove(key);
    }
}
