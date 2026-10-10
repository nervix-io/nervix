//! What the browser gives the backup dialog: console WebSockets and the browser's timers, a place
//! for a downloaded archive until it is saved, the archive file an operator chose, and the tab's
//! session storage.
//!
//! The download and restore drivers and the pending backup record speak only to the traits here,
//! so everything they decide runs alike against the page's browser and against the in-memory one
//! their tests drive. The page's implementations hold nothing but the calls into the browser.

use std::{future::Future, time::Duration};

use futures_util::{FutureExt as _, SinkExt as _, StreamExt as _};
use gloo_net::websocket::{Message as WebSocketMessage, WebSocketError, futures::WebSocket};
use nervix_approx_into::{ApproxInto as _, CheckedApproxInto as _};
use nervix_recovery::Discarded as _;
use thiserror::Error;
use wasm_bindgen::JsCast as _;

use crate::wait_for_browser_delay;

/// The media type the browser saves an archive as.
const ARCHIVE_MEDIA_TYPE: &str = "application/x-tar";

/// How long the browser keeps the address of a saved archive before the console releases it.
const SAVED_ARCHIVE_LIFETIME: Duration = Duration::from_secs(60);

/// One message a console WebSocket received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Received {
    /// A binary message, which carries one frame.
    Binary(Vec<u8>),
    /// A text message, which no console call carries.
    Text,
    /// The connection closed or failed.
    Ended,
}

/// The connection failed while a message was sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("the console connection failed")]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "the browser adapter reports an ordinary unavailable socket capability"
    )
)]
pub(crate) struct SocketFailed;

/// The browser could not keep or save a downloaded archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("the browser could not keep or save the archive")]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "the browser adapter reports an ordinary unavailable download capability"
    )
)]
pub(crate) struct ArchiveSaveFailed;

/// A console WebSocket that carries one download or one restore attempt.
pub(crate) trait CallSocket {
    /// Sends one binary message.
    async fn send(&mut self, payload: Vec<u8>) -> Result<(), SocketFailed>;

    /// Waits for the next message.
    async fn receive(&mut self) -> Received;

    /// The next message when one already arrived, or `None` when none has.
    fn try_receive(&mut self) -> Option<Received>;

    /// Bytes the connection has queued and the browser has not sent yet.
    fn buffered_amount(&self) -> u32;
}

/// Where a download keeps the verified chunks of one attempt until the whole archive is saved.
pub(crate) trait DownloadedArchive {
    /// Keeps one verified chunk after those kept before it.
    fn keep(&mut self, chunk: &[u8]) -> Result<(), ArchiveSaveFailed>;

    /// Hands the whole archive to the browser's downloads, saved as `file_name`.
    fn save(self, file_name: &str) -> Result<(), ArchiveSaveFailed>;
}

/// The browser a download or a restore runs in.
pub(crate) trait Browser {
    type Socket: CallSocket;
    type Archive: DownloadedArchive;

    /// Opens a console WebSocket at `url`, or `None` when the browser refuses to.
    fn open(&self, url: &str) -> Option<Self::Socket>;

    /// Waits for `duration` as the browser's timers measure it.
    fn wait(&self, duration: Duration) -> impl Future<Output = ()>;

    /// A place for the archive one download attempt receives.
    fn new_archive(&self) -> Self::Archive;
}

/// The archive file a restore reads.
pub(crate) trait ArchiveFile {
    /// The file's size in bytes, or `None` when the browser reports no whole byte count.
    fn size(&self) -> Option<u64>;

    /// Bytes `start` up to `end` of the file, or `None` when the browser could not read them.
    async fn read(&self, start: u64, end: u64) -> Option<Vec<u8>>;
}

/// The storage the tab keeps the backup a reload resumes in.
pub(crate) trait RecordStorage {
    /// The entry `key` names, or `None` when there is none or it cannot be read.
    fn get(&self, key: &str) -> Option<String>;

    /// Writes the entry `key` names. A write the storage refuses only loses the resume.
    fn set(&self, key: &str, value: &str);

    /// Removes the entry `key` names.
    fn remove(&self, key: &str);
}

/// The browser the page runs in.
pub(crate) struct PageBrowser;

impl Browser for PageBrowser {
    type Socket = PageSocket;
    type Archive = BlobArchive;

    fn open(&self, url: &str) -> Option<PageSocket> {
        match WebSocket::open(url) {
            Ok(socket) => Some(PageSocket { socket }),
            Err(_) => None,
        }
    }

    fn wait(&self, duration: Duration) -> impl Future<Output = ()> {
        wait_for_browser_delay(duration)
    }

    fn new_archive(&self) -> BlobArchive {
        BlobArchive {
            parts: js_sys::Array::new(),
        }
    }
}

/// A console WebSocket of the page.
pub(crate) struct PageSocket {
    socket: WebSocket,
}

impl PageSocket {
    fn received(message: Option<Result<WebSocketMessage, WebSocketError>>) -> Received {
        match message {
            Some(Ok(WebSocketMessage::Bytes(payload))) => Received::Binary(payload),
            Some(Ok(WebSocketMessage::Text(_))) => Received::Text,
            Some(Err(_)) | None => Received::Ended,
        }
    }
}

impl CallSocket for PageSocket {
    async fn send(&mut self, payload: Vec<u8>) -> Result<(), SocketFailed> {
        match self.socket.send(WebSocketMessage::Bytes(payload)).await {
            Ok(()) => Ok(()),
            Err(_) => Err(SocketFailed),
        }
    }

    async fn receive(&mut self) -> Received {
        Self::received(self.socket.next().await)
    }

    fn try_receive(&mut self) -> Option<Received> {
        let message = self.socket.next().now_or_never()?;
        Some(Self::received(message))
    }

    fn buffered_amount(&self) -> u32 {
        self.socket.buffered_amount()
    }
}

/// A downloaded archive the browser keeps as one blob per chunk, which it may hold out of the
/// page's memory, so the archive never accumulates as one buffer in the page.
pub(crate) struct BlobArchive {
    parts: js_sys::Array,
}

impl DownloadedArchive for BlobArchive {
    fn keep(&mut self, chunk: &[u8]) -> Result<(), ArchiveSaveFailed> {
        let bytes = js_sys::Uint8Array::from(chunk);
        let Ok(blob) = web_sys::Blob::new_with_u8_array_sequence(&js_sys::Array::of1(&bytes))
        else {
            return Err(ArchiveSaveFailed);
        };
        self.parts.push(&blob);
        Ok(())
    }

    fn save(self, file_name: &str) -> Result<(), ArchiveSaveFailed> {
        let options = web_sys::BlobPropertyBag::new();
        options.set_type(ARCHIVE_MEDIA_TYPE);
        let Ok(archive) = web_sys::Blob::new_with_blob_sequence_and_options(&self.parts, &options)
        else {
            return Err(ArchiveSaveFailed);
        };
        let Ok(address) = web_sys::Url::create_object_url_with_blob(&archive) else {
            return Err(ArchiveSaveFailed);
        };
        let saved = save_address(&address, file_name);
        // The browser reads the archive through its address while it saves it, so the address is
        // released only once the save has long begun.
        wasm_bindgen_futures::spawn_local(async move {
            wait_for_browser_delay(SAVED_ARCHIVE_LIFETIME).await;
            web_sys::Url::revoke_object_url(&address).discarded(
                "an address the browser no longer resolves needs no release; the archive goes \
                 with the page",
            );
        });
        saved
    }
}

/// Starts the browser's download of the archive at `address`, saved as `file_name`.
fn save_address(address: &str, file_name: &str) -> Result<(), ArchiveSaveFailed> {
    let Some(window) = web_sys::window() else {
        return Err(ArchiveSaveFailed);
    };
    let Some(document) = window.document() else {
        return Err(ArchiveSaveFailed);
    };
    let Ok(element) = document.create_element("a") else {
        return Err(ArchiveSaveFailed);
    };
    let Ok(anchor) = element.dyn_into::<web_sys::HtmlAnchorElement>() else {
        return Err(ArchiveSaveFailed);
    };
    anchor.set_href(address);
    anchor.set_download(file_name);
    let Some(body) = document.body() else {
        return Err(ArchiveSaveFailed);
    };
    if body.append_child(&anchor).is_err() {
        return Err(ArchiveSaveFailed);
    }
    anchor.click();
    body.remove_child(&anchor).discarded(
        "the anchor only started the download; one left in the page changes nothing the operator \
         sees",
    );
    Ok(())
}

impl ArchiveFile for web_sys::File {
    fn size(&self) -> Option<u64> {
        web_sys::Blob::size(self).checked_approx_into()
    }

    async fn read(&self, start: u64, end: u64) -> Option<Vec<u8>> {
        let slice = self
            .slice_with_f64_and_f64(start.approx_into(), end.approx_into())
            .ok()?;
        let buffer = wasm_bindgen_futures::JsFuture::from(slice.array_buffer())
            .await
            .ok()?;
        Some(js_sys::Uint8Array::new(&buffer).to_vec())
    }
}

impl RecordStorage for web_sys::Storage {
    fn get(&self, key: &str) -> Option<String> {
        // A storage that refuses a read holds nothing the console can resume.
        self.get_item(key).unwrap_or_default()
    }

    fn set(&self, key: &str, value: &str) {
        self.set_item(key, value)
            .discarded("a tab whose storage is full only cannot resume this backup after a reload");
    }

    fn remove(&self, key: &str) {
        self.remove_item(key)
            .discarded("removing an entry the storage does not hold changes nothing");
    }
}

/// The tab's session storage, or `None` where the browser offers none, as with storage disabled.
/// Such a tab still backs up; it only cannot resume a backup after a reload.
pub(crate) fn session_storage() -> Option<web_sys::Storage> {
    let window = web_sys::window()?;
    window.session_storage().ok()?
}

/// The tab's session storage, as the record storage the dialog holds.
pub(crate) fn boxed_session_storage() -> Option<Box<dyn RecordStorage>> {
    let storage = session_storage()?;
    let boxed: Box<dyn RecordStorage> = Box::new(storage);
    Some(boxed)
}

#[cfg(test)]
pub(crate) mod in_memory;
