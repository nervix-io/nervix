//! The accepted-frame handoff from signaling to ordinary source reads.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** One bounded frame queue, socket backpressure while full, and retained headers.
//! - **Depends on.** The signaling sink, source messages and execution primitives.
//! - **Must not know.** Sockets, Plexus, registry state, graph admission or retry policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the bounded signaling handoff")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "capacity is validated before the source opens")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::RetainedIngestHeaders;
use nervix_primitives::sync::{Arc, mpsc};

use super::{SignalingDataEmitter, WebsocketDataFrame, WebsocketsConnectionIngestorMessage};
use crate::WebsocketSignalingError;

pub(super) struct HandshakeFrames {
    capacity: usize,
    queued: Option<QueuedFrames>,
}

struct QueuedFrames {
    sender: mpsc::Sender<WebsocketsConnectionIngestorMessage>,
    receiver: mpsc::Receiver<WebsocketsConnectionIngestorMessage>,
}

impl HandshakeFrames {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            queued: None,
        }
    }

    fn queue(&mut self) -> &mut QueuedFrames {
        if self.queued.is_none() {
            let (sender, receiver) = mpsc::channel(self.capacity);
            self.queued = Some(QueuedFrames { sender, receiver });
        }
        self.queued
            .as_mut()
            .verified("the queue was installed above")
    }

    pub(super) fn sink(
        &mut self,
        headers: Arc<RetainedIngestHeaders>,
    ) -> QueuedSignalingDataEmitter {
        QueuedSignalingDataEmitter {
            sender: self.queue().sender.clone(),
            headers,
        }
    }

    pub(super) fn for_handshake(&self) -> Self {
        Self::new(self.capacity)
    }

    /// Only completed signaling transfers its queue; failure or cancellation drops the attempt.
    pub(super) async fn complete(
        self,
        handshake: impl std::future::Future<Output = error_stack::Result<(), WebsocketSignalingError>>,
    ) -> error_stack::Result<Self, WebsocketSignalingError> {
        handshake.await?;
        Ok(self)
    }

    pub(super) fn pop_front(&mut self) -> Option<WebsocketsConnectionIngestorMessage> {
        let queue = self.queued.as_mut()?;
        match queue.receiver.try_recv() {
            Ok(message) => Some(message),
            Err(mpsc::error::TryRecvError::Empty) => {
                // Steady socket reads reach only task-owned absence once handoff drains.
                self.queued = None;
                None
            }
            Err(error) => {
                Err(error).assured("the handoff keeps its sender for as long as it reads")
            }
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        match &self.queued {
            Some(queue) => queue.receiver.is_empty(),
            None => true,
        }
    }

    pub(super) fn clear(&mut self) {
        self.queued = None;
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        match &self.queued {
            Some(queue) => queue.receiver.len(),
            None => 0,
        }
    }

    #[cfg(test)]
    pub(super) fn push_back(&mut self, message: WebsocketsConnectionIngestorMessage) {
        self.queue()
            .sender
            .try_send(message)
            .assured("the lifecycle fixtures retain one frame in a queue with room");
    }
}

pub(super) struct QueuedSignalingDataEmitter {
    sender: mpsc::Sender<WebsocketsConnectionIngestorMessage>,
    // A handed-over message may outlive the connection that supplied these headers.
    headers: Arc<RetainedIngestHeaders>,
}

impl SignalingDataEmitter for QueuedSignalingDataEmitter {
    async fn accept(&self, frame: WebsocketDataFrame) {
        let message = WebsocketsConnectionIngestorMessage::new(frame, self.headers.clone());
        self.sender
            .send(message)
            .await
            .assured("the source owns the receiver throughout its signaling future");
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests;
