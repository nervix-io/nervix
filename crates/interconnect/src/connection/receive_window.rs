//! The HTTP/2 receive windows a client connection advertises, and the memory that backs them.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The stream and connection windows each pool's client connections advertise, the
//!   check that the executor's `bulk_window` budget backs one bulk receive window, and the window
//!   reservation every bulk stream and every bulk connection's binding exchange holds.
//! - **Depends on.** The transport's options, the pool classes and the executor's memory classes.
//! - **Must not know.** What a bulk response carries, or how its reader consumes it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use super::*;

/// The HTTP/2 receive credit one client connection advertises: what each of its streams, and the
/// whole connection, may receive before the reader returns any of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ReceiveWindows {
    pub(super) stream: u32,
    pub(super) connection: u32,
}

impl TransportOptions {
    /// The windows a client connection of `class` advertises. A bulk connection grants each of
    /// its streams the bulk receive window, and itself room for all of them at once, so no stream
    /// waits for another's credit; every other pool advertises the initial windows.
    pub(super) fn receive_windows(&self, class: PoolClass) -> ReceiveWindows {
        if !class.grants_bulk_receive_window() {
            return ReceiveWindows {
                stream: self.initial_stream_window_bytes,
                connection: self.initial_connection_window_bytes,
            };
        }
        let stream = u32::try_from(self.bulk_receive_window_bytes()).assured(
            "a resolved bulk receive window fits HTTP/2's window with every stream of its \
             connection",
        );
        let streams = u32::try_from(class.stream_slots_per_connection())
            .assured("a bulk connection carries a handful of streams");
        let connection = stream.checked_mul(streams).assured(
            "a resolved bulk receive window fits HTTP/2's window with every stream of its \
             connection",
        );
        ReceiveWindows { stream, connection }
    }

    /// What one bulk stream's window holds, which its lease reserves before the stream opens.
    pub(super) fn bulk_receive_window_bytes(&self) -> u64 {
        self.bulk_receive_window.value().as_u64()
    }
}

impl TransportState {
    /// Refuse a transport whose `bulk_window` budget cannot back even one bulk receive window,
    /// before it binds: every bulk stream would otherwise wait for a window that never comes.
    pub(super) fn validate_bulk_receive_window(
        options: &TransportOptions,
        executor: &Executor,
    ) -> Result<(), Report<TransportError>> {
        let window = options.bulk_receive_window_bytes();
        let budget = executor.memory(MemoryClass::BulkWindow).capacity_bytes;
        if budget < window {
            return Err(Report::new(TransportError::BulkReceiveWindowUnbacked {
                window,
                budget,
            }));
        }
        Ok(())
    }

    /// Back the receive window a connection's binding exchange grants its peer. The exchange is the
    /// connection's first stream, so a bulk connection grants it the bulk receive window like every
    /// stream after it. The connection's setup deadline bounds the wait.
    pub(super) async fn reserve_binding_receive_window(
        &self,
        key: &ConnectionSlotKey,
    ) -> Result<Option<Reservation>, Report<TransportError>> {
        if !key.class.grants_bulk_receive_window() {
            return Ok(None);
        }
        let window = self
            .executor
            .reserve(
                MemoryClass::BulkWindow,
                self.options.bulk_receive_window_bytes(),
            )
            .await
            .map_err(|error| {
                error.change_context(TransportError::BulkReceiveWindowRefused {
                    peer: key.node_id.clone(),
                })
            })?;
        Ok(Some(window))
    }

    /// Reserve one bulk stream's receive window, waiting behind the streams that hold the class
    /// until one returns its window. A stream holding a window needs nothing more from the class
    /// to finish, so the wait ends when one does, or at the request's own deadline.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "every bulk stream request reserves its receive window before it is sent"
        )
    )]
    pub(super) async fn reserve_bulk_receive_window(
        &self,
        node_id: &ClusterNodeName,
        deadline: Instant,
    ) -> Result<Reservation, Report<TransportError>> {
        let window = self.options.bulk_receive_window_bytes();
        nervix_primitives::select! {
            _ = self.admission_closed.cancelled() => {
                Err(Report::new(TransportError::ShuttingDown))
            }
            _ = sleep_until(deadline) => {
                Err(Report::new(TransportError::RequestTimeout {
                    peer: node_id.clone(),
                    timeout: self.options.request_timeout,
                }))
            }
            reserved = self.executor.reserve(MemoryClass::BulkWindow, window) => {
                reserved.map_err(|error| {
                    error.change_context(TransportError::BulkReceiveWindowRefused {
                        peer: node_id.clone(),
                    })
                })
            }
        }
    }
}
