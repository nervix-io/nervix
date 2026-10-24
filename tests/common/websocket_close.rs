//! The reply a scenario peer sends after it has observed a WebSocket Close frame.
//!
//! Layer: test harness.
//! - **Owns.** Finishing the queued reply to a received Close frame.
//! - **Depends on.** The WebSocket sink contract and its typed transport errors.
//! - **Must not know.** Scenario state, connector internals or node shutdown policy.

use std::io::{self, ErrorKind};

use futures_util::{Sink, SinkExt as _};
use tokio_tungstenite::tungstenite::{Error as WebsocketError, Message};

/// Flushes the reply only after the caller has read a Close frame from this peer.
/// Unexpected driver errors remain the typed cause of the fixture's I/O result.
pub(crate) async fn reply_to_close(
    socket: &mut (impl Sink<Message, Error = WebsocketError> + Unpin),
) -> io::Result<()> {
    match socket.flush().await {
        Ok(()) | Err(WebsocketError::ConnectionClosed) => Ok(()),
        // The initiator can finish its local Close write and dispose of TCP before reading
        // the reply. The received Close frame already supplied the fixture's observation.
        Err(WebsocketError::Io(error))
            if matches!(
                error.kind(),
                ErrorKind::ConnectionReset | ErrorKind::BrokenPipe
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(io::Error::other(error)),
    }
}
