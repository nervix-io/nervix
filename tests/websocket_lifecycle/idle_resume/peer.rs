//! A responsive peer that accepts exactly one connection and releases data on a command or ping.
//!
//! Layer: test harness.
//! - **Owns.** Its one real transport, bounded commands and payload receipts, and a failure if
//!   the client replaces a connection whose peer answers its pings.
//! - **Depends on.** Execution primitives, JSON and the WebSocket transport.
//! - **Must not know.** The scenario world, graph, decoder workers or connector idle state.

use std::{io, time::Duration};

use futures_util::{SinkExt as _, StreamExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::AbortOnDropHandle,
    time::timeout,
};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

const PEER_WAIT: Duration = Duration::from_secs(60);

enum Command {
    Send(u64),
    AfterPing(u64),
}

pub(crate) struct HandoffPeer {
    commands: mpsc::Sender<Command>,
    sent: mpsc::Receiver<u64>,
    task: AbortOnDropHandle<io::Result<()>>,
}

impl HandoffPeer {
    pub(super) fn start(listener: TcpListener) -> Self {
        let (commands, mut receive_commands) = mpsc::channel(2);
        let (sent, receipts) = mpsc::channel(2);
        let task = nervix_primitives::task::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = tokio_tungstenite::accept_async(stream)
                .await
                .map_err(io::Error::other)?;
            let mut after_ping = None;
            loop {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::select! {
                    command = receive_commands.recv() => {
                        match command {
                            Some(Command::Send(value)) => Self::write(&mut socket, &sent, value).await?,
                            Some(Command::AfterPing(value)) => {
                                assert!(after_ping.is_none(), "one recovery payload waits for its ping");
                                after_ping = Some(value);
                            }
                            None => return Ok(()),
                        }
                    }
                    frame = socket.next() => {
                        let frame = frame.ok_or_else(|| io::Error::other("the original responsive WebSocket connection ended"))?
                            .map_err(io::Error::other)?;
                        match frame {
                            Message::Ping(_) => {
                                socket.flush().await.map_err(io::Error::other)?;
                                if let Some(value) = after_ping.take() {
                                    Self::write(&mut socket, &sent, value).await?;
                                }
                            }
                            Message::Close(_) => return Err(io::Error::other("the client closed its responsive peer during the handoff regression")),
                            _ => {}
                        }
                    }
                }
            }
        });
        Self {
            commands,
            sent: receipts,
            task: AbortOnDropHandle::new(task),
        }
    }

    async fn write(
        socket: &mut WebSocketStream<TcpStream>,
        sent: &mpsc::Sender<u64>,
        value: u64,
    ) -> io::Result<()> {
        let payload = serde_json::json!({"user_id": value}).to_string();
        socket
            .send(Message::Text(payload))
            .await
            .map_err(io::Error::other)?;
        sent.try_send(value)
            .assured("the scenario consumes each of its two payload receipts");
        Ok(())
    }

    pub(super) fn send_payload(&self, value: u64) {
        self.commands
            .try_send(Command::Send(value))
            .assured("one payload command fits its bounded channel");
    }

    pub(super) fn send_after_ping(&self, value: u64) {
        self.commands
            .try_send(Command::AfterPing(value))
            .assured("one recovery command fits its bounded channel");
    }

    pub(super) async fn payload_sent(&mut self, expected: u64) {
        let value = timeout(PEER_WAIT, self.sent.recv())
            .await
            .assured("the original peer delivers its payload within the fixture budget")
            .assured("the original responsive connection remains alive");
        assert_eq!(value, expected);
    }

    pub(crate) async fn stop(self) -> io::Result<()> {
        self.task.abort();
        match timeout(PEER_WAIT, self.task)
            .await
            .map_err(io::Error::other)?
        {
            Err(error) if error.is_cancelled() => Ok(()),
            Ok(outcome) => outcome,
            Err(error) => Err(io::Error::other(error)),
        }
    }
}
