//! A scenario forwarder that exposes publisher-return attribution against the real RabbitMQ broker.
//!
//! Layer: test harness.
//! - **Owns.** One loopback listener and the first two positive publisher confirmations of each
//!   connection, released after the broker accepted both messages.
//! - **Depends on.** The primitive sockets, tasks and cancellation boundary and Tokio I/O traits.
//! - **Must not know.** Nervix records, payloads, routing decisions or broker storage.
//!
//! All client traffic and broker returns pass unchanged. Once the broker confirmed both messages,
//! the forwarder emits an individual acknowledgement of message one and a multiple acknowledgement
//! through message two. A return of message two therefore precedes the first acknowledgement,
//! which is a valid ordering independent of the broker's original confirmation grouping.

use std::{io, net::SocketAddr};

use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::{CancellationToken, watch},
    task::JoinHandle,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const METHOD_FRAME: u8 = 1;
const FRAME_END: u8 = 0xce;
const BASIC_ACK: [u8; 4] = [0, 60, 0, 80];
const MAX_FRAME_BYTES: usize = 128 * 1024;

#[derive(Debug)]
pub(crate) struct RabbitMqConfirmOrder {
    local_addr: SocketAddr,
    rewritten: watch::Sender<bool>,
    stopped: CancellationToken,
    listener: JoinHandle<()>,
}

impl RabbitMqConfirmOrder {
    pub(crate) async fn start(broker: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let local_addr = listener.local_addr()?;
        let (rewritten, _) = watch::channel(false);
        let stopped = CancellationToken::new();
        let task = nervix_primitives::task::spawn(Self::accept(
            listener,
            broker,
            rewritten.clone(),
            stopped.clone(),
        ));
        Ok(Self {
            local_addr,
            rewritten,
            stopped,
            listener: task,
        })
    }

    pub(crate) fn port(&self) -> u16 {
        self.local_addr.port()
    }

    pub(crate) fn rewrote_confirmations(&self) -> bool {
        *self.rewritten.borrow()
    }

    async fn accept(
        listener: TcpListener,
        broker: SocketAddr,
        rewritten: watch::Sender<bool>,
        stopped: CancellationToken,
    ) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let Ok((client, _)) = listener.accept().await else {
                return;
            };
            nervix_primitives::task::spawn(Self::forward(
                client,
                broker,
                rewritten.clone(),
                stopped.clone(),
            ));
        }
    }

    async fn forward(
        client: TcpStream,
        broker: SocketAddr,
        rewritten: watch::Sender<bool>,
        stopped: CancellationToken,
    ) {
        let Ok(broker) = TcpStream::connect(broker).await else {
            return;
        };
        let (mut client_read, mut client_write) = client.into_split();
        let (mut broker_read, mut broker_write) = broker.into_split();
        nervix_primitives::select! {
            _ = stopped.cancelled() => {}
            _ = tokio::io::copy(&mut client_read, &mut broker_write) => {}
            _ = Self::broker_frames(&mut broker_read, &mut client_write, &rewritten) => {}
        }
    }

    async fn broker_frames(
        reader: &mut (impl AsyncRead + Unpin),
        writer: &mut (impl AsyncWrite + Unpin),
        rewritten: &watch::Sender<bool>,
    ) -> io::Result<()> {
        let mut first = false;
        let mut second = false;
        let mut released = false;
        loop {
            nervix_primitives::task::consume_budget().await;
            let mut header = [0; 7];
            reader.read_exact(&mut header).await?;
            let length = u32::from_be_bytes([header[3], header[4], header[5], header[6]]);
            let length = usize::try_from(length).map_err(io::Error::other)?;
            if length > MAX_FRAME_BYTES {
                return Err(io::Error::other("RabbitMQ fixture frame exceeds its bound"));
            }
            let mut payload = vec![0; length];
            reader.read_exact(&mut payload).await?;
            if reader.read_u8().await? != FRAME_END {
                return Err(io::Error::other(
                    "RabbitMQ fixture received an invalid frame end",
                ));
            }
            if !released && header[0] == METHOD_FRAME && payload.starts_with(&BASIC_ACK) {
                if payload.len() != 13 {
                    return Err(io::Error::other(
                        "RabbitMQ fixture received an invalid basic.ack",
                    ));
                }
                let tag = u64::from_be_bytes([
                    payload[4],
                    payload[5],
                    payload[6],
                    payload[7],
                    payload[8],
                    payload[9],
                    payload[10],
                    payload[11],
                ]);
                let multiple = payload[12] & 1 != 0;
                first |= tag == 1 || (multiple && tag >= 1);
                second |= tag == 2 || (multiple && tag >= 2);
                if first && second {
                    let channel = u16::from_be_bytes([header[1], header[2]]);
                    Self::ack(writer, channel, 1, false).await?;
                    Self::ack(writer, channel, 2, true).await?;
                    writer.flush().await?;
                    rewritten.send_replace(true);
                    released = true;
                }
                continue;
            }
            writer.write_all(&header).await?;
            writer.write_all(&payload).await?;
            writer.write_u8(FRAME_END).await?;
            writer.flush().await?;
        }
    }

    async fn ack(
        writer: &mut (impl AsyncWrite + Unpin),
        channel: u16,
        tag: u64,
        multiple: bool,
    ) -> io::Result<()> {
        writer.write_u8(METHOD_FRAME).await?;
        writer.write_u16(channel).await?;
        writer.write_u32(13).await?;
        writer.write_all(&BASIC_ACK).await?;
        writer.write_u64(tag).await?;
        writer.write_u8(u8::from(multiple)).await?;
        writer.write_u8(FRAME_END).await
    }
}

impl Drop for RabbitMqConfirmOrder {
    fn drop(&mut self) {
        self.listener.abort();
        self.stopped.cancel();
    }
}
