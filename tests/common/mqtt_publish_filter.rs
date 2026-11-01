//! A forwarder a scenario stands in front of the MQTT broker to drop chosen client packets.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One loopback listener, the connections it forwards to the broker, the MQTT packet
//!   framing of what a client sends through it, and which of those packets it drops.
//! - **Depends on.** Tokio's TCP sockets and I/O traits, and the boundary's tasks, cancellation and
//!   watch channel.
//! - **Must not know.** Nervix, what the broker answers, or what a packet carries beyond its type
//!   and its bytes.
//!
//! A dropped PUBLISH never reaches the broker, which therefore neither delivers nor answers it. A
//! dropped PUBREL leaves a QoS 2 exchange after the broker's PUBREC, so the broker delivers the
//! publish and never sends its PUBCOMP. Every other packet the client sends, and everything the
//! broker sends back, passes unchanged, so a scenario can keep one record's answer from ever coming
//! while the broker answers the records around it.

use std::{io, net::SocketAddr};

use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::{CancellationToken, watch},
    task::JoinHandle,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// The control packet type of PUBLISH, which the high nibble of a packet's first byte holds.
const PUBLISH: u8 = 3;

/// The control packet type of PUBREL, the client's answer to a broker's PUBREC.
const PUBREL: u8 = 6;

/// The most bytes a Remaining Length takes: a variable byte integer whose bytes each carry seven
/// value bits and a continuation bit.
const REMAINING_LENGTH_BYTES: usize = 4;

/// The client packets a filter leaves out.
#[derive(Debug, Clone, PartialEq, Eq)]
enum DroppedPackets {
    /// Every PUBLISH whose bytes carry this payload.
    PublishesCarrying(Vec<u8>),
    /// Every PUBREL.
    Releases,
}

/// A listener that forwards every client connection to the broker and leaves out the packets it
/// was told to drop.
#[derive(Debug)]
pub(crate) struct MqttPublishFilter {
    local_addr: SocketAddr,
    /// The packets dropped from now on, or none while every packet passes.
    dropped: watch::Sender<Option<DroppedPackets>>,
    /// Cancelled to close every connection the filter forwarded, when the filter stops.
    connections: CancellationToken,
    listener: JoinHandle<()>,
}

impl MqttPublishFilter {
    /// Listens on a loopback port and forwards every connection it accepts to `broker`.
    pub(crate) async fn start(broker: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let local_addr = listener.local_addr()?;
        let (dropped_packets, dropped) = watch::channel(None);
        let connections = CancellationToken::new();
        let listener = nervix_primitives::task::spawn(Self::accept(
            listener,
            broker,
            dropped,
            connections.clone(),
        ));
        Ok(Self {
            local_addr,
            dropped: dropped_packets,
            connections,
            listener,
        })
    }

    /// The loopback port clients connect to instead of the broker's.
    pub(crate) fn local_port(&self) -> u16 {
        self.local_addr.port()
    }

    /// Drops, from now on, every PUBLISH packet a client sends whose bytes carry `payload`.
    pub(crate) fn drop_publishes_carrying(&self, payload: Vec<u8>) {
        self.dropped
            .send_replace(Some(DroppedPackets::PublishesCarrying(payload)));
    }

    /// Drops, from now on, every PUBREL a client sends.
    pub(crate) fn drop_releases(&self) {
        self.dropped.send_replace(Some(DroppedPackets::Releases));
    }

    /// Forwards every packet from now on.
    pub(crate) fn forward_every_packet(&self) {
        self.dropped.send_replace(None);
    }

    async fn accept(
        listener: TcpListener,
        broker: SocketAddr,
        dropped: watch::Receiver<Option<DroppedPackets>>,
        connections: CancellationToken,
    ) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let Ok((client, _)) = listener.accept().await else {
                return;
            };
            nervix_primitives::task::spawn(Self::forward(
                client,
                broker,
                dropped.clone(),
                connections.clone(),
            ));
        }
    }

    /// Carries one connection to `broker` until either side closes it or the filter stops.
    async fn forward(
        client: TcpStream,
        broker: SocketAddr,
        dropped: watch::Receiver<Option<DroppedPackets>>,
        stopped: CancellationToken,
    ) {
        let Ok(broker) = TcpStream::connect(broker).await else {
            return;
        };
        let (mut client_read, mut client_write) = client.into_split();
        let (mut broker_read, mut broker_write) = broker.into_split();
        nervix_primitives::select! {
            _ = stopped.cancelled() => {}
            _ = Self::forward_client_packets(&mut client_read, &mut broker_write, &dropped) => {}
            _ = tokio::io::copy(&mut broker_read, &mut client_write) => {}
        }
    }

    /// Forwards what a client sends one whole packet at a time, leaving out each dropped packet.
    async fn forward_client_packets(
        client: &mut (impl AsyncRead + Unpin),
        broker: &mut (impl AsyncWrite + Unpin),
        dropped: &watch::Receiver<Option<DroppedPackets>>,
    ) -> io::Result<()> {
        loop {
            nervix_primitives::task::consume_budget().await;
            let Some(packet) = MqttPacket::read(client).await? else {
                return broker.shutdown().await;
            };
            let dropped_now = match dropped.borrow().as_ref() {
                Some(DroppedPackets::PublishesCarrying(payload)) => {
                    packet.is_publish_carrying(payload)
                }
                Some(DroppedPackets::Releases) => packet.is_release(),
                None => false,
            };
            if dropped_now {
                continue;
            }
            broker.write_all(&packet.bytes).await?;
        }
    }
}

impl Drop for MqttPublishFilter {
    fn drop(&mut self) {
        self.listener.abort();
        self.connections.cancel();
    }
}

/// One whole MQTT control packet: its first byte, its Remaining Length and the bytes that counts.
struct MqttPacket {
    bytes: Vec<u8>,
}

impl MqttPacket {
    /// Reads the next packet, or none when the stream ends before another one begins.
    async fn read(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Option<Self>> {
        let mut first = [0_u8; 1];
        if reader.read(&mut first).await? == 0 {
            return Ok(None);
        }
        let mut bytes = Vec::from(first);
        let remaining = Self::read_remaining_length(reader, &mut bytes).await?;
        let header_bytes = bytes.len();
        let packet_bytes = header_bytes.checked_add(remaining).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "an MQTT packet is longer than memory can hold",
            )
        })?;
        bytes.resize(packet_bytes, 0);
        reader.read_exact(&mut bytes[header_bytes..]).await?;
        Ok(Some(Self { bytes }))
    }

    /// Reads a Remaining Length, least significant seven bits first, and appends its bytes to
    /// `bytes`.
    async fn read_remaining_length(
        reader: &mut (impl AsyncRead + Unpin),
        bytes: &mut Vec<u8>,
    ) -> io::Result<usize> {
        let mut remaining = 0_usize;
        for position in 0..REMAINING_LENGTH_BYTES {
            let byte = reader.read_u8().await?;
            bytes.push(byte);
            // At most four bytes of seven bits each: the value fits 28 bits.
            let shift = position * 7;
            remaining |= usize::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(remaining);
            }
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "an MQTT Remaining Length takes at most four bytes",
        ))
    }

    /// The control packet type this packet's first byte holds.
    fn packet_type(&self) -> Option<u8> {
        let first = self.bytes.first()?;
        Some(first >> 4)
    }

    /// Whether this packet is a PUBREL.
    fn is_release(&self) -> bool {
        self.packet_type() == Some(PUBREL)
    }

    /// Whether this packet is a PUBLISH whose bytes carry `payload`. Every PUBLISH carries an empty
    /// payload.
    fn is_publish_carrying(&self, payload: &[u8]) -> bool {
        if self.packet_type() != Some(PUBLISH) {
            return false;
        }
        if payload.is_empty() {
            return true;
        }
        // A packet holds a few hundred bytes at most in these scenarios, so a scan bounds itself.
        self.bytes
            .windows(payload.len())
            .any(|window| window == payload)
    }
}
