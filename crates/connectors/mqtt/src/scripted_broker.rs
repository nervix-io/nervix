//! A loopback MQTT 5 broker a check scripts packet by packet, for the MQTT connector's checks.
//!
//! Outside the layer order: a test harness.
//!
//! - **Owns.** One loopback listener, the connection a connector's client opens to it, and the packets a
//!   check reads from that client or writes to it.
//! - **Depends on.** Tokio's TCP sockets and I/O traits.
//! - **Must not know.** The connector's source or sink, the runtime host, or how a real broker decides.

use nervix_primitives::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// The control packet type of CONNECT, which the high nibble of a packet's first byte holds.
const CONNECT: u8 = 1;

/// The control packet type of PUBLISH.
const PUBLISH: u8 = 3;

/// The control packet type of PUBACK.
const PUBACK: u8 = 4;

/// The control packet type of SUBSCRIBE.
const SUBSCRIBE: u8 = 8;

/// The most bytes a variable byte integer takes, at seven value bits each.
const VARIABLE_INTEGER_BYTES: usize = 4;

/// A broker on a loopback port that answers only what its check tells it to.
pub(crate) struct ScriptedBroker {
    listener: TcpListener,
}

/// The connection a sink's client opened to a scripted broker.
pub(crate) struct ScriptedConnection {
    stream: TcpStream,
}

/// A PUBLISH the client sent: the packet identifier the broker answers it under, and its payload.
pub(crate) struct ReceivedPublish {
    pub(crate) packet_id: u16,
    pub(crate) payload: Vec<u8>,
}

impl ScriptedBroker {
    pub(crate) async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port is available");
        Self { listener }
    }

    /// The address a sink's client dials to reach this broker.
    pub(crate) fn addr(&self) -> String {
        let address = self
            .listener
            .local_addr()
            .expect("a bound listener has an address");
        format!("mqtt://{address}")
    }

    /// Accepts the client's connection and answers its CONNECT with a CONNACK that resumes no
    /// session.
    pub(crate) async fn accept(&self) -> ScriptedConnection {
        // CONNACK: no session present, success, and an empty property section.
        self.accept_answering(&[0x20, 0x03, 0x00, 0x00, 0x00]).await
    }

    /// Accepts the client's connection and answers its CONNECT with a CONNACK that resumes no
    /// session and declares `maximum` as the largest packet the broker receives.
    pub(crate) async fn accept_declaring_packet_limit(&self, maximum: u32) -> ScriptedConnection {
        let [first, second, third, fourth] = maximum.to_be_bytes();
        // CONNACK: no session present, success, and one property, the Maximum Packet Size.
        self.accept_answering(&[
            0x20, 0x08, 0x00, 0x00, 0x05, 0x27, first, second, third, fourth,
        ])
        .await
    }

    /// Accepts the client's connection and answers its CONNECT with a CONNACK that resumes no
    /// session and announces a Server Keep Alive of `seconds`, which the client must keep to.
    pub(crate) async fn accept_announcing_keep_alive(&self, seconds: u16) -> ScriptedConnection {
        let [high, low] = seconds.to_be_bytes();
        // CONNACK: no session present, success, and one property, the Server Keep Alive.
        self.accept_answering(&[0x20, 0x06, 0x00, 0x00, 0x03, 0x13, high, low])
            .await
    }

    /// Accepts the client's connection and answers its CONNECT with `connack`.
    async fn accept_answering(&self, connack: &[u8]) -> ScriptedConnection {
        let mut connection = self.accept_without_reply().await;
        connection.write(connack).await;
        connection
    }

    /// Accepts and reads CONNECT without answering, so a check can exercise the handshake budget.
    pub(crate) async fn accept_without_reply(&self) -> ScriptedConnection {
        let (stream, _) = self
            .listener
            .accept()
            .await
            .expect("the sink's client connects to the scripted broker");
        let mut connection = ScriptedConnection { stream };
        let (packet_type, _) = connection.read_packet().await;
        assert_eq!(packet_type, CONNECT, "the client opens with a CONNECT");
        connection
    }
}

impl ScriptedConnection {
    /// Reads the next packet the client sends, which must be a QoS 1 or QoS 2 PUBLISH.
    pub(crate) async fn read_publish(&mut self) -> ReceivedPublish {
        let (packet_type, body) = self.read_packet().await;
        assert_eq!(packet_type, PUBLISH, "the client sends a PUBLISH");
        // The topic with its two-byte length, the packet identifier, and the property section with
        // its length come before the payload.
        let topic_bytes = usize::from(u16::from_be_bytes([body[0], body[1]]));
        let packet_id_at = 2 + topic_bytes;
        let packet_id = u16::from_be_bytes([body[packet_id_at], body[packet_id_at + 1]]);
        let properties_at = packet_id_at + 2;
        let (properties_bytes, length_bytes) = variable_integer(&body[properties_at..]);
        let payload_at = properties_at + length_bytes + properties_bytes;
        ReceivedPublish {
            packet_id,
            payload: body[payload_at..].to_vec(),
        }
    }

    /// Reads the next packet the client sends, which must be a QoS 0 PUBLISH, and returns its
    /// payload.
    pub(crate) async fn read_publish_at_most_once(&mut self) -> Vec<u8> {
        let (packet_type, body) = self.read_packet().await;
        assert_eq!(packet_type, PUBLISH, "the client sends a PUBLISH");
        // The topic with its two-byte length and the property section with its length come before
        // the payload; a QoS 0 PUBLISH carries no packet identifier.
        let topic_bytes = usize::from(u16::from_be_bytes([body[0], body[1]]));
        let properties_at = 2 + topic_bytes;
        let (properties_bytes, length_bytes) = variable_integer(&body[properties_at..]);
        let payload_at = properties_at + length_bytes + properties_bytes;
        body[payload_at..].to_vec()
    }

    /// Reads the client's SUBSCRIBE and answers it with a SUBACK that grants QoS 1.
    pub(crate) async fn grant_subscription(&mut self) {
        let (packet_type, body) = self.read_packet().await;
        assert_eq!(
            packet_type, SUBSCRIBE,
            "the client subscribes once connected"
        );
        // SUBACK: the SUBSCRIBE's packet identifier, an empty property section, QoS 1 granted.
        self.write(&[0x90, 0x04, body[0], body[1], 0, 1]).await;
    }

    /// A QoS 1 PUBLISH to `events` under the packet identifier `packet_id`, carrying `payload`.
    pub(crate) fn publish_packet(packet_id: u16, payload: &[u8]) -> Vec<u8> {
        let topic = b"events";
        let [topic_high, topic_low] = u16::try_from(topic.len())
            .expect("the topic is six bytes")
            .to_be_bytes();
        let [id_high, id_low] = packet_id.to_be_bytes();
        let mut body = vec![topic_high, topic_low];
        body.extend_from_slice(topic);
        // The packet identifier, and an empty property section.
        body.extend_from_slice(&[id_high, id_low, 0]);
        body.extend_from_slice(payload);
        let mut packet = vec![0x32];
        // The Remaining Length: seven value bits a byte, least significant first, each byte but the
        // last with its continuation bit set.
        let mut remaining = body.len();
        loop {
            let value = u8::try_from(remaining % 128).expect("a remainder of 128 fits a byte");
            remaining /= 128;
            if remaining == 0 {
                packet.push(value);
                break;
            }
            packet.push(value | 0x80);
        }
        packet.extend(body);
        packet
    }

    /// Writes a QoS 1 PUBLISH to `events` under the packet identifier `packet_id`, carrying
    /// `payload`.
    pub(crate) async fn write_publish(&mut self, packet_id: u16, payload: &[u8]) {
        self.write(&Self::publish_packet(packet_id, payload)).await;
    }

    /// Reads the next packet the client sends, which must be a PUBACK, and returns the packet
    /// identifier it acknowledges.
    pub(crate) async fn read_puback(&mut self) -> u16 {
        let (packet_type, body) = self.read_packet().await;
        assert_eq!(packet_type, PUBACK, "the client sends a PUBACK");
        u16::from_be_bytes([body[0], body[1]])
    }

    /// Answers the PUBLISH `packet_id` with a PUBACK carrying the reason code `reason`.
    pub(crate) async fn puback(&mut self, packet_id: u16, reason: u8) {
        let [high, low] = packet_id.to_be_bytes();
        self.write(&[0x40, 0x03, high, low, reason]).await;
    }

    /// Reads one packet: its type and the bytes its Remaining Length counts.
    pub(crate) async fn read_packet(&mut self) -> (u8, Vec<u8>) {
        let first = self
            .stream
            .read_u8()
            .await
            .expect("the client sends another packet");
        let mut length = Vec::with_capacity(VARIABLE_INTEGER_BYTES);
        loop {
            let byte = self
                .stream
                .read_u8()
                .await
                .expect("the client sends the packet's Remaining Length");
            length.push(byte);
            if byte & 0x80 == 0 {
                break;
            }
        }
        let (remaining, _) = variable_integer(&length);
        let mut body = vec![0_u8; remaining];
        self.stream
            .read_exact(&mut body)
            .await
            .expect("the client sends the whole packet");
        (first >> 4, body)
    }

    pub(crate) async fn write(&mut self, bytes: &[u8]) {
        self.stream
            .write_all(bytes)
            .await
            .expect("the client reads what the scripted broker writes");
    }
}

/// The value of the variable byte integer `bytes` starts with, least significant seven bits first,
/// and how many bytes it takes.
fn variable_integer(bytes: &[u8]) -> (usize, usize) {
    let mut value = 0_usize;
    for (position, byte) in bytes.iter().take(VARIABLE_INTEGER_BYTES).enumerate() {
        value |= usize::from(byte & 0x7f) << (7 * position);
        if byte & 0x80 == 0 {
            return (value, position + 1);
        }
    }
    panic!("a variable byte integer ends within {VARIABLE_INTEGER_BYTES} bytes");
}
