//! Layer: test harness, outside product execution.
//! - **Owns.** A per-scenario MQTT forwarder, its advertised packet bound and captured publishes.
//! - **Depends on.** The shared packet reader, real broker, MQTT wire codec and bounded primitives.
//! - **Must not know.** Nervix emission, record errors, retained payloads or runtime scheduling.

use std::{io, net::SocketAddr};

use bytes::BytesMut;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::{
        Arc, CancellationToken,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    task::JoinHandle,
};
use nervix_recovery::NoReceiver as _;
use rumqttc::{Publish, mqttbytes::v5::Packet};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _};

use crate::common::mqtt_publish_filter::MqttPacket;

pub(super) struct CapturedPublish {
    pub(super) publish: Publish,
    pub(super) forwarded: bool,
}

pub(crate) struct Endpoint {
    port: u16,
    captured: mpsc::Receiver<CapturedPublish>,
    stopped: CancellationToken,
    listener: JoinHandle<()>,
}

impl Endpoint {
    pub(super) async fn start(
        broker: SocketAddr,
        maximum: Option<u32>,
        drop_first: bool,
    ) -> io::Result<Self> {
        let socket = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let port = socket.local_addr()?.port();
        let stopped = CancellationToken::new();
        let connections = stopped.clone();
        let (captured, receiver) = mpsc::channel(16);
        let first = Arc::new(AtomicBool::new(drop_first));
        let listener = nervix_primitives::task::spawn(async move {
            loop {
                nervix_primitives::task::consume_budget().await;
                let Ok((client, _)) = socket.accept().await else {
                    return;
                };
                nervix_primitives::task::spawn(Self::forward(
                    client,
                    broker,
                    maximum,
                    first.clone(),
                    captured.clone(),
                    connections.clone(),
                ));
            }
        });
        Ok(Self {
            port,
            captured: receiver,
            stopped,
            listener,
        })
    }

    pub(super) fn port(&self) -> u16 {
        self.port
    }

    pub(super) async fn next(&mut self) -> CapturedPublish {
        nervix_primitives::time::timeout(super::BROKER_TIMEOUT, self.captured.recv())
            .await
            .assured("the emitter sends its publication within the observation bound")
            .assured("the endpoint remains alive")
    }

    async fn forward(
        client: TcpStream,
        broker: SocketAddr,
        maximum: Option<u32>,
        first: Arc<AtomicBool>,
        captured: mpsc::Sender<CapturedPublish>,
        stopped: CancellationToken,
    ) {
        let connected = nervix_primitives::select! {
            _ = stopped.cancelled() => return,
            connected = TcpStream::connect(broker) => connected,
        };
        let Ok(broker) = connected else {
            return;
        };
        let (mut client_read, mut client_write) = client.into_split();
        let (mut broker_read, mut broker_write) = broker.into_split();
        nervix_primitives::select! {
            _ = stopped.cancelled() => {}
            _ = Self::client_packets(&mut client_read, &mut broker_write, &first, &captured) => {}
            _ = Self::broker_packets(&mut broker_read, &mut client_write, maximum) => {}
        }
    }

    async fn client_packets(
        client: &mut (impl AsyncRead + Unpin),
        broker: &mut (impl AsyncWrite + Unpin),
        first: &AtomicBool,
        captured: &mpsc::Sender<CapturedPublish>,
    ) -> io::Result<()> {
        while let Some(packet) = MqttPacket::read(client).await? {
            nervix_primitives::task::consume_budget().await;
            let mut bytes = BytesMut::from(packet.bytes.as_slice());
            if let Packet::Publish(publish) = Packet::read(&mut bytes, None)
                .assured("the MQTT client writes complete valid packets")
            {
                // One independent boolean: only the first publication is withheld, across all
                // connections. No other state is published through this atomic.
                let forwarded = !first.swap(false, Ordering::Relaxed);
                captured
                    .send(CapturedPublish { publish, forwarded })
                    .await
                    .means_shutdown("the MQTT publication scenario ended");
                if !forwarded {
                    continue;
                }
            }
            broker.write_all(&packet.bytes).await?;
        }
        broker.shutdown().await
    }

    async fn broker_packets(
        broker: &mut (impl AsyncRead + Unpin),
        client: &mut (impl AsyncWrite + Unpin),
        maximum: Option<u32>,
    ) -> io::Result<()> {
        while let Some(packet) = MqttPacket::read(broker).await? {
            nervix_primitives::task::consume_budget().await;
            let mut bytes = BytesMut::from(packet.bytes.as_slice());
            if let (Some(maximum), Packet::ConnAck(mut connack)) = (
                maximum,
                Packet::read(&mut bytes, None).assured("the real broker writes valid packets"),
            ) {
                connack
                    .properties
                    .as_mut()
                    .assured("the MQTT 5 test broker declares its properties")
                    .max_packet_size = Some(maximum);
                bytes.clear();
                connack
                    .write(&mut bytes)
                    .assured("the smaller packet bound is valid");
                client.write_all(&bytes).await?;
            } else {
                client.write_all(&packet.bytes).await?;
            }
        }
        client.shutdown().await
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.listener.abort();
        self.stopped.cancel();
    }
}
