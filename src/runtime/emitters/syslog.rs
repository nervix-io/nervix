use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use rustls_pki_types::ServerName;
use thiserror::Error;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, UdpSocket, lookup_host},
};
use tokio_rustls::{TlsConnector, client::TlsStream};

use super::*;
use crate::runtime::syslog::{
    MAX_UDP_PAYLOAD_SIZE, SyslogClientConfig, SyslogDirection, SyslogFraming, SyslogProtocol,
};

pub(in crate::runtime) struct SyslogEmitter {
    config: SyslogClientConfig,
    sender: SyslogSender,
}

enum SyslogSender {
    Udp(UdpSocket),
    Tcp(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

#[derive(Debug, Error)]
enum SyslogPayloadError {
    #[error("encoded Syslog UDP payload is {size} bytes; maximum is {maximum}")]
    OversizedUdp { size: usize, maximum: usize },
    #[error(
        "encoded Syslog payload contains LF, which is not allowed with non-transparent framing"
    )]
    NonTransparentLf,
}

impl SyslogEmitter {
    pub(in crate::runtime) async fn new(
        client: &CreateClientSyslog,
        resolved: Option<&ResolvedClientConfig>,
    ) -> EmitterRuntimeResult<Self> {
        let entries = resolved
            .map(|resolved| resolved.entries.as_slice())
            .unwrap_or(client.config.as_slice());
        let config = SyslogClientConfig::parse(entries, SyslogDirection::Emit)
            .map_err(emitter_config_error)?;
        let sender = Self::connect(&config).await?;
        Ok(Self { config, sender })
    }

    async fn connect(config: &SyslogClientConfig) -> EmitterRuntimeResult<SyslogSender> {
        match config.protocol {
            SyslogProtocol::Udp => {
                let destination = resolve_first(&config.addr).await?;
                let local = SocketAddr::new(
                    match destination.ip() {
                        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                    },
                    0,
                );
                let socket = UdpSocket::bind(local).await.map_err(emitter_init_error)?;
                socket
                    .connect(destination)
                    .await
                    .map_err(emitter_init_error)?;
                Ok(SyslogSender::Udp(socket))
            }
            SyslogProtocol::Tcp => {
                let stream = TcpStream::connect(&config.addr)
                    .await
                    .map_err(emitter_init_error)?;
                stream.set_nodelay(true).map_err(emitter_init_error)?;
                Ok(SyslogSender::Tcp(stream))
            }
            SyslogProtocol::Tls => {
                let stream = TcpStream::connect(&config.addr)
                    .await
                    .map_err(emitter_init_error)?;
                stream.set_nodelay(true).map_err(emitter_init_error)?;
                let server_name =
                    ServerName::try_from(config.server_name.clone()).map_err(|error| {
                        emitter_config_error(format!(
                            "invalid Syslog client config key 'addr' TLS server name '{}': {error}",
                            config.server_name
                        ))
                    })?;
                let connector =
                    TlsConnector::from(config.tls_client_config().map_err(emitter_config_error)?);
                let stream = connector
                    .connect(server_name, stream)
                    .await
                    .map_err(emitter_init_error)?;
                Ok(SyslogSender::Tls(Box::new(stream)))
            }
        }
    }

    pub(in crate::runtime) async fn publish_records(
        &mut self,
        records: Vec<EncodedBrokerRecord>,
    ) -> PerRecordPublishOutcome {
        let mut outcome = PerRecordPublishOutcome::empty();
        let mut current_batch = None;
        // A write failure retains the entire current batch for retry, including records whose
        // frames were already accepted by the socket and may therefore be delivered twice.
        let mut staged_deliveries = Vec::new();
        for record in records {
            tokio::task::consume_budget().await;
            let position = (record.batch_index, record.row_index);
            if current_batch.is_some_and(|batch| batch != record.batch_index) {
                for position in staged_deliveries.drain(..) {
                    outcome.deliver(position);
                }
            }
            current_batch = Some(record.batch_index);
            if let Err(reason) = self.validate_payload(&record.payload) {
                outcome.reject(position, reason.to_string());
                continue;
            }
            let published =
                await_emitter_confirmation(&record.acks, self.publish_payload(&record.payload))
                    .await;
            match published {
                Ok(()) => staged_deliveries.push(position),
                Err(error) => {
                    outcome.fail(error);
                    return outcome;
                }
            }
        }
        for position in staged_deliveries {
            outcome.deliver(position);
        }
        outcome
    }

    fn validate_payload(&self, payload: &[u8]) -> Result<(), SyslogPayloadError> {
        if self.config.protocol == SyslogProtocol::Udp && payload.len() > MAX_UDP_PAYLOAD_SIZE {
            return Err(SyslogPayloadError::OversizedUdp {
                size: payload.len(),
                maximum: MAX_UDP_PAYLOAD_SIZE,
            });
        }
        if self.config.protocol == SyslogProtocol::Tcp
            && self.config.framing == SyslogFraming::NonTransparent
            && payload.contains(&b'\n')
        {
            return Err(SyslogPayloadError::NonTransparentLf);
        }
        Ok(())
    }

    async fn publish_payload(&mut self, payload: &[u8]) -> EmitterRuntimeResult<()> {
        match &mut self.sender {
            SyslogSender::Udp(socket) => {
                let written = socket.send(payload).await.map_err(emitter_publish_error)?;
                if written != payload.len() {
                    return Err(emitter_publish_error(format!(
                        "Syslog UDP socket accepted {written} of {} bytes",
                        payload.len()
                    )));
                }
            }
            SyslogSender::Tcp(stream) => {
                write_stream_frame(stream, self.config.framing, payload).await?;
            }
            SyslogSender::Tls(stream) => {
                write_stream_frame(stream.as_mut(), SyslogFraming::OctetCounting, payload).await?;
            }
        }
        Ok(())
    }
}

async fn resolve_first(addr: &str) -> EmitterRuntimeResult<SocketAddr> {
    lookup_host(addr)
        .await
        .map_err(emitter_init_error)?
        .next()
        .ok_or_else(|| emitter_init_error(format!("Syslog addr '{addr}' resolved to no addresses")))
}

async fn write_stream_frame(
    stream: &mut (impl tokio::io::AsyncWrite + Unpin),
    framing: SyslogFraming,
    payload: &[u8],
) -> EmitterRuntimeResult<()> {
    match framing {
        SyslogFraming::OctetCounting => {
            let prefix = format!("{} ", payload.len());
            stream
                .write_all(prefix.as_bytes())
                .await
                .map_err(emitter_publish_error)?;
            stream
                .write_all(payload)
                .await
                .map_err(emitter_publish_error)?;
        }
        SyslogFraming::NonTransparent => {
            stream
                .write_all(payload)
                .await
                .map_err(emitter_publish_error)?;
            stream
                .write_all(b"\n")
                .await
                .map_err(emitter_publish_error)?;
        }
    }
    stream.flush().await.map_err(emitter_publish_error)
}

#[cfg(test)]
mod tests {
    use nervix_models::ClientConfigEntry;
    use tokio::io::AsyncReadExt as _;

    use super::*;

    fn config(protocol: &str, framing: Option<&str>) -> SyslogClientConfig {
        let mut entries = vec![
            ClientConfigEntry {
                key: "protocol".to_string(),
                value: protocol.to_string(),
            },
            ClientConfigEntry {
                key: "addr".to_string(),
                value: "127.0.0.1:5514".to_string(),
            },
        ];
        if let Some(framing) = framing {
            entries.push(ClientConfigEntry {
                key: "framing".to_string(),
                value: framing.to_string(),
            });
        }
        SyslogClientConfig::parse(&entries, SyslogDirection::Emit)
            .expect("test Syslog emitter config must parse")
    }

    async fn emitter(config: SyslogClientConfig) -> SyslogEmitter {
        let receiver = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("test UDP receiver must bind");
        let sender = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("test UDP sender must bind");
        sender
            .connect(
                receiver
                    .local_addr()
                    .expect("receiver must have an address"),
            )
            .await
            .expect("test UDP sender must connect");
        SyslogEmitter {
            config,
            sender: SyslogSender::Udp(sender),
        }
    }

    #[tokio::test]
    async fn stream_writer_emits_both_rfc6587_framings() {
        for (framing, expected) in [
            (SyslogFraming::OctetCounting, b"5 hello".as_slice()),
            (SyslogFraming::NonTransparent, b"hello\n".as_slice()),
        ] {
            let (mut writer, mut reader) = tokio::io::duplex(64);
            write_stream_frame(&mut writer, framing, b"hello")
                .await
                .expect("Syslog stream frame must write");
            drop(writer);
            let mut actual = Vec::new();
            reader
                .read_to_end(&mut actual)
                .await
                .expect("Syslog stream frame must be readable");
            assert_eq!(actual, expected);
        }
    }

    #[tokio::test]
    async fn emitter_rejects_udp_oversize_and_non_transparent_lf() {
        let udp = emitter(config("udp", None)).await;
        let oversized = vec![0_u8; MAX_UDP_PAYLOAD_SIZE + 1];
        let maximum = vec![0_u8; MAX_UDP_PAYLOAD_SIZE];
        assert!(udp.validate_payload(&oversized).is_err());
        assert!(udp.validate_payload(&maximum).is_ok());

        let tcp = emitter(config("tcp", Some("non-transparent"))).await;
        assert!(tcp.validate_payload(b"line one\nline two").is_err());
        assert!(tcp.validate_payload(b"one line").is_ok());
    }
}
