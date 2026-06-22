use ::zeromq::{PushSocket, Socket, SocketSend};

use super::*;

pub(in crate::runtime) struct ZeroMqEmitter {
    socket: Option<PushSocket>,
}

impl ZeroMqEmitter {
    pub(in crate::runtime) async fn new(
        client: &CreateClientZeroMq,
        resolved: Option<&ResolvedClientConfig>,
    ) -> EmitterRuntimeResult<Self> {
        let socket = Self::push_socket_from_config(
            resolved
                .map(|config| config.entries.as_slice())
                .unwrap_or(client.config.as_slice()),
        )
        .await?;
        Ok(Self {
            socket: Some(socket),
        })
    }

    async fn push_socket_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> EmitterRuntimeResult<PushSocket> {
        let addr = Self::addr_from_config(config)?;
        let bind = Self::bind_from_config(config);
        let mut socket = PushSocket::new();
        if bind {
            socket.bind(&addr).await.map_err(emitter_init_error)?;
        } else {
            socket.connect(&addr).await.map_err(emitter_init_error)?;
        }
        Ok(socket)
    }

    fn addr_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> EmitterRuntimeResult<String> {
        emitter_config_value(config, "addr", || {
            "missing ZeroMQ client config key 'addr'".to_string()
        })
    }

    fn bind_from_config(config: &[nervix_models::ClientConfigEntry]) -> bool {
        optional_client_config_value(config, "bind")
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    pub(in crate::runtime) async fn publish(&mut self, payload: &[u8]) -> EmitterRuntimeResult<()> {
        let Some(socket) = self.socket.as_mut() else {
            return Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable("no initialized zeromq sink client"));
        };
        socket
            .send(payload.to_vec().into())
            .await
            .map_err(emitter_publish_error)
    }
}
