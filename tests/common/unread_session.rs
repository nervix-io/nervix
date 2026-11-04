//! A native peer that grants no response bytes and keeps its connection observed independently.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** A real HTTP/2 Exchange, its zero response window, a bounded set of cancel requests,
//!   and the receipt time of the connection's ending without reading any response data.
//! - **Depends on.** The public client wire encoder, h2, the harness credentials and observer.
//! - **Must not know.** Session queues, server shutdown internals or how many replies fill them.

use std::{io, net::SocketAddr, num::NonZeroU64};

use bytes::{BufMut as _, Bytes, BytesMut};
use nervix_client_wire::{
    CancelRequest, ClientMessage, ClientRequest, RequestId, SessionLimits, grpc::EXCHANGE_PATH,
};
use nervix_primitives::net::TcpStream;

use super::{
    cluster::test_basic_authorization,
    runner_observation::{ArrivalObservation, ObservedArrival},
};

/// More requests than the peer needs, bounded independently of the server's private queues.
const REQUEST_COUNT: u64 = 8192;
const REQUEST_BYTES: usize = 1024 * 1024;

pub(crate) struct UnreadSession {
    request: h2::SendStream<Bytes>,
    response: h2::RecvStream,
    connection: ArrivalObservation<Result<(), h2::Error>>,
}

impl UnreadSession {
    pub(crate) async fn open(endpoint: &str) -> io::Result<Self> {
        let endpoint = url::Url::parse(endpoint).map_err(io::Error::other)?;
        let host = match endpoint.host() {
            Some(url::Host::Ipv4(address)) => address,
            _ => {
                return Err(io::Error::other(
                    "an unread peer needs a literal IPv4 endpoint",
                ));
            }
        };
        if endpoint.scheme() != "http" {
            return Err(io::Error::other(
                "an unread peer needs the plaintext test listener",
            ));
        }
        let port = endpoint
            .port()
            .ok_or_else(|| io::Error::other("the native test endpoint needs a port"))?;
        let socket = TcpStream::connect(SocketAddr::from((host, port))).await?;
        let (sender, connection) = h2::client::Builder::new()
            .initial_window_size(0)
            .max_send_buffer_size(REQUEST_BYTES)
            .handshake(socket)
            .await
            .map_err(io::Error::other)?;
        let connection = ArrivalObservation::start(connection).await;
        let mut sender = sender.ready().await.map_err(io::Error::other)?;
        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri(
                endpoint
                    .join(EXCHANGE_PATH)
                    .map_err(io::Error::other)?
                    .as_str(),
            )
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header(http::header::TE, "trailers")
            .header(http::header::AUTHORIZATION, test_basic_authorization())
            .body(())
            .map_err(io::Error::other)?;
        let (response, mut request) = sender
            .send_request(request, false)
            .map_err(io::Error::other)?;
        let response = response.await.map_err(io::Error::other)?;
        if response.status() != http::StatusCode::OK {
            return Err(io::Error::other(format!(
                "the unread Exchange was refused: {:?}",
                response.headers()
            )));
        }
        if let Some(status) = tonic::Status::from_header_map(response.headers()) {
            return Err(io::Error::other(format!(
                "the unread Exchange ended: {status}"
            )));
        }
        let requests = Self::requests().await?;
        request
            .send_data(requests, false)
            .map_err(io::Error::other)?;
        Ok(Self {
            request,
            response: response.into_body(),
            connection,
        })
    }

    async fn requests() -> io::Result<Bytes> {
        let mut requests = BytesMut::with_capacity(REQUEST_BYTES);
        for sequence in 1..=REQUEST_COUNT {
            nervix_primitives::task::consume_budget().await;
            let request_id = NonZeroU64::new(sequence)
                .ok_or_else(|| io::Error::other("request sequences begin at one"))?;
            let message = ClientMessage {
                request_id: RequestId::new(request_id),
                request: ClientRequest::Cancel(CancelRequest {
                    target: RequestId::new(NonZeroU64::MAX),
                }),
            };
            let frame = message
                .encode(&SessionLimits::DEFAULT)
                .map_err(io::Error::other)?;
            let length = u32::try_from(frame.bytes().len()).map_err(io::Error::other)?;
            let message_bytes = frame
                .bytes()
                .len()
                .checked_add(5)
                .ok_or_else(|| io::Error::other("the gRPC message size overflowed"))?;
            let total = requests
                .len()
                .checked_add(message_bytes)
                .ok_or_else(|| io::Error::other("the unread request size overflowed"))?;
            if total > REQUEST_BYTES {
                return Err(io::Error::other(
                    "the unread requests exceed their byte bound",
                ));
            }
            requests.put_u8(0);
            requests.put_u32(length);
            requests.put_slice(frame.bytes());
        }
        Ok(requests.freeze())
    }

    pub(crate) async fn ended(self) -> ObservedArrival<Result<(), h2::Error>> {
        let Self {
            request,
            response,
            connection,
        } = self;
        let ended = connection.finish().await;
        // Retain both halves until the connection ends. Dropping either before that would let
        // the client, rather than the node's connection cut, end the held call.
        drop(request);
        drop(response);
        ended
    }
}
