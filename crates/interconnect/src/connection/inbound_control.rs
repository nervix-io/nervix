//! Answering one control request a peer sends over an admitted connection.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Reading one inbound control envelope with the room its decoding takes, refusing an
//!   envelope of another pool class, running a request's handler with the request's charge given
//!   back and encoding its answer, and delivering every other control envelope to the application.
//! - **Depends on.** The parent transport's request handlers and incoming delivery, the bounded wire
//!   codecs, and the peer's reset, which ends every wait.
//! - **Must not know.** What a request asks for or how its handler answers it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::future::poll_fn;

use bytes::Bytes;
use error_stack::Report;
use h2::{RecvStream, server};
use http::{Request, StatusCode};

use super::{
    InboundPeer, TransportState, declared_length, exchange_reset, read_body_to_decode,
    send_response, until_reset,
};
use crate::{ControlEnvelope, Envelope, TransportError, wire, wire::AnswerEncoding};

impl TransportState {
    pub(super) async fn handle_control(
        &self,
        peer: InboundPeer,
        request: Request<RecvStream>,
        mut respond: server::SendResponse<Bytes>,
    ) -> Result<(), Report<TransportError>> {
        let declared = declared_length(
            request.headers(),
            peer.class.control_body_limit(&self.executor),
        )?;
        let reading = read_body_to_decode(
            &self.executor,
            peer.class.memory_class(),
            declared,
            self.options.progress_timeout,
            request.into_body(),
        );
        let Some(received) = until_reset(&mut respond, reading).await? else {
            return Ok(());
        };
        // The decoding waits for a place on the class while it holds the body's charge, so the
        // peer's reset ends that wait too and gives the charge back.
        let Some(decoded) = until_reset(
            &mut respond,
            wire::decode_rkyv_within::<ControlEnvelope>(
                &self.executor,
                peer.class.cpu_class(),
                received.bytes,
                received.decoding,
            ),
        )
        .await?
        else {
            return Ok(());
        };
        let (control, reservation) = decoded.into_parts();
        if control.pool_class() != peer.class {
            send_response(
                respond,
                StatusCode::FORBIDDEN,
                None,
                self.options.progress_timeout,
            )
            .await?;
            return Ok(());
        }

        if let ControlEnvelope::Request(request) = control {
            // The handler owns the request from here, and its own work takes charges of this
            // class, so the request's charge is given back before it runs: no handler waits for
            // room behind the request it serves. The connection's stream places bound the request
            // meanwhile.
            drop(reservation);
            let response = nervix_primitives::select! {
                response = self.requests.handle(
                    &self.executor,
                    peer.node_id,
                    peer.advertised_host,
                    peer.process_epoch,
                    request,
                ) => response,
                reset = poll_fn(|context| respond.poll_reset(context)) => {
                    reset.map_err(TransportError::from)?;
                    return Ok(());
                }
            };
            let (response, payload_reservation) = response.into_parts();
            // The envelope's encoding copies the answer's payload, whose charge is given back
            // first, so the encoding never waits for room while holding it. The handler already
            // ran, so a full class holds the answer back rather than throwing the work away, and
            // the caller's reset ends the wait.
            drop(payload_reservation);
            let encoding = wire::encode_answer(
                &self.executor,
                peer.class.memory_class(),
                peer.class.cpu_class(),
                peer.class.control_body_limit(&self.executor),
                ControlEnvelope::Response(response),
                exchange_reset(&mut respond),
            )
            .await?;
            let response = match encoding {
                AnswerEncoding::Encoded(response) => response,
                AnswerEncoding::Ended => return Ok(()),
            };
            send_response(
                respond,
                StatusCode::OK,
                Some(response),
                self.options.progress_timeout,
            )
            .await?;
            return Ok(());
        }

        if let ControlEnvelope::Response(_) = control {
            send_response(
                respond,
                StatusCode::BAD_REQUEST,
                None,
                self.options.progress_timeout,
            )
            .await?;
            return Ok(());
        }

        self.deliver_incoming(
            peer.addr,
            peer.node_id,
            Envelope::Control(control),
            Some(reservation),
        )?;
        send_response(
            respond,
            StatusCode::NO_CONTENT,
            None,
            self.options.progress_timeout,
        )
        .await
    }
}
