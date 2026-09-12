//! The unauthenticated liveness, readiness and metrics listener.
//!
//! Layer: edges.
//!
//! - **Owns.** The observability HTTP listener and the three probe paths it answers.
//! - **Depends on.** The application handle it reads health and metrics from.
//! - **Must not know.** Session commands, transactions or cluster coordination.

use std::convert::Infallible;

use error_stack::{Report, ResultExt};
use http_body_util::Full;
use hyper::{
    Method, Request as HyperRequest, Response as HyperResponse, StatusCode,
    body::{Bytes, Incoming as HyperIncoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use nervix_consensus::Observer;
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::{AppError, http_endpoint::text_response};

const OBSERVABILITY_LIVEZ_PATH: &str = "/livez";

const OBSERVABILITY_READYZ_PATH: &str = "/readyz";

const OBSERVABILITY_METRICS_PATH: &str = "/metrics";

async fn handle_observability_request(
    consensus: Observer,
    runtime: crate::runtime::Runtime,
    request: HyperRequest<HyperIncoming>,
) -> Result<HyperResponse<Full<Bytes>>, Infallible> {
    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, OBSERVABILITY_LIVEZ_PATH) => text_response(StatusCode::OK, "live\n"),
        (&Method::GET, OBSERVABILITY_READYZ_PATH) => {
            if consensus.current_leader().await.is_some() {
                text_response(StatusCode::OK, "ready\n")
            } else {
                text_response(StatusCode::SERVICE_UNAVAILABLE, "leader unknown\n")
            }
        }
        (&Method::GET, OBSERVABILITY_METRICS_PATH) => {
            text_response(StatusCode::OK, runtime.metrics().prometheus_text())
        }
        (&Method::GET, _) => text_response(StatusCode::NOT_FOUND, "not found"),
        _ => text_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
    };

    Ok(response)
}

pub(in crate::application) async fn serve_observability_http(
    consensus: Observer,
    runtime: crate::runtime::Runtime,
    listener: TcpListener,
    shutdown: CancellationToken,
) -> Result<(), Report<AppError>> {
    let mut connection_tasks = JoinSet::new();

    loop {
        tokio::task::consume_budget().await;
        let accepted = tokio::select! {
            _ = shutdown.cancelled() => {
                break;
            }
            accepted = listener.accept() => {
                accepted.change_context(AppError::ServeObservability)
            }
        };
        let (stream, _) = accepted?;
        stream
            .set_nodelay(true)
            .change_context(AppError::ServeObservability)?;
        let consensus = consensus.clone();
        let runtime = runtime.clone();
        connection_tasks.spawn(async move {
            let io = TokioIo::new(stream);
            if let Err(error) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |request| {
                        handle_observability_request(consensus.clone(), runtime.clone(), request)
                    }),
                )
                .await
            {
                warn!(error = %error, "observability connection failed");
            }
        });
    }
    connection_tasks.abort_all();
    while connection_tasks.join_next().await.is_some() {}
    Ok(())
}
