//! HTTP/2 client request execution on an admitted connection.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Request framing, bounded response reads, deadlines and reset observations.
//! - **Depends on.** Admitted client connections, charged buffers and transport options.
//! - **Must not know.** Graph routing, scheduling decisions or connector behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use super::*;

impl ClientConnection {
    pub(super) async fn request_stream_raw(
        &self,
        state: &TransportState,
        request: RawRequest<'_>,
    ) -> Result<(RecvStream, u64), Report<TransportError>> {
        if self.closed.is_cancelled() {
            return Err(Report::new(TransportError::Closed(
                self.key.endpoint.clone(),
            )));
        }
        let RawRequest {
            path,
            body,
            response_class,
            response_limit,
            timeout: timeout_duration,
            headers,
        } = request;
        let operation = async {
            let sender = self
                .sender
                .clone()
                .ready()
                .await
                .map_err(TransportError::from)?;
            let mut request_url = url::Url::parse("https://localhost/")
                .assured("the fixed HTTPS request base is a valid URL");
            request_url
                .set_host(Some(&self.request_host))
                .map_err(|error| {
                    Report::new(error).change_context(TransportError::InvalidServerName(
                        self.request_host.clone(),
                    ))
                })?;
            request_url.set_path(path);
            let mut builder = Request::builder()
                .method(Method::POST)
                .version(Version::HTTP_2)
                .uri(request_url.as_str());
            for (name, value) in headers {
                builder = builder.header(*name, *value);
            }
            let request = builder.body(()).map_err(|error| {
                TransportError::with_cause(Report::new(error), TransportError::Http)
            })?;
            let end_stream = body.as_ref().is_none_or(ChargedBytes::is_empty);
            let (response, mut send_stream) = {
                let mut sender = sender;
                sender
                    .send_request(request, end_stream)
                    .map_err(TransportError::from)?
            };
            if let Some(body) = body
                && !body.is_empty()
            {
                send_body(&mut send_stream, body).await?;
            }
            let response = response.await.map_err(TransportError::from)?;
            let status = response.status();
            if !status.is_success() {
                return Err(read_error_response(
                    &state.executor,
                    response_class,
                    response_limit,
                    state.options.progress_timeout,
                    response,
                )
                .await);
            }
            let content_length = response
                .headers()
                .get(http::header::CONTENT_LENGTH)
                .ok_or_else(|| {
                    TransportError::Decode(
                        "streamed response omitted its content length".to_string(),
                    )
                })?
                .to_str()
                .map_err(|error| {
                    TransportError::with_cause(Report::new(error), TransportError::Decode)
                })?
                .parse::<u64>()
                .map_err(|error| {
                    TransportError::with_cause(Report::new(error), TransportError::Decode)
                })?;
            Ok((response.into_body(), content_length))
        };
        match timeout(timeout_duration, operation).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => {
                state.observations.stream_reset(
                    self.key.class,
                    StreamResetReason::of(error.current_context()),
                );
                Err(error)
            }
            Err(_) => {
                state
                    .observations
                    .stream_reset(self.key.class, StreamResetReason::Deadline);
                Err(Report::new(TransportError::RequestTimeout {
                    peer: self.key.node_id.clone(),
                    timeout: timeout_duration,
                }))
            }
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the transport performs this operation for each admitted frame or stream \
                      request"
        )
    )]
    pub(super) async fn request_raw(
        &self,
        state: &TransportState,
        request: RawRequest<'_>,
    ) -> Result<ChargedBytes, Report<TransportError>> {
        if self.closed.is_cancelled() {
            return Err(Report::new(TransportError::Closed(
                self.key.endpoint.clone(),
            )));
        }
        let RawRequest {
            path,
            body,
            response_class,
            response_limit,
            timeout: timeout_duration,
            headers,
        } = request;
        let operation = async {
            let sender = self
                .sender
                .clone()
                .ready()
                .await
                .map_err(TransportError::from)?;
            let mut request_url = url::Url::parse("https://localhost/")
                .assured("the fixed HTTPS request base is a valid URL");
            request_url
                .set_host(Some(&self.request_host))
                .map_err(|error| {
                    Report::new(error).change_context(TransportError::InvalidServerName(
                        self.request_host.clone(),
                    ))
                })?;
            request_url.set_path(path);
            let mut builder = Request::builder()
                .method(Method::POST)
                .version(Version::HTTP_2)
                .uri(request_url.as_str());
            for (name, value) in headers {
                builder = builder.header(*name, *value);
            }
            let request = builder.body(()).map_err(|error| {
                TransportError::with_cause(Report::new(error), TransportError::Http)
            })?;
            let end_stream = body.as_ref().is_none_or(ChargedBytes::is_empty);
            let (response, mut stream) = {
                let mut sender = sender;
                sender
                    .send_request(request, end_stream)
                    .map_err(TransportError::from)?
            };
            if let Some(body) = body
                && !body.is_empty()
            {
                send_body(&mut stream, body).await?;
            }
            let response = response.await.map_err(TransportError::from)?;
            let status = response.status();
            let response = read_body(
                &state.executor,
                response_class.memory_class(),
                response_limit,
                state.options.progress_timeout,
                response.into_body(),
            )
            .await?;
            if !status.is_success() {
                return Err(Report::new(TransportError::RemoteRejected {
                    status: status.as_u16(),
                    message: String::from_utf8_lossy(response.as_ref()).into_owned(),
                }));
            }
            Ok(response)
        };
        match timeout(timeout_duration, operation).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => {
                state.observations.stream_reset(
                    self.key.class,
                    StreamResetReason::of(error.current_context()),
                );
                Err(error)
            }
            Err(_) => {
                state
                    .observations
                    .stream_reset(self.key.class, StreamResetReason::Deadline);
                Err(Report::new(TransportError::RequestTimeout {
                    peer: self.key.node_id.clone(),
                    timeout: timeout_duration,
                }))
            }
        }
    }
}
