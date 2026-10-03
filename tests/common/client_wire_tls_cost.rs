//! Matched command timing over plaintext and TLS client sessions.
//!
//! Outside the layer order: a benchmark harness. Product code must not name it.
//!
//! - **Owns.** Raw timing samples for the same command through both public client transports.
//! - **Depends on.** The native client and the Cucumber cluster fixture.
//! - **Must not know.** Server scheduling, execution plans or transport internals.

use std::{fs, path::PathBuf, time::Duration};

use anyhow::{Context as _, Result, ensure};
use nervix_client_core::Client;
use nervix_primitives::time::{Instant, timeout};
use serde::Serialize;

use super::cluster::{client_connect_options, client_domain};

const SAMPLES: usize = 100;
const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const COMMAND: &str = "DESCRIBE DOMAIN;";

#[derive(Serialize)]
struct Report {
    schema_version: u32,
    transport: &'static str,
    command: &'static str,
    samples: usize,
    connect_nanoseconds: u64,
    command_microseconds: Vec<u64>,
    prepare_microseconds: Vec<u64>,
    prepared_execution_microseconds: Vec<u64>,
}

pub(crate) async fn capture(grpc_uri: &str, domain: &str) -> Result<PathBuf> {
    let transport = if grpc_uri.starts_with("https://") {
        "https"
    } else {
        ensure!(grpc_uri.starts_with("http://"), "unsupported gRPC URI");
        "http"
    };
    let started = Instant::now();
    let client = timeout(
        OPERATION_TIMEOUT,
        Client::connect_with_options(
            grpc_uri,
            client_domain(domain),
            client_connect_options(grpc_uri)?,
        ),
    )
    .await
    .context("client connection timed out")??;
    let connect_nanoseconds = u64::try_from(started.elapsed().as_nanos())
        .context("connection timing does not fit u64")?;
    let warmup = client.execute(COMMAND).await?;
    ensure!(warmup.succeeded(), "TLS cost warm-up command failed");
    let mut command_microseconds = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        nervix_primitives::task::consume_budget().await;
        let started = Instant::now();
        let outcome = timeout(OPERATION_TIMEOUT, client.execute(COMMAND))
            .await
            .context("TLS cost command timed out")??;
        ensure!(outcome.succeeded(), "TLS cost command failed");
        command_microseconds.push(
            u64::try_from(started.elapsed().as_micros())
                .context("command timing does not fit u64")?,
        );
    }
    let mut prepare_microseconds = Vec::with_capacity(SAMPLES);
    let mut prepared_execution_microseconds = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        nervix_primitives::task::consume_budget().await;
        let started = Instant::now();
        let prepared = client.prepare_execution(COMMAND).await;
        prepare_microseconds.push(
            u64::try_from(started.elapsed().as_micros())
                .context("preparation timing does not fit u64")?,
        );
        let started = Instant::now();
        let outcome = timeout(OPERATION_TIMEOUT, client.execute_prepared(&prepared))
            .await
            .context("prepared TLS cost command timed out")??;
        ensure!(outcome.succeeded(), "prepared TLS cost command failed");
        prepared_execution_microseconds.push(
            u64::try_from(started.elapsed().as_micros())
                .context("prepared execution timing does not fit u64")?,
        );
    }
    let output_dir = std::env::var_os("NERVIX_CLIENT_WIRE_TLS_OUTPUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/client-wire-tls-cost"));
    fs::create_dir_all(&output_dir).context("failed to create TLS cost output directory")?;
    let output = output_dir.join(format!("{transport}.json"));
    let report = Report {
        schema_version: 1,
        transport,
        command: COMMAND,
        samples: SAMPLES,
        connect_nanoseconds,
        command_microseconds,
        prepare_microseconds,
        prepared_execution_microseconds,
    };
    fs::write(&output, serde_json::to_vec_pretty(&report)?)
        .context("failed to write TLS cost report")?;
    Ok(output)
}
