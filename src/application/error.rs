//! What can go wrong before this node is serving.
//!
//! Layer: edges.
//!
//! - **Owns.** The error the binary returns when startup cannot complete.
//! - **Depends on.** Nothing but the messages its variants carry.
//! - **Must not know.** Any failure a running node recovers from on its own.

use thiserror::Error;
#[derive(Debug, Error)]
pub enum AppError {
    #[error("failed to build the Tokio runtime")]
    BuildRuntime,
    #[error("failed to parse server address")]
    ParseAddress,
    #[error("failed to bind gRPC listen address")]
    BindGrpcListenAddress,
    #[error("failed to parse HTTP listen address")]
    ParseHttpListenAddress,
    #[error("failed to bind HTTP listen address")]
    BindHttpListenAddress,
    #[error("failed to parse HTTPS listen address")]
    ParseHttpsListenAddress,
    #[error("failed to parse observability listen address")]
    ParseObservabilityListenAddress,
    #[error("failed to parse web console listen address")]
    ParseWebConsoleListenAddress,
    #[error("failed to parse web console https listen address")]
    ParseWebConsoleHttpsListenAddress,
    #[error("failed to bind HTTPS listen address")]
    BindHttpsListenAddress,
    #[error("failed to bind observability listen address")]
    BindObservabilityListenAddress,
    #[error("failed to bind web console listen address")]
    BindWebConsoleListenAddress,
    #[error("failed to bind web console https listen address")]
    BindWebConsoleHttpsListenAddress,
    #[error("failed to parse gRPC advertise address")]
    ParseGrpcAdvertiseAddress,
    #[error("failed to parse gRPC https listen address")]
    ParseGrpcHttpsListenAddress,
    #[error("failed to parse gRPC https advertise address")]
    ParseGrpcHttpsAdvertiseAddress,
    #[error("failed to parse interconnect listen address")]
    ParseInterconnectListenAddress,
    #[error("failed to parse interconnect advertise address")]
    ParseInterconnectAdvertiseAddress,
    #[error("failed to derive interconnect address from gRPC address")]
    DeriveInterconnectAddress,
    #[error("gRPC https mode requires an https listen address")]
    MissingGrpcHttpsListenAddress,
    #[error("gRPC https mode requires an https advertise address")]
    MissingGrpcHttpsAdvertiseAddress,
    #[error("web console https listener requires a TLS certificate")]
    MissingWebConsoleTlsCertificate,
    #[error("web console https listener requires a TLS private key")]
    MissingWebConsoleTlsPrivateKey,
    #[error("web console TLS certificate/key requires an https listen address")]
    MissingWebConsoleHttpsListenAddress,
    #[error("failed to open registry")]
    OpenRegistry,
    #[error("failed to open resource store")]
    OpenResourceStore,
    #[error("failed to start consensus")]
    StartConsensus,
    #[error("failed to synchronize registry from consensus schedule: {0}")]
    SynchronizeRegistry(String),
    #[error("failed to apply startup runtime changes: {0}")]
    ApplyStartupRuntime(String),
    #[error("failed to open runtime state store")]
    OpenRuntimeState,
    #[error("failed to load interconnect tls configuration")]
    LoadInterconnectTls,
    #[error("failed to load gRPC tls configuration")]
    LoadGrpcTls,
    #[error("failed to load web console tls configuration")]
    LoadWebConsoleTls,
    #[error("failed to start interconnect transport")]
    StartInterconnect,
    #[error("failed to register an interconnect request handler")]
    RegisterInterconnectRequestHandler,
    #[error("failed to start cluster membership")]
    StartCluster,
    #[error("failed to stop cluster membership")]
    ShutdownCluster,
    #[error("memory high watermark requires memory low watermark")]
    MissingMemoryPressureLowWatermark,
    #[error("memory low watermark requires memory high watermark")]
    MissingMemoryPressureHighWatermark,
    #[error("invalid memory pressure configuration")]
    InvalidMemoryPressureConfig,
    #[error("failed to initialize memory pressure monitor")]
    InitMemoryPressureMonitor,
    #[error("gRPC server failed")]
    Serve,
    #[error("HTTP server failed")]
    ServeHttp,
    #[error("HTTPS server failed")]
    ServeHttps,
    #[error("observability server failed")]
    ServeObservability,
    #[error("web console server failed")]
    ServeWebConsole,
    #[error("failed to initialize tracing")]
    InitTracing,
}
