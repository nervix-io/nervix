//! MongoDB TLS options and the private identity file its driver reads.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Merging explicit TLS files into native options and preparing a private combined
//!   identity on the bounded filesystem executor, with its lifetime handed to the client.
//! - **Depends on.** Resolved connector entries, native driver options and the execution boundary.
//! - **Must not know.** Models, resource installation, graph execution or another connector.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns MongoDB TLS preparation")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "receives resolved client entries")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(lifecycle, reason = "prepares one named client's TLS identity")
)]

use std::{
    fs::File,
    io::{self, Write as _},
    path::PathBuf,
};

use error_stack::{Report, ResultExt as _};
use mongodb::options::{ClientOptions, Tls, TlsOptions};
use nervix_connector::{ClientConfigError, ClientTlsPaths, client_tls_paths};
use nervix_execution::{Executor, MemoryClass, StorageClass};
use nervix_models::ClientConfigEntry;
use tempfile::NamedTempFile;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum MongoDbTlsError {
    #[error("invalid TLS configuration")]
    Configuration,
    #[error("TLS files cannot be used with tls=false")]
    Disabled,
    #[error("failed to prepare the TLS identity")]
    PrepareIdentity,
    #[error("TLS preparation exceeds the addressable working set")]
    PreparationSize,
    #[error("failed to create the private TLS identity file")]
    CreateIdentity,
    #[error("failed to open the {part}")]
    OpenIdentityPart { part: IdentityPart },
    #[error("failed to copy the {part}")]
    CopyIdentityPart { part: IdentityPart },
    #[error("failed to separate the identity PEM blocks")]
    SeparateIdentityParts,
}

#[derive(Debug, Clone, Copy, strum::Display)]
pub(crate) enum IdentityPart {
    #[strum(serialize = "TLS certificate")]
    Certificate,
    #[strum(serialize = "TLS private key")]
    PrivateKey,
}

struct IdentityFiles {
    certificate: PathBuf,
    private_key: PathBuf,
}

enum ClientTls {
    Native(Option<Tls>),
    Identity {
        options: TlsOptions,
        files: IdentityFiles,
    },
}

/// Native options whose TLS configuration has one owner until it is prepared.
pub(crate) struct MongoDbClientOptions {
    options: ClientOptions,
    tls: ClientTls,
}

/// The native driver's complete options and the file they borrow until the client ends.
pub(crate) struct PreparedMongoDbClient {
    pub(crate) options: ClientOptions,
    pub(crate) identity: Option<NamedTempFile>,
}

impl MongoDbClientOptions {
    pub(crate) fn new(
        mut options: ClientOptions,
        config: &[ClientConfigEntry],
    ) -> error_stack::Result<Self, MongoDbTlsError> {
        let native = options.tls.take();
        let ClientTlsPaths {
            ca_file,
            cert_file,
            key_file,
        } = client_tls_paths(config);
        let identity = match (cert_file, key_file) {
            (Some(certificate), Some(private_key)) => Some(IdentityFiles {
                certificate,
                private_key,
            }),
            (None, None) => None,
            _ => {
                return Err(Report::new(ClientConfigError::IncompleteTlsIdentity)
                    .change_context(MongoDbTlsError::Configuration));
            }
        };
        if ca_file.is_none() && identity.is_none() {
            return Ok(Self {
                options,
                tls: ClientTls::Native(native),
            });
        }
        let mut tls = match native {
            Some(Tls::Enabled(tls)) => tls,
            None => TlsOptions::default(),
            Some(Tls::Disabled) => return Err(Report::new(MongoDbTlsError::Disabled)),
        };
        if let Some(ca_file) = ca_file {
            tls.ca_file_path = Some(ca_file);
        }
        let tls = match identity {
            Some(files) => ClientTls::Identity {
                options: tls,
                files,
            },
            None => ClientTls::Native(Some(Tls::Enabled(tls))),
        };
        Ok(Self { options, tls })
    }

    pub(crate) async fn prepare(
        self,
        executor: &Executor,
    ) -> error_stack::Result<PreparedMongoDbClient, MongoDbTlsError> {
        let Self { mut options, tls } = self;
        let identity = match tls {
            ClientTls::Native(tls) => {
                options.tls = tls;
                None
            }
            ClientTls::Identity {
                options: mut tls,
                files,
            } => {
                let identity = files.prepare(executor).await?;
                tls.cert_key_file_path = Some(identity.path().to_path_buf());
                options.tls = Some(Tls::Enabled(tls));
                Some(identity)
            }
        };
        Ok(PreparedMongoDbClient { options, identity })
    }
}

impl IdentityFiles {
    async fn prepare(
        self,
        executor: &Executor,
    ) -> error_stack::Result<NamedTempFile, MongoDbTlsError> {
        let root = std::env::temp_dir();
        // The source files are streamed rather than collected into a PEM buffer. Reserve the
        // owned inputs and temporary path construction; the worker's I/O buffer stays on its stack.
        let root_paths = root
            .capacity()
            .checked_mul(2)
            .ok_or_else(|| Report::new(MongoDbTlsError::PreparationSize))?;
        let file_paths = std::mem::size_of::<NamedTempFile>()
            .checked_mul(2)
            .ok_or_else(|| Report::new(MongoDbTlsError::PreparationSize))?;
        let mut bytes = std::mem::size_of::<Self>();
        for capacity in [
            self.certificate.capacity(),
            self.private_key.capacity(),
            root_paths,
            file_paths,
        ] {
            bytes = bytes
                .checked_add(capacity)
                .ok_or_else(|| Report::new(MongoDbTlsError::PreparationSize))?;
        }
        let bytes = u64::try_from(bytes).change_context(MongoDbTlsError::PreparationSize)?;
        let reservation = executor
            .reserve(MemoryClass::Management, bytes)
            .await
            .change_context(MongoDbTlsError::PrepareIdentity)?;
        executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_, cancellation| {
                    cancellation
                        .check()
                        .change_context(MongoDbTlsError::PrepareIdentity)?;
                    let mut identity = NamedTempFile::new_in(root)
                        .change_context(MongoDbTlsError::CreateIdentity)?;
                    for (path, part) in [
                        (&self.certificate, IdentityPart::Certificate),
                        (&self.private_key, IdentityPart::PrivateKey),
                    ] {
                        cancellation
                            .check()
                            .change_context(MongoDbTlsError::PrepareIdentity)?;
                        let mut source = File::open(path)
                            .change_context(MongoDbTlsError::OpenIdentityPart { part })?;
                        io::copy(&mut source, identity.as_file_mut())
                            .change_context(MongoDbTlsError::CopyIdentityPart { part })?;
                        identity
                            .write_all(b"\n")
                            .change_context(MongoDbTlsError::SeparateIdentityParts)?;
                    }
                    Ok(identity)
                },
            )
            .await
            .change_context(MongoDbTlsError::PrepareIdentity)?
    }
}
