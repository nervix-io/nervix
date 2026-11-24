//! Resource-mounted NATS credentials preparation.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Filesystem admission, the credentials-byte charge and clearing the temporary file.
//! - **Depends on.** The node executor and the NATS authentication decoder.
//! - **Must not know.** Graph routing, placement, source records or retry policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns admitted NATS credentials-file preparation")
)]

use std::{fs::File, io::Read as _};

use async_nats::ConnectOptions;
use error_stack::{Report, ResultExt as _};
use nervix_execution::{Cancellation, Executor, MemoryClass, Reservation, StorageClass};
use zeroize::Zeroizing;

use super::{NatsClientOptionsError, authentication};

pub(super) async fn options(
    executor: &Executor,
    path: String,
) -> Result<ConnectOptions, Report<NatsClientOptionsError>> {
    let charge = executor
        .try_reserve(MemoryClass::Credentials, 0)
        .change_context(NatsClientOptionsError::CredentialsPreparation)?;
    executor
        .run_storage(
            StorageClass::Filesystem,
            charge,
            move |charge, cancellation| read_options(&path, charge, cancellation),
        )
        .await
        .change_context(NatsClientOptionsError::CredentialsPreparation)?
}

fn read_options(
    path: &str,
    mut charge: Reservation,
    cancellation: &Cancellation,
) -> Result<ConnectOptions, Report<NatsClientOptionsError>> {
    cancellation
        .check()
        .change_context(NatsClientOptionsError::CredentialsPreparation)?;
    let mut file = File::open(path).map_err(file_failure)?;
    // Mounted resource versions are immutable. Charge their exact length before allocating or
    // reading any content, and keep that charge until the temporary bytes have been cleared.
    let bytes = file.metadata().map_err(file_failure)?.len();
    charge
        .grow_to(bytes)
        .change_context(NatsClientOptionsError::CredentialsPreparation)?;
    let length = usize::try_from(bytes).map_err(|source| {
        Report::new(NatsClientOptionsError::CredentialsPreparation).attach(source)
    })?;
    let mut contents = Zeroizing::new(vec![0; length]);
    file.read_exact(&mut contents).map_err(file_failure)?;
    cancellation
        .check()
        .change_context(NatsClientOptionsError::CredentialsPreparation)?;
    let contents = std::str::from_utf8(&contents).map_err(|source| {
        file_failure(std::io::Error::new(std::io::ErrorKind::InvalidData, source))
    })?;
    authentication::credentials_options(contents)
        .change_context(NatsClientOptionsError::CredentialsFile)
}

fn file_failure(source: std::io::Error) -> Report<NatsClientOptionsError> {
    // Confidential paths can occur in filesystem failures; the typed attachment stays opaque.
    Report::new(NatsClientOptionsError::CredentialsFile).attach(source)
}
