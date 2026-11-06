//! Repository tooling, outside the product layer order.
//! Owns: atomic evidence publication, file digests and physical process timestamps.
//! Depends on: filesystem I/O, SHA-256 and typed orchestration errors.
//! Must not know: diagnostic policy, lease admission or product clocks.

use std::{
    fs,
    io::Read as _,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use aws_lc_rs::digest::{Context, SHA256};
use error_stack::ResultExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use serde::{Serialize, de::DeserializeOwned};

use crate::error::{Error, Result};

pub fn read<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes = fs::read(path).change_context(Error::Io {
        operation: "read evidence",
        path: path.into(),
    })?;
    serde_json::from_slice(&bytes).change_context(Error::Json { path: path.into() })
}

pub fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).change_context(Error::Io {
            operation: "create evidence directory",
            path: parent.into(),
        })?;
    }
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    let mut bytes = serde_json::to_vec(value).change_context(Error::Json { path: path.into() })?;
    bytes.push(b'\n');
    fs::write(&temporary, bytes).change_context(Error::Io {
        operation: "write evidence",
        path: temporary.clone(),
    })?;
    fs::rename(&temporary, path).change_context(Error::Io {
        operation: "publish evidence",
        path: path.into(),
    })
}

pub fn digest(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path).change_context(Error::Io {
        operation: "open digest input",
        path: path.into(),
    })?;
    // Non-security SHA-256 fingerprints identify compiler inputs and complete evidence.
    let mut context = Context::new(&SHA256);
    let mut bytes = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut bytes).change_context(Error::Io {
            operation: "read digest input",
            path: path.into(),
        })?;
        if count == 0 {
            break;
        }
        context.update(&bytes[..count]);
    }
    Ok(hex(context.finish().as_ref()))
}

pub fn fingerprint(value: &impl Serialize) -> Result<String> {
    let bytes = serde_json::to_vec(value).change_context(Error::Json {
        path: "identity payload".into(),
    })?;
    let mut context = Context::new(&SHA256);
    context.update(&bytes);
    Ok(hex(context.finish().as_ref()))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(
        bytes
            .len()
            .checked_mul(2)
            .assured("a SHA-256 digest is exactly 32 bytes"),
    );
    for byte in bytes {
        write!(output, "{byte:02x}").assured("writing into a String cannot fail");
    }
    output
}

pub fn unix_ns() -> i128 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(elapsed) => i128::try_from(elapsed.as_nanos()).assured(
            "the operating-system physical timestamp fits the lease's signed nanosecond coordinate",
        ),
        Err(before) => -i128::try_from(before.duration().as_nanos()).assured(
            "the operating-system physical timestamp fits the lease's signed nanosecond coordinate",
        ),
    }
}

pub fn remove_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error_stack::Report::new(error).change_context(Error::Io {
            operation: "remove evidence",
            path: path.into(),
        })),
    }
}
