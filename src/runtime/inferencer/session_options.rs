//! The options of every ONNX Runtime session Nervix builds: a branch's inference session and a
//! model binding's metadata inspection alike.
//!
//! Layer: data plane.
//! Owns: how ONNX Runtime may use threads inside one session, set explicitly rather than left to
//! the library's defaults, and the one builder that applies it.
//! Depends on: ONNX Runtime.
//! Must not know: Models, tensor plans, executor admission or what a session is built for.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        data_plane,
        reason = "configures the native sessions the data plane runs and a binding inspection \
                  loads"
    )
)]

use ort::{
    environment::Environment,
    session::{Session, builder::SessionBuilder},
};

/// How ONNX Runtime may use threads inside one session.
pub(crate) struct OnnxSessionOptions;

impl OnnxSessionOptions {
    /// The threads ONNX Runtime may use inside one session, for operators and for independent
    /// graph nodes alike.
    ///
    /// A session never starts threads of its own. Each model run occupies exactly one of the
    /// node's extension workers, so parallelism across branches comes from the extension class's
    /// bounded admission, which sizes the node's model CPU use. A binding inspection loads and
    /// optimizes its model on the one bulk worker it was admitted to; ONNX Runtime's default would
    /// start a pool the size of the machine's physical cores for every inspection.
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "each model run occupies one extension worker and each inspection one bulk \
                      worker; parallelism comes from the executor's bounded admission, not ONNX \
                      Runtime's pools"
        )
    )]
    const THREADS: usize = 1;

    /// A session builder in `environment` that carries every option above.
    pub(crate) fn builder(
        environment: &Environment,
    ) -> error_stack::Result<SessionBuilder, ort::Error> {
        let builder = Session::builder(environment)?;
        let builder = builder
            .with_intra_threads(Self::THREADS)
            .map_err(ort::Error::<()>::from)?;
        let builder = builder
            .with_inter_threads(Self::THREADS)
            .map_err(ort::Error::<()>::from)?;
        Ok(builder)
    }
}
