//! The options of every ONNX Runtime session Nervix builds: a branch's inference session and a
//! model binding's metadata inspection alike.
//!
//! Layer: data plane.
//! Owns: how ONNX Runtime may use threads and memory inside one session, set explicitly rather
//! than left to the library's defaults, and the builders that apply them.
//! Depends on: ONNX Runtime and the vocabulary's working-memory policy.
//! Must not know: Models, tensor plans, executor admission or what a session runs.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        data_plane,
        reason = "configures the native sessions the data plane runs and a binding inspection \
                  loads"
    )
)]

use nervix_models::InferencerWorkingMemory;
use ort::{
    environment::Environment,
    ep::{CPU, ExecutionProvider as _},
    session::{
        Session,
        builder::{GraphOptimizationLevel, SessionBuilder},
    },
};

/// How ONNX Runtime may use threads and memory inside one session.
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

    /// How far ONNX Runtime rewrites a model's graph when it builds a session: every optimization
    /// it offers. That is the library's own default, written here so that the level is Nervix's
    /// decision and a library upgrade cannot change it unnoticed.
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "inference and inspection run the model ONNX Runtime fully optimizes, the \
                      level every session has used"
        )
    )]
    const GRAPH_OPTIMIZATION: GraphOptimizationLevel = GraphOptimizationLevel::All;

    /// The working memory of a binding inspection's session, which only reads the model's ports
    /// and never runs it, so it has no working memory worth keeping.
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "a binding inspection never runs the model, so its session keeps no working \
                      memory"
        )
    )]
    const INSPECTION_WORKING_MEMORY: InferencerWorkingMemory = InferencerWorkingMemory::Released;

    /// A builder for the session one concrete branch runs its model in, keeping the working memory
    /// its inferencer states.
    pub(crate) fn inference(
        environment: &Environment,
        working_memory: InferencerWorkingMemory,
    ) -> error_stack::Result<SessionBuilder, ort::Error> {
        Self::builder(environment, working_memory)
    }

    /// A builder for the session a binding inspection reads the model's ports from.
    pub(crate) fn inspection(
        environment: &Environment,
    ) -> error_stack::Result<SessionBuilder, ort::Error> {
        Self::builder(environment, Self::INSPECTION_WORKING_MEMORY)
    }

    /// A session builder in `environment` with every option above.
    ///
    /// The working memory sets two of ONNX Runtime's options together. `Retained` keeps the CPU
    /// memory arena, the pool a session takes intermediate tensors from and never shrinks, and
    /// memory patterns, one cached allocation plan per set of input shapes. `Released` turns both
    /// off, so a run allocates its intermediate tensors as it needs them and frees them when it
    /// ends. One without the other keeps memory for nothing: a pattern without the arena still
    /// caches a plan per shape, and an arena without patterns still keeps its largest run.
    ///
    /// The CPU execution provider is registered directly, as the startup probe registers CUDA: a
    /// refusal is then an error here, where registering it through `with_execution_providers`
    /// would only log the refusal and leave the arena on, and log every success at info level.
    fn builder(
        environment: &Environment,
        working_memory: InferencerWorkingMemory,
    ) -> error_stack::Result<SessionBuilder, ort::Error> {
        let keeps_working_memory = match working_memory {
            InferencerWorkingMemory::Released => false,
            InferencerWorkingMemory::Retained => true,
        };
        let builder = Session::builder(environment)?;
        let builder = builder
            .with_intra_threads(Self::THREADS)
            .map_err(ort::Error::<()>::from)?;
        let builder = builder
            .with_inter_threads(Self::THREADS)
            .map_err(ort::Error::<()>::from)?;
        let builder = builder
            .with_optimization_level(Self::GRAPH_OPTIMIZATION)
            .map_err(ort::Error::<()>::from)?;
        let mut builder = builder
            .with_memory_pattern(keeps_working_memory)
            .map_err(ort::Error::<()>::from)?;
        CPU::default()
            .with_arena_allocator(keeps_working_memory)
            .register(&mut builder)?;
        Ok(builder)
    }
}

#[cfg(test)]
mod tests;
