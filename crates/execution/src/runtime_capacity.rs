//! The native process runtime's capacity, resolved before its worker threads start.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Runtime worker, blocking-thread and stack setting declarations and validation.
//! - **Depends on.** Resolved CPU capacity, executor worker counts and the primitive runtime.
//! - **Must not know.** Graphs, sessions, detection or execution jobs.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns caller-driven native runtime capacity and construction"
    )
)]

use std::num::{NonZeroU64, NonZeroUsize};

use error_stack::Report;
use meticulous::OptionExt as _;
#[cfg(not(feature = "shuttle"))]
use meticulous::ResultExt as _;
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};
use thiserror::Error;
use ubyte::ByteUnit;

use crate::WorkerCounts;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeThreadOverrides {
    pub worker_threads: Option<NonZeroUsize>,
    pub max_blocking_threads: Option<NonZeroUsize>,
    pub thread_stack_size: Option<ByteUnit>,
}

impl RuntimeThreadOverrides {
    /// Validate explicit scalar bounds without observing the host or constructing a runtime.
    pub fn validate(&self) -> Result<(), Report<RuntimeThreadError>> {
        let context = RuntimeThreadContext {
            cpu_count: NonZeroUsize::MIN,
            executor_workers: NonZeroUsize::MIN,
            memory: None,
        };
        let stack = self
            .thread_stack_size
            .unwrap_or_else(RuntimeThreadStackSize::default_bytes);
        RuntimeThreadStackSize::validate(&stack, &context)?;
        RuntimeThreadSettings::validate_totals(
            self.worker_threads.unwrap_or(NonZeroUsize::MIN),
            self.max_blocking_threads.unwrap_or(NonZeroUsize::MIN),
            stack,
        )
    }
}

#[derive(Debug, Error)]
pub enum RuntimeThreadError {
    #[error("the native runtime could not start")]
    StartRuntime,
    #[error("the node CPU capacity cannot be represented on this host")]
    CpuCount,
    #[error("runtime-worker-threads ({threads}) exceeds the node CPU claim ({cpus})")]
    WorkerCapacity { threads: usize, cpus: usize },
    #[error("the runtime thread capacities do not add up to an addressable total")]
    ThreadTotal,
    #[error("the runtime threads and their stack sizes have no addressable total")]
    StackTotal,
    #[error("the executor worker capacities do not add up to an addressable total")]
    WorkerTotal,
    #[error("runtime-max-blocking-threads ({threads}) cannot hold the {required} executor workers")]
    BlockingCapacity { threads: usize, required: usize },
    #[error("runtime-thread-stack-size ({bytes}) must be positive and addressable")]
    StackSize { bytes: ByteUnit },
    #[error("runtime-thread-stack-size ({bytes}) exceeds node memory ({memory})")]
    StackAboveMemory { bytes: ByteUnit, memory: ByteUnit },
}

pub struct RuntimeThreadContext {
    cpu_count: NonZeroUsize,
    executor_workers: NonZeroUsize,
    memory: Option<NonZeroU64>,
}

impl RuntimeThreadContext {
    fn new(
        cpus: Option<NonZeroU64>,
        workers: WorkerCounts,
        memory: Option<NonZeroU64>,
    ) -> Result<Self, Report<RuntimeThreadError>> {
        let cpu_count = match cpus {
            Some(cpus) => NonZeroUsize::new(
                usize::try_from(cpus.get())
                    .map_err(|_| Report::new(RuntimeThreadError::CpuCount))?,
            )
            .verified("a positive count stays positive"),
            None => NonZeroUsize::MIN,
        };
        let mut count = 0usize;
        for workers in [
            workers.control_cpu,
            workers.credentials_cpu,
            workers.data_cpu,
            workers.extension_cpu,
            workers.bulk_cpu,
            workers.consensus_storage,
            workers.filesystem_storage,
            *workers.connector_storage.value(),
        ] {
            count = count
                .checked_add(workers.get())
                .ok_or_else(|| Report::new(RuntimeThreadError::WorkerTotal))?;
        }
        Ok(Self {
            cpu_count,
            executor_workers: NonZeroUsize::new(count)
                .verified("every executor has a control worker"),
            memory,
        })
    }
}

pub struct RuntimeWorkerThreads;
impl SettingDefinition for RuntimeWorkerThreads {
    type Class = NodeCapabilities;
    type Value = NonZeroUsize;
    type Context = RuntimeThreadContext;
    type Error = RuntimeThreadError;
    const NAME: &'static str = "runtime-worker-threads";
    const DERIVATION: Option<&'static str> =
        Some("the resolved node CPU count, or one when it is unknown");
    fn default_value(context: &Self::Context) -> Self::Value {
        context.cpu_count
    }
    fn validate(value: &Self::Value, context: &Self::Context) -> Result<(), Report<Self::Error>> {
        if value.get() > context.cpu_count.get() {
            return Err(Report::new(RuntimeThreadError::WorkerCapacity {
                threads: value.get(),
                cpus: context.cpu_count.get(),
            }));
        }
        Ok(())
    }
}

pub struct RuntimeMaxBlockingThreads;
impl SettingDefinition for RuntimeMaxBlockingThreads {
    type Class = NodeCapabilities;
    type Value = NonZeroUsize;
    type Context = RuntimeThreadContext;
    type Error = RuntimeThreadError;
    const NAME: &'static str = "runtime-max-blocking-threads";
    const DERIVATION: Option<&'static str> = Some(
        "all admitted executor workers, retaining at least the runtime's 512-thread allowance for \
         registered external and standard I/O work",
    );
    fn default_value(context: &Self::Context) -> Self::Value {
        NonZeroUsize::new(context.executor_workers.get().max(512))
            .verified("the positive executor count bounds this maximum")
    }
    fn validate(value: &Self::Value, context: &Self::Context) -> Result<(), Report<Self::Error>> {
        if value.get() < context.executor_workers.get() {
            return Err(Report::new(RuntimeThreadError::BlockingCapacity {
                threads: value.get(),
                required: context.executor_workers.get(),
            }));
        }
        Ok(())
    }
}

pub struct RuntimeThreadStackSize;
impl RuntimeThreadStackSize {
    pub fn default_bytes() -> ByteUnit {
        <Self as SettingDefinition>::default_value(&RuntimeThreadContext {
            cpu_count: NonZeroUsize::MIN,
            executor_workers: NonZeroUsize::MIN,
            memory: None,
        })
    }
}
impl SettingDefinition for RuntimeThreadStackSize {
    type Class = NodeCapabilities;
    type Value = ByteUnit;
    type Context = RuntimeThreadContext;
    type Error = RuntimeThreadError;
    const NAME: &'static str = "runtime-thread-stack-size";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &Self::Context) -> Self::Value {
        ByteUnit::Mebibyte(8)
    }
    fn validate(bytes: &Self::Value, context: &Self::Context) -> Result<(), Report<Self::Error>> {
        if bytes.as_u64() == 0 || usize::try_from(bytes.as_u64()).is_err() {
            return Err(Report::new(RuntimeThreadError::StackSize { bytes: *bytes }));
        }
        if let Some(memory) = context.memory
            && bytes.as_u64() > memory.get()
        {
            return Err(Report::new(RuntimeThreadError::StackAboveMemory {
                bytes: *bytes,
                memory: ByteUnit::Byte(memory.get()),
            }));
        }
        Ok(())
    }
}

/// A complete runtime profile whose values have been validated together.
///
/// ```
/// use nervix_execution::{ExecutionConfig, RuntimeThreadOverrides, RuntimeThreadSettings};
/// let settings = RuntimeThreadSettings::resolve(
///     RuntimeThreadOverrides::default(), None, ExecutionConfig::default().workers, None,
/// ).expect("the declared defaults fit");
/// assert_eq!(settings.worker_threads().get(), 1);
/// ```
///
/// A profile cannot be changed by replacing an individually validated field. Resolve the complete
/// overrides again so the CPU, executor, stack and thread-total relationships are checked.
///
/// ```compile_fail
/// use nervix_execution::{ExecutionConfig, RuntimeThreadOverrides, RuntimeThreadSettings};
/// let mut settings = RuntimeThreadSettings::resolve(
///     RuntimeThreadOverrides::default(), None, ExecutionConfig::default().workers, None,
/// ).expect("the declared defaults fit");
/// settings.worker_threads = settings.worker_threads;
/// ```
#[derive(Debug, Clone, Copy)]
pub struct RuntimeThreadSettings {
    worker_threads: Setting<RuntimeWorkerThreads>,
    max_blocking_threads: Setting<RuntimeMaxBlockingThreads>,
    thread_stack_size: Setting<RuntimeThreadStackSize>,
}

impl RuntimeThreadSettings {
    pub fn worker_threads(&self) -> NonZeroUsize {
        *self.worker_threads.value()
    }
    pub fn max_blocking_threads(&self) -> NonZeroUsize {
        *self.max_blocking_threads.value()
    }
    pub fn thread_stack_size(&self) -> ByteUnit {
        *self.thread_stack_size.value()
    }

    pub fn resolve(
        overrides: RuntimeThreadOverrides,
        cpus: Option<NonZeroU64>,
        workers: WorkerCounts,
        memory: Option<NonZeroU64>,
    ) -> Result<Self, Report<RuntimeThreadError>> {
        overrides.validate()?;
        let context = RuntimeThreadContext::new(cpus, workers, memory)?;
        let settings = Self {
            worker_threads: Setting::resolve(overrides.worker_threads, &context)?,
            max_blocking_threads: Setting::resolve(overrides.max_blocking_threads, &context)?,
            thread_stack_size: Setting::resolve(overrides.thread_stack_size, &context)?,
        };
        Self::validate_totals(
            *settings.worker_threads.value(),
            *settings.max_blocking_threads.value(),
            *settings.thread_stack_size.value(),
        )?;
        Ok(settings)
    }

    fn validate_totals(
        workers: NonZeroUsize,
        blocking: NonZeroUsize,
        stack: ByteUnit,
    ) -> Result<(), Report<RuntimeThreadError>> {
        let total = workers
            .get()
            .checked_add(blocking.get())
            .ok_or_else(|| Report::new(RuntimeThreadError::ThreadTotal))?;
        let stack = usize::try_from(stack.as_u64())
            .map_err(|_| Report::new(RuntimeThreadError::StackSize { bytes: stack }))?;
        total
            .checked_mul(stack)
            .ok_or_else(|| Report::new(RuntimeThreadError::StackTotal))?;
        Ok(())
    }

    #[cfg(not(feature = "shuttle"))]
    pub fn build(
        &self,
    ) -> error_stack::Result<nervix_primitives::runtime::Runtime, RuntimeThreadError> {
        nervix_primitives::runtime::Builder::new_multi_thread()
            .worker_threads(self.worker_threads.value().get())
            .max_blocking_threads(self.max_blocking_threads.value().get())
            .thread_stack_size(
                usize::try_from(self.thread_stack_size.value().as_u64())
                    .verified("the setting validated addressability"),
            )
            .enable_all()
            .build()
            .map_err(|error| Report::new(error).change_context(RuntimeThreadError::StartRuntime))
    }
}

#[cfg(all(test, not(any(feature = "shuttle", feature = "loom"))))]
mod tests {
    use super::*;

    fn workers() -> WorkerCounts {
        crate::WorkerSettings::resolve(&crate::ExecutionOverrides::default(), NonZeroU64::new(8))
            .expect("eight CPUs have a valid allocation")
            .counts()
    }

    #[test]
    fn the_native_runtime_uses_its_resolved_worker_count() {
        let settings = RuntimeThreadSettings::resolve(
            RuntimeThreadOverrides {
                worker_threads: NonZeroUsize::new(3),
                ..Default::default()
            },
            NonZeroU64::new(8),
            workers(),
            NonZeroU64::new(ByteUnit::Gibibyte(1).as_u64()),
        )
        .expect("the runtime settings fit the node");
        assert_eq!(settings.max_blocking_threads.value().get(), 512);
        assert_eq!(*settings.thread_stack_size.value(), ByteUnit::Mebibyte(8));
        let runtime = settings.build().expect("the bounded test runtime starts");
        assert_eq!(runtime.metrics().num_workers(), 3);
    }

    #[test]
    fn blocking_capacity_cannot_hide_an_admitted_executor_worker() {
        let error = RuntimeThreadSettings::resolve(
            RuntimeThreadOverrides {
                max_blocking_threads: NonZeroUsize::new(1),
                ..Default::default()
            },
            NonZeroU64::new(8),
            workers(),
            None,
        )
        .expect_err("one thread cannot back all admitted workers");
        assert!(matches!(
            error.current_context(),
            RuntimeThreadError::BlockingCapacity { .. }
        ));
    }
    #[test]
    fn runtime_overrides_refuse_unaddressable_counts_before_host_detection() {
        let error = RuntimeThreadOverrides {
            max_blocking_threads: NonZeroUsize::new(usize::MAX),
            ..Default::default()
        }
        .validate()
        .expect_err("one worker cannot be added to usize::MAX");
        assert!(matches!(
            error.current_context(),
            RuntimeThreadError::ThreadTotal
        ));
        let error = RuntimeThreadOverrides {
            thread_stack_size: Some(ByteUnit::Byte(0)),
            ..Default::default()
        }
        .validate()
        .expect_err("a zero stack is an invalid scalar");
        assert!(matches!(
            error.current_context(),
            RuntimeThreadError::StackSize { .. }
        ));
    }

    #[test]
    fn runtime_workers_cannot_exceed_the_cpu_claim() {
        let error = RuntimeThreadSettings::resolve(
            RuntimeThreadOverrides {
                worker_threads: NonZeroUsize::new(9),
                ..Default::default()
            },
            NonZeroU64::new(8),
            workers(),
            None,
        )
        .expect_err("an explicit worker count must fit the CPU claim");
        assert!(matches!(
            error.current_context(),
            RuntimeThreadError::WorkerCapacity {
                threads: 9,
                cpus: 8
            }
        ));
    }

    #[test]
    fn stack_capacity_fits_node_memory_and_the_addressable_thread_total() {
        let error = RuntimeThreadSettings::resolve(
            RuntimeThreadOverrides {
                thread_stack_size: Some(ByteUnit::Gibibyte(2)),
                ..Default::default()
            },
            NonZeroU64::new(8),
            workers(),
            NonZeroU64::new(ByteUnit::Gibibyte(1).as_u64()),
        )
        .expect_err("an individual stack cannot exceed node memory");
        assert!(matches!(
            error.current_context(),
            RuntimeThreadError::StackAboveMemory { .. }
        ));
        let error = RuntimeThreadSettings::resolve(
            RuntimeThreadOverrides {
                thread_stack_size: Some(ByteUnit::Byte(
                    u64::try_from(usize::MAX / 2).expect("supported address spaces fit u64"),
                )),
                ..Default::default()
            },
            NonZeroU64::new(8),
            workers(),
            None,
        )
        .expect_err("all potential stacks must have an addressable total");
        assert!(matches!(
            error.current_context(),
            RuntimeThreadError::StackTotal
        ));
    }
}
