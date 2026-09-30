//! Where an execution runs, what it is charged, and how admission refuses or cancels it.

use std::{num::NonZeroUsize, sync::Arc as StdArc};

use arrow_array::{Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use nervix_execution::{ExecutionConfig, WorkerCounts};
use nervix_models::Timestamp;
use nervix_primitives::sync::{
    atomic::{AtomicUsize, Ordering},
    blocking::{Mutex, mpsc},
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};

use super::*;
use crate::{
    CompileBinding, CompileOptions, compile_program_with_options_for_bindings,
    test_support::parse_program,
};

/// Answers `read_header` with the name it was asked for, after announcing each call and, when the
/// test holds it, waiting for the test to release it. Its policy decides which class the program
/// runs in.
#[derive(Debug)]
struct ScriptedHeaderInjector {
    policy: FunctionExecutionPolicy,
    calls: StdArc<AtomicUsize>,
    /// The test awaits this, so waiting for a call never blocks the test's own runtime thread.
    entered: UnboundedSender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl FunctionInjector for ScriptedHeaderInjector {
    fn execution_policy(&self, _function: &FunctionName) -> FunctionExecutionPolicy {
        self.policy
    }

    fn inject_with_context(
        &self,
        _function: &FunctionName,
        arguments: &[TypedArray],
        _rows: &RowSelection,
        _span: Span,
        _now: Timestamp,
        _prior_error_rows: RowErrorMask<'_>,
    ) -> error_stack::Result<InjectedResult, RuntimeError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.entered
            .send(())
            .expect("the test holds the entry receiver");
        // A disconnected release is how the test lets every later call through, so both outcomes
        // release the call.
        match self.release.lock().recv() {
            Ok(()) | Err(mpsc::RecvError) => {}
        }
        let [TypedArray::Utf8(names)] = arguments else {
            panic!("read_header must receive one Utf8 array");
        };
        Ok(InjectedResult::success(TypedArray::Utf8(names.clone())))
    }
}

/// What a test holds to drive a scripted injector: the calls it counted, the announcement of each
/// call, and the release that lets a waiting call return.
struct InjectorControl {
    calls: StdArc<AtomicUsize>,
    entered: UnboundedReceiver<()>,
    release: mpsc::Sender<()>,
}

fn scripted_injector(policy: FunctionExecutionPolicy) -> (ExecutionContext, InjectorControl) {
    let (entered_sender, entered) = unbounded_channel();
    let (release, release_receiver) = mpsc::channel();
    let calls = StdArc::new(AtomicUsize::new(0));
    let injector = ScriptedHeaderInjector {
        policy,
        calls: StdArc::clone(&calls),
        entered: entered_sender,
        release: Mutex::new(release_receiver),
    };
    let context = ExecutionContext {
        now: Timestamp::from_unix_nanos(1),
        injector: Some(triomphe::Arc::new(Box::new(injector))),
    };
    (
        context,
        InjectorControl {
            calls,
            entered,
            release,
        },
    )
}

/// A program that reads two headers, each through its own injected call.
fn two_header_program() -> triomphe::Arc<CompiledProgram> {
    let input_schema = StdArc::new(Schema::new(vec![
        Field::new("first", DataType::Utf8, false),
        Field::new("second", DataType::Utf8, false),
        Field::new("first_value", DataType::Utf8, true),
        Field::new("second_value", DataType::Utf8, true),
    ]));
    let parsed = parse_program(
        "SET first_value = read_header(input.first), second_value = read_header(input.second)",
    )
    .expect("the program must parse");
    let compiled = compile_program_with_options_for_bindings(
        &parsed,
        input_schema.clone(),
        [CompileBinding::writable("input", input_schema)],
        CompileOptions {
            allow_header_reads: true,
            ..CompileOptions::default()
        },
    )
    .expect("the header program must compile");
    triomphe::Arc::new(compiled)
}

fn header_batch(program: &CompiledProgram, rows: usize) -> TypedBatch {
    let names = |name: &str| TypedArray::Utf8(StringArray::from(vec![name; rows]));
    TypedBatch::try_new(
        program.input_schema.clone(),
        vec![
            names("first"),
            names("second"),
            TypedArray::uninitialized(DataType::Utf8, rows),
            TypedArray::uninitialized(DataType::Utf8, rows),
        ],
    )
    .expect("the batch matches the program's input")
}

fn increment_program() -> triomphe::Arc<CompiledProgram> {
    let input_schema = StdArc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
        Field::new("next", DataType::Int64, true),
    ]));
    let parsed = parse_program("SET next = input.value + 1").expect("the program must parse");
    let compiled = compile_program_with_options_for_bindings(
        &parsed,
        input_schema.clone(),
        [CompileBinding::writable("input", input_schema)],
        CompileOptions::default(),
    )
    .expect("the increment program must compile");
    triomphe::Arc::new(compiled)
}

fn increment_batch(program: &CompiledProgram, rows: usize) -> TypedBatch {
    let values = (0..rows)
        .map(|row| i64::try_from(row).expect("a test batch is far smaller than i64::MAX"))
        .collect::<Vec<_>>();
    TypedBatch::try_new(
        program.input_schema.clone(),
        vec![
            TypedArray::Int64(Int64Array::from(values)),
            TypedArray::uninitialized(DataType::Int64, rows),
        ],
    )
    .expect("the batch matches the program's input")
}

/// An executor with one worker and one waiting place per class, so a test can fill a class.
fn single_worker_executor() -> Executor {
    let one = NonZeroUsize::MIN;
    Executor::new(ExecutionConfig {
        workers: WorkerCounts {
            control_cpu: one,
            credentials_cpu: one,
            data_cpu: one,
            extension_cpu: one,
            bulk_cpu: one,
            consensus_storage: one,
            filesystem_storage: one,
            pending_jobs: one,
        },
        ..ExecutionConfig::default()
    })
    .expect("the default budgets hold the default operation limits")
}

async fn until(mut condition: impl FnMut() -> bool) {
    while !condition() {
        nervix_primitives::task::yield_now().await;
    }
}

#[nervix_primitives::test]
async fn a_small_batch_without_extension_calls_runs_inline() {
    let executor = Executor::default();
    let program = increment_program();
    let batch = increment_batch(&program, INLINE_ROW_LIMIT);
    let context = ExecutionContext::new(Timestamp::from_unix_nanos(1));

    execute_program_with_selection_in_context(&executor, &program, &batch, &context)
        .await
        .expect("the program executes");

    let snapshot = executor.snapshot();
    assert_eq!(snapshot.data_cpu.admitted, 0);
    assert_eq!(snapshot.extension_cpu.admitted, 0);
}

#[nervix_primitives::test]
async fn a_large_batch_runs_on_the_data_workers_under_a_relay_charge_of_its_columns() {
    let executor = Executor::default();
    let program = two_header_program();
    let batch = header_batch(&program, INLINE_ROW_LIMIT + 1);
    let expected_charge = batch.payload_bytes().expect("Arrow measures Utf8 columns");
    let (context, mut control) = scripted_injector(FunctionExecutionPolicy::Inline);

    let execution = nervix_primitives::task::spawn({
        let executor = executor.clone();
        async move {
            execute_program_with_selection_in_context(&executor, &program, &batch, &context).await
        }
    });
    control
        .entered
        .recv()
        .await
        .expect("the first call announces itself");
    let running = executor.snapshot();
    assert_eq!(running.data_cpu.running, 1);
    assert_eq!(running.extension_cpu.admitted, 0);
    assert_eq!(running.relay_memory.reserved_bytes, expected_charge);

    drop(control.release);
    execution
        .await
        .expect("the execution task completes")
        .expect("the program executes");
    let finished = executor.snapshot();
    assert_eq!(finished.data_cpu.completed, 1);
    assert_eq!(finished.relay_memory.reserved_bytes, 0);
}

#[nervix_primitives::test]
async fn an_extension_call_runs_on_the_extension_workers_whatever_the_batch_size() {
    let executor = Executor::default();
    let program = two_header_program();
    let batch = header_batch(&program, 1);
    let (context, control) = scripted_injector(FunctionExecutionPolicy::Extension);
    drop(control.release);

    let result = execute_program_with_selection_in_context(&executor, &program, &batch, &context)
        .await
        .expect("the program executes");

    assert_eq!(result.batch.row_count(), 1);
    assert_eq!(control.calls.load(Ordering::Acquire), 2);
    let snapshot = executor.snapshot();
    assert_eq!(snapshot.extension_cpu.admitted, 1);
    assert_eq!(snapshot.extension_cpu.completed, 1);
    assert_eq!(snapshot.data_cpu.admitted, 0);
    assert_eq!(snapshot.relay_memory.reserved_bytes, 0);
}

#[nervix_primitives::test]
async fn a_full_class_refuses_the_execution_as_not_admitted() {
    let executor = single_worker_executor();
    let program = two_header_program();
    let (held_context, mut held) = scripted_injector(FunctionExecutionPolicy::Extension);
    let holding = nervix_primitives::task::spawn({
        let executor = executor.clone();
        let program = program.clone();
        let batch = header_batch(&program, 1);
        async move {
            execute_program_with_selection_in_context(&executor, &program, &batch, &held_context)
                .await
        }
    });
    held.entered
        .recv()
        .await
        .expect("the held execution takes the worker");
    let (queued_context, queued) = scripted_injector(FunctionExecutionPolicy::Extension);
    drop(queued.release);
    let waiting = nervix_primitives::task::spawn({
        let executor = executor.clone();
        let program = program.clone();
        let batch = header_batch(&program, 1);
        async move {
            execute_program_with_selection_in_context(&executor, &program, &batch, &queued_context)
                .await
        }
    });
    until(|| executor.snapshot().extension_cpu.pending == 1).await;

    let (refused_context, refused_control) = scripted_injector(FunctionExecutionPolicy::Extension);
    let refused = execute_program_with_selection_in_context(
        &executor,
        &program,
        &header_batch(&program, 1),
        &refused_context,
    )
    .await
    .expect_err("a class holding its whole wait queue refuses the execution");

    assert!(matches!(
        refused.current_context(),
        RuntimeError::ExecutionNotAdmitted
    ));
    assert_eq!(refused_control.calls.load(Ordering::Acquire), 0);
    assert_eq!(executor.snapshot().extension_cpu.refused, 1);
    drop(held.release);
    holding
        .await
        .expect("the held execution completes")
        .expect("the held program executes");
    waiting
        .await
        .expect("the queued execution completes")
        .expect("the queued program executes");
    assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 0);
}

#[nervix_primitives::test]
async fn an_abandoned_execution_stops_at_its_next_instruction() {
    let executor = Executor::default();
    let program = two_header_program();
    let batch = header_batch(&program, 1);
    let (context, mut control) = scripted_injector(FunctionExecutionPolicy::Extension);

    let execution = nervix_primitives::task::spawn({
        let executor = executor.clone();
        async move {
            execute_program_with_selection_in_context(&executor, &program, &batch, &context).await
        }
    });
    control
        .entered
        .recv()
        .await
        .expect("the first call announces itself");
    execution.abort();
    let aborted = execution
        .await
        .expect_err("the aborted execution task does not complete");
    assert!(aborted.is_cancelled());
    // The worker still runs the first call, and keeps its charge, until the test releases it.
    assert_eq!(executor.snapshot().extension_cpu.running, 1);
    assert!(executor.snapshot().relay_memory.reserved_bytes > 0);

    drop(control.release);
    until(|| executor.snapshot().extension_cpu.completed == 1).await;
    assert_eq!(
        control.calls.load(Ordering::Acquire),
        1,
        "the execution ran the instruction after its caller stopped waiting"
    );
    assert_eq!(executor.snapshot().relay_memory.reserved_bytes, 0);
}
