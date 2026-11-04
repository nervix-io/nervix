//! A host blocks on a call from a thread of its own, and the call's futures and every frame that
//! polls them share that thread's stack. A Java thread has 1 MiB unless its host asks for more, and
//! the conformance probes load the unoptimized library, in which each frame keeps its own copy of
//! every future it builds or moves. A prepared command therefore has to complete on a thread with
//! a fraction of that stack.

use meticulous::ResultExt as _;
use nervix_client_wire::{ClientRequest, ReplyBody};

use super::{Loopback, completed, domain, wire_outcome};

/// Half the stack a Java thread has by default.
const HOST_THREAD_STACK_BYTES: usize = 512 * 1024;

#[test]
fn a_prepared_command_completes_on_a_host_thread_with_half_a_java_stack() {
    let runtime = nervix_primitives::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .assured("the test runtime starts");
    let mut loopback = {
        let _entered = runtime.enter();
        Loopback::new(Some(domain("tenant")))
    };
    let client = loopback.client.clone();
    let execution = runtime.block_on(client.prepare_execution("CREATE RESOURCE models;"));
    let answering = runtime.spawn(async move {
        let command = loopback.next_request().await;
        let ClientRequest::Command(sent) = &command.request else {
            panic!("the statement is sent as a command");
        };
        let mut outcome = wire_outcome("command-1", completed(), "created resource 'models'");
        outcome.execution_reference = sent.execution_reference.clone();
        loopback
            .answer(command.request_id, ReplyBody::Command(Box::new(outcome)))
            .await;
        loopback
    });
    let handle = runtime.handle().clone();
    // A call that needs more stack than the host thread has aborts the whole test process.
    let host = nervix_primitives::thread::Builder::new()
        .stack_size(HOST_THREAD_STACK_BYTES)
        .spawn(move || handle.block_on(client.execute_prepared(&execution)))
        .assured("the host thread starts");
    let outcome = host
        .join()
        .assured("a call that fits the host thread's stack returns to it")
        .assured("the answered command completes");
    assert!(outcome.succeeded());
    runtime
        .block_on(answering)
        .assured("the answering task ends");
}
