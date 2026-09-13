use nervix_roto::UdfExecutor;
use nervix_vm::{FunctionInjector, TypedArray, program::{FunctionName, Span}};

fn forbidden(
    injector: &UdfExecutor,
    function: &FunctionName,
    arguments: &[TypedArray],
    span: Span,
) {
    let _ = <UdfExecutor as FunctionInjector>::inject(
        injector,
        function,
        arguments,
        arguments.len(),
        span,
    );
}

fn main() {}
