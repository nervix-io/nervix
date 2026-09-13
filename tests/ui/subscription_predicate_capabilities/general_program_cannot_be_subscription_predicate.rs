use nervix_server::runtime::subscription_predicate_capability_compile_tests::CompiledSubscriptionPredicate;
use nervix_vm::CompiledProgram;

fn forbidden(program: CompiledProgram) -> CompiledSubscriptionPredicate {
    program.into()
}

fn main() {}
