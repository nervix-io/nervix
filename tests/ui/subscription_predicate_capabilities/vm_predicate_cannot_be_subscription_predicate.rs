use nervix_server::runtime::subscription_predicate_capability_compile_tests::CompiledSubscriptionPredicate;
use nervix_vm::CompiledPredicate;

fn forbidden(predicate: CompiledPredicate) -> CompiledSubscriptionPredicate {
    predicate.into()
}

fn main() {}
