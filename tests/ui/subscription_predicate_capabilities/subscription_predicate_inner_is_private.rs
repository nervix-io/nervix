use nervix_server::runtime::subscription_predicate_capability_compile_tests::CompiledSubscriptionPredicate;

fn forbidden(predicate: CompiledSubscriptionPredicate) {
    let _ = predicate.predicate;
}

fn main() {}
