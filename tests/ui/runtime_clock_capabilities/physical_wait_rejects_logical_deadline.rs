use nervix_server::runtime::clock_capability_compile_tests::{
    LogicalDeadline, PhysicalDeadlineCapability,
};

async fn forbidden(capability: PhysicalDeadlineCapability, deadline: LogicalDeadline) {
    capability.wait_until(deadline).await;
}

fn main() {}
