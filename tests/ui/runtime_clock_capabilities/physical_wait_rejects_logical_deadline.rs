use nervix_server::runtime::clock_capability_compile_tests::{
    LogicalDeadline, PhysicalDeadline, PhysicalDeadlineCapability,
};

async fn forbidden(capability: PhysicalDeadlineCapability, deadline: LogicalDeadline) {
    let deadline: PhysicalDeadline = deadline;
    capability.wait_until(deadline).await;
}

fn main() {}
