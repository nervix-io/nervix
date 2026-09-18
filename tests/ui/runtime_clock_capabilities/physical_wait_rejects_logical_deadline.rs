use nervix_connector::physical_time::{PhysicalDeadline, PhysicalDeadlineCapability};
use nervix_server::runtime::clock_capability_compile_tests::LogicalDeadline;

async fn forbidden(capability: PhysicalDeadlineCapability, deadline: LogicalDeadline) {
    let deadline: PhysicalDeadline = deadline;
    capability.wait_until(deadline).await;
}

fn main() {}
