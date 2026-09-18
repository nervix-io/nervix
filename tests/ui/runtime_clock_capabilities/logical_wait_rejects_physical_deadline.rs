use nervix_connector::physical_time::PhysicalDeadline;
use nervix_server::runtime::clock_capability_compile_tests::{DomainClock, LogicalDeadline};
use tokio_util::sync::CancellationToken;

async fn forbidden(clock: &DomainClock, deadline: PhysicalDeadline) {
    let deadline: LogicalDeadline = deadline;
    drop(clock.wait_until(deadline, &CancellationToken::new()).await);
}

fn main() {}
