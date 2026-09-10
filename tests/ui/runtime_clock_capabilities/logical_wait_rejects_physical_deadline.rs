use nervix_server::runtime::clock_capability_compile_tests::{
    DomainClock, LogicalDeadline, PhysicalDeadline,
};
use tokio_util::sync::CancellationToken;

async fn forbidden(clock: &DomainClock, deadline: PhysicalDeadline) {
    let deadline: LogicalDeadline = deadline;
    drop(clock.wait_until(deadline, &CancellationToken::new()).await);
}

fn main() {}
