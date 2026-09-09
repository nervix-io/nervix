use nervix_server::runtime::clock_capability_compile_tests::{
    DomainClock, PhysicalDeadline,
};
use tokio_util::sync::CancellationToken;

async fn forbidden(clock: &DomainClock, deadline: PhysicalDeadline) {
    drop(clock.wait_until(deadline, &CancellationToken::new()).await);
}

fn main() {}
