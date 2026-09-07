use super::*;

#[derive(Debug, Clone)]
pub(in crate::runtime) struct RuntimeReconnectBackoff {
    pub(super) initial: Duration,
    pub(super) next: Duration,
    pub(super) max: Duration,
}

impl Default for RuntimeReconnectBackoff {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(250),
            next: Duration::from_millis(250),
            max: Duration::from_secs(30),
        }
    }
}

impl RuntimeReconnectBackoff {
    pub(in crate::runtime) fn from_policy(policy: ParsedRetryPolicy) -> Self {
        Self {
            initial: policy.backoff,
            next: policy.backoff,
            max: policy.max_backoff,
        }
    }

    pub(in crate::runtime) fn reset(&mut self) {
        self.next = self.initial;
    }

    pub(in crate::runtime) fn next_delay(&self) -> Duration {
        self.next
    }

    pub(in crate::runtime) fn take_next_delay(&mut self) -> Duration {
        let delay = self.next;
        // Saturation is the policy here: the backoff doubles until it reaches the configured
        // ceiling and stays there, so a doubling that leaves `Duration` clamps to that ceiling.
        self.next = self.next.saturating_mul(2).min(self.max);
        delay
    }

    pub(in crate::runtime) async fn wait(
        &mut self,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> bool {
        let delay = self.take_next_delay();
        tokio::select! {
            changed = shutdown_rx.changed() => {
                !(changed.is_err() || *shutdown_rx.borrow())
            }
            _ = sleep(delay) => true,
        }
    }
    pub(in crate::runtime) async fn wait_with_ack_alive(
        &mut self,
        shutdown_rx: &mut watch::Receiver<bool>,
        acks: &AckSet,
    ) -> bool {
        let delay = self.take_next_delay();
        Self::wait_duration_with_ack_alive(delay, shutdown_rx, acks).await
    }

    pub(in crate::runtime) async fn wait_duration_with_ack_alive(
        delay: Duration,
        shutdown_rx: &mut watch::Receiver<bool>,
        acks: &AckSet,
    ) -> bool {
        let deadline = Instant::now() + delay;
        loop {
            tokio::task::consume_budget().await;
            acks.ack_alive();
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO);
            if remaining.is_zero() {
                return true;
            }
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    return !(changed.is_err() || *shutdown_rx.borrow());
                }
                _ = sleep(remaining.min(Duration::from_millis(100))) => {}
            }
        }
    }
}
