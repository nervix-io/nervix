use nervix_connector::ParsedRetryPolicy;

use super::*;

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "each admitted retry renews the retained acknowledgement obligation"
    )
)]
pub(in crate::runtime) trait AcknowledgementKeepalive {
    fn keep_alive(&self);

    /// Await `future` while reporting these acknowledgements alive every
    /// `REMOTE_ACK_ALIVE_INTERVAL`, as a remote receiver reports the acknowledgements it holds, so
    /// an ingestor's `ACK TIMEOUT` does not expire while work done on their behalf is still under
    /// way.
    async fn keep_alive_while<F>(&self, future: F) -> F::Output
    where
        F: std::future::Future,
    {
        self.keep_alive();
        self.keep_alive_while_pending(future).await
    }

    /// Await `future`, reporting these acknowledgements alive each time another
    /// `REMOTE_ACK_ALIVE_INTERVAL` passes with `future` still pending. Unlike
    /// [`Self::keep_alive_while`] it reports nothing up front, so work that ends within one
    /// interval, as a relay delivery ordinarily does, reports nothing at all.
    async fn keep_alive_while_pending<F>(&self, future: F) -> F::Output
    where
        F: std::future::Future,
    {
        let mut future = std::pin::pin!(future);
        loop {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                biased;
                result = &mut future => return result,
                _ = sleep(REMOTE_ACK_ALIVE_INTERVAL) => {}
            }
            self.keep_alive();
        }
    }
}

impl AcknowledgementKeepalive for AckSet {
    fn keep_alive(&self) {
        self.ack_alive();
    }
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct RuntimeReconnectBackoff {
    pub(super) initial: Duration,
    pub(super) next: Duration,
    pub(super) max: Duration,
    pub(super) settings: nervix_connector::RetryBackoffSettings,
}

impl Default for RuntimeReconnectBackoff {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(250),
            next: Duration::from_millis(250),
            max: Duration::from_secs(30),
            settings: nervix_connector::RetryBackoffSettings::default(),
        }
    }
}

impl RuntimeReconnectBackoff {
    pub(in crate::runtime) fn from_policy(
        policy: ParsedRetryPolicy,
        settings: nervix_connector::RetryBackoffSettings,
    ) -> Self {
        Self {
            initial: policy.backoff,
            next: policy.backoff,
            max: policy.max_backoff,
            settings,
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
        self.next = self.settings.next_delay(self.next, self.max);
        delay
    }

    #[cfg(test)]
    pub(in crate::runtime) async fn wait(
        &mut self,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> bool {
        let delay = self.take_next_delay();
        nervix_primitives::select! {
            changed = shutdown_rx.changed() => {
                !(changed.is_err() || *shutdown_rx.borrow())
            }
            _ = sleep(delay) => true,
        }
    }

    pub(in crate::runtime) async fn wait_with_ack_alive(
        &mut self,
        shutdown_rx: &mut watch::Receiver<bool>,
        acks: &(impl AcknowledgementKeepalive + ?Sized),
    ) -> bool {
        let delay = self.take_next_delay();
        Self::wait_duration_with_ack_alive(delay, shutdown_rx, acks).await
    }

    pub(in crate::runtime) async fn wait_duration_with_ack_alive(
        delay: Duration,
        shutdown_rx: &mut watch::Receiver<bool>,
        acks: &(impl AcknowledgementKeepalive + ?Sized),
    ) -> bool {
        let deadline = Instant::now() + delay;
        loop {
            nervix_primitives::task::consume_budget().await;
            acks.keep_alive();
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO);
            if remaining.is_zero() {
                return true;
            }
            nervix_primitives::select! {
                changed = shutdown_rx.changed() => {
                    return !(changed.is_err() || *shutdown_rx.borrow());
                }
                _ = sleep(remaining.min(Duration::from_millis(100))) => {}
            }
        }
    }
}
