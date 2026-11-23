//! Acknowledgement renewal for retained runtime work and connector reconnect cadence.
//!
//! Layer: data plane.
//! - **Owns.** Borrowed acknowledgement scopes, the beat they renew at, and bounded reconnect
//!   delays.
//! - **Depends on.** Runtime ACK sets, the installed remote acknowledgement timing, retry settings
//!   and primitive timers.
//! - **Must not know.** Source protocols, graph planning, worker admission or error policies.

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

    /// False proves this scope has no acknowledgement obligation. Owners of large retained state
    /// may answer conservatively and defer walking it until a renewal is actually due.
    fn needs_keepalive(&self) -> bool {
        true
    }
}

/// Held work reports the acknowledgements it holds alive at the committed
/// `REMOTE_ACK_ALIVE_INTERVAL`, as a remote receiver reports the records of a delivery it holds.
///
/// An owner of held work keeps this node's installed timing from its creation. A keepalive scope
/// polls its work before it arms anything, so work that completes at its first poll reads no
/// interval and arms no timer. The scope reads the interval when it first finds its work pending
/// and keeps it until the work ends, as a delivery keeps the timing it captured, so a committed
/// change reaches a running owner at its next scope without rebuilding the owner.
impl SharedRemoteAckTiming {
    /// Await `future` while reporting `acks` alive as it starts and every alive interval it stays
    /// pending, so an ingestor's `ACK TIMEOUT` does not expire while work done on their behalf is
    /// still under way.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "held work renews its retained acknowledgement obligation while it waits"
        )
    )]
    pub(in crate::runtime) async fn keep_alive_while<A, F>(&self, acks: &A, future: F) -> F::Output
    where
        A: AcknowledgementKeepalive + ?Sized,
        F: std::future::Future,
    {
        if acks.needs_keepalive() {
            acks.keep_alive();
        }
        self.keep_alive_while_pending(acks, future).await
    }

    /// Await `future`, reporting `acks` alive each time another alive interval passes with
    /// `future` still pending. Unlike [`Self::keep_alive_while`] it reports nothing up front, so
    /// work that ends within one interval, as a relay delivery ordinarily does, reports nothing at
    /// all.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "held work renews its retained acknowledgement obligation while it waits"
        )
    )]
    pub(in crate::runtime) async fn keep_alive_while_pending<A, F>(
        &self,
        acks: &A,
        future: F,
    ) -> F::Output
    where
        A: AcknowledgementKeepalive + ?Sized,
        F: std::future::Future,
    {
        // Pin before selecting the empty-ACK path so this wrapper stores the owned future once.
        // Borrowing that one future on both paths also keeps nested delivery futures bounded.
        let mut future = std::pin::pin!(future);
        if !acks.needs_keepalive() {
            return future.await;
        }
        let mut interval = None;
        loop {
            nervix_primitives::task::consume_budget().await;
            // The work is polled first, and the beat reads the interval and arms its timer only
            // once the work is pending.
            nervix_primitives::select! {
                biased;
                result = &mut future => return result,
                _ = self.ack_alive_beat(&mut interval) => {}
            }
            acks.keep_alive();
        }
    }

    /// Waits one alive interval of a keepalive scope. The scope's first beat reads the committed
    /// interval into `interval`, and every later beat of the scope waits that same interval.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "held work renews its retained acknowledgement obligation while it waits"
        )
    )]
    pub(in crate::runtime) async fn ack_alive_beat(&self, interval: &mut Option<Duration>) {
        let interval = *interval.get_or_insert_with(|| self.ack_alive_interval());
        sleep(interval).await;
    }
}

impl AcknowledgementKeepalive for AckSet {
    fn needs_keepalive(&self) -> bool {
        !self.is_empty()
    }

    fn keep_alive(&self) {
        self.ack_alive();
    }
}

impl AcknowledgementKeepalive for [AckSet] {
    fn needs_keepalive(&self) -> bool {
        self.iter().any(|acks| !acks.is_empty())
    }

    fn keep_alive(&self) {
        for acks in self {
            acks.ack_alive();
        }
    }
}

impl AcknowledgementKeepalive for [RelayMessage] {
    fn needs_keepalive(&self) -> bool {
        self.iter().any(|message| !message.acks.is_empty())
    }

    fn keep_alive(&self) {
        for message in self {
            message.acks.ack_alive();
        }
    }
}

impl AcknowledgementKeepalive for [CorrelatorPendingMessage] {
    fn needs_keepalive(&self) -> bool {
        self.iter().any(|pending| !pending.message.acks.is_empty())
    }

    fn keep_alive(&self) {
        for pending in self {
            pending.message.acks.ack_alive();
        }
    }
}

impl AcknowledgementKeepalive for CorrelatorBranchState {
    fn needs_keepalive(&self) -> bool {
        !self.pending_left.is_empty() || !self.pending_right.is_empty()
    }

    fn keep_alive(&self) {
        self.pending_left.as_slice().keep_alive();
        self.pending_right.as_slice().keep_alive();
    }
}

impl AcknowledgementKeepalive for [(CorrelatorPendingMessage, CorrelatorPendingMessage)] {
    fn needs_keepalive(&self) -> bool {
        !self.is_empty()
    }

    fn keep_alive(&self) {
        for (left, right) in self {
            left.message.acks.ack_alive();
            right.message.acks.ack_alive();
        }
    }
}

impl AcknowledgementKeepalive for [ReordererOutputBuffer] {
    fn needs_keepalive(&self) -> bool {
        self.iter().any(ReordererOutputBuffer::has_acknowledgements)
    }

    fn keep_alive(&self) {
        for buffer in self {
            buffer.ack_alive();
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct RuntimeReconnectBackoff {
    pub(super) initial: Duration,
    pub(super) next: Duration,
    pub(super) max: Duration,
    pub(super) settings: nervix_connector::RetryBackoffSettings,
}

pub(super) fn reconnect_policy(settings: nervix_models::DataPlaneSettings) -> ParsedRetryPolicy {
    ParsedRetryPolicy {
        backoff: settings.reconnect_initial(),
        max_backoff: settings.reconnect_max(),
    }
}

impl Default for RuntimeReconnectBackoff {
    fn default() -> Self {
        Self::from_settings(
            nervix_models::DataPlaneSettings::default(),
            nervix_connector::RetryBackoffSettings::default(),
        )
    }
}

impl RuntimeReconnectBackoff {
    pub(super) fn from_settings(
        settings: nervix_models::DataPlaneSettings,
        retry: nervix_connector::RetryBackoffSettings,
    ) -> Self {
        Self::from_policy(reconnect_policy(settings), retry)
    }

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
        timing: &SharedRemoteAckTiming,
        shutdown_rx: &mut watch::Receiver<bool>,
        acks: &(impl AcknowledgementKeepalive + ?Sized),
    ) -> bool {
        let delay = self.take_next_delay();
        Self::wait_duration_with_ack_alive(delay, timing, shutdown_rx, acks).await
    }

    /// Waits `delay`, reporting `acks` alive as the wait starts, every committed alive interval of
    /// `timing` and as it ends, unless shutdown ends it first. The wait reads the interval once,
    /// when it first has time left to wait.
    pub(in crate::runtime) async fn wait_duration_with_ack_alive(
        delay: Duration,
        timing: &SharedRemoteAckTiming,
        shutdown_rx: &mut watch::Receiver<bool>,
        acks: &(impl AcknowledgementKeepalive + ?Sized),
    ) -> bool {
        Self::wait_reporting_alive(Instant::now() + delay, None, timing, shutdown_rx, acks).await
    }

    /// Waits one committed alive interval of `timing`, reporting `acks` alive as the wait starts
    /// and as it ends, unless shutdown ends it first.
    pub(in crate::runtime) async fn wait_ack_alive_interval(
        timing: &SharedRemoteAckTiming,
        shutdown_rx: &mut watch::Receiver<bool>,
        acks: &(impl AcknowledgementKeepalive + ?Sized),
    ) -> bool {
        let interval = timing.ack_alive_interval();
        Self::wait_reporting_alive(
            Instant::now() + interval,
            Some(interval),
            timing,
            shutdown_rx,
            acks,
        )
        .await
    }

    /// Waits until `deadline`, reporting `acks` alive as the wait starts, every alive interval and
    /// as it ends. `interval` is the alive interval the wait already read, if any.
    async fn wait_reporting_alive(
        deadline: Instant,
        mut interval: Option<Duration>,
        timing: &SharedRemoteAckTiming,
        shutdown_rx: &mut watch::Receiver<bool>,
        acks: &(impl AcknowledgementKeepalive + ?Sized),
    ) -> bool {
        loop {
            nervix_primitives::task::consume_budget().await;
            acks.keep_alive();
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO);
            if remaining.is_zero() {
                return true;
            }
            let beat = *interval.get_or_insert_with(|| timing.ack_alive_interval());
            nervix_primitives::select! {
                changed = shutdown_rx.changed() => {
                    return !(changed.is_err() || *shutdown_rx.borrow());
                }
                _ = sleep(remaining.min(beat)) => {}
            }
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use super::*;
    use crate::runtime_ack::AckProgress;

    /// A publication holding `interval` as its committed alive interval.
    fn installed(interval: Duration) -> SharedRemoteAckTiming {
        let timing = SharedRemoteAckTiming::default();
        timing.install(nervix_models::RemoteAckTiming {
            remote_ack_alive_interval: interval,
            ..nervix_models::RemoteAckTiming::default()
        });
        timing
    }

    /// A keepalive scope beats at the alive interval it read when its work first waited, until the
    /// work ends, and the scope after a committed change beats at the changed interval.
    #[nervix_primitives::test(start_paused = true)]
    async fn a_keepalive_scope_keeps_its_interval_and_the_next_scope_reads_a_change() {
        let first = Duration::from_millis(300);
        let changed = Duration::from_millis(40);
        let timing = installed(first);
        let (acks, mut completion) = AckSet::root();

        let (release, released) = oneshot::channel::<()>();
        let started = Instant::now();
        let scope = timing.keep_alive_while_pending(&acks, released);
        let mut scope = std::pin::pin!(scope);
        for beat in 1..=2_u32 {
            let progress = nervix_primitives::select! {
                biased;
                _ = &mut scope => panic!("the held work is still pending"),
                progress = completion.wait_for_progress() => progress,
            };
            assert_eq!(progress, AckProgress::Alive);
            let elapsed = started.elapsed();
            assert!(
                elapsed >= first * beat && elapsed < first * beat + changed,
                "beat {beat} came {elapsed:?} after the scope started"
            );
            // A change committed while the scope runs reaches only the scopes after it.
            timing.install(nervix_models::RemoteAckTiming {
                remote_ack_alive_interval: changed,
                ..nervix_models::RemoteAckTiming::default()
            });
        }
        release
            .send(())
            .assured("the scope still holds the receiver it waits on");
        scope
            .await
            .assured("the released work completes with the value sent to it");

        let (release, released) = oneshot::channel::<()>();
        let restarted = Instant::now();
        let scope = timing.keep_alive_while_pending(&acks, released);
        let mut scope = std::pin::pin!(scope);
        let progress = nervix_primitives::select! {
            biased;
            _ = &mut scope => panic!("the held work is still pending"),
            progress = completion.wait_for_progress() => progress,
        };
        assert_eq!(progress, AckProgress::Alive);
        let elapsed = restarted.elapsed();
        assert!(
            elapsed >= changed && elapsed < first,
            "the scope after the change beat {elapsed:?} after it started"
        );
        release
            .send(())
            .assured("the scope still holds the receiver it waits on");
        scope
            .await
            .assured("the released work completes with the value sent to it");
    }

    /// Work that completes at its first poll reports nothing alive.
    #[nervix_primitives::test(start_paused = true)]
    async fn work_that_completes_at_once_reports_nothing() {
        let timing = installed(Duration::from_millis(300));
        let (acks, mut completion) = AckSet::root();

        timing
            .keep_alive_while_pending(&acks, std::future::ready(()))
            .await;

        assert!(
            futures_util::FutureExt::now_or_never(completion.wait_for_progress()).is_none(),
            "a scope whose work completed at its first poll reports nothing"
        );
    }

    /// A wait of one alive interval lasts the committed interval and reports its acknowledgements
    /// alive as it starts and as it ends.
    #[nervix_primitives::test(start_paused = true)]
    async fn a_wait_of_one_alive_interval_lasts_the_committed_interval() {
        let committed = Duration::from_millis(300);
        let timing = installed(committed);
        let (_shutdown, mut shutdown_rx) = watch::channel(false);
        let (acks, mut completion) = AckSet::root();

        let started = Instant::now();
        assert!(
            RuntimeReconnectBackoff::wait_ack_alive_interval(&timing, &mut shutdown_rx, &acks)
                .await
        );

        let elapsed = started.elapsed();
        assert!(
            elapsed >= committed && elapsed < committed * 2,
            "the wait lasted {elapsed:?}"
        );
        assert_eq!(completion.wait_for_progress().await, AckProgress::Alive);
    }
}
