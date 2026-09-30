//! The frames one session sends its client, on a control lane and a subscription lane.
//!
//! Layer: edges.
//!
//! - **Owns.** The two bounded lanes a session's frames wait in for its transport, the order the
//!   transport takes them in, withdrawing the frames a subscription generation still has queued,
//!   and the ending that is the last frame of a session.
//! - **Depends on.** Tokio channels and cancellation, and the client wire frame type.
//! - **Must not know.** What a frame says, which request or subscription produced it, or how a
//!   transport writes it.
//!
//! Replies, transfer parts and session events travel on the control lane. Subscription rows and
//! the notices about them travel on the subscription lane. The transport always takes a queued
//! control frame first, so neither a reply nor the unsubscribe that stops a subscription waits
//! behind rows the client has not read, and a blocking subscription whose client reads slowly
//! holds only its own relay. The lanes cannot reorder what the transport already took: a frame
//! handed to the transport stays ahead of every frame queued after it.
//! An attached clock's tick occupies one replaceable control slot. Its producer can overwrite
//! that slot until transport takes it, or withdraw it when the clock state changes.
//!
//! Rows follow their subscription's opening reply, because a subscription queues rows only after
//! that reply is queued and the transport takes the reply first. A subscription's notices share
//! the lane with its rows and stay in order with them. Withdrawing a subscription discards every
//! frame it still has queued, so nothing about it reaches the client after the reply that deleted
//! it.
//!
//! Once the session ends no producer waits on the client any more. The transport takes what the
//! control lane already holds, then the ending if the session said why it ended, and nothing after
//! it.

use futures_util::{Stream, stream};
use nervix_client_wire::{EncodedFrame, ServerFrame};
use nervix_primitives::sync::{
    CancellationToken,
    atomic::{AtomicBool, Ordering},
    blocking::Mutex,
    mpsc,
    mpsc::error::{TryRecvError, TrySendError},
};
use triomphe::Arc;

/// How many control frames a session queues for its transport before their producers wait. A
/// frame is at most the session frame limit, so this bounds the reply and event bytes a session
/// holds for a client that reads slowly.
pub(in crate::application) const SESSION_CONTROL_CAPACITY: usize = 16;

/// How many subscription frames a session queues before a blocking subscription waits and a
/// dropping one discards rows. It bounds the row bytes a session holds for a client, shared by
/// every subscription of the session.
pub(in crate::application) const SESSION_SUBSCRIPTION_CAPACITY: usize = 16;

/// The frame was not queued: the session ended, the transport stopped taking frames, or the
/// subscription that sent it was withdrawn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application) struct LaneClosed;

/// Why a subscription frame found no place on its lane without waiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application) enum LaneRefusal {
    /// The lane is full, and the frame was not queued.
    Full,
    /// The lane is closed, for any of the reasons [`LaneClosed`] names.
    Closed,
}

/// What a session and its transport share about the session's end.
struct SessionEnding {
    /// Cancelled when the session ends, whatever ended it.
    ended: CancellationToken,
    /// The frame that says why the session ended, when it said so.
    frame: Mutex<Option<EncodedFrame<ServerFrame>>>,
}

struct OutboundInner {
    control: mpsc::Sender<ControlFrame>,
    subscriptions: mpsc::Sender<LaneFrame>,
    /// Also held by the transport's [`SessionFrames`], which writes the ending last.
    ending: Arc<SessionEnding>,
}

/// Where every producer of one session queues its frames. Cloning it is one reference count.
#[derive(Clone)]
pub(in crate::application) struct SessionOutbound {
    inner: Arc<OutboundInner>,
}

/// A control frame whose producer may replace its contents until the transport takes it. One
/// queued slot therefore holds only the newest tick of an attached domain.
#[derive(Clone)]
pub(in crate::application) struct ReplaceableControlFrame {
    frame: Arc<Mutex<Option<EncodedFrame<ServerFrame>>>>,
}

impl ReplaceableControlFrame {
    pub(in crate::application) fn new(frame: EncodedFrame<ServerFrame>) -> Self {
        Self {
            frame: Arc::new(Mutex::new(Some(frame))),
        }
    }

    /// Replaces a queued frame. Returns the supplied frame once the transport has taken the slot.
    pub(in crate::application) fn replace(
        &self,
        frame: EncodedFrame<ServerFrame>,
    ) -> Result<(), EncodedFrame<ServerFrame>> {
        let mut pending = self.frame.lock();
        if pending.is_none() {
            return Err(frame);
        }
        *pending = Some(frame);
        Ok(())
    }

    fn take(&self) -> Option<EncodedFrame<ServerFrame>> {
        self.frame.lock().take()
    }

    /// Removes a superseded frame still waiting on the control lane.
    pub(in crate::application) fn withdraw(&self) {
        drop(self.frame.lock().take());
    }
}

enum ControlFrame {
    Direct(EncodedFrame<ServerFrame>),
    Replaceable(ReplaceableControlFrame),
}

impl ControlFrame {
    fn take(self) -> Option<EncodedFrame<ServerFrame>> {
        match self {
            Self::Direct(frame) => Some(frame),
            Self::Replaceable(slot) => slot.take(),
        }
    }
}

/// One frame on the subscription lane, with the generation that queued it.
struct LaneFrame {
    frame: EncodedFrame<ServerFrame>,
    withdrawal: Arc<LaneWithdrawal>,
}

/// Whether one subscription generation still delivers.
struct LaneWithdrawal {
    /// Read by the transport for every frame the generation queued. Set before `stop` is
    /// cancelled, so a frame the generation queues while it stops is discarded too.
    withdrawn: AtomicBool,
    /// Cancelled when the generation is withdrawn or its session ends. It wakes every wait of the
    /// generation, including one for room on the lane.
    stop: CancellationToken,
}

/// One subscription generation's place on its session's subscription lane.
pub(in crate::application) struct SubscriptionLane {
    sender: mpsc::Sender<LaneFrame>,
    withdrawal: Arc<LaneWithdrawal>,
}

/// What a session keeps to withdraw one of its subscription generations.
#[derive(Clone)]
pub(in crate::application) struct SubscriptionWithdrawal {
    withdrawal: Arc<LaneWithdrawal>,
}

/// The frames a session's transport writes, in the order it writes them.
pub(in crate::application) struct SessionFrames {
    control: mpsc::Receiver<ControlFrame>,
    subscriptions: mpsc::Receiver<LaneFrame>,
    ending: Arc<SessionEnding>,
    control_open: bool,
    subscriptions_open: bool,
    /// The ending, or the end of the session without one, has been written.
    finished: bool,
}

/// The lanes of a new session, whose end cancels `ended`.
pub(in crate::application) fn channel(
    ended: CancellationToken,
) -> (SessionOutbound, SessionFrames) {
    let (control, control_frames) = mpsc::channel(SESSION_CONTROL_CAPACITY);
    let (subscriptions, subscription_frames) = mpsc::channel(SESSION_SUBSCRIPTION_CAPACITY);
    let ending = Arc::new(SessionEnding {
        ended,
        frame: Mutex::new(None),
    });
    let outbound = SessionOutbound {
        inner: Arc::new(OutboundInner {
            control,
            subscriptions,
            ending: ending.clone(),
        }),
    };
    let frames = SessionFrames {
        control: control_frames,
        subscriptions: subscription_frames,
        ending,
        control_open: true,
        subscriptions_open: true,
        finished: false,
    };
    (outbound, frames)
}

impl SessionOutbound {
    /// Queues a control frame, waiting for room until the session ends.
    pub(in crate::application) async fn send(
        &self,
        frame: EncodedFrame<ServerFrame>,
    ) -> Result<(), LaneClosed> {
        nervix_primitives::select! {
            biased;
            _ = self.inner.ending.ended.cancelled() => Err(LaneClosed),
            sent = self.inner.control.send(ControlFrame::Direct(frame)) => sent.map_err(|_| LaneClosed),
        }
    }

    /// Queues a replaceable slot on the control lane. The producer retains its handle and may
    /// update the slot while this send waits for room or until the transport takes it.
    pub(in crate::application) async fn send_replaceable(
        &self,
        slot: ReplaceableControlFrame,
    ) -> Result<(), LaneClosed> {
        nervix_primitives::select! {
            biased;
            _ = self.inner.ending.ended.cancelled() => Err(LaneClosed),
            sent = self.inner.control.send(ControlFrame::Replaceable(slot)) => sent.map_err(|_| LaneClosed),
        }
    }

    /// Ends the session. The transport writes the control frames already queued, then `ending`
    /// when there is one, and nothing after it. This never waits on the client.
    pub(in crate::application) fn end(&self, ending: Option<EncodedFrame<ServerFrame>>) {
        if let Some(frame) = ending {
            let mut slot = self.inner.ending.frame.lock();
            if slot.is_none() && !self.inner.ending.ended.is_cancelled() {
                *slot = Some(frame);
            }
        }
        self.inner.ending.ended.cancel();
    }

    /// Resolves once the session has ended.
    pub(in crate::application) async fn ended(&self) {
        self.inner.ending.ended.cancelled().await;
    }

    /// A place on the subscription lane for one new subscription generation.
    pub(in crate::application) fn subscription_lane(&self) -> SubscriptionLane {
        SubscriptionLane {
            sender: self.inner.subscriptions.clone(),
            withdrawal: Arc::new(LaneWithdrawal {
                withdrawn: AtomicBool::new(false),
                stop: self.inner.ending.ended.child_token(),
            }),
        }
    }
}

impl LaneWithdrawal {
    fn is_withdrawn(&self) -> bool {
        self.withdrawn.load(Ordering::Acquire)
    }
}

impl SubscriptionLane {
    /// Queues a frame, waiting for room until the generation is withdrawn or its session ends.
    pub(in crate::application) async fn send(
        &self,
        frame: EncodedFrame<ServerFrame>,
    ) -> Result<(), LaneClosed> {
        let queued = LaneFrame {
            frame,
            withdrawal: self.withdrawal.clone(),
        };
        nervix_primitives::select! {
            biased;
            _ = self.withdrawal.stop.cancelled() => Err(LaneClosed),
            sent = self.sender.send(queued) => sent.map_err(|_| LaneClosed),
        }
    }

    /// Queues a frame if the lane has room now.
    pub(in crate::application) fn try_send(
        &self,
        frame: EncodedFrame<ServerFrame>,
    ) -> Result<(), LaneRefusal> {
        if self.withdrawal.stop.is_cancelled() {
            return Err(LaneRefusal::Closed);
        }
        let queued = LaneFrame {
            frame,
            withdrawal: self.withdrawal.clone(),
        };
        match self.sender.try_send(queued) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(LaneRefusal::Full),
            Err(TrySendError::Closed(_)) => Err(LaneRefusal::Closed),
        }
    }

    /// Resolves once the generation is withdrawn or its session ends.
    pub(in crate::application) async fn withdrawn(&self) {
        self.withdrawal.stop.cancelled().await;
    }

    /// The handle a session keeps to withdraw this generation.
    pub(in crate::application) fn withdrawal(&self) -> SubscriptionWithdrawal {
        SubscriptionWithdrawal {
            withdrawal: self.withdrawal.clone(),
        }
    }
}

impl SubscriptionWithdrawal {
    /// Withdraws the generation: every frame it still has queued is discarded, and every wait it
    /// is in ends.
    pub(in crate::application) fn withdraw(&self) {
        self.withdrawal.withdrawn.store(true, Ordering::Release);
        self.withdrawal.stop.cancel();
    }
}

impl SessionFrames {
    /// The frames as a stream, for a transport that pulls them.
    pub(in crate::application) fn into_stream(
        self,
    ) -> impl Stream<Item = EncodedFrame<ServerFrame>> + Send + 'static {
        stream::unfold(self, |mut frames| async move {
            let frame = frames.next().await?;
            Some((frame, frames))
        })
    }

    /// The next frame to write, or `None` once the session wrote its ending or every producer is
    /// gone and nothing is left to write.
    ///
    /// A queued control frame always comes first. A subscription frame comes only while the
    /// control lane is empty and the session has not ended, and never when the generation that
    /// queued it has been withdrawn.
    pub(in crate::application) async fn next(&mut self) -> Option<EncodedFrame<ServerFrame>> {
        let ended = self.ending.ended.clone();
        loop {
            nervix_primitives::task::consume_budget().await;
            if self.finished {
                return None;
            }
            if self.control_open {
                match self.control.try_recv() {
                    Ok(frame) => {
                        if let Some(frame) = frame.take() {
                            return Some(frame);
                        }
                        continue;
                    }
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => self.control_open = false,
                }
            }
            if ended.is_cancelled() {
                self.finished = true;
                return self.ending.frame.lock().take();
            }
            if self.subscriptions_open {
                match self.subscriptions.try_recv() {
                    Ok(queued) => {
                        if queued.withdrawal.is_withdrawn() {
                            continue;
                        }
                        return Some(queued.frame);
                    }
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => self.subscriptions_open = false,
                }
            }
            if !self.control_open && !self.subscriptions_open {
                self.finished = true;
                return None;
            }
            // The branches are polled in priority order, so a subscription frame is taken here
            // only while the control lane is empty and the session has not ended.
            nervix_primitives::select! {
                biased;
                frame = self.control.recv(), if self.control_open => match frame {
                    Some(frame) => {
                        if let Some(frame) = frame.take() {
                            return Some(frame);
                        }
                    }
                    None => self.control_open = false,
                },
                _ = ended.cancelled() => {}
                queued = self.subscriptions.recv(), if self.subscriptions_open => match queued {
                    Some(queued) => {
                        if !queued.withdrawal.is_withdrawn() {
                            return Some(queued.frame);
                        }
                    }
                    None => self.subscriptions_open = false,
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use meticulous::ResultExt as _;
    use nervix_client_wire::{
        NoticeLevel, ServerEvent, ServerMessage, ServerNotice, SessionLimits, VerifiedFrame,
    };
    use nervix_primitives::time::timeout;

    use super::*;

    /// Generously longer than any step here takes, so only a hang reaches it.
    const WAIT: Duration = Duration::from_secs(30);

    fn frame(text: &str) -> EncodedFrame<ServerFrame> {
        ServerNotice {
            level: NoticeLevel::Info,
            message: text.to_string(),
        }
        .encode(&SessionLimits::DEFAULT)
        .assured("a short notice fits a frame")
    }

    fn text(frame: EncodedFrame<ServerFrame>) -> String {
        let frame = VerifiedFrame::verify(frame.into_bytes(), &SessionLimits::DEFAULT)
            .assured("the test encodes frames the session limits admit");
        let ServerMessage::Event(ServerEvent::Notice(notice)) =
            ServerMessage::decode(&frame).assured("a frame decodes")
        else {
            panic!("the test queues only notices");
        };
        notice.message
    }

    async fn next_text(frames: &mut SessionFrames) -> Option<String> {
        let next = timeout(WAIT, frames.next())
            .await
            .assured("the transport is handed a frame or the end within the deadline");
        next.map(text)
    }

    #[nervix_primitives::test]
    async fn control_frames_go_ahead_of_queued_subscription_frames() {
        let (outbound, mut frames) = channel(CancellationToken::new());
        let lane = outbound.subscription_lane();
        lane.send(frame("row 1")).await.assured("the lane has room");
        lane.send(frame("row 2")).await.assured("the lane has room");
        outbound
            .send(frame("reply"))
            .await
            .assured("the control lane has room");

        assert_eq!(next_text(&mut frames).await.as_deref(), Some("reply"));
        assert_eq!(next_text(&mut frames).await.as_deref(), Some("row 1"));
        assert_eq!(next_text(&mut frames).await.as_deref(), Some("row 2"));

        drop(lane);
        drop(outbound);
        assert_eq!(
            next_text(&mut frames).await,
            None,
            "the frames end once every producer is gone and nothing is left"
        );
    }

    #[nervix_primitives::test]
    async fn replaceable_control_slot_keeps_the_newest_frame_while_the_lane_is_full() {
        let (outbound, mut frames) = channel(CancellationToken::new());
        for _ in 0..SESSION_CONTROL_CAPACITY {
            outbound
                .send(frame("filler"))
                .await
                .assured("the control lane has exactly this many places");
        }
        let slot = ReplaceableControlFrame::new(frame("tick 1"));
        let sending = {
            let outbound = outbound.clone();
            let slot = slot.clone();
            nervix_primitives::task::spawn(async move { outbound.send_replaceable(slot).await })
        };
        nervix_primitives::task::yield_now().await;
        slot.replace(frame("tick 2"))
            .assured("the transport has not taken the slot");
        assert_eq!(next_text(&mut frames).await.as_deref(), Some("filler"));
        sending
            .await
            .assured("the send task completes")
            .assured("the lane remains open");
        slot.replace(frame("tick 3"))
            .assured("the queued slot is still replaceable");
        for _ in 1..SESSION_CONTROL_CAPACITY {
            assert_eq!(next_text(&mut frames).await.as_deref(), Some("filler"));
        }
        assert_eq!(next_text(&mut frames).await.as_deref(), Some("tick 3"));
        assert!(slot.replace(frame("tick 4")).is_err());
    }

    #[nervix_primitives::test]
    async fn withdrawn_tick_slot_does_not_precede_the_new_state() {
        let (outbound, mut frames) = channel(CancellationToken::new());
        let lane = outbound.subscription_lane();
        let slot = ReplaceableControlFrame::new(frame("superseded tick"));
        outbound
            .send_replaceable(slot.clone())
            .await
            .assured("the control lane has room");
        outbound
            .send(frame("new state"))
            .await
            .assured("the control lane has room");
        lane.send(frame("row")).await.assured("the lane has room");
        slot.withdraw();
        assert_eq!(next_text(&mut frames).await.as_deref(), Some("new state"));
        assert_eq!(next_text(&mut frames).await.as_deref(), Some("row"));
        assert!(slot.replace(frame("later tick")).is_err());
    }

    #[nervix_primitives::test]
    async fn a_withdrawn_generation_queues_nothing_more_and_its_queued_frames_are_discarded() {
        let (outbound, mut frames) = channel(CancellationToken::new());
        let withdrawn = outbound.subscription_lane();
        let kept = outbound.subscription_lane();
        withdrawn
            .send(frame("withdrawn 1"))
            .await
            .assured("the lane has room");
        kept.send(frame("kept")).await.assured("the lane has room");
        withdrawn
            .send(frame("withdrawn 2"))
            .await
            .assured("the lane has room");

        withdrawn.withdrawal().withdraw();
        assert_eq!(
            withdrawn.send(frame("withdrawn 3")).await,
            Err(LaneClosed),
            "a withdrawn generation queues nothing more"
        );
        assert_eq!(
            withdrawn.try_send(frame("withdrawn 4")),
            Err(LaneRefusal::Closed)
        );
        timeout(WAIT, withdrawn.withdrawn())
            .await
            .assured("a withdrawn generation's waits end");
        outbound
            .send(frame("reply"))
            .await
            .assured("the control lane has room");

        assert_eq!(next_text(&mut frames).await.as_deref(), Some("reply"));
        assert_eq!(next_text(&mut frames).await.as_deref(), Some("kept"));
        drop(withdrawn);
        drop(kept);
        drop(outbound);
        assert_eq!(next_text(&mut frames).await, None);
    }

    #[nervix_primitives::test]
    async fn the_ending_follows_the_queued_control_frames_and_nothing_follows_it() {
        let (outbound, mut frames) = channel(CancellationToken::new());
        let lane = outbound.subscription_lane();
        lane.send(frame("row")).await.assured("the lane has room");
        outbound
            .send(frame("first reply"))
            .await
            .assured("the control lane has room");
        outbound
            .send(frame("second reply"))
            .await
            .assured("the control lane has room");

        outbound.end(Some(frame("ending")));
        assert_eq!(outbound.send(frame("late reply")).await, Err(LaneClosed));
        assert_eq!(lane.send(frame("late row")).await, Err(LaneClosed));

        assert_eq!(next_text(&mut frames).await.as_deref(), Some("first reply"));
        assert_eq!(
            next_text(&mut frames).await.as_deref(),
            Some("second reply")
        );
        assert_eq!(next_text(&mut frames).await.as_deref(), Some("ending"));
        assert_eq!(
            next_text(&mut frames).await,
            None,
            "nothing follows the ending, including rows queued before it"
        );
    }

    #[nervix_primitives::test]
    async fn ending_a_session_never_waits_on_a_client_that_reads_nothing() {
        let (outbound, _unread) = channel(CancellationToken::new());
        for index in 0..SESSION_CONTROL_CAPACITY {
            outbound
                .send(frame(&format!("reply {index}")))
                .await
                .assured("the control lane has room until it is full");
        }
        let lane = outbound.subscription_lane();
        for index in 0..SESSION_SUBSCRIPTION_CAPACITY {
            lane.send(frame(&format!("row {index}")))
                .await
                .assured("the subscription lane has room until it is full");
        }
        assert_eq!(lane.try_send(frame("dropped row")), Err(LaneRefusal::Full));
        let waiting_reply = {
            let outbound = outbound.clone();
            nervix_primitives::task::spawn(
                async move { outbound.send(frame("waiting reply")).await },
            )
        };
        let waiting_row =
            nervix_primitives::task::spawn(async move { lane.send(frame("waiting row")).await });

        outbound.end(Some(frame("ending")));
        let reply = timeout(WAIT, waiting_reply)
            .await
            .assured("the end of the session ends a wait for room")
            .assured("the waiting producer does not panic");
        assert_eq!(reply, Err(LaneClosed));
        let row = timeout(WAIT, waiting_row)
            .await
            .assured("the end of the session ends a subscription's wait for room")
            .assured("the waiting producer does not panic");
        assert_eq!(row, Err(LaneClosed));
    }
}
