//! Validated session bounds and the node's bounded session work.
//!
//! Layer: vocabulary.
//! - **Owns.** Session capability definitions, dependent defaults and complete validated values.
//! - **Depends on.** Scalar values, setting classes and serialization primitives.
//! - **Must not know.** Frames, transports, clients, queues or running sessions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns validated session capability vocabulary")
)]

use error_stack::ResultExt as _;
use rkyv::rancor::{Fallible, Source};

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SessionBoundsError {
    #[error("a frame limit of {frame_bytes} bytes is outside {minimum}..={maximum}")]
    FrameBytesOutOfRange {
        frame_bytes: usize,
        minimum: usize,
        maximum: usize,
    },
    #[error(
        "a transfer limit of {transfer_bytes} bytes is outside the frame limit {frame_bytes} to \
         {maximum}"
    )]
    TransferBytesOutOfRange {
        transfer_bytes: usize,
        frame_bytes: usize,
        maximum: usize,
    },
    #[error("a nesting limit of {nesting_depth} is outside {minimum}..={maximum}")]
    NestingDepthOutOfRange {
        nesting_depth: usize,
        minimum: usize,
        maximum: usize,
    },
    #[error("a string limit of {string_bytes} bytes exceeds the transfer limit {transfer_bytes}")]
    StringBytesAboveTransfer {
        string_bytes: usize,
        transfer_bytes: usize,
    },
    #[error("{dimension} value {value} is not representable on this platform")]
    Platform { dimension: &'static str, value: u64 },
}

fn native(value: NonZeroU64, dimension: &'static str) -> Result<usize, Report<SessionBoundsError>> {
    usize::try_from(value.get()).map_err(|error| {
        Report::new(error).change_context(SessionBoundsError::Platform {
            dimension,
            value: value.get(),
        })
    })
}

fn default_scalar(value: usize) -> NonZeroU64 {
    let value = u64::try_from(value).assured("each declared scalar default fits u64");
    NonZeroU64::new(value).assured("each declared scalar default is positive")
}

pub struct SessionFrameBytes;
impl SessionFrameBytes {
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "this definition owns the single session frame default"
        )
    )]
    pub const DEFAULT: usize = 4 * 1024 * 1024;
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "a frame must hold the fixed session opening and useful transfer envelope"
        )
    )]
    pub const MINIMUM: usize = 1024;
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "32-bit FlatBuffers offsets and the verifier's eightfold apparent-size \
                      budget bound a frame"
        )
    )]
    pub const MAXIMUM: usize = 256 * 1024 * 1024;
}
impl SettingDefinition for SessionFrameBytes {
    type Class = NodeCapabilities;
    type Value = NonZeroU64;
    type Context = ();
    type Error = SessionBoundsError;
    const NAME: &'static str = "session-frame-bytes";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> NonZeroU64 {
        default_scalar(Self::DEFAULT)
    }
    fn validate(value: &NonZeroU64, _: &()) -> Result<(), Report<Self::Error>> {
        let frame_bytes = native(*value, Self::NAME)?;
        if !(Self::MINIMUM..=Self::MAXIMUM).contains(&frame_bytes) {
            return Err(Report::new(SessionBoundsError::FrameBytesOutOfRange {
                frame_bytes,
                minimum: Self::MINIMUM,
                maximum: Self::MAXIMUM,
            }));
        }
        Ok(())
    }
}

pub struct SessionTransferBytes;
impl SessionTransferBytes {
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "this definition owns the single session transfer default"
        )
    )]
    pub const DEFAULT: usize = 64 * 1024 * 1024;
}
impl SettingDefinition for SessionTransferBytes {
    type Class = NodeCapabilities;
    type Value = NonZeroU64;
    type Context = Setting<SessionFrameBytes>;
    type Error = SessionBoundsError;
    const NAME: &'static str = "session-transfer-bytes";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &Self::Context) -> NonZeroU64 {
        default_scalar(Self::DEFAULT)
    }
    fn validate(value: &NonZeroU64, frame: &Self::Context) -> Result<(), Report<Self::Error>> {
        let transfer_bytes = native(*value, Self::NAME)?;
        let frame_bytes = native(*frame.value(), SessionFrameBytes::NAME)?;
        if !(frame_bytes..=SessionFrameBytes::MAXIMUM).contains(&transfer_bytes) {
            return Err(Report::new(SessionBoundsError::TransferBytesOutOfRange {
                transfer_bytes,
                frame_bytes,
                maximum: SessionFrameBytes::MAXIMUM,
            }));
        }
        Ok(())
    }
}

pub struct SessionNestingDepth;
impl SessionNestingDepth {
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "this definition owns the single session nesting default"
        )
    )]
    pub const DEFAULT: usize = 64;
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "every nonrecursive session schema structure fits sixteen table levels"
        )
    )]
    pub const MINIMUM: usize = 16;
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "the verifier and value decoder recurse once per level within the qualified \
                      stack bound"
        )
    )]
    pub const MAXIMUM: usize = 128;
}
impl SettingDefinition for SessionNestingDepth {
    type Class = NodeCapabilities;
    type Value = NonZeroU64;
    type Context = ();
    type Error = SessionBoundsError;
    const NAME: &'static str = "session-nesting-depth";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> NonZeroU64 {
        default_scalar(Self::DEFAULT)
    }
    fn validate(value: &NonZeroU64, _: &()) -> Result<(), Report<Self::Error>> {
        let nesting_depth = native(*value, Self::NAME)?;
        if !(Self::MINIMUM..=Self::MAXIMUM).contains(&nesting_depth) {
            return Err(Report::new(SessionBoundsError::NestingDepthOutOfRange {
                nesting_depth,
                minimum: Self::MINIMUM,
                maximum: Self::MAXIMUM,
            }));
        }
        Ok(())
    }
}

pub struct SessionCollectionEntries;
impl SessionCollectionEntries {
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "this definition owns the single session vector-entry default"
        )
    )]
    pub const DEFAULT: usize = 1 << 18;
}
impl SettingDefinition for SessionCollectionEntries {
    type Class = NodeCapabilities;
    type Value = NonZeroU64;
    type Context = ();
    type Error = SessionBoundsError;
    const NAME: &'static str = "session-collection-entries";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> NonZeroU64 {
        default_scalar(Self::DEFAULT)
    }
    fn validate(value: &NonZeroU64, _: &()) -> Result<(), Report<Self::Error>> {
        native(*value, Self::NAME).map(|_| ())
    }
}

pub struct SessionStringBytes;
impl SessionStringBytes {
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "the default session string bound follows the transfer definition"
        )
    )]
    pub const DEFAULT: usize = SessionTransferBytes::DEFAULT;
}
impl SettingDefinition for SessionStringBytes {
    type Class = NodeCapabilities;
    type Value = NonZeroU64;
    type Context = Setting<SessionTransferBytes>;
    type Error = SessionBoundsError;
    const NAME: &'static str = "session-string-bytes";
    const DERIVATION: Option<&'static str> = Some("the resolved session transfer byte bound");
    fn default_value(transfer: &Self::Context) -> NonZeroU64 {
        *transfer.value()
    }
    fn validate(value: &NonZeroU64, transfer: &Self::Context) -> Result<(), Report<Self::Error>> {
        let string_bytes = native(*value, Self::NAME)?;
        let transfer_bytes = native(*transfer.value(), SessionTransferBytes::NAME)?;
        if string_bytes > transfer_bytes {
            return Err(Report::new(SessionBoundsError::StringBytesAboveTransfer {
                string_bytes,
                transfer_bytes,
            }));
        }
        Ok(())
    }
}

/// Complete checked bounds, shared by node configuration and each session transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Archive, RkyvSerialize)]
#[serde(deny_unknown_fields)]
pub struct SessionBounds {
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    frame_bytes: Setting<SessionFrameBytes>,
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    transfer_bytes: Setting<SessionTransferBytes>,
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    nesting_depth: Setting<SessionNestingDepth>,
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    collection_entries: Setting<SessionCollectionEntries>,
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    string_bytes: Setting<SessionStringBytes>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionBoundValues {
    pub frame_bytes: Option<NonZeroU64>,
    pub transfer_bytes: Option<NonZeroU64>,
    pub nesting_depth: Option<NonZeroU64>,
    pub collection_entries: Option<NonZeroU64>,
    pub string_bytes: Option<NonZeroU64>,
}

impl SessionBounds {
    pub fn resolve(values: SessionBoundValues) -> Result<Self, Report<SessionBoundsError>> {
        let frame_bytes = Setting::resolve(values.frame_bytes, &())?;
        let transfer_bytes = Setting::resolve(values.transfer_bytes, &frame_bytes)?;
        let nesting_depth = Setting::resolve(values.nesting_depth, &())?;
        let collection_entries = Setting::resolve(values.collection_entries, &())?;
        let string_bytes = Setting::resolve(values.string_bytes, &transfer_bytes)?;
        Ok(Self {
            frame_bytes,
            transfer_bytes,
            string_bytes,
            nesting_depth,
            collection_entries,
        })
    }

    pub fn frame_bytes(&self) -> NonZeroU64 {
        *self.frame_bytes.value()
    }
    pub fn transfer_bytes(&self) -> NonZeroU64 {
        *self.transfer_bytes.value()
    }
    pub fn nesting_depth(&self) -> NonZeroU64 {
        *self.nesting_depth.value()
    }
    pub fn collection_entries(&self) -> NonZeroU64 {
        *self.collection_entries.value()
    }
    pub fn string_bytes(&self) -> NonZeroU64 {
        *self.string_bytes.value()
    }

    pub fn values(&self) -> SessionBoundValues {
        SessionBoundValues {
            frame_bytes: Some(self.frame_bytes()),
            transfer_bytes: Some(self.transfer_bytes()),
            nesting_depth: Some(self.nesting_depth()),
            collection_entries: Some(self.collection_entries()),
            string_bytes: Some(self.string_bytes()),
        }
    }

    pub fn described(&self) -> Vec<(&'static str, String)> {
        vec![
            (SessionFrameBytes::NAME, format!("{}B", self.frame_bytes())),
            (
                SessionTransferBytes::NAME,
                format!("{}B", self.transfer_bytes()),
            ),
            (SessionNestingDepth::NAME, self.nesting_depth().to_string()),
            (
                SessionCollectionEntries::NAME,
                self.collection_entries().to_string(),
            ),
            (
                SessionStringBytes::NAME,
                format!("{}B", self.string_bytes()),
            ),
        ]
    }
}

impl Default for SessionBounds {
    fn default() -> Self {
        Self::resolve(SessionBoundValues::default())
            .assured("the session bound definitions have consistent defaults")
    }
}

impl<'de> Deserialize<'de> for SessionBounds {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            frame_bytes: NonZeroU64,
            transfer_bytes: NonZeroU64,
            nesting_depth: NonZeroU64,
            collection_entries: NonZeroU64,
            string_bytes: NonZeroU64,
        }
        let fields = Fields::deserialize(deserializer)?;
        Self::resolve(SessionBoundValues {
            frame_bytes: Some(fields.frame_bytes),
            transfer_bytes: Some(fields.transfer_bytes),
            nesting_depth: Some(fields.nesting_depth),
            collection_entries: Some(fields.collection_entries),
            string_bytes: Some(fields.string_bytes),
        })
        .map_err(|error| D::Error::custom(format_args!("{error:#}")))
    }
}

impl<D: Fallible + ?Sized> RkyvDeserialize<SessionBounds, D> for ArchivedSessionBounds
where
    D::Error: Source,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<SessionBounds, D::Error> {
        SessionBounds::resolve(SessionBoundValues {
            frame_bytes: Some(self.frame_bytes.deserialize(deserializer)?),
            transfer_bytes: Some(self.transfer_bytes.deserialize(deserializer)?),
            nesting_depth: Some(self.nesting_depth.deserialize(deserializer)?),
            collection_entries: Some(self.collection_entries.deserialize(deserializer)?),
            string_bytes: Some(self.string_bytes.deserialize(deserializer)?),
        })
        .map_err(|error| D::Error::new(error.into_error()))
    }
}

capability_definition!(SessionNoticeCapacity, "session-notice-capacity", Count, 256);
capability_definition!(
    SessionLeadershipCheckInterval,
    "session-leadership-check-interval",
    Duration,
    Duration::from_millis(250)
);
capability_definition!(
    SessionSnapshotInterval,
    "session-snapshot-interval",
    Duration,
    Duration::from_millis(500)
);

pub struct SessionQueueMemoryBytes;
impl SettingDefinition for SessionQueueMemoryBytes {
    type Class = NodeCapabilities;
    type Value = NonZeroU64;
    type Context = Setting<SessionFrameBytes>;
    type Error = NodeOperationalError;
    const NAME: &'static str = "session-queue-memory-bytes";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &Self::Context) -> NonZeroU64 {
        NonZeroU64::new(128 * 1024 * 1024).assured("the queue memory default is positive")
    }
    fn validate(value: &NonZeroU64, frame: &Self::Context) -> Result<(), Report<Self::Error>> {
        let capacity = value.get() / frame.value().get() / 2;
        if capacity == 0 {
            return Err(Report::new(NodeOperationalError::QueueMemory));
        }
        if capacity > MAX_CHANNEL_CAPACITY {
            return Err(Report::new(NodeOperationalError::QueueCapacity));
        }
        Ok(())
    }
}

pub struct SessionSubscriptionRowsPerFrame;
impl SettingDefinition for SessionSubscriptionRowsPerFrame {
    type Class = NodeCapabilities;
    type Value = NonZeroU64;
    type Context = Setting<SessionCollectionEntries>;
    type Error = NodeOperationalError;
    const NAME: &'static str = "session-subscription-rows-per-frame";
    const DERIVATION: Option<&'static str> =
        Some("at most the resolved session collection-entry bound");
    fn default_value(entries: &Self::Context) -> NonZeroU64 {
        NonZeroU64::new(256)
            .assured("the subscription row default is positive")
            .min(*entries.value())
    }
    fn validate(value: &NonZeroU64, entries: &Self::Context) -> Result<(), Report<Self::Error>> {
        if value > entries.value() {
            return Err(Report::new(NodeOperationalError::SubscriptionRows));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Archive, RkyvSerialize)]
#[serde(deny_unknown_fields)]
pub struct SessionCapabilities {
    bounds: SessionBounds,
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    queue_memory_bytes: Setting<SessionQueueMemoryBytes>,
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    subscription_rows_per_frame: Setting<SessionSubscriptionRowsPerFrame>,
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    notice_capacity: Setting<SessionNoticeCapacity>,
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    leadership_check_interval: Setting<SessionLeadershipCheckInterval>,
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    snapshot_interval: Setting<SessionSnapshotInterval>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionCapabilityValues {
    pub bounds: SessionBoundValues,
    pub queue_memory_bytes: Option<NonZeroU64>,
    pub subscription_rows_per_frame: Option<NonZeroU64>,
    pub notice_capacity: Option<NonZeroU64>,
    pub leadership_check_interval: Option<Duration>,
    pub snapshot_interval: Option<Duration>,
}

impl SessionCapabilities {
    pub fn resolve(values: SessionCapabilityValues) -> Result<Self, Report<NodeOperationalError>> {
        let bounds = SessionBounds::resolve(values.bounds)
            .change_context(NodeOperationalError::SessionBounds)?;
        Ok(Self {
            queue_memory_bytes: Setting::resolve(values.queue_memory_bytes, &bounds.frame_bytes)?,
            subscription_rows_per_frame: Setting::resolve(
                values.subscription_rows_per_frame,
                &bounds.collection_entries,
            )?,
            notice_capacity: Setting::resolve(values.notice_capacity, &())?,
            leadership_check_interval: Setting::resolve(values.leadership_check_interval, &())?,
            snapshot_interval: Setting::resolve(values.snapshot_interval, &())?,
            bounds,
        })
    }

    pub fn bounds(&self) -> &SessionBounds {
        &self.bounds
    }
    pub fn queue_memory_bytes(&self) -> NonZeroU64 {
        *self.queue_memory_bytes.value()
    }
    pub fn subscription_rows_per_frame(&self) -> NonZeroU64 {
        *self.subscription_rows_per_frame.value()
    }
    pub fn notice_capacity(&self) -> NonZeroU64 {
        *self.notice_capacity.value()
    }
    pub fn leadership_check_interval(&self) -> Duration {
        *self.leadership_check_interval.value()
    }
    pub fn snapshot_interval(&self) -> Duration {
        *self.snapshot_interval.value()
    }

    pub fn queue_capacity(&self) -> usize {
        usize::try_from(self.queue_memory_bytes().get() / self.bounds.frame_bytes().get() / 2)
            .assured(
                "the session memory validation bounds each lane by the portable channel capacity",
            )
    }

    pub fn download_capacity(&self) -> usize {
        usize::try_from(
            (self.queue_memory_bytes().get() / self.bounds.frame_bytes().get() / 8).max(1),
        )
        .assured("a download's capacity never exceeds the validated session lane capacity")
    }

    pub fn values(&self) -> SessionCapabilityValues {
        SessionCapabilityValues {
            bounds: self.bounds.values(),
            queue_memory_bytes: Some(self.queue_memory_bytes()),
            subscription_rows_per_frame: Some(self.subscription_rows_per_frame()),
            notice_capacity: Some(self.notice_capacity()),
            leadership_check_interval: Some(self.leadership_check_interval()),
            snapshot_interval: Some(self.snapshot_interval()),
        }
    }

    pub fn described(&self) -> Vec<(&'static str, String)> {
        let mut values = self.bounds.described();
        values.extend([
            (
                SessionQueueMemoryBytes::NAME,
                format!("{}B", self.queue_memory_bytes()),
            ),
            (
                SessionSubscriptionRowsPerFrame::NAME,
                self.subscription_rows_per_frame().to_string(),
            ),
            (
                SessionNoticeCapacity::NAME,
                self.notice_capacity().to_string(),
            ),
            (
                SessionLeadershipCheckInterval::NAME,
                crate::timing_text(self.leadership_check_interval()),
            ),
            (
                SessionSnapshotInterval::NAME,
                crate::timing_text(self.snapshot_interval()),
            ),
        ]);
        values
    }
}

impl Default for SessionCapabilities {
    fn default() -> Self {
        Self::resolve(SessionCapabilityValues::default())
            .assured("the session capability defaults satisfy their relationships")
    }
}

impl<'de> Deserialize<'de> for SessionCapabilities {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            bounds: SessionBounds,
            queue_memory_bytes: NonZeroU64,
            subscription_rows_per_frame: NonZeroU64,
            notice_capacity: NonZeroU64,
            leadership_check_interval: Duration,
            snapshot_interval: Duration,
        }
        let fields = Fields::deserialize(deserializer)?;
        Self::resolve(SessionCapabilityValues {
            bounds: fields.bounds.values(),
            queue_memory_bytes: Some(fields.queue_memory_bytes),
            subscription_rows_per_frame: Some(fields.subscription_rows_per_frame),
            notice_capacity: Some(fields.notice_capacity),
            leadership_check_interval: Some(fields.leadership_check_interval),
            snapshot_interval: Some(fields.snapshot_interval),
        })
        .map_err(|error| D::Error::custom(format_args!("{error:#}")))
    }
}

impl<D: Fallible + ?Sized> RkyvDeserialize<SessionCapabilities, D> for ArchivedSessionCapabilities
where
    D::Error: Source,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<SessionCapabilities, D::Error> {
        let bounds: SessionBounds = self.bounds.deserialize(deserializer)?;
        SessionCapabilities::resolve(SessionCapabilityValues {
            bounds: bounds.values(),
            queue_memory_bytes: Some(self.queue_memory_bytes.deserialize(deserializer)?),
            subscription_rows_per_frame: Some(
                self.subscription_rows_per_frame.deserialize(deserializer)?,
            ),
            notice_capacity: Some(self.notice_capacity.deserialize(deserializer)?),
            leadership_check_interval: Some(
                self.leadership_check_interval.deserialize(deserializer)?,
            ),
            snapshot_interval: Some(self.snapshot_interval.deserialize(deserializer)?),
        })
        .map_err(|error| D::Error::new(error.into_error()))
    }
}
