//! The committed record of one node: its capacity claim, its detected properties and the tags its
//! operator provisioned it with.
//!
//! Layer: vocabulary.
//! - **Owns.** The node record, the closed set of detected properties with a typed reason for every
//!   value that was not detected, operator tags and their validation, and the bounds that keep one
//!   record a single Raft command.
//! - **Depends on.** The capacity claim, node identities and serialization primitives.
//! - **Must not know.** How properties are detected, consensus, transport or placement policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{collections::BTreeMap, fmt, num::NonZeroU64, str::FromStr};

use error_stack::Report;
use rkyv::{
    Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize,
    rancor::{Fallible, Source},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ClusterNodeIdentity, NodeCapabilities, NodeCapabilityRecord};

/// The longest a tag key may be, in bytes.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the node record is one Raft command every node accepts under the 1 MiB \
                  largest-command floor; 64 tags of a 63-byte key and a 255-byte value keep a \
                  record's tags under 21 KB"
    )
)]
pub const MAX_TAG_KEY_BYTES: usize = 63;

/// The longest a tag value may be, in bytes.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the node record is one Raft command every node accepts under the 1 MiB \
                  largest-command floor; 64 tags of a 63-byte key and a 255-byte value keep a \
                  record's tags under 21 KB"
    )
)]
pub const MAX_TAG_VALUE_BYTES: usize = 255;

/// The most tags one node may carry.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the node record is one Raft command every node accepts under the 1 MiB \
                  largest-command floor; 64 tags of a 63-byte key and a 255-byte value keep a \
                  record's tags under 21 KB"
    )
)]
pub const MAX_NODE_TAGS: usize = 64;

/// The longest text one detected property may hold, in bytes.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the node record is one Raft command every node accepts under the 1 MiB \
                  largest-command floor; bounded property text and lists keep a record's \
                  properties under 35 KB"
    )
)]
pub const MAX_PROPERTY_TEXT_BYTES: usize = 255;

/// The most devices or execution providers one detected property lists.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the node record is one Raft command every node accepts under the 1 MiB \
                  largest-command floor; bounded property text and lists keep a record's \
                  properties under 35 KB"
    )
)]
pub const MAX_PROPERTY_LIST_ITEMS: usize = 64;

/// Whether `character` is a control character or a line or paragraph separator, which a value
/// printed on one line of a description cannot hold.
fn breaks_a_line(character: char) -> bool {
    character.is_control() || matches!(character, '\u{2028}' | '\u{2029}')
}

/// Why a tag, or a set of tags, cannot be a node's. Each variant names the tag and the rule it
/// broke.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "tag construction returns pure syntax, alphabet and bound refusals"
    )
)]
pub enum NodeTagError {
    #[error("node tag '{tag}' has no '=' between its key and its value")]
    MissingSeparator { tag: String },
    #[error("node tag '{tag}' has an empty key")]
    EmptyKey { tag: String },
    #[error(
        "node tag key '{key}' must start with a lower-case letter or a digit and hold only \
         lower-case letters, digits, '.', '_' and '-'"
    )]
    KeyAlphabet { key: String },
    #[error("node tag key '{key}' is {actual} bytes long; a key holds at most {max} bytes")]
    KeyTooLong {
        key: String,
        actual: usize,
        max: usize,
    },
    #[error("node tag '{key}' has an empty value")]
    EmptyValue { key: String },
    #[error("node tag '{key}' has a value {actual} bytes long; a value holds at most {max} bytes")]
    ValueTooLong {
        key: String,
        actual: usize,
        max: usize,
    },
    #[error(
        "node tag '{key}' has a value holding a comma, a control character or a line separator; a \
         comma separates tags and a value is printable text on one line"
    )]
    ValueCharacter { key: String },
    #[error("node tag key '{key}' is given more than once")]
    RepeatedKey { key: String },
    #[error("{actual} node tags are given; a node holds at most {max}")]
    TooMany { actual: usize, max: usize },
}

/// The key of one operator tag: a lower-case letter or digit followed by lower-case letters,
/// digits, `.`, `_` and `-`, at most [`MAX_TAG_KEY_BYTES`] bytes.
///
/// Decoding is construction: the serde and archived forms accept only text `parse` accepts.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
)]
#[serde(try_from = "String", into = "String")]
#[rkyv(derive(PartialEq, Eq, PartialOrd, Ord))]
pub struct TagKey(String);

impl TagKey {
    /// Validates `key` as a tag key.
    pub fn parse(key: &str) -> Result<Self, NodeTagError> {
        let Some(first) = key.chars().next() else {
            return Err(NodeTagError::EmptyKey {
                tag: key.to_string(),
            });
        };
        let starts = first.is_ascii_lowercase() || first.is_ascii_digit();
        let holds = key.chars().all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '.' | '_' | '-')
        });
        if !starts || !holds {
            return Err(NodeTagError::KeyAlphabet {
                key: key.to_string(),
            });
        }
        if key.len() > MAX_TAG_KEY_BYTES {
            return Err(NodeTagError::KeyTooLong {
                key: key.to_string(),
                actual: key.len(),
                max: MAX_TAG_KEY_BYTES,
            });
        }
        Ok(Self(key.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for TagKey {
    type Error = NodeTagError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

impl From<TagKey> for String {
    fn from(value: TagKey) -> Self {
        value.0
    }
}

impl<D> RkyvDeserialize<TagKey, D> for ArchivedTagKey
where
    D: Fallible + ?Sized,
    D::Error: Source,
{
    fn deserialize(&self, _: &mut D) -> Result<TagKey, D::Error> {
        TagKey::parse(self.0.as_str()).map_err(D::Error::new)
    }
}

impl fmt::Display for TagKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// The value of one operator tag: printable text without a comma, from one to
/// [`MAX_TAG_VALUE_BYTES`] bytes.
///
/// A value is validated with the key it belongs to, which every refusal names: [`NodeTags`] holds
/// every value and validates each when it is constructed or decoded.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(transparent)]
pub struct TagValue(String);

impl TagValue {
    /// Validates `value` as the value of the tag keyed `key`, which the refusal names.
    pub fn parse(key: &TagKey, value: &str) -> Result<Self, NodeTagError> {
        if value.is_empty() {
            return Err(NodeTagError::EmptyValue {
                key: key.to_string(),
            });
        }
        if value.len() > MAX_TAG_VALUE_BYTES {
            return Err(NodeTagError::ValueTooLong {
                key: key.to_string(),
                actual: value.len(),
                max: MAX_TAG_VALUE_BYTES,
            });
        }
        if value
            .chars()
            .any(|character| character == ',' || breaks_a_line(character))
        {
            return Err(NodeTagError::ValueCharacter {
                key: key.to_string(),
            });
        }
        Ok(Self(value.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TagValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// One operator tag, written `key=value`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeTag {
    pub key: TagKey,
    pub value: TagValue,
}

impl FromStr for NodeTag {
    type Err = NodeTagError;

    /// Reads `key=value`. The key ends at the first `=`, so a value may hold further ones.
    fn from_str(tag: &str) -> Result<Self, Self::Err> {
        let Some((key, value)) = tag.split_once('=') else {
            return Err(NodeTagError::MissingSeparator {
                tag: tag.to_string(),
            });
        };
        if key.is_empty() {
            return Err(NodeTagError::EmptyKey {
                tag: tag.to_string(),
            });
        }
        let key = TagKey::parse(key)?;
        let value = TagValue::parse(&key, value)?;
        Ok(Self { key, value })
    }
}

impl fmt::Display for NodeTag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}={}", self.key, self.value)
    }
}

/// The tags a node was provisioned with, in key order: at most [`MAX_NODE_TAGS`], one value per
/// key. A node without tags has an empty set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize)]
#[serde(
    try_from = "BTreeMap<TagKey, TagValue>",
    into = "BTreeMap<TagKey, TagValue>"
)]
pub struct NodeTags {
    tags: BTreeMap<TagKey, TagValue>,
}

impl NodeTags {
    /// The tags the command line gives, one `key=value` each, refused at the first tag that breaks
    /// a rule, then as a whole when a key repeats or there are too many.
    pub fn from_assignments<'a>(
        assignments: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, Report<NodeTagError>> {
        let mut tags = BTreeMap::new();
        for assignment in assignments {
            let tag = NodeTag::from_str(assignment).map_err(Report::new)?;
            if tags.contains_key(&tag.key) {
                return Err(Report::new(NodeTagError::RepeatedKey {
                    key: tag.key.to_string(),
                }));
            }
            tags.insert(tag.key, tag.value);
        }
        Self::try_from(tags).map_err(Report::new)
    }

    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    pub fn len(&self) -> usize {
        self.tags.len()
    }

    /// The value of the tag keyed `key`, when the node has one.
    pub fn get(&self, key: &str) -> Option<&TagValue> {
        self.tags.get(key)
    }

    /// Every tag in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&TagKey, &TagValue)> {
        self.tags.iter()
    }
}

impl std::borrow::Borrow<str> for TagKey {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl TryFrom<BTreeMap<TagKey, TagValue>> for NodeTags {
    type Error = NodeTagError;

    /// Validates every value with its key, then the number of tags.
    fn try_from(tags: BTreeMap<TagKey, TagValue>) -> Result<Self, Self::Error> {
        for (key, value) in &tags {
            TagValue::parse(key, value.as_str())?;
        }
        if tags.len() > MAX_NODE_TAGS {
            return Err(NodeTagError::TooMany {
                actual: tags.len(),
                max: MAX_NODE_TAGS,
            });
        }
        Ok(Self { tags })
    }
}

impl From<NodeTags> for BTreeMap<TagKey, TagValue> {
    fn from(value: NodeTags) -> Self {
        value.tags
    }
}

impl<D> RkyvDeserialize<NodeTags, D> for ArchivedNodeTags
where
    D: Fallible + ?Sized,
    D::Error: Source,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<NodeTags, D::Error> {
        let tags: BTreeMap<TagKey, TagValue> = self.tags.deserialize(deserializer)?;
        NodeTags::try_from(tags).map_err(D::Error::new)
    }
}

impl FromStr for NodeTags {
    type Err = Report<NodeTagError>;

    /// Reads the comma-separated form the command line takes, splitting on every comma as the
    /// command line does.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::from_assignments(text.split(','))
    }
}

impl fmt::Display for NodeTags {
    /// The command-line form: `key=value` in key order, separated by commas.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut separator = "";
        for (key, value) in &self.tags {
            write!(formatter, "{separator}{key}={value}")?;
            separator = ",";
        }
        Ok(())
    }
}

/// Why detected text cannot be a property's value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "property construction returns pure length and character refusals"
    )
)]
pub enum PropertyValueError {
    #[error("a detected property value is empty")]
    Empty,
    #[error("a detected property value is {actual} bytes long; a value holds at most {max} bytes")]
    TooLong { actual: usize, max: usize },
    #[error("a detected property value holds a control character or a line separator")]
    ControlCharacter,
    #[error("a detected property lists {actual} items; a list holds at most {max}")]
    TooManyItems { actual: usize, max: usize },
    #[error("a detected device list is empty; a node without devices reports none")]
    NoDevices,
}

/// Printable detected text, from one to [`MAX_PROPERTY_TEXT_BYTES`] bytes.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct PropertyText(String);

impl PropertyText {
    pub fn new(text: impl Into<String>) -> Result<Self, PropertyValueError> {
        let text = text.into();
        if text.is_empty() {
            return Err(PropertyValueError::Empty);
        }
        if text.len() > MAX_PROPERTY_TEXT_BYTES {
            return Err(PropertyValueError::TooLong {
                actual: text.len(),
                max: MAX_PROPERTY_TEXT_BYTES,
            });
        }
        if text.chars().any(breaks_a_line) {
            return Err(PropertyValueError::ControlCharacter);
        }
        Ok(Self(text))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for PropertyText {
    type Error = PropertyValueError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<PropertyText> for String {
    fn from(value: PropertyText) -> Self {
        value.0
    }
}

impl<D> RkyvDeserialize<PropertyText, D> for ArchivedPropertyText
where
    D: Fallible + ?Sized,
    D::Error: Source,
{
    fn deserialize(&self, _: &mut D) -> Result<PropertyText, D::Error> {
        PropertyText::new(self.0.as_str()).map_err(D::Error::new)
    }
}

impl fmt::Display for PropertyText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Why a property holds no detected value.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum UndetectedReason {
    /// The operating system reported no value.
    NotReported,
    /// The library that reports the value is not installed, or did not load.
    LibraryUnavailable,
    /// The library loaded, and the driver it talks to is not running.
    DriverUnavailable,
    /// The library loaded and refused the query.
    QueryFailed,
    /// Detection did not finish within the node's detection timeout.
    DeadlineExpired,
    /// The detection work failed before it reported anything.
    DetectionFailed,
    /// The reported value exceeds what the node record holds.
    ExceedsRecordBound,
}

impl fmt::Display for UndetectedReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotReported => "the operating system reported no value",
            Self::LibraryUnavailable => "the library that reports it did not load",
            Self::DriverUnavailable => "the driver the library talks to is not running",
            Self::QueryFailed => "the library refused the query",
            Self::DeadlineExpired => "detection did not finish within the node detection timeout",
            Self::DetectionFailed => "the detection work failed before it reported",
            Self::ExceedsRecordBound => "the reported value exceeds what the node record holds",
        })
    }
}

/// A detected value, or why there is none.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum Detected<T> {
    Value(T),
    Undetected(UndetectedReason),
}

impl<T> Detected<T> {
    /// The detected value, when there is one.
    pub fn value(&self) -> Option<&T> {
        match self {
            Self::Value(value) => Some(value),
            Self::Undetected(_) => None,
        }
    }

    /// The same outcome, borrowing the value.
    pub fn as_ref(&self) -> Detected<&T> {
        match self {
            Self::Value(value) => Detected::Value(value),
            Self::Undetected(reason) => Detected::Undetected(*reason),
        }
    }
}

/// One GPU as NVML reports it.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct GpuDevice {
    pub model: Detected<PropertyText>,
    pub memory_bytes: Detected<NonZeroU64>,
}

/// The GPUs NVML reports, in device order: from one to [`MAX_PROPERTY_LIST_ITEMS`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize)]
#[serde(try_from = "Vec<GpuDevice>", into = "Vec<GpuDevice>")]
pub struct GpuDevices(Vec<GpuDevice>);

impl GpuDevices {
    pub fn as_slice(&self) -> &[GpuDevice] {
        &self.0
    }
}

impl TryFrom<Vec<GpuDevice>> for GpuDevices {
    type Error = PropertyValueError;

    fn try_from(devices: Vec<GpuDevice>) -> Result<Self, Self::Error> {
        if devices.is_empty() {
            return Err(PropertyValueError::NoDevices);
        }
        if devices.len() > MAX_PROPERTY_LIST_ITEMS {
            return Err(PropertyValueError::TooManyItems {
                actual: devices.len(),
                max: MAX_PROPERTY_LIST_ITEMS,
            });
        }
        Ok(Self(devices))
    }
}

impl From<GpuDevices> for Vec<GpuDevice> {
    fn from(value: GpuDevices) -> Self {
        value.0
    }
}

impl<D> RkyvDeserialize<GpuDevices, D> for ArchivedGpuDevices
where
    D: Fallible + ?Sized,
    D::Error: Source,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<GpuDevices, D::Error> {
        let devices: Vec<GpuDevice> = self.0.deserialize(deserializer)?;
        GpuDevices::try_from(devices).map_err(D::Error::new)
    }
}

/// The GPUs of a node. A node whose NVML library is not installed, or reports no device, has none,
/// and names no driver.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum GpuInventory {
    /// NVML reported these devices and the driver serving them.
    Devices {
        devices: GpuDevices,
        driver: Detected<PropertyText>,
    },
    /// NVML loaded and reported no device, so no driver serves one.
    NoDevice,
    /// No inventory was read.
    Undetected(UndetectedReason),
}

impl GpuInventory {
    /// `node.gpu.count`: the devices NVML reported, 0 without NVML or a device.
    pub fn count(&self) -> usize {
        match self {
            Self::Devices { devices, .. } => devices.as_slice().len(),
            Self::NoDevice | Self::Undetected(_) => 0,
        }
    }

    /// The devices in device order, none without NVML or a device.
    pub fn devices(&self) -> &[GpuDevice] {
        match self {
            Self::Devices { devices, .. } => devices.as_slice(),
            Self::NoDevice | Self::Undetected(_) => &[],
        }
    }

    /// `node.gpu.memory`: the memory of the smallest device whose memory NVML reported.
    pub fn smallest_memory_bytes(&self) -> Option<NonZeroU64> {
        let mut smallest: Option<NonZeroU64> = None;
        for device in self.devices() {
            let Some(memory) = device.memory_bytes.value() else {
                continue;
            };
            smallest = match smallest {
                Some(current) if current <= *memory => Some(current),
                _ => Some(*memory),
            };
        }
        smallest
    }

    /// `node.gpu.driver`: the driver serving the devices, or why it was not read; none without a
    /// device.
    pub fn driver(&self) -> Option<Detected<&PropertyText>> {
        match self {
            Self::Devices { driver, .. } => Some(driver.as_ref()),
            Self::NoDevice => None,
            Self::Undetected(reason) => Some(Detected::Undetected(*reason)),
        }
    }
}

/// The execution providers an ONNX Runtime library reports: at most
/// [`MAX_PROPERTY_LIST_ITEMS`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize)]
#[serde(try_from = "Vec<PropertyText>", into = "Vec<PropertyText>")]
pub struct ExecutionProviders(Vec<PropertyText>);

impl ExecutionProviders {
    pub fn as_slice(&self) -> &[PropertyText] {
        &self.0
    }
}

impl TryFrom<Vec<PropertyText>> for ExecutionProviders {
    type Error = PropertyValueError;

    fn try_from(providers: Vec<PropertyText>) -> Result<Self, Self::Error> {
        if providers.len() > MAX_PROPERTY_LIST_ITEMS {
            return Err(PropertyValueError::TooManyItems {
                actual: providers.len(),
                max: MAX_PROPERTY_LIST_ITEMS,
            });
        }
        Ok(Self(providers))
    }
}

impl From<ExecutionProviders> for Vec<PropertyText> {
    fn from(value: ExecutionProviders) -> Self {
        value.0
    }
}

impl<D> RkyvDeserialize<ExecutionProviders, D> for ArchivedExecutionProviders
where
    D: Fallible + ?Sized,
    D::Error: Source,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<ExecutionProviders, D::Error> {
        let providers: Vec<PropertyText> = self.0.deserialize(deserializer)?;
        ExecutionProviders::try_from(providers).map_err(D::Error::new)
    }
}

/// How the one startup attempt to register the CUDA execution provider in this process ended.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum CudaProviderRegistration {
    /// The provider registered: its provider library and the CUDA runtime loaded.
    Registered,
    /// The loaded ONNX Runtime library does not include the CUDA execution provider.
    NotInLibrary,
    /// The library includes the provider, and registering it failed: its provider library or the
    /// CUDA runtime did not load.
    RegistrationFailed,
}

impl CudaProviderRegistration {
    pub const fn registered(self) -> bool {
        matches!(self, Self::Registered)
    }
}

impl fmt::Display for CudaProviderRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Registered => "the CUDA execution provider registered",
            Self::NotInLibrary => "the loaded ONNX Runtime has no CUDA execution provider",
            Self::RegistrationFailed => {
                "the CUDA execution provider did not register: its library or the CUDA runtime did \
                 not load"
            }
        })
    }
}

/// The ONNX Runtime library this process loaded, as it reports itself.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OnnxRuntime {
    pub version: Detected<PropertyText>,
    pub providers: Detected<ExecutionProviders>,
    pub cuda: CudaProviderRegistration,
}

/// The ONNX Runtime of a node, or why none was detected.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum OnnxRuntimeDetection {
    Loaded(OnnxRuntime),
    Undetected(UndetectedReason),
}

impl OnnxRuntimeDetection {
    /// `node.onnx.cuda`: the CUDA execution provider registered in this process.
    pub fn cuda(&self) -> bool {
        match self {
            Self::Loaded(runtime) => runtime.cuda.registered(),
            Self::Undetected(_) => false,
        }
    }
}

/// What the node detected about itself at startup. Never overridden: an operator states intent
/// with tags.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct NodeProperties {
    /// `node.architecture`: the architecture the binary was built for.
    pub architecture: PropertyText,
    /// `node.cpu.model`.
    pub cpu_model: Detected<PropertyText>,
    /// `node.gpu.count`, `node.gpu.models`, `node.gpu.memory` and `node.gpu.driver`.
    pub gpus: GpuInventory,
    /// `node.onnx.version`, `node.onnx.providers` and `node.onnx.cuda`.
    pub onnx: OnnxRuntimeDetection,
}

impl NodeProperties {
    /// `node.cuda.available`: exactly `node.gpu.count > 0 AND node.onnx.cuda`.
    pub fn cuda_available(&self) -> bool {
        self.gpus.count() > 0 && self.onnx.cuda()
    }
}

/// One record per node name: the run's capacity claim, its detected properties and its tags.
/// A newer run of the node replaces it, and it is kept after the node departs until `DROP NODE`.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct NodeRecord {
    pub claim: NodeCapabilityRecord,
    pub properties: NodeProperties,
    pub tags: NodeTags,
}

impl NodeRecord {
    #[cfg_attr(
        nervix_lint,
        nervix::identity(node, reason = "the record's typed node run identity")
    )]
    pub const fn identity(&self) -> &ClusterNodeIdentity {
        &self.claim.identity
    }

    pub const fn capabilities(&self) -> &NodeCapabilities {
        &self.claim.capabilities
    }
}

/// A node record as consensus committed it.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct CommittedNodeRecord {
    /// The revision, the Raft log index, that committed this record.
    pub revision: u64,
    pub record: NodeRecord,
}

#[cfg(test)]
#[path = "node_record_tests.rs"]
mod tests;
