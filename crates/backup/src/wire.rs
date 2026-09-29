//! The rkyv shapes record sections are encoded in.
//!
//! These shapes are the archive contract written down, and nothing outside this crate names them.
//! They hold primitive values only — text, integers and digests — because a vocabulary type's
//! archived form does not validate what it decodes: a name read back through rkyv is not checked
//! against the name grammar. Every value is therefore validated once, when a record converts from
//! its wire shape, and a change to a vocabulary type can never change an archive's bytes.
//!
//! Changing any shape here changes the bytes of its record kind, so it also raises that kind's
//! format version.

use rkyv::{Archive, Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct ManifestWire {
    pub(crate) format_major: u16,
    pub(crate) producer_version: String,
    pub(crate) language_version: String,
    pub(crate) cluster_id: String,
    pub(crate) captured_at_unix_nanos: i64,
    pub(crate) scope: ScopeWire,
    pub(crate) resources: ResourcesWire,
    pub(crate) domains: Vec<DomainCaptureWire>,
    pub(crate) sections: Vec<SectionEntryWire>,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) enum ScopeWire {
    Cluster,
    Domain(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) enum ResourcesWire {
    Included,
    Omitted,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct DomainCaptureWire {
    pub(crate) domain: String,
    pub(crate) revision: u64,
    pub(crate) raft_term: u64,
    pub(crate) raft_index: u64,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct SectionEntryWire {
    pub(crate) path: String,
    pub(crate) content: SectionContentWire,
    pub(crate) length: u64,
    pub(crate) digest: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) enum SectionContentWire {
    Record { kind: u16 },
    Nspl,
    ResourceArchive,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct UsersWire {
    pub(crate) users: Vec<UserWire>,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct UserWire {
    pub(crate) name: String,
    pub(crate) password_hash: String,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct DomainWire {
    pub(crate) domain: String,
    pub(crate) pace: PaceWire,
    pub(crate) placement: PlacementWire,
    pub(crate) status: StatusWire,
    pub(crate) start_version: u64,
    pub(crate) start_point: StartPointWire,
    pub(crate) clock: Option<ClockMappingWire>,
    pub(crate) logical_frontier_unix_nanos: Option<i64>,
    pub(crate) resources: Vec<DeclaredResourceWire>,
}

#[derive(Debug, Clone, Copy, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) enum PaceWire {
    Unpaced,
    Paced { period_nanos: u64, skew_nanos: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) enum PlacementWire {
    RequireColocation,
    PreferColocation,
    Neutral,
    SuggestSeparation,
}

#[derive(Debug, Clone, Copy, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) enum StatusWire {
    Stopped,
    Running,
    Paused,
}

#[derive(Debug, Clone, Copy, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) enum StartPointWire {
    Resume,
    Now {
        time_rate: f64,
    },
    At {
        timestamp_unix_nanos: i64,
        time_rate: f64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct ClockMappingWire {
    pub(crate) wall_started_at_unix_nanos: i64,
    pub(crate) logical_start_unix_nanos: i64,
    pub(crate) time_rate: f64,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct DeclaredResourceWire {
    pub(crate) resource: String,
    pub(crate) next_version: u64,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct ResourceVersionWire {
    pub(crate) domain: String,
    pub(crate) resource: String,
    pub(crate) version: u64,
    pub(crate) state: VersionStateWire,
    pub(crate) published: Option<PublishedVersionWire>,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) enum VersionStateWire {
    Completed,
    Failed { reason: String },
    Unfinished,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct PublishedVersionWire {
    pub(crate) root_checksum: String,
    pub(crate) manifest_checksum: String,
    pub(crate) file_count: u64,
    pub(crate) total_bytes: u64,
    pub(crate) archive_bytes: u64,
    pub(crate) created_at_unix_nanos: i64,
    pub(crate) created_by_node: NodeNameWire,
}

/// A node name as an archive encodes it: text that decoding validates against the node name
/// grammar before it becomes a node identity.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct NodeNameWire(pub(crate) String);
