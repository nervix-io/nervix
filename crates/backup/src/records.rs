//! The records an archive holds beside its NSPL: users, a domain's lifecycle and catalog, and the
//! catalog metadata of each resource version.
//!
//! Each record embeds vocabulary types and converts from its wire shape through their validating
//! constructors, so a name, a clock period or a time rate read from an archive is exactly as valid
//! as one written by a statement.

use std::num::NonZeroU64;

use error_stack::Report;
use nervix_models::{
    ClusterNodeName, DomainClockPeriod, DomainClockSkew, DomainClockState, DomainName, DomainPace,
    DomainStartPoint, DomainStatus, DomainTimeRate, PlacementPolicy, ResourceName, Timestamp,
    UserName,
};

use crate::{
    error::{ArchiveReadError, ArchiveWriteError},
    section::{ArchiveRecord, RecordKind, decode_record, encode_record},
    wire::{
        ClockMappingWire, DeclaredResourceWire, DomainWire, NodeNameWire, PaceWire, PlacementWire,
        PublishedVersionWire, ResourceVersionWire, StartPointWire, StatusWire, UserWire, UsersWire,
        VersionStateWire,
    },
};

/// The users record's format version.
const USERS_VERSION: u16 = 1;
/// The domain record's format version.
const DOMAIN_VERSION: u16 = 1;
/// The resource version record's format version.
const RESOURCE_VERSION_VERSION: u16 = 1;

/// Every user of a cluster backup, in name order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsersRecord {
    pub users: Vec<UserRecord>,
}

/// One user: its name and its password hash, a PHC string. The password itself is never stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserRecord {
    pub name: UserName,
    pub password_hash: String,
}

/// A domain's lifecycle, clock and resource catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainRecord {
    pub domain: DomainName,
    pub pace: DomainPace,
    /// The placement policy nodes of the domain default to.
    pub placement: PlacementPolicy,
    pub status: DomainStatus,
    /// How many times the domain has been started.
    pub start_version: u64,
    /// Where the latest start began the domain's time.
    pub start_point: DomainStartPoint,
    /// The committed mapping from UTC to the domain's logical time. Absent while none is
    /// committed.
    pub clock: Option<DomainClockState>,
    /// The logical instant the committed mapping had reached when the domain was read. Absent when
    /// the domain has no committed mapping.
    pub logical_frontier: Option<Timestamp>,
    /// Every resource the domain declares, in name order.
    pub resources: Vec<DeclaredResource>,
}

/// A resource a domain declares, and the version its next upload will be assigned. Every version
/// below it was assigned once, whatever became of it, and is never assigned again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredResource {
    pub resource: ResourceName,
    pub next_version: NonZeroU64,
}

/// The catalog metadata of one resource version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceVersionRecord {
    /// The domain that declares the resource.
    pub domain: DomainName,
    pub resource: ResourceName,
    pub version: NonZeroU64,
    pub state: ResourceVersionState,
    /// What the version holds, once its archive was published. Absent for a version whose upload
    /// ended before it published anything.
    pub published: Option<PublishedResourceVersion>,
}

/// What became of a resource version's upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceVersionState {
    /// The version is installed on every live node and can be bound.
    Completed,
    /// The upload failed, for the reason its installation reported.
    Failed { reason: String },
    /// The upload was still installing when the domain was read.
    Unfinished,
}

/// What a published resource version holds, with the checksums `DESCRIBE RESOURCE` reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedResourceVersion {
    pub root_checksum: String,
    pub manifest_checksum: String,
    pub file_count: u64,
    pub total_bytes: u64,
    /// The size of the version's original archive.
    pub archive_bytes: u64,
    pub created_at: Timestamp,
    pub created_by_node: ClusterNodeName,
}

impl ArchiveRecord for UsersRecord {
    const KIND: RecordKind = RecordKind::Users;
    const VERSION: u16 = USERS_VERSION;

    fn encode(&self) -> Result<Vec<u8>, Report<ArchiveWriteError>> {
        let wire = UsersWire {
            users: self
                .users
                .iter()
                .map(|user| UserWire {
                    name: user.name.as_str().to_string(),
                    password_hash: user.password_hash.clone(),
                })
                .collect(),
        };
        encode_record(Self::KIND, Self::VERSION, &wire)
    }

    fn decode(path: &str, bytes: &[u8]) -> Result<Self, Report<ArchiveReadError>> {
        let wire: UsersWire = decode_record(path, Self::KIND, Self::VERSION, bytes)?;
        let mut users = Vec::with_capacity(wire.users.len());
        for user in wire.users {
            let name = UserName::parse(&user.name)
                .map_err(|error| error.change_context(invalid_value(path, "user name")))?;
            if user.password_hash.is_empty() {
                return Err(Report::new(invalid_value(path, "password hash")));
            }
            users.push(UserRecord {
                name,
                password_hash: user.password_hash,
            });
        }
        Ok(Self { users })
    }
}

impl ArchiveRecord for DomainRecord {
    const KIND: RecordKind = RecordKind::Domain;
    const VERSION: u16 = DOMAIN_VERSION;

    fn encode(&self) -> Result<Vec<u8>, Report<ArchiveWriteError>> {
        let pace = match self.pace {
            DomainPace::Unpaced => PaceWire::Unpaced,
            DomainPace::Paced { period, skew } => PaceWire::Paced {
                period_nanos: period.as_nanos(),
                skew_nanos: skew.as_nanos(),
            },
        };
        let placement = match self.placement {
            PlacementPolicy::RequireColocation => PlacementWire::RequireColocation,
            PlacementPolicy::PreferColocation => PlacementWire::PreferColocation,
            PlacementPolicy::Neutral => PlacementWire::Neutral,
            PlacementPolicy::SuggestSeparation => PlacementWire::SuggestSeparation,
        };
        let status = match self.status {
            DomainStatus::Stopped => StatusWire::Stopped,
            DomainStatus::Running => StatusWire::Running,
            DomainStatus::Paused => StatusWire::Paused,
        };
        let start_point = match &self.start_point {
            DomainStartPoint::Resume => StartPointWire::Resume,
            DomainStartPoint::Now { time_rate } => StartPointWire::Now {
                time_rate: time_rate.get(),
            },
            DomainStartPoint::At {
                timestamp,
                time_rate,
            } => StartPointWire::At {
                timestamp_unix_nanos: timestamp.unix_nanos(),
                time_rate: time_rate.get(),
            },
        };
        let clock = self.clock.as_ref().map(|clock| ClockMappingWire {
            wall_started_at_unix_nanos: clock.wall_started_at().unix_nanos(),
            logical_start_unix_nanos: clock.logical_start().unix_nanos(),
            time_rate: clock.time_rate().get(),
        });
        let wire = DomainWire {
            domain: self.domain.as_str().to_string(),
            pace,
            placement,
            status,
            start_version: self.start_version,
            start_point,
            clock,
            logical_frontier_unix_nanos: self.logical_frontier.map(Timestamp::unix_nanos),
            resources: self
                .resources
                .iter()
                .map(|declared| DeclaredResourceWire {
                    resource: declared.resource.as_str().to_string(),
                    next_version: declared.next_version.get(),
                })
                .collect(),
        };
        encode_record(Self::KIND, Self::VERSION, &wire)
    }

    fn decode(path: &str, bytes: &[u8]) -> Result<Self, Report<ArchiveReadError>> {
        let wire: DomainWire = decode_record(path, Self::KIND, Self::VERSION, bytes)?;
        let domain = DomainName::parse(&wire.domain)
            .map_err(|error| error.change_context(invalid_value(path, "domain name")))?;
        let pace = match wire.pace {
            PaceWire::Unpaced => DomainPace::Unpaced,
            PaceWire::Paced {
                period_nanos,
                skew_nanos,
            } => {
                let Some(period_nanos) = NonZeroU64::new(period_nanos) else {
                    return Err(Report::new(invalid_value(path, "clock period")));
                };
                DomainPace::Paced {
                    period: DomainClockPeriod::from_nanos(period_nanos),
                    skew: DomainClockSkew::from_nanos(skew_nanos),
                }
            }
        };
        let placement = match wire.placement {
            PlacementWire::RequireColocation => PlacementPolicy::RequireColocation,
            PlacementWire::PreferColocation => PlacementPolicy::PreferColocation,
            PlacementWire::Neutral => PlacementPolicy::Neutral,
            PlacementWire::SuggestSeparation => PlacementPolicy::SuggestSeparation,
        };
        let status = match wire.status {
            StatusWire::Stopped => DomainStatus::Stopped,
            StatusWire::Running => DomainStatus::Running,
            StatusWire::Paused => DomainStatus::Paused,
        };
        let start_point = match wire.start_point {
            StartPointWire::Resume => DomainStartPoint::Resume,
            StartPointWire::Now { time_rate } => DomainStartPoint::Now {
                time_rate: time_rate_value(path, time_rate)?,
            },
            StartPointWire::At {
                timestamp_unix_nanos,
                time_rate,
            } => DomainStartPoint::At {
                timestamp: Timestamp::from_unix_nanos(timestamp_unix_nanos),
                time_rate: time_rate_value(path, time_rate)?,
            },
        };
        let clock = match wire.clock {
            Some(clock) => Some(DomainClockState::new(
                Timestamp::from_unix_nanos(clock.wall_started_at_unix_nanos),
                Timestamp::from_unix_nanos(clock.logical_start_unix_nanos),
                time_rate_value(path, clock.time_rate)?,
            )),
            None => None,
        };
        let mut resources = Vec::with_capacity(wire.resources.len());
        for declared in wire.resources {
            let resource = ResourceName::parse(&declared.resource)
                .map_err(|error| error.change_context(invalid_value(path, "resource name")))?;
            let Some(next_version) = NonZeroU64::new(declared.next_version) else {
                return Err(Report::new(invalid_value(path, "next resource version")));
            };
            resources.push(DeclaredResource {
                resource,
                next_version,
            });
        }
        Ok(Self {
            domain,
            pace,
            placement,
            status,
            start_version: wire.start_version,
            start_point,
            clock,
            logical_frontier: wire
                .logical_frontier_unix_nanos
                .map(Timestamp::from_unix_nanos),
            resources,
        })
    }
}

impl ArchiveRecord for ResourceVersionRecord {
    const KIND: RecordKind = RecordKind::ResourceVersion;
    const VERSION: u16 = RESOURCE_VERSION_VERSION;

    fn encode(&self) -> Result<Vec<u8>, Report<ArchiveWriteError>> {
        let state = match &self.state {
            ResourceVersionState::Completed => VersionStateWire::Completed,
            ResourceVersionState::Failed { reason } => VersionStateWire::Failed {
                reason: reason.clone(),
            },
            ResourceVersionState::Unfinished => VersionStateWire::Unfinished,
        };
        let published = self
            .published
            .as_ref()
            .map(|published| PublishedVersionWire {
                root_checksum: published.root_checksum.clone(),
                manifest_checksum: published.manifest_checksum.clone(),
                file_count: published.file_count,
                total_bytes: published.total_bytes,
                archive_bytes: published.archive_bytes,
                created_at_unix_nanos: published.created_at.unix_nanos(),
                created_by_node: NodeNameWire(published.created_by_node.as_str().to_string()),
            });
        let wire = ResourceVersionWire {
            domain: self.domain.as_str().to_string(),
            resource: self.resource.as_str().to_string(),
            version: self.version.get(),
            state,
            published,
        };
        encode_record(Self::KIND, Self::VERSION, &wire)
    }

    fn decode(path: &str, bytes: &[u8]) -> Result<Self, Report<ArchiveReadError>> {
        let wire: ResourceVersionWire = decode_record(path, Self::KIND, Self::VERSION, bytes)?;
        let domain = DomainName::parse(&wire.domain)
            .map_err(|error| error.change_context(invalid_value(path, "domain name")))?;
        let resource = ResourceName::parse(&wire.resource)
            .map_err(|error| error.change_context(invalid_value(path, "resource name")))?;
        let Some(version) = NonZeroU64::new(wire.version) else {
            return Err(Report::new(invalid_value(path, "resource version")));
        };
        let state = match wire.state {
            VersionStateWire::Completed => ResourceVersionState::Completed,
            VersionStateWire::Failed { reason } => ResourceVersionState::Failed { reason },
            VersionStateWire::Unfinished => ResourceVersionState::Unfinished,
        };
        let published = match wire.published {
            Some(published) => {
                let created_by_node = ClusterNodeName::parse(&published.created_by_node.0)
                    .map_err(|error| error.change_context(invalid_value(path, "node name")))?;
                Some(PublishedResourceVersion {
                    root_checksum: published.root_checksum,
                    manifest_checksum: published.manifest_checksum,
                    file_count: published.file_count,
                    total_bytes: published.total_bytes,
                    archive_bytes: published.archive_bytes,
                    created_at: Timestamp::from_unix_nanos(published.created_at_unix_nanos),
                    created_by_node,
                })
            }
            None => None,
        };
        Ok(Self {
            domain,
            resource,
            version,
            state,
            published,
        })
    }
}

fn invalid_value(path: &str, field: &'static str) -> ArchiveReadError {
    ArchiveReadError::InvalidValue {
        path: path.to_string(),
        field,
    }
}

fn time_rate_value(path: &str, rate: f64) -> Result<DomainTimeRate, Report<ArchiveReadError>> {
    DomainTimeRate::try_from(rate)
        .map_err(|error| Report::new(error).change_context(invalid_value(path, "time rate")))
}
