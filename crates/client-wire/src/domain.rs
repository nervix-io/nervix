//! Domain and cluster observations: the domain list, active-domain selection, live graph snapshots
//! and the cluster summary.

use std::{num::NonZeroU64, time::Duration};

use error_stack::Report;
use flatbuffers::{ForwardsUOffset, Vector, WIPOffset};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    DomainClockPeriod, DomainClockSkew, DomainName, DomainPace, DomainStatus, NodeRef, ResourceName,
};

use crate::{
    codec::{DecodeError, Decoder, EncodeError, EncodedUnion, Encoder, wire_enum},
    common::{decode_node_ref, encode_node_ref},
    frame::{EncodedFrame, ServerFrame, VerifiedFrame},
    limits::SessionLimits,
    server::finish_server_message,
    wire,
};

wire_enum!(ALL_DOMAIN_STATUSES: DomainStatus => wire::DomainStatus { Stopped, Running, Paused });

/// One domain as the cluster currently describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainInfo {
    pub domain: DomainName,
    pub status: DomainStatus,
    pub pace: DomainPace,
}

impl DomainInfo {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::DomainInfo<'fbb>>, Report<EncodeError>> {
        let domain = encoder.text("DomainInfo.domain", self.domain.as_str())?;
        let pace = match self.pace {
            DomainPace::Unpaced => EncodedUnion::new(
                wire::DomainPace::UnpacedDomain,
                wire::UnpacedDomain::create(encoder.fbb(), &wire::UnpacedDomainArgs {}),
            ),
            DomainPace::Paced { period, skew } => EncodedUnion::new(
                wire::DomainPace::PacedDomain,
                wire::PacedDomain::create(
                    encoder.fbb(),
                    &wire::PacedDomainArgs {
                        period_nanos: period.as_nanos(),
                        skew_nanos: skew.as_nanos(),
                    },
                ),
            ),
        };
        Ok(wire::DomainInfo::create(
            encoder.fbb(),
            &wire::DomainInfoArgs {
                domain: Some(domain),
                status: Some(self.status.clone().into()),
                pace_type: pace.discriminant,
                pace: Some(pace.value),
            },
        ))
    }

    fn decode(
        decoder: Decoder<'_>,
        info: wire::DomainInfo<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        let domain = decoder.name("DomainInfo.domain", info.domain())?;
        let status = decoder.required_enumeration("DomainInfo.status", info.status())?;
        let pace = if let Some(paced) = info.pace_as_paced_domain() {
            let period = decoder.non_zero("PacedDomain.period_nanos", paced.period_nanos())?;
            let period = DomainClockPeriod::try_from(Duration::from_nanos(period.get()))
                .assured("a non-zero nanosecond count is a valid domain clock period");
            let skew = DomainClockSkew::try_from(Duration::from_nanos(paced.skew_nanos()))
                .assured("any nanosecond count held in u64 is a valid domain clock skew");
            DomainPace::Paced { period, skew }
        } else if let wire::DomainPace::UnpacedDomain = info.pace_type() {
            DomainPace::Unpaced
        } else {
            return Err(decoder.unknown_union("DomainInfo.pace", info.pace_type().0));
        };
        Ok(Self {
            domain,
            status,
            pace,
        })
    }

    fn encode_all<'fbb>(
        encoder: &mut Encoder<'fbb>,
        field: &'static str,
        domains: &[Self],
    ) -> Result<WIPOffset<Vector<'fbb, ForwardsUOffset<wire::DomainInfo<'fbb>>>>, Report<EncodeError>>
    {
        encoder.table_vector(field, domains, Self::encode)
    }

    fn decode_all<'a>(
        decoder: Decoder<'_>,
        field: &'static str,
        domains: Vector<'a, ForwardsUOffset<wire::DomainInfo<'a>>>,
    ) -> Result<Vec<Self>, Report<DecodeError>> {
        decoder.table_vector(field, domains, |domain| Self::decode(decoder, domain))
    }
}

/// The domains a session asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainList {
    pub domains: Vec<DomainInfo>,
}

impl DomainList {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<EncodeError>> {
        let domains = DomainInfo::encode_all(encoder, "DomainList.domains", &self.domains)?;
        let list = wire::DomainList::create(
            encoder.fbb(),
            &wire::DomainListArgs {
                domains: Some(domains),
            },
        );
        Ok(EncodedUnion::new(wire::ReplyBody::DomainList, list))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        list: wire::DomainList<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        let domains = DomainInfo::decode_all(decoder, "DomainList.domains", list.domains())?;
        Ok(Self { domains })
    }
}

/// The outcome of selecting the domain a session observes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainSelection {
    Selected(DomainName),
    NotFound(DomainName),
}

impl DomainSelection {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<EncodeError>> {
        let selection = match self {
            Self::Selected(domain) => {
                let domain = encoder.text("DomainSelected.domain", domain.as_str())?;
                let selected = wire::DomainSelected::create(
                    encoder.fbb(),
                    &wire::DomainSelectedArgs {
                        domain: Some(domain),
                    },
                );
                EncodedUnion::new(wire::DomainSelection::DomainSelected, selected)
            }
            Self::NotFound(domain) => {
                let domain = encoder.text("DomainNotFound.domain", domain.as_str())?;
                let not_found = wire::DomainNotFound::create(
                    encoder.fbb(),
                    &wire::DomainNotFoundArgs {
                        domain: Some(domain),
                    },
                );
                EncodedUnion::new(wire::DomainSelection::DomainNotFound, not_found)
            }
        };
        let outcome = wire::DomainSelectionOutcome::create(
            encoder.fbb(),
            &wire::DomainSelectionOutcomeArgs {
                selection_type: selection.discriminant,
                selection: Some(selection.value),
            },
        );
        Ok(EncodedUnion::new(
            wire::ReplyBody::DomainSelectionOutcome,
            outcome,
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        outcome: wire::DomainSelectionOutcome<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        if let Some(selected) = outcome.selection_as_domain_selected() {
            let domain = decoder.name("DomainSelected.domain", selected.domain())?;
            return Ok(Self::Selected(domain));
        }
        if let Some(not_found) = outcome.selection_as_domain_not_found() {
            let domain = decoder.name("DomainNotFound.domain", not_found.domain())?;
            return Ok(Self::NotFound(domain));
        }
        Err(decoder.unknown_union(
            "DomainSelectionOutcome.selection",
            outcome.selection_type().0,
        ))
    }
}

/// The domains of the cluster, pushed when they change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainsObserved {
    pub domains: Vec<DomainInfo>,
}

impl DomainsObserved {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<ServerFrame>, Report<EncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let domains =
            DomainInfo::encode_all(&mut encoder, "DomainsObserved.domains", &self.domains)?;
        let observed = wire::DomainsObserved::create(
            encoder.fbb(),
            &wire::DomainsObservedArgs {
                domains: Some(domains),
            },
        );
        finish_server_message(
            encoder,
            EncodedUnion::new(wire::ServerBody::DomainsObserved, observed),
        )
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        observed: wire::DomainsObserved<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        let domains =
            DomainInfo::decode_all(decoder, "DomainsObserved.domains", observed.domains())?;
        Ok(Self { domains })
    }
}

/// An entity a domain snapshot lists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainEntity {
    Model(NodeRef),
    Resource {
        name: ResourceName,
        /// The newest completed version, while any version has completed.
        latest_version: Option<NonZeroU64>,
    },
}

impl DomainEntity {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::DomainEntityEntry<'fbb>>, Report<EncodeError>> {
        let entity = match self {
            Self::Model(node) => {
                let node = encode_node_ref(encoder, node)?;
                let model = wire::ModelEntity::create(
                    encoder.fbb(),
                    &wire::ModelEntityArgs { node: Some(node) },
                );
                EncodedUnion::new(wire::DomainEntity::ModelEntity, model)
            }
            Self::Resource {
                name,
                latest_version,
            } => {
                let name = encoder.text("ResourceEntity.name", name.as_str())?;
                let latest_version = latest_version.map(NonZeroU64::get);
                let resource = wire::ResourceEntity::create(
                    encoder.fbb(),
                    &wire::ResourceEntityArgs {
                        name: Some(name),
                        latest_version,
                    },
                );
                EncodedUnion::new(wire::DomainEntity::ResourceEntity, resource)
            }
        };
        Ok(wire::DomainEntityEntry::create(
            encoder.fbb(),
            &wire::DomainEntityEntryArgs {
                entity_type: entity.discriminant,
                entity: Some(entity.value),
            },
        ))
    }

    fn decode(
        decoder: Decoder<'_>,
        entry: wire::DomainEntityEntry<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        if let Some(model) = entry.entity_as_model_entity() {
            return Ok(Self::Model(decode_node_ref(decoder, model.node())?));
        }
        if let Some(resource) = entry.entity_as_resource_entity() {
            let name = decoder.name("ResourceEntity.name", resource.name())?;
            let latest_version = match resource.latest_version() {
                Some(version) => Some(decoder.non_zero("ResourceEntity.latest_version", version)?),
                None => None,
            };
            return Ok(Self::Resource {
                name,
                latest_version,
            });
        }
        Err(decoder.unknown_union("DomainEntityEntry.entity", entry.entity_type().0))
    }
}

/// A live snapshot of one domain's dataflow graph and entities.
///
/// The graph stays in the frame that carried it; [`DomainSnapshotObserved::graph_json`] reads it
/// in place.
#[derive(Debug, Clone)]
pub struct DomainSnapshotObserved {
    frame: VerifiedFrame<ServerFrame>,
    domain: DomainName,
    entities: Vec<DomainEntity>,
}

impl DomainSnapshotObserved {
    /// Encodes a snapshot of `domain` whose graph `graph_json` describes.
    pub fn encode(
        domain: &DomainName,
        graph_json: &str,
        entities: &[DomainEntity],
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<ServerFrame>, Report<EncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let domain = encoder.text("DomainSnapshotObserved.domain", domain.as_str())?;
        let graph_json = encoder.text("DomainSnapshotObserved.graph_json", graph_json)?;
        let entities = encoder.table_vector(
            "DomainSnapshotObserved.entities",
            entities,
            DomainEntity::encode,
        )?;
        let snapshot = wire::DomainSnapshotObserved::create(
            encoder.fbb(),
            &wire::DomainSnapshotObservedArgs {
                domain: Some(domain),
                graph_json: Some(graph_json),
                entities: Some(entities),
            },
        );
        finish_server_message(
            encoder,
            EncodedUnion::new(wire::ServerBody::DomainSnapshotObserved, snapshot),
        )
    }

    pub(crate) fn decode(
        frame: &VerifiedFrame<ServerFrame>,
        decoder: Decoder<'_>,
        snapshot: wire::DomainSnapshotObserved<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        let domain = decoder.name("DomainSnapshotObserved.domain", snapshot.domain())?;
        decoder.check_text("DomainSnapshotObserved.graph_json", snapshot.graph_json())?;
        let entities = decoder.table_vector(
            "DomainSnapshotObserved.entities",
            snapshot.entities(),
            |entry| DomainEntity::decode(decoder, entry),
        )?;
        Ok(Self {
            frame: frame.clone(),
            domain,
            entities,
        })
    }

    pub fn domain(&self) -> &DomainName {
        &self.domain
    }

    /// The live dataflow graph as a JSON document, read in place from the frame.
    pub fn graph_json(&self) -> &str {
        self.frame
            .root()
            .body_as_domain_snapshot_observed()
            .assured("this value is only decoded from a frame whose body is a domain snapshot")
            .graph_json()
    }

    pub fn entities(&self) -> &[DomainEntity] {
        &self.entities
    }

    /// The frame the graph is read from, and the bytes it keeps alive.
    pub fn frame(&self) -> &VerifiedFrame<ServerFrame> {
        &self.frame
    }
}

/// A summary of the cluster's running work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterObserved {
    pub running_domains: u64,
    pub graph_nodes: u64,
    pub relays: u64,
}

impl ClusterObserved {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<ServerFrame>, Report<EncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let observed = wire::ClusterObserved::create(
            encoder.fbb(),
            &wire::ClusterObservedArgs {
                running_domains: self.running_domains,
                graph_nodes: self.graph_nodes,
                relays: self.relays,
            },
        );
        finish_server_message(
            encoder,
            EncodedUnion::new(wire::ServerBody::ClusterObserved, observed),
        )
    }

    pub(crate) fn decode(observed: wire::ClusterObserved<'_>) -> Self {
        Self {
            running_domains: observed.running_domains(),
            graph_nodes: observed.graph_nodes(),
            relays: observed.relays(),
        }
    }
}
