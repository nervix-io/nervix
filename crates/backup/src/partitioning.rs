//! Captured inherited topology in an archive, independent of cluster placement.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Primitive archive encoding and validation of topology, epochs and holding units.
//! - **Depends on.** The vocabulary and this crate's bounded record framing.
//! - **Must not know.** Discovery, scheduling, checkpoint installation or broker drivers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the archive encoding boundary declared above")
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
};

use error_stack::Report;
use nervix_models::{
    BranchName, BranchPartitionEpoch, DomainName, ExternalPartitionSet, IngestorName,
    InheritedPartitionLayout, PartitionEpochNumber, PartitionSourceFingerprint, PartitionTopology,
    PartitionTopologySnapshot, PartitionTransition, ResolvedBranchPartitions,
    SourcePartitionObservation,
};

use crate::{
    ArchiveReadError, ArchiveRecord, ArchiveWriteError, RecordKind,
    section::{decode_record, encode_record},
    wire::{
        ExternalPartitionSetWire, InheritedGroupingWire, InheritedLayoutWire, PartitionEpochWire,
        PartitionLayoutWire, PartitionSourceWire, PartitionTopologyWire,
    },
};

/// Present only when a captured domain uses inherited coordinates or still holds their state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionTopologyRecord {
    pub domain: DomainName,
    pub snapshot: PartitionTopologySnapshot,
}

fn layout_wire(layout: ResolvedBranchPartitions) -> PartitionLayoutWire {
    match layout {
        ResolvedBranchPartitions::Direct(count) => PartitionLayoutWire::Direct(count.get()),
        ResolvedBranchPartitions::Inherited(count) => PartitionLayoutWire::Inherited(count.get()),
    }
}

fn invalid(path: &str, field: &'static str) -> Report<ArchiveReadError> {
    Report::new(invalid_value(path, field))
}

fn invalid_value(path: &str, field: &'static str) -> ArchiveReadError {
    ArchiveReadError::InvalidValue {
        path: path.to_owned(),
        field,
    }
}

fn layout(
    path: &str,
    wire: PartitionLayoutWire,
) -> Result<ResolvedBranchPartitions, Report<ArchiveReadError>> {
    match wire {
        PartitionLayoutWire::Direct(count) => Ok(ResolvedBranchPartitions::Direct(
            NonZeroU16::new(count).ok_or_else(|| invalid(path, "partition count"))?,
        )),
        PartitionLayoutWire::Inherited(count) => Ok(ResolvedBranchPartitions::Inherited(
            NonZeroU16::new(count).ok_or_else(|| invalid(path, "partition count"))?,
        )),
    }
}

/// Validates the independently captured source observations and their agreed branch grouping.
fn decode_topology(
    path: &str,
    sources: Vec<PartitionSourceWire>,
    branches: Vec<InheritedLayoutWire>,
    suspended: Vec<String>,
) -> Result<PartitionTopology, Report<ArchiveReadError>> {
    let mut topology = PartitionTopology::default();
    for source in sources {
        let name = IngestorName::parse(&source.name)
            .map_err(|cause| cause.change_context(invalid_value(path, "ingestor name")))?;
        let partitions = match source.partitions {
            None => None,
            Some(ExternalPartitionSetWire::Unpartitioned) => {
                Some(ExternalPartitionSet::Unpartitioned)
            }
            Some(ExternalPartitionSetWire::Partitioned(count)) => {
                Some(ExternalPartitionSet::Partitioned(
                    NonZeroU32::new(count)
                        .ok_or_else(|| invalid(path, "external partition count"))?,
                ))
            }
        };
        if topology
            .sources
            .insert(
                name,
                SourcePartitionObservation {
                    configuration: PartitionSourceFingerprint::from_digest(source.configuration),
                    partitions,
                    epoch: source.epoch,
                },
            )
            .is_some()
        {
            return Err(invalid(path, "source observation"));
        }
    }
    for branch in branches {
        let name = BranchName::parse(&branch.name)
            .map_err(|cause| cause.change_context(invalid_value(path, "branch name")))?;
        let grouping = match branch.layout {
            InheritedGroupingWire::Identity(count) => InheritedPartitionLayout::Identity(
                NonZeroU16::new(count).ok_or_else(|| invalid(path, "partition count"))?,
            ),
            InheritedGroupingWire::Grouped {
                source_count,
                groups,
            } => InheritedPartitionLayout::Grouped {
                source_count: NonZeroU32::new(source_count)
                    .ok_or_else(|| invalid(path, "external partition count"))?,
                groups: NonZeroU16::new(groups).ok_or_else(|| invalid(path, "partition count"))?,
            },
        };
        if topology.branches.insert(name, grouping).is_some() {
            return Err(invalid(path, "branch grouping"));
        }
    }
    for suspended in suspended {
        let name = IngestorName::parse(&suspended)
            .map_err(|cause| cause.change_context(invalid_value(path, "ingestor name")))?;
        if !topology.sources.contains_key(&name) || !topology.suspended.insert(name) {
            return Err(invalid(path, "suspended source"));
        }
    }
    Ok(topology)
}

impl ArchiveRecord for PartitionTopologyRecord {
    const KIND: RecordKind = RecordKind::PartitionTopology;
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "the record header fences the one supported captured partition topology shape"
        )
    )]
    const VERSION: u16 = 1;

    fn encode(&self) -> Result<Vec<u8>, Report<ArchiveWriteError>> {
        let snapshot = &self.snapshot;
        let wire = PartitionTopologyWire {
            domain: self.domain.as_str().to_owned(),
            sources: snapshot
                .topology
                .sources
                .iter()
                .map(|(name, source)| PartitionSourceWire {
                    name: name.as_str().to_owned(),
                    configuration: *source.configuration.digest(),
                    partitions: source.partitions.map(|set| match set {
                        ExternalPartitionSet::Unpartitioned => {
                            ExternalPartitionSetWire::Unpartitioned
                        }
                        ExternalPartitionSet::Partitioned(count) => {
                            ExternalPartitionSetWire::Partitioned(count.get())
                        }
                    }),
                    epoch: source.epoch,
                })
                .collect(),
            branches: snapshot
                .topology
                .branches
                .iter()
                .map(|(name, grouping)| InheritedLayoutWire {
                    name: name.as_str().to_owned(),
                    layout: match grouping {
                        InheritedPartitionLayout::Identity(count) => {
                            InheritedGroupingWire::Identity(count.get())
                        }
                        InheritedPartitionLayout::Grouped {
                            source_count,
                            groups,
                        } => InheritedGroupingWire::Grouped {
                            source_count: source_count.get(),
                            groups: groups.get(),
                        },
                    },
                })
                .collect(),
            suspended: snapshot
                .topology
                .suspended
                .iter()
                .map(|name| name.as_str().to_owned())
                .collect(),
            epochs: snapshot
                .epochs
                .iter()
                .map(|(name, epoch)| PartitionEpochWire {
                    name: name.as_str().to_owned(),
                    number: epoch.number.get(),
                    partitions: layout_wire(epoch.partitions),
                    previous: epoch
                        .transition
                        .as_ref()
                        .map(|transition| layout_wire(transition.previous)),
                })
                .collect(),
            holding: snapshot
                .holding_units
                .iter()
                .map(|(name, units)| {
                    (
                        name.as_str().to_owned(),
                        units.iter().copied().map(Into::into).collect(),
                    )
                })
                .collect(),
            intake_units: snapshot
                .intake_units
                .iter()
                .map(|(name, count)| (name.as_str().to_owned(), count.get()))
                .collect(),
        };
        encode_record(Self::KIND, Self::VERSION, &wire)
    }

    fn decode(path: &str, bytes: &[u8]) -> Result<Self, Report<ArchiveReadError>> {
        let wire: PartitionTopologyWire = decode_record(path, Self::KIND, Self::VERSION, bytes)?;
        let domain = DomainName::parse(&wire.domain)
            .map_err(|cause| cause.change_context(invalid_value(path, "domain name")))?;
        let topology = decode_topology(path, wire.sources, wire.branches, wire.suspended)?;
        let mut epochs = BTreeMap::new();
        for epoch in wire.epochs {
            let name = BranchName::parse(&epoch.name)
                .map_err(|cause| cause.change_context(invalid_value(path, "branch name")))?;
            let number = PartitionEpochNumber::new(
                NonZeroU64::new(epoch.number).ok_or_else(|| invalid(path, "partition epoch"))?,
            );
            let partitions = layout(path, epoch.partitions)?;
            let transition = epoch
                .previous
                .map(|previous| {
                    layout(path, previous).map(|previous| PartitionTransition { previous })
                })
                .transpose()?;
            if epochs
                .insert(
                    name,
                    BranchPartitionEpoch {
                        number,
                        partitions,
                        transition,
                    },
                )
                .is_some()
            {
                return Err(invalid(path, "branch epoch"));
            }
        }
        for (branch, grouping) in &topology.branches {
            if epochs
                .get(branch)
                .is_none_or(|epoch| epoch.partitions != grouping.partitions())
            {
                return Err(invalid(path, "grouping epoch"));
            }
        }
        let mut holding_units = BTreeMap::new();
        for (branch, units) in wire.holding {
            let name = BranchName::parse(&branch)
                .map_err(|cause| cause.change_context(invalid_value(path, "branch name")))?;
            let Some(epoch) = epochs.get(&name) else {
                return Err(invalid(path, "holding branch"));
            };
            if epoch.transition.is_none() {
                return Err(invalid(path, "holding transition"));
            }
            let mut held = BTreeSet::new();
            for unit in units {
                let unit = unit.into();
                if epoch.partitions.schedules(unit) || !held.insert(unit) {
                    return Err(invalid(path, "holding unit"));
                }
            }
            if holding_units.insert(name, held).is_some() {
                return Err(invalid(path, "holding branch"));
            }
        }
        let mut intake_units = BTreeMap::new();
        for (name, count) in wire.intake_units {
            let name = IngestorName::parse(&name)
                .map_err(|cause| cause.change_context(invalid_value(path, "ingestor name")))?;
            let count = NonZeroU16::new(count).ok_or_else(|| invalid(path, "intake unit count"))?;
            if intake_units.insert(name, count).is_some() {
                return Err(invalid(path, "intake unit"));
            }
        }
        Ok(Self {
            domain,
            snapshot: PartitionTopologySnapshot {
                topology,
                epochs,
                holding_units,
                intake_units,
            },
        })
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests remain outside product execution")
)]
pub(super) mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_arbitrary::{Arbitrary, Domain};

    use super::*;

    pub(crate) fn generated(
        arbitrary: &mut Arbitrary<'_>,
        domain: DomainName,
    ) -> PartitionTopologyRecord {
        let ingestor = IngestorName::parse("input").assured("the fixture name is valid");
        let branch = BranchName::parse("by_tenant").assured("the fixture name is valid");
        let count = arbitrary.entropy().pick([
            1,
            12,
            16,
            u32::from(u16::MAX),
            u32::from(u16::MAX) + 1,
            i32::MAX.cast_unsigned() + 1,
        ]);
        let grouped = count > u32::from(u16::MAX) || arbitrary.entropy().flag();
        let groups = NonZeroU16::new(1 + u16::from(arbitrary.entropy().byte() % 16))
            .assured("the draw is positive");
        let layout = if grouped {
            InheritedPartitionLayout::Grouped {
                source_count: NonZeroU32::new(count).assured("fixture counts are positive"),
                groups,
            }
        } else {
            InheritedPartitionLayout::Identity(
                NonZeroU16::new(u16::try_from(count).assured("the identity count fits"))
                    .assured("fixture counts are positive"),
            )
        };
        let pending = arbitrary.entropy().flag();
        let observed = if pending {
            None
        } else if count == 1 && arbitrary.entropy().flag() {
            Some(ExternalPartitionSet::Unpartitioned)
        } else {
            Some(ExternalPartitionSet::Partitioned(
                NonZeroU32::new(count).assured("fixture counts are positive"),
            ))
        };
        let activated = !pending || arbitrary.entropy().flag();
        let mut digest = [0; 32];
        for byte in &mut digest {
            *byte = arbitrary.entropy().byte();
        }
        let epoch = BranchPartitionEpoch {
            number: PartitionEpochNumber::new(arbitrary.positive_u64()),
            partitions: layout.partitions(),
            transition: arbitrary.entropy().flag().then_some(PartitionTransition {
                previous: ResolvedBranchPartitions::Direct(
                    NonZeroU16::new(16).assured("sixteen is positive"),
                ),
            }),
        };
        let holding = epoch.holding_units();
        PartitionTopologyRecord {
            domain,
            snapshot: PartitionTopologySnapshot {
                topology: PartitionTopology {
                    sources: BTreeMap::from([(
                        ingestor.clone(),
                        SourcePartitionObservation {
                            configuration: PartitionSourceFingerprint::from_digest(digest),
                            partitions: observed,
                            epoch: arbitrary.entropy().any_u64(),
                        },
                    )]),
                    branches: if activated {
                        BTreeMap::from([(branch.clone(), layout)])
                    } else {
                        BTreeMap::new()
                    },
                    suspended: if pending {
                        BTreeSet::from([ingestor.clone()])
                    } else {
                        BTreeSet::new()
                    },
                },
                epochs: if activated {
                    BTreeMap::from([(branch.clone(), epoch)])
                } else {
                    BTreeMap::new()
                },
                holding_units: if activated && !holding.is_empty() {
                    BTreeMap::from([(branch, holding.into_iter().collect())])
                } else {
                    BTreeMap::new()
                },
                intake_units: if activated {
                    BTreeMap::from([(ingestor, groups)])
                } else {
                    BTreeMap::new()
                },
            },
        }
    }

    #[test]
    fn bolero_partition_topology_preserves_captured_epochs_and_units() {
        bolero::check!()
            .with_iterations(128)
            .with_max_len(512)
            .for_each(|bytes| {
                let record = generated(
                    &mut Arbitrary::new(bytes, Domain::Vocabulary),
                    DomainName::parse("orders").assured("valid fixture name"),
                );
                crate::archive_properties::assert_record(&record);
            });
    }

    #[test]
    fn a_zero_count_and_a_duplicate_source_are_typed_current_record_refusals() {
        let record = generated(
            &mut Arbitrary::new(&[255; 512], Domain::Vocabulary),
            DomainName::parse("orders").assured("valid fixture name"),
        );
        let bytes = record.encode().assured("the fixture record encodes");
        let wire: PartitionTopologyWire = decode_record(
            "partitions.rkyv",
            PartitionTopologyRecord::KIND,
            PartitionTopologyRecord::VERSION,
            &bytes,
        )
        .assured("the current record validates");
        let mut duplicate = wire.clone();
        duplicate.sources.push(duplicate.sources[0].clone());
        let encoded = encode_record(
            PartitionTopologyRecord::KIND,
            PartitionTopologyRecord::VERSION,
            &duplicate,
        )
        .assured("the malformed primitive record encodes");
        assert!(matches!(
            PartitionTopologyRecord::decode("partitions.rkyv", &encoded)
                .expect_err("duplicates are refused")
                .current_context(),
            ArchiveReadError::InvalidValue {
                field: "source observation",
                ..
            }
        ));
        let mut empty = wire;
        empty.intake_units.push(("another_input".to_owned(), 0));
        let encoded = encode_record(
            PartitionTopologyRecord::KIND,
            PartitionTopologyRecord::VERSION,
            &empty,
        )
        .assured("the malformed primitive record encodes");
        assert!(matches!(
            PartitionTopologyRecord::decode("partitions.rkyv", &encoded)
                .expect_err("zero intake counts are refused")
                .current_context(),
            ArchiveReadError::InvalidValue {
                field: "intake unit count",
                ..
            }
        ));
    }
}
