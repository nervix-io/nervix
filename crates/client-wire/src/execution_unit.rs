//! The session representation of shared execution-unit identities.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** One mapping between typed units and their session union.
//! - **Depends on.** The vocabulary and verified wire tables.
//! - **Must not know.** Placement decisions, runtime ownership or inspection behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the boundary declared above")
)]

use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_models::Unit;

use crate::{
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError},
    wire,
};

pub(crate) fn decode_placement_unit(
    decoder: Decoder<'_>,
    kind: wire::PlacementUnit,
    partition: Option<wire::PartitionUnit<'_>>,
    instance: Option<wire::InstanceUnit<'_>>,
    field: &'static str,
) -> Result<Unit, Report<WireDecodeError>> {
    match kind {
        wire::PlacementUnit::SingleUnit => Ok(Unit::SINGLE),
        wire::PlacementUnit::PartitionUnit => Ok(Unit::partition(
            partition
                .assured("verified unit discriminant names its partition")
                .partition(),
        )),
        wire::PlacementUnit::InstanceUnit => Unit::try_instance(
            instance
                .assured("verified unit discriminant names its instance")
                .instance(),
        )
        .map_err(|cause| {
            Report::new(cause).change_context(WireDecodeError::InvalidValue {
                field,
                kind: "execution unit",
            })
        }),
        unknown => Err(decoder.unknown_union(field, unknown.0)),
    }
}

/// The placement unit of a moved node, as the schema's union spells it.
pub(crate) fn encode_placement_unit<'fbb>(
    encoder: &mut Encoder<'fbb>,
    unit: Unit,
) -> EncodedUnion<wire::PlacementUnit> {
    match unit.kind() {
        nervix_models::UnitKind::Single => EncodedUnion::new(
            wire::PlacementUnit::SingleUnit,
            wire::SingleUnit::create(encoder.fbb(), &wire::SingleUnitArgs {}),
        ),
        nervix_models::UnitKind::Partition(partition) => EncodedUnion::new(
            wire::PlacementUnit::PartitionUnit,
            wire::PartitionUnit::create(encoder.fbb(), &wire::PartitionUnitArgs { partition }),
        ),
        nervix_models::UnitKind::Instance(instance) => EncodedUnion::new(
            wire::PlacementUnit::InstanceUnit,
            wire::InstanceUnit::create(encoder.fbb(), &wire::InstanceUnitArgs { instance }),
        ),
    }
}
