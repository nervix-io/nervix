//! Mapping the typed inspection directly to the current FlatBuffers tables.

use bytes::Bytes;
use error_stack::{Report, ResultExt as _};
use flatbuffers::WIPOffset;
use meticulous::OptionExt as _;
use nervix_models::{ClusterNodeIdentity, ClusterNodeIncarnation, DomainNodeRef};

use super::*;
use crate::{
    SessionLimits,
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_enum},
    common::{decode_node_ref, encode_node_ref},
    execution_unit::{decode_placement_unit, encode_placement_unit},
    wire,
};

wire_enum!(ALL_STATE_INSPECTION_STAGES: StateInspectionStage => wire::StateInspectionStage {
    Live, Stored,
});
wire_enum!(ALL_STATE_INSPECTION_REJECTIONS: StateInspectionRejection => wire::StateInspectionRejection {
    UnknownEntity, NotMaterialized, InvalidSelection, InvalidLimit, StaleContinuation,
    OwnerUnavailable, NotReady, Deadline, ReplyTooLarge,
});

impl StateInspection {
    pub(crate) fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::StateInspection<'fbb>>, Report<WireEncodeError>> {
        let entity = match &self.entity {
            StateInspectionEntity::DomainNode(entity) => {
                let domain =
                    encoder.text("DomainNodeInspectionEntity.domain", entity.domain.as_str())?;
                let node = encode_node_ref(encoder, &entity.node)?;
                let entity = wire::DomainNodeInspectionEntity::create(
                    encoder.fbb(),
                    &wire::DomainNodeInspectionEntityArgs {
                        domain: Some(domain),
                        node: Some(node),
                    },
                );
                EncodedUnion::new(
                    wire::StateInspectionEntity::DomainNodeInspectionEntity,
                    entity,
                )
            }
            StateInspectionEntity::ClusterLicense => {
                let entity = wire::ClusterLicenseInspectionEntity::create(
                    encoder.fbb(),
                    &wire::ClusterLicenseInspectionEntityArgs::default(),
                );
                EncodedUnion::new(
                    wire::StateInspectionEntity::ClusterLicenseInspectionEntity,
                    entity,
                )
            }
        };
        let result = match &self.result {
            StateInspectionResult::Page(page) => EncodedUnion::new(
                wire::StateInspectionResult::StateInspectionPage,
                page.encode(encoder)?,
            ),
            StateInspectionResult::Rejected(reason) => {
                let rejected = wire::StateInspectionRefused::create(
                    encoder.fbb(),
                    &wire::StateInspectionRefusedArgs {
                        reason: Some((*reason).into()),
                    },
                );
                EncodedUnion::new(
                    wire::StateInspectionResult::StateInspectionRefused,
                    rejected,
                )
            }
        };
        Ok(wire::StateInspection::create(
            encoder.fbb(),
            &wire::StateInspectionArgs {
                entity_type: entity.discriminant,
                entity: Some(entity.value),
                result_type: result.discriminant,
                result: Some(result.value),
            },
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        inspected: wire::StateInspection<'_>,
        backing: &Bytes,
        limits: &SessionLimits,
    ) -> Result<Self, Report<WireDecodeError>> {
        let entity = match inspected.entity_type() {
            wire::StateInspectionEntity::DomainNodeInspectionEntity => {
                let entity = inspected
                    .entity_as_domain_node_inspection_entity()
                    .assured("the verified union names a domain-node entity");
                let domain = decoder.name("DomainNodeInspectionEntity.domain", entity.domain())?;
                let node = decode_node_ref(decoder, entity.node())?;
                StateInspectionEntity::DomainNode(DomainNodeRef::new(domain, node))
            }
            wire::StateInspectionEntity::ClusterLicenseInspectionEntity => {
                StateInspectionEntity::ClusterLicense
            }
            unknown => return Err(decoder.unknown_union("StateInspection.entity", unknown.0)),
        };
        let result = match inspected.result_type() {
            wire::StateInspectionResult::StateInspectionPage => {
                let page = inspected
                    .result_as_state_inspection_page()
                    .assured("verified union discriminant names the page");
                StateInspectionResult::Page(StateInspectionPage::decode(
                    decoder, page, backing, limits,
                )?)
            }
            wire::StateInspectionResult::StateInspectionRefused => {
                let refused = inspected
                    .result_as_state_inspection_refused()
                    .assured("verified union discriminant names the refusal");
                let reason = decoder
                    .required_enumeration("StateInspectionRefused.reason", refused.reason())?;
                StateInspectionResult::Rejected(reason)
            }
            unknown => return Err(decoder.unknown_union("StateInspection.result", unknown.0)),
        };
        Ok(Self { entity, result })
    }
}

fn encode_counts<'fbb>(
    counts: &StateInspectionCounts,
    encoder: &mut Encoder<'fbb>,
) -> WIPOffset<wire::StateInspectionCounts<'fbb>> {
    wire::StateInspectionCounts::create(
        encoder.fbb(),
        &wire::StateInspectionCountsArgs {
            branches: counts.branches,
            rows: counts.rows,
            bytes: counts.bytes,
        },
    )
}
fn decode_counts(counts: wire::StateInspectionCounts<'_>) -> StateInspectionCounts {
    StateInspectionCounts {
        branches: counts.branches(),
        rows: counts.rows(),
        bytes: counts.bytes(),
    }
}

impl StateInspectionPage {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::StateInspectionPage<'fbb>>, Report<WireEncodeError>> {
        let counts = encode_counts(&self.counts, encoder);
        let units =
            encoder.table_vector("StateInspectionPage.units", &self.units, |unit, encoder| {
                unit.encode(encoder)
            })?;
        let continuation = encoder.optional_text(
            "StateInspectionPage.continuation",
            self.continuation.as_deref(),
        )?;
        let wasm_binding = match &self.wasm_binding {
            Some(binding) => Some(super::wasm::encode_binding(binding, encoder)?),
            None => None,
        };
        Ok(wire::StateInspectionPage::create(
            encoder.fbb(),
            &wire::StateInspectionPageArgs {
                counts: Some(counts),
                unit_count: self.unit_count,
                units: Some(units),
                continuation,
                wasm_binding,
            },
        ))
    }

    fn decode(
        decoder: Decoder<'_>,
        page: wire::StateInspectionPage<'_>,
        backing: &Bytes,
        limits: &SessionLimits,
    ) -> Result<Self, Report<WireDecodeError>> {
        let counts = decode_counts(page.counts());
        let units = decoder.table_vector("StateInspectionPage.units", page.units(), |unit| {
            StateInspectionUnit::decode(decoder, unit, backing, limits)
        })?;
        let continuation = match page.continuation() {
            Some(value) => Some(decoder.text("StateInspectionPage.continuation", value)?),
            None => None,
        };
        let wasm_binding = match page.wasm_binding() {
            Some(binding) => Some(super::wasm::decode_binding(decoder, binding)?),
            None => None,
        };
        Ok(Self {
            counts,
            unit_count: page.unit_count(),
            units,
            continuation,
            wasm_binding,
        })
    }
}

impl StateInspectionUnit {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::StateInspectionUnit<'fbb>>, Report<WireEncodeError>> {
        let unit = encode_placement_unit(encoder, self.unit);
        let owner = encoder.text("StateInspectionUnit.owner", self.owner.as_str())?;
        let mut replicas = Vec::with_capacity(self.replicas.len());
        encoder.entries("StateInspectionUnit.replicas", self.replicas.len())?;
        for replica in &self.replicas {
            replicas.push(encoder.text("StateInspectionUnit.replicas", replica.as_str())?);
        }
        let replicas = encoder.tables("StateInspectionUnit.replicas", &replicas)?;
        let origin = encode_origin(&self.origin, encoder)?;
        let counts = encode_counts(&self.counts, encoder);
        let sections = encoder.table_vector(
            "StateInspectionUnit.sections",
            &self.sections,
            |section, encoder| section.encode(encoder),
        )?;
        let wasm = match &self.wasm {
            Some(state) => Some(super::wasm::encode_unit(state, encoder)?),
            None => None,
        };
        Ok(wire::StateInspectionUnit::create(
            encoder.fbb(),
            &wire::StateInspectionUnitArgs {
                unit_type: unit.discriminant,
                unit: Some(unit.value),
                owner: Some(owner),
                replicas: Some(replicas),
                origin: Some(origin),
                counts: Some(counts),
                sections: Some(sections),
                wasm,
            },
        ))
    }

    fn decode(
        decoder: Decoder<'_>,
        table: wire::StateInspectionUnit<'_>,
        backing: &Bytes,
        limits: &SessionLimits,
    ) -> Result<Self, Report<WireDecodeError>> {
        let unit = decode_placement_unit(
            decoder,
            table.unit_type(),
            table.unit_as_partition_unit(),
            table.unit_as_instance_unit(),
            "StateInspectionUnit.unit",
        )?;
        let owner = decoder.name("StateInspectionUnit.owner", table.owner())?;
        decoder.entries("StateInspectionUnit.replicas", table.replicas().len())?;
        let mut replicas = Vec::with_capacity(table.replicas().len());
        for replica in table.replicas() {
            replicas.push(decoder.name("StateInspectionUnit.replicas", replica)?);
        }
        let origin = decode_origin(decoder, table.origin())?;
        let counts = decode_counts(table.counts());
        let sections = decoder.table_vector(
            "StateInspectionUnit.sections",
            table.sections(),
            |section| StateInspectionSection::decode(decoder, section, backing, limits),
        )?;
        let wasm = match table.wasm() {
            Some(state) => Some(super::wasm::decode_unit(decoder, state)?),
            None => None,
        };
        Ok(Self {
            unit,
            owner,
            replicas,
            origin,
            counts,
            sections,
            wasm,
        })
    }
}

fn encode_origin<'fbb>(
    origin: &StateInspectionOrigin,
    encoder: &mut Encoder<'fbb>,
) -> Result<WIPOffset<wire::StateInspectionOrigin<'fbb>>, Report<WireEncodeError>> {
    let node = encoder.text("StateInspectionOrigin.node", origin.node.node_id().as_str())?;
    let fingerprint = encoder.bytes("StateInspectionOrigin.fingerprint", &origin.fingerprint)?;
    Ok(wire::StateInspectionOrigin::create(
        encoder.fbb(),
        &wire::StateInspectionOriginArgs {
            node: Some(node),
            incarnation: origin.node.incarnation().get(),
            fingerprint: Some(fingerprint),
            generation: origin.generation,
            revision: origin.revision,
            stage: Some(origin.stage.into()),
        },
    ))
}

fn decode_origin(
    decoder: Decoder<'_>,
    origin: wire::StateInspectionOrigin<'_>,
) -> Result<StateInspectionOrigin, Report<WireDecodeError>> {
    let node = decoder.name("StateInspectionOrigin.node", origin.node())?;
    let node = ClusterNodeIdentity::new(node, ClusterNodeIncarnation::new(origin.incarnation()));
    let stage = decoder.required_enumeration("StateInspectionOrigin.stage", origin.stage())?;
    Ok(StateInspectionOrigin {
        node,
        fingerprint: origin.fingerprint().bytes().try_into().map_err(|_| {
            Report::new(WireDecodeError::InvalidValue {
                field: "StateInspectionOrigin.fingerprint",
                kind: "32-byte generation fingerprint",
            })
        })?,
        generation: origin.generation(),
        revision: origin.revision(),
        stage,
    })
}
impl StateInspectionSection {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::StateInspectionSection<'fbb>>, Report<WireEncodeError>> {
        if self.name.is_empty() || !self.valid_watermarks() {
            return Err(Report::new(WireEncodeError::InvalidValue {
                field: "StateInspectionSection",
                kind: "named section with distinct required datetime watermark columns",
            }));
        }
        let name = encoder.text("StateInspectionSection.name", &self.name)?;
        let schema = self.schema.encode(encoder, 8)?;
        let frames = encoder.table_vector(
            "StateInspectionSection.frames",
            &self.frames,
            |frame, encoder| {
                frame
                    .conform(&self.schema)
                    .change_context(WireEncodeError::InvalidValue {
                        field: "StateInspectionSection.frames",
                        kind: "rows conforming to the section schema",
                    })?;
                let data = encoder.bytes("StateRowFrame.data", frame.bytes())?;
                Ok(wire::StateRowFrame::create(
                    encoder.fbb(),
                    &wire::StateRowFrameArgs { data: Some(data) },
                ))
            },
        )?;
        let watermarks = self.watermarks.map(|columns| {
            wire::StateWatermarkColumns::create(
                encoder.fbb(),
                &wire::StateWatermarkColumnsArgs {
                    low: columns.low,
                    high: columns.high,
                },
            )
        });
        Ok(wire::StateInspectionSection::create(
            encoder.fbb(),
            &wire::StateInspectionSectionArgs {
                name: Some(name),
                schema: Some(schema),
                frames: Some(frames),
                omitted_rows: self.omitted_rows,
                watermarks,
            },
        ))
    }

    fn decode(
        decoder: Decoder<'_>,
        section: wire::StateInspectionSection<'_>,
        backing: &Bytes,
        limits: &SessionLimits,
    ) -> Result<Self, Report<WireDecodeError>> {
        let name = decoder.text("StateInspectionSection.name", section.name())?;
        let schema = RowSchema::decode(decoder, section.schema())?;
        let frames =
            decoder.table_vector("StateInspectionSection.frames", section.frames(), |table| {
                let bytes = backing.slice_ref(table.data().bytes());
                let frame = StateRowFrame::from_bytes(bytes, limits)?;
                frame.conform(&schema)?;
                Ok(frame)
            })?;
        let watermarks = section.watermarks().map(|columns| StateWatermarkColumns {
            low: columns.low(),
            high: columns.high(),
        });
        if name.is_empty() || !valid_watermark_columns(&schema, watermarks) {
            return Err(Report::new(WireDecodeError::InvalidValue {
                field: "StateInspectionSection",
                kind: "named section with distinct required datetime watermark columns",
            }));
        }
        Ok(Self {
            name,
            schema,
            frames,
            omitted_rows: section.omitted_rows(),
            watermarks,
        })
    }
}
