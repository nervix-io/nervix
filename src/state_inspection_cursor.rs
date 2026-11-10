//! Opaque continuations for one domain-owned state inspection.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Canonical cursor encoding and identity checks for a captured read.
//! - **Depends on.** Typed names and fingerprints, serde JSON and maintained base64 encoding.
//! - **Must not know.** Schedules, rows, state storage, sessions or how generations are captured.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the boundary declared above")
)]

use error_stack::{Report, ResultExt as _};
use nervix_models::{
    DomainName, NodeRef, SchemaFingerprint, StateInspectionBranchSelection, StateInspectionPosition,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StateInspectionCursor {
    pub(crate) domain: DomainName,
    /// The node whose state the read inspects: a relay's materialized state, or the rows a
    /// reorderer or correlator holds.
    pub(crate) entity: NodeRef,
    pub(crate) schema: SchemaFingerprint,
    pub(crate) control_revision: u64,
    pub(crate) generations: [u8; 32],
    pub(crate) selection: StateInspectionBranchSelection,
    pub(crate) position: StateInspectionPosition,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StateInspectionCursorError {
    #[error("the inspection continuation exceeds its {limit}-byte bound")]
    TooLarge { limit: usize },
    #[error("the inspection continuation is not canonical base64")]
    Base64,
    #[error("the inspection continuation does not contain the current cursor shape")]
    Shape,
    #[error("the inspection continuation is not canonically encoded")]
    NonCanonical,
}

impl StateInspectionCursor {
    pub(crate) fn encode(
        &self,
        limit: usize,
    ) -> Result<String, Report<StateInspectionCursorError>> {
        let bytes = serde_json::to_vec(self).change_context(StateInspectionCursorError::Shape)?;
        let token = base64_simd::URL_SAFE_NO_PAD.encode_to_string(&bytes);
        if token.len() > limit {
            return Err(Report::new(StateInspectionCursorError::TooLarge { limit }));
        }
        Ok(token)
    }

    pub(crate) fn decode(
        token: &str,
        limit: usize,
    ) -> Result<Self, Report<StateInspectionCursorError>> {
        if token.len() > limit {
            return Err(Report::new(StateInspectionCursorError::TooLarge { limit }));
        }
        let bytes = base64_simd::URL_SAFE_NO_PAD
            .decode_to_vec(token)
            .change_context(StateInspectionCursorError::Base64)?;
        let cursor: Self =
            serde_json::from_slice(&bytes).change_context(StateInspectionCursorError::Shape)?;
        let canonical = cursor.encode(limit)?;
        if canonical != token {
            return Err(Report::new(StateInspectionCursorError::NonCanonical));
        }
        Ok(cursor)
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{BranchKeyFingerprint, ModelKind, ModelName, RelayName, Unit};

    use super::*;

    fn cursor() -> StateInspectionCursor {
        StateInspectionCursor {
            domain: DomainName::parse("sales").assured("valid domain"),
            entity: NodeRef::new(
                ModelKind::Relay,
                RelayName::parse("events").assured("valid relay"),
            ),
            schema: SchemaFingerprint::from_digest([7; 32]),
            control_revision: 31,
            generations: [13; 32],
            selection: StateInspectionBranchSelection::Branch(BranchKeyFingerprint::new([29; 32])),
            position: StateInspectionPosition::Rows {
                unit: Unit::partition(3),
                branch: Some(BranchKeyFingerprint::new([31; 32])),
                offset: u64::MAX,
            },
        }
    }

    #[test]
    fn cursor_keeps_the_entire_identity_and_position_and_checks_its_bound() {
        let original = cursor();
        let token = original.encode(4096).assured("the fixed shape fits");
        assert_eq!(
            StateInspectionCursor::decode(&token, token.len())
                .assured("the exact bound is inclusive"),
            original
        );
        assert!(StateInspectionCursor::decode(&token, token.len() - 1).is_err());
        assert!(original.encode(token.len() - 1).is_err());
        assert!(StateInspectionCursor::decode(&(token + "="), 4096).is_err());
    }

    #[test]
    fn bolero_state_inspection_cursor_round_trips() {
        bolero::check!()
            .with_iterations(128)
            .with_max_len(512)
            .for_each(|bytes: &[u8]| {
                let mut arbitrary =
                    nervix_arbitrary::Arbitrary::new(bytes, nervix_arbitrary::Domain::Vocabulary);
                let mut original = cursor();
                original.domain = arbitrary.name();
                let kind = match arbitrary.entropy().byte() % 6 {
                    0 => ModelKind::Relay,
                    1 => ModelKind::Reorderer,
                    2 => ModelKind::Correlator,
                    3 => ModelKind::Deduplicator,
                    4 => ModelKind::WindowProcessor,
                    _ => ModelKind::Inferencer,
                };
                original.entity = NodeRef::new(kind, arbitrary.name::<ModelName>());
                original.schema = SchemaFingerprint::from_digest(std::array::from_fn(|_| {
                    arbitrary.entropy().byte()
                }));
                original.control_revision = arbitrary.positive_u64().get();
                original.generations = std::array::from_fn(|_| arbitrary.entropy().byte());
                let fingerprint =
                    BranchKeyFingerprint::new(std::array::from_fn(|_| arbitrary.entropy().byte()));
                original.selection = match arbitrary.entropy().byte() % 3 {
                    0 => StateInspectionBranchSelection::All,
                    1 => StateInspectionBranchSelection::Unbranched,
                    _ => StateInspectionBranchSelection::Branch(fingerprint),
                };
                original.position = if arbitrary.entropy().flag() {
                    StateInspectionPosition::AfterUnit(Unit::partition(u32::from(
                        arbitrary.entropy().byte(),
                    )))
                } else {
                    StateInspectionPosition::Rows {
                        unit: Unit::SINGLE,
                        branch: arbitrary.entropy().flag().then_some(fingerprint),
                        offset: arbitrary.positive_u64().get(),
                    }
                };
                let limit = nervix_client_wire::SessionLimits::DEFAULT.string_bytes();
                let token = original
                    .encode(limit)
                    .assured("a bounded generated cursor fits");
                let decoded = StateInspectionCursor::decode(&token, limit)
                    .assured("the current canonical shape decodes");
                assert_eq!(decoded, original);
                assert_eq!(
                    decoded.encode(limit).assured("decoded cursor encodes"),
                    token
                );
            });
    }
}
