//! Complete session-limit announcements in authenticated gRPC response metadata.
//!
//! Layer: edges.
//! - **Owns.** The five required numeric headers and checked conversions in both directions.
//! - **Depends on.** Checked session limits and tonic metadata.
//! - **Must not know.** Authentication, HTTP bodies, dispatch or client recovery.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the gRPC session opening metadata boundary")
)]

use std::num::NonZeroUsize;

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use tonic::metadata::MetadataMap;

use crate::{SessionLimitSettings, SessionLimits};

const FRAME: &str = "nervix-session-frame-bytes";
const TRANSFER: &str = "nervix-session-transfer-bytes";
const DEPTH: &str = "nervix-session-nesting-depth";
const ENTRIES: &str = "nervix-session-collection-entries";
const STRING: &str = "nervix-session-string-bytes";
const CHANGED: &str = "nervix-session-bounds-changed";

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SessionAnnouncementError {
    #[error("session opening metadata is missing {field}")]
    Missing { field: &'static str },
    #[error("session opening metadata repeats {field}")]
    Repeated { field: &'static str },
    #[error("session opening metadata {field} must be a positive decimal platform-sized integer")]
    Number { field: &'static str },
    #[error("session opening metadata has inconsistent limits")]
    Limits,
    #[error("the session-bound refusal marker must occur once with value 1")]
    RefusalMarker,
    #[error("the peer accepted offered bounds {offered:?} but announced {announced:?}")]
    OfferMismatch {
        offered: SessionLimits,
        announced: SessionLimits,
    },
}

/// Typed provenance retained when a foreign gRPC API carries an opening refusal as a status.
#[derive(Debug)]
pub struct SessionOpeningRejection {
    report: Report<SessionAnnouncementError>,
}

impl SessionOpeningRejection {
    pub fn into_status(report: Report<SessionAnnouncementError>) -> tonic::Status {
        Self::with_code(report, tonic::Code::Internal)
    }

    /// A companion may already have handed its body over before detecting a malformed opening.
    pub fn with_possible_admission(report: Report<SessionAnnouncementError>) -> tonic::Status {
        Self::with_code(report, tonic::Code::Unknown)
    }

    fn with_code(report: Report<SessionAnnouncementError>, code: tonic::Code) -> tonic::Status {
        let status = tonic::Status::new(code, format!("{report:#}"));
        Self::attach(report, status)
    }

    /// Retains opening provenance while preserving an existing transport status and metadata.
    pub fn attach(
        report: Report<SessionAnnouncementError>,
        mut status: tonic::Status,
    ) -> tonic::Status {
        status.set_source(nervix_primitives::sync::StdArc::new(Self { report }));
        status
    }

    pub fn report(&self) -> &Report<SessionAnnouncementError> {
        &self.report
    }
}

impl std::fmt::Display for SessionOpeningRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:#}", self.report)
    }
}

impl std::error::Error for SessionOpeningRejection {}

pub fn write(metadata: &mut MetadataMap, limits: SessionLimits) {
    for (field, value) in [
        (FRAME, limits.frame_bytes()),
        (TRANSFER, limits.transfer_bytes()),
        (DEPTH, limits.nesting_depth()),
        (ENTRIES, limits.collection_entries()),
        (STRING, limits.string_bytes()),
    ] {
        metadata.insert(
            field,
            value
                .to_string()
                .parse()
                .assured("a positive size renders as valid ASCII metadata"),
        );
    }
}

pub fn read(metadata: &MetadataMap) -> Result<SessionLimits, Report<SessionAnnouncementError>> {
    SessionLimits::try_from(SessionLimitSettings {
        frame_bytes: number(metadata, FRAME)?,
        transfer_bytes: number(metadata, TRANSFER)?,
        nesting_depth: number(metadata, DEPTH)?,
        collection_entries: number(metadata, ENTRIES)?,
        string_bytes: number(metadata, STRING)?,
    })
    .change_context(SessionAnnouncementError::Limits)
}

/// Before reading an upload or restore body, confirm the complete bounds the sender offered.
/// A mismatch proves this call admitted no input and returns the current bounds for another call.
pub fn offer_refusal(metadata: &MetadataMap, actual: SessionLimits) -> Option<tonic::Status> {
    let offered = match read(metadata) {
        Ok(offered) => offered,
        Err(report) => {
            return Some(SessionOpeningRejection::with_code(
                report,
                tonic::Code::InvalidArgument,
            ));
        }
    };
    if offered == actual {
        return None;
    }
    let mut status = tonic::Status::failed_precondition("the offered session bounds changed");
    write(status.metadata_mut(), actual);
    status.metadata_mut().insert(
        CHANGED,
        "1".parse()
            .assured("the fixed refusal marker is ASCII metadata"),
    );
    Some(status)
}

/// A well-formed offer refusal is an ordinary pre-admission outcome, distinct from transport
/// failures and from a malformed refusal that cannot establish the call's disposition.
pub fn changed(
    status: &tonic::Status,
) -> Result<Option<SessionLimits>, Report<SessionAnnouncementError>> {
    if status.code() != tonic::Code::FailedPrecondition {
        return Ok(None);
    }
    let values = status.metadata().get_all(CHANGED);
    let mut values = values.iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() || value != "1" {
        return Err(Report::new(SessionAnnouncementError::RefusalMarker));
    }
    read(status.metadata()).map(Some)
}

fn number(
    metadata: &MetadataMap,
    field: &'static str,
) -> Result<NonZeroUsize, Report<SessionAnnouncementError>> {
    let values = metadata.get_all(field);
    let mut values = values.iter();
    let value = values
        .next()
        .ok_or_else(|| Report::new(SessionAnnouncementError::Missing { field }))?;
    if values.next().is_some() {
        return Err(Report::new(SessionAnnouncementError::Repeated { field }));
    }
    let value = value
        .to_str()
        .map_err(Report::new)
        .change_context(SessionAnnouncementError::Number { field })?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Report::new(SessionAnnouncementError::Number { field }));
    }
    value
        .parse()
        .map_err(Report::new)
        .change_context(SessionAnnouncementError::Number { field })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn announcement() -> MetadataMap {
        let mut metadata = MetadataMap::new();
        write(&mut metadata, SessionLimits::DEFAULT);
        metadata
    }

    #[test]
    fn attaching_opening_provenance_preserves_the_complete_transport_status() {
        let report = Report::new(SessionAnnouncementError::Missing { field: FRAME });
        let mut original = tonic::Status::unavailable("HTTP 503 before a session opened");
        original
            .metadata_mut()
            .insert("retry-after", "3".parse().expect("ASCII metadata"));
        let status = SessionOpeningRejection::attach(report, original);
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(status.message(), "HTTP 503 before a session opened");
        assert_eq!(
            status
                .metadata()
                .get("retry-after")
                .expect("retained metadata"),
            "3"
        );
        let cause = std::error::Error::source(&status)
            .expect("the opening provenance survives the foreign boundary")
            .downcast_ref::<SessionOpeningRejection>()
            .expect("the source keeps its typed opening report");
        assert_eq!(
            cause.report().current_context(),
            &SessionAnnouncementError::Missing { field: FRAME }
        );
    }

    #[test]
    fn an_offer_is_checked_before_input_and_its_refusal_keeps_all_current_bounds() {
        let current = SessionLimits::from(
            &nervix_models::SessionBounds::resolve(nervix_models::SessionBoundValues {
                frame_bytes: std::num::NonZeroU64::new(2048),
                transfer_bytes: std::num::NonZeroU64::new(2 * 1024 * 1024),
                nesting_depth: std::num::NonZeroU64::new(32),
                collection_entries: std::num::NonZeroU64::new(8192),
                string_bytes: std::num::NonZeroU64::new(1024 * 1024),
            })
            .expect("the complete current bounds validate"),
        );
        let mut offered = announcement();
        let refusal = offer_refusal(&offered, current).expect("the bounds differ");
        assert_eq!(
            changed(&refusal).expect("the complete refusal decodes"),
            Some(current)
        );
        write(&mut offered, current);
        assert!(offer_refusal(&offered, current).is_none());
        assert_eq!(
            changed(&tonic::Status::permission_denied("refused"))
                .expect("a different refusal has no bound update"),
            None
        );
        for field in [FRAME, TRANSFER, DEPTH, ENTRIES, STRING] {
            let mut incomplete = refusal.clone();
            incomplete.metadata_mut().remove(field);
            assert!(changed(&incomplete).is_err(), "a refusal requires {field}");
        }
        let mut repeated = refusal.clone();
        repeated
            .metadata_mut()
            .append(CHANGED, "1".parse().expect("ASCII marker"));
        assert!(matches!(
            changed(&repeated)
                .expect_err("the marker is ambiguous")
                .current_context(),
            SessionAnnouncementError::RefusalMarker
        ));
    }

    #[test]
    fn each_numeric_dimension_is_required_exactly_once() {
        for field in [FRAME, TRANSFER, DEPTH, ENTRIES, STRING] {
            let mut missing = announcement();
            missing.remove(field);
            assert_eq!(
                read(&missing)
                    .expect_err("a damaged current announcement lost one dimension")
                    .current_context(),
                &SessionAnnouncementError::Missing { field }
            );
            let mut repeated = announcement();
            repeated.append(field, "1".parse().expect("ASCII integer"));
            assert_eq!(
                read(&repeated)
                    .expect_err("a dimension is ambiguous")
                    .current_context(),
                &SessionAnnouncementError::Repeated { field }
            );
        }
    }

    #[test]
    fn malformed_current_numeric_fields_keep_a_typed_refusal() {
        for text in ["0", "", "-1", "+1", " 1", "184467440737095516160"] {
            let mut metadata = announcement();
            metadata.insert(
                ENTRIES,
                text.parse().expect("the mutation is valid ASCII metadata"),
            );
            assert_eq!(
                read(&metadata)
                    .expect_err("the scalar is not a supported positive decimal size")
                    .current_context(),
                &SessionAnnouncementError::Number { field: ENTRIES }
            );
        }
    }

    #[test]
    fn inconsistent_dimensions_preserve_the_bound_owners_report() {
        let mut metadata = announcement();
        metadata.insert(TRANSFER, "1024".parse().expect("ASCII integer"));
        let error = read(&metadata).expect_err("a transfer must hold a frame");
        assert_eq!(error.current_context(), &SessionAnnouncementError::Limits);
        assert!(matches!(
            error.downcast_ref::<crate::LimitsError>(),
            Some(crate::LimitsError::TransferBytesOutOfRange { .. })
        ));
    }
}
