//! What one answer of the leader's resource upload path decided.
//!
//! Layer: edges.
//!
//! - **Owns.** How the console reads one answer of `/console/resources/upload`: an installed
//!   version, a refusal that decided nothing, or a final failure.
//! - **Depends on.** HTTP status codes.
//! - **Must not know.** How the files are read, how a request is built, sent or paced, or how the
//!   dialog shows an outcome.
//!
//! The upload path answers `409 Conflict` when the node that received the upload could not propose
//! for it as the leader: another node leads, no leader is known, or the node could not confirm its
//! lease. That refusal decided nothing, and the upload identity makes a repetition safe: the
//! leader continues or reports an upload already recorded under that identity instead of assigning
//! another version. The console therefore sends the same files under the same identity again, as
//! the native client sends an upload again after a redirect or a refusal without a leader. Every
//! other answer is final.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

/// The status of a refusal that decided nothing.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the console upload path's HTTP contract answers 409 Conflict for a node that \
                  could not propose for the upload as the leader"
    )
)]
const NOT_DECIDED_STATUS: u16 = 409;

/// What one answer of the upload path decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UploadAnswer {
    /// The leader installed a version, which the text names.
    Installed(String),
    /// The node that received the upload could not propose for it as the leader, so nothing was
    /// decided. The text says why.
    NotDecided(String),
    /// The upload failed for good. The text says why.
    Failed(String),
}

impl UploadAnswer {
    /// Reads the answer a response with `status` and body `text` gives. An empty body is
    /// described by its status.
    pub(crate) fn of_response(status: u16, text: String) -> Self {
        if (200..300).contains(&status) {
            return Self::Installed(text);
        }
        let reason = if text.is_empty() {
            format!("upload failed with HTTP {status}")
        } else {
            text
        };
        if status == NOT_DECIDED_STATUS {
            return Self::NotDecided(reason);
        }
        Self::Failed(reason)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_successful_answer_names_the_installed_version() {
        let answer = UploadAnswer::of_response(200, "uploaded resource version 3".to_string());
        assert_eq!(
            answer,
            UploadAnswer::Installed("uploaded resource version 3".to_string())
        );
    }

    #[test]
    fn a_conflict_is_a_refusal_that_decided_nothing() {
        let reason = "failed to begin upload for resource 'bundle': this node cannot propose as \
                      the leader: this node leads term 1 but could not confirm its lease";
        let answer = UploadAnswer::of_response(409, reason.to_string());
        assert_eq!(answer, UploadAnswer::NotDecided(reason.to_string()));
        let empty = UploadAnswer::of_response(409, String::new());
        assert_eq!(
            empty,
            UploadAnswer::NotDecided("upload failed with HTTP 409".to_string())
        );
    }

    #[test]
    fn every_other_status_is_a_final_failure() {
        for status in [400, 401, 404, 413, 500, 503] {
            let answer = UploadAnswer::of_response(status, "refused".to_string());
            assert_eq!(answer, UploadAnswer::Failed("refused".to_string()));
            let empty = UploadAnswer::of_response(status, String::new());
            assert_eq!(
                empty,
                UploadAnswer::Failed(format!("upload failed with HTTP {status}"))
            );
        }
    }
}
