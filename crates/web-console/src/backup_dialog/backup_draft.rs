//! The backup form's draft and the `BACKUP` Model it lowers to.

use error_stack::Report;
use nervix_models::{
    Backup, BackupCapture, BackupResources, BackupScope, DomainName, parse_duration_text,
};
use thiserror::Error;

/// The file name the form proposes for an archive until the operator names one.
const DEFAULT_DESTINATION: &str = "nervix-backup.nvxb";

/// What a backup covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackupScopeChoice {
    /// Every user and every domain.
    Cluster,
    /// One domain.
    Domain,
}

/// How a backup captures runtime state, as the form offers it. A quiesced capture takes its drain
/// timeout from the draft's own text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CaptureChoice {
    /// Pause and drain each running domain before its state is read.
    Quiesced,
    /// `WITHOUT PAUSE`: read the latest published checkpoints while domains keep running.
    Live,
    /// `WITHOUT STATE`: configuration only.
    ConfigurationOnly,
}

/// The backup form as the operator fills it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackupDraft {
    pub(crate) scope: BackupScopeChoice,
    /// The domain a domain backup covers, as typed or chosen.
    pub(crate) domain: String,
    /// The name the browser saves the archive under, which the statement names as its file.
    pub(crate) destination: String,
    pub(crate) resources: BackupResources,
    pub(crate) capture: CaptureChoice,
    /// A quiesced capture's drain timeout as NSPL duration text; empty keeps the domain's own.
    pub(crate) timeout: String,
}

/// Why a backup draft does not lower to a statement.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum BackupDraftError {
    #[error("Choose the domain to back up")]
    MissingDomain,
    #[error("'{text}' is not a domain name")]
    InvalidDomain { text: String },
    #[error("Name the archive file")]
    MissingDestination,
    #[error("'{text}' is not a duration such as 30s")]
    InvalidTimeout { text: String },
}

impl BackupDraft {
    /// A draft backing up `domain` with every default, or the cluster when no domain is selected.
    pub(crate) fn for_domain(domain: Option<&DomainName>) -> Self {
        let (scope, domain) = match domain {
            Some(domain) => (BackupScopeChoice::Domain, domain.to_string()),
            None => (BackupScopeChoice::Cluster, String::new()),
        };
        Self {
            scope,
            domain,
            destination: DEFAULT_DESTINATION.to_string(),
            resources: BackupResources::Included,
            capture: CaptureChoice::Quiesced,
            timeout: String::new(),
        }
    }

    /// The `BACKUP` Model the draft lowers to.
    pub(crate) fn model(&self) -> Result<Backup, Report<BackupDraftError>> {
        let scope = match self.scope {
            BackupScopeChoice::Cluster => BackupScope::Cluster,
            BackupScopeChoice::Domain => {
                let text = self.domain.trim();
                if text.is_empty() {
                    return Err(Report::new(BackupDraftError::MissingDomain));
                }
                let domain = match DomainName::parse(text) {
                    Ok(domain) => domain,
                    Err(error) => {
                        return Err(error.change_context(BackupDraftError::InvalidDomain {
                            text: text.to_string(),
                        }));
                    }
                };
                BackupScope::Domain(Some(domain))
            }
        };
        let destination = self.destination.trim();
        if destination.is_empty() {
            return Err(Report::new(BackupDraftError::MissingDestination));
        }
        let capture = match self.capture {
            CaptureChoice::Quiesced => BackupCapture::Quiesced {
                timeout: self.quiesce_timeout()?,
            },
            CaptureChoice::Live => BackupCapture::Live,
            CaptureChoice::ConfigurationOnly => BackupCapture::ConfigurationOnly,
        };
        Ok(Backup {
            scope,
            destination: destination.to_string(),
            resources: self.resources,
            capture,
        })
    }

    fn quiesce_timeout(&self) -> Result<Option<std::time::Duration>, Report<BackupDraftError>> {
        let text = self.timeout.trim();
        if text.is_empty() {
            return Ok(None);
        }
        match parse_duration_text(text) {
            Ok(timeout) => Ok(Some(timeout)),
            Err(error) => Err(error.change_context(BackupDraftError::InvalidTimeout {
                text: text.to_string(),
            })),
        }
    }
}
