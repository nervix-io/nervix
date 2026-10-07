//! The restore form's draft and the `RESTORE` Model it lowers to.

use error_stack::Report;
use nervix_models::{
    DomainName, ExistingUserPolicy, Restore, RestoreLifecycle, RestoreMode, RestoreScope,
    RestoreState,
};
use thiserror::Error;

/// What a restore recreates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum RestoreScopeChoice {
    /// Every user and every domain of a cluster archive.
    Cluster,
    /// One domain of an archive.
    #[default]
    Domain,
}

/// The restore form as the operator fills it. The archive is chosen apart from the draft, because
/// the browser holds the file; the draft only names it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct RestoreDraft {
    pub(crate) scope: RestoreScopeChoice,
    /// What a cluster restore does with an archived user the cluster already has.
    pub(crate) existing_users: ExistingUserPolicy,
    /// The archived domain a domain restore reads, as typed.
    pub(crate) domain: String,
    /// `AS <new_name>` as typed; empty restores the domain under its archived name.
    pub(crate) target: String,
    pub(crate) lifecycle: RestoreLifecycle,
    pub(crate) state: RestoreState,
}

/// Why a restore draft does not lower to a statement.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum RestoreDraftError {
    #[error("Choose the archive to restore")]
    MissingArchive,
    #[error("Name the archived domain to restore")]
    MissingDomain,
    #[error("'{text}' is not a domain name")]
    InvalidDomain { text: String },
    #[error("'{text}' is not a domain name to restore under")]
    InvalidTarget { text: String },
}

impl RestoreDraft {
    /// The draft a typed `RESTORE` fills, whose archive the operator still chooses.
    pub(crate) fn of_model(restore: &Restore) -> Self {
        let mut draft = Self {
            lifecycle: restore.lifecycle,
            state: restore.state,
            ..Self::default()
        };
        match &restore.scope {
            RestoreScope::Cluster { existing_users } => {
                draft.scope = RestoreScopeChoice::Cluster;
                draft.existing_users = *existing_users;
            }
            RestoreScope::Domain { domain, target } => {
                draft.scope = RestoreScopeChoice::Domain;
                draft.domain = domain.to_string();
                if let Some(target) = target {
                    draft.target = target.to_string();
                }
            }
        }
        draft
    }

    /// The `RESTORE` Model the draft lowers to in `mode`, reading the archive named `source`.
    pub(crate) fn model(
        &self,
        source: Option<&str>,
        mode: RestoreMode,
    ) -> Result<Restore, Report<RestoreDraftError>> {
        let Some(source) = source else {
            return Err(Report::new(RestoreDraftError::MissingArchive));
        };
        let scope = match self.scope {
            RestoreScopeChoice::Cluster => RestoreScope::Cluster {
                existing_users: self.existing_users,
            },
            RestoreScopeChoice::Domain => RestoreScope::Domain {
                domain: self.archived_domain()?,
                target: self.restored_name()?,
            },
        };
        Ok(Restore {
            scope,
            source: source.to_string(),
            mode,
            state: self.state,
            lifecycle: self.lifecycle,
        })
    }

    fn archived_domain(&self) -> Result<DomainName, Report<RestoreDraftError>> {
        let text = self.domain.trim();
        if text.is_empty() {
            return Err(Report::new(RestoreDraftError::MissingDomain));
        }
        match DomainName::parse(text) {
            Ok(domain) => Ok(domain),
            Err(error) => Err(error.change_context(RestoreDraftError::InvalidDomain {
                text: text.to_string(),
            })),
        }
    }

    fn restored_name(&self) -> Result<Option<DomainName>, Report<RestoreDraftError>> {
        let text = self.target.trim();
        if text.is_empty() {
            return Ok(None);
        }
        match DomainName::parse(text) {
            Ok(target) => Ok(Some(target)),
            Err(error) => Err(error.change_context(RestoreDraftError::InvalidTarget {
                text: text.to_string(),
            })),
        }
    }
}
