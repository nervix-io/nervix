//! Semantic questions shared by text completion and typed choice controls.
//!
//! Layer: vocabulary.
//! - **Owns.** The kind of entity or value a completion candidate denotes, and which codecs and
//!   relays a reference position accepts: the one rule text completion and the typed choices
//!   both apply to each candidate.
//! - **Depends on.** Model identity vocabulary and the codec and relay Models.
//! - **Must not know.** Parser labels, source spans, registries, sessions or presentation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use crate::{
    BranchName, CodecWireFormat, CreateRelay, EmitterEntityKind, IngestorEntityKind,
    IngestorReferenceKind, ModelKind, RelayBranching, RelayName, SchemaName, WireSchemaName,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BuiltinFunctionScope {
    Ordinary,
    IngestorEntity(IngestorEntityKind),
    EmitterInvocation(EmitterEntityKind),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SemanticReference {
    Model(ModelKind),
    /// The client or endpoint an ingestor's body reads through, which the pair its head names
    /// narrows to one connector or one endpoint type.
    IngestorReference(IngestorReferenceKind),
    /// A codec a clause uses in one direction.
    Codec(CodecDirection),
    /// A relay that has to agree with relays its statement already names.
    Relay(RelayQuestion),
    SchemaField(SchemaName),
    WireSchemaField(ModelKind, WireSchemaName),
    RelayField(RelayName),
    BuiltinFunction(BuiltinFunctionScope),
    Resource,
    ResourceVersion,
    CompletedResourceVersion,
    SessionSubscription,
    RuntimeNode,
    Domain,
}

/// The direction a clause uses the codec it names in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CodecDirection {
    /// `DECODE USING`: the codec turns incoming payloads into records.
    Decoding,
    /// `ENCODE USING`: the codec turns records into outgoing payloads.
    Encoding,
}

impl CodecDirection {
    /// Whether a codec of `format` works in this direction, by the format's own rule, which the
    /// clause's validation applies too.
    pub fn admits<Version>(self, format: &CodecWireFormat<Version>) -> bool {
        match self {
            Self::Decoding => format.supports_decoding(),
            Self::Encoding => format.supports_encoding(),
        }
    }
}

/// Which relays a relay reference accepts, given the relays its statement already names.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RelayQuestion {
    /// A processor's further input beside its `first` input, or a correlator's timeout relay
    /// beside the `first` input of its side: the same schema on exactly the same branching.
    ProcessorInput { first: RelayName },
    /// The output or error relay of a processor that keeps the branch of `source`, its first input
    /// or a generator's materialized relay: exactly that branching.
    ProcessorBranch { source: RelayName },
    /// Materialized state a processor reads beside its `first` input: a relay with materialized
    /// state on exactly that input's branching.
    ProcessorState { first: RelayName },
    /// An emitter's further input beside its `first` input: the same schema on any branching.
    EmitterInput { first: RelayName },
    /// Materialized state an emitter reads: a relay with materialized state on the one branching
    /// all its `inputs` share.
    EmitterState { inputs: Vec<RelayName> },
    /// An emitter's error relay: the one branching all its `inputs` share.
    EmitterError { inputs: Vec<RelayName> },
    /// A relay on exactly `branch`, or an unbranched one for `None`: the relay of an ingestor
    /// route on a chosen branch, and an ingestor's error relay, which is never branched.
    OnBranch(Option<BranchName>),
    /// A generator's materialized relay: a relay with materialized state on exactly `branch`, or
    /// an unbranched one for `None`.
    MaterializedOnBranch(Option<BranchName>),
}

impl RelayQuestion {
    /// The rule this question applies, with the relays it names read through `configured`.
    /// `None` when one of them is not configured, so no relay can agree with it.
    pub fn rule<'a>(
        &'a self,
        configured: impl Fn(&RelayName) -> Option<&'a CreateRelay>,
    ) -> Option<RelayRule<'a>> {
        let rule = match self {
            Self::ProcessorInput { first } => RelayRule::ProcessorInput {
                first: configured(first)?,
            },
            Self::ProcessorBranch { source } => RelayRule::ProcessorBranch {
                source: configured(source)?,
            },
            Self::ProcessorState { first } => RelayRule::ProcessorState {
                first: configured(first)?,
            },
            Self::EmitterInput { first } => RelayRule::EmitterInput {
                first: configured(first)?,
            },
            Self::EmitterState { inputs } => RelayRule::EmitterState {
                inputs: configured_relays(inputs, &configured)?,
            },
            Self::EmitterError { inputs } => RelayRule::EmitterError {
                inputs: configured_relays(inputs, &configured)?,
            },
            Self::OnBranch(branch) => RelayRule::OnBranch(branch.as_ref()),
            Self::MaterializedOnBranch(branch) => RelayRule::MaterializedOnBranch(branch.as_ref()),
        };
        Some(rule)
    }
}

/// Every relay of `names` read through `configured`, in order, or `None` when one is missing.
fn configured_relays<'a>(
    names: &[RelayName],
    configured: &impl Fn(&RelayName) -> Option<&'a CreateRelay>,
) -> Option<Vec<&'a CreateRelay>> {
    let mut relays = Vec::with_capacity(names.len());
    for name in names {
        relays.push(configured(name)?);
    }
    Some(relays)
}

/// A relay question with the relays it names read from one configuration: the one rule text
/// completion and the typed choices both apply to every candidate relay.
#[derive(Debug, Clone)]
pub enum RelayRule<'a> {
    /// [`RelayQuestion::ProcessorInput`] beside the configured `first` input.
    ProcessorInput { first: &'a CreateRelay },
    /// [`RelayQuestion::ProcessorBranch`] beside the configured `source`.
    ProcessorBranch { source: &'a CreateRelay },
    /// [`RelayQuestion::ProcessorState`] beside the configured `first` input.
    ProcessorState { first: &'a CreateRelay },
    /// [`RelayQuestion::EmitterInput`] beside the configured `first` input.
    EmitterInput { first: &'a CreateRelay },
    /// [`RelayQuestion::EmitterState`] beside every configured input.
    EmitterState { inputs: Vec<&'a CreateRelay> },
    /// [`RelayQuestion::EmitterError`] beside every configured input.
    EmitterError { inputs: Vec<&'a CreateRelay> },
    /// [`RelayQuestion::OnBranch`].
    OnBranch(Option<&'a BranchName>),
    /// [`RelayQuestion::MaterializedOnBranch`].
    MaterializedOnBranch(Option<&'a BranchName>),
}

impl<'a> RelayRule<'a> {
    /// Whether the position accepts `candidate`.
    pub fn admits(&self, candidate: &CreateRelay) -> bool {
        match self {
            Self::ProcessorInput { first } => {
                first.schema == candidate.schema && first.branching == candidate.branching
            }
            Self::ProcessorBranch { source } => source.branching == candidate.branching,
            Self::ProcessorState { first } => {
                candidate.materialized_state.is_some() && first.branching == candidate.branching
            }
            Self::EmitterInput { first } => first.schema == candidate.schema,
            Self::EmitterState { inputs } => {
                candidate.materialized_state.is_some()
                    && Self::shared_branching(inputs) == Some(&candidate.branching)
            }
            Self::EmitterError { inputs } => {
                Self::shared_branching(inputs) == Some(&candidate.branching)
            }
            Self::OnBranch(branch) => candidate.branching.branch() == *branch,
            Self::MaterializedOnBranch(branch) => {
                candidate.materialized_state.is_some() && candidate.branching.branch() == *branch
            }
        }
    }

    /// The configured relays the rule compares candidates with, in the order its question names
    /// them.
    pub fn named(&self) -> Vec<&'a CreateRelay> {
        match self {
            Self::ProcessorInput { first }
            | Self::ProcessorState { first }
            | Self::EmitterInput { first } => vec![*first],
            Self::ProcessorBranch { source } => vec![*source],
            Self::EmitterState { inputs } | Self::EmitterError { inputs } => inputs.clone(),
            Self::OnBranch(_) | Self::MaterializedOnBranch(_) => Vec::new(),
        }
    }

    /// The branching every relay of `inputs` shares, when they share one.
    fn shared_branching(inputs: &[&'a CreateRelay]) -> Option<&'a RelayBranching> {
        let (first, rest) = inputs.split_first()?;
        for input in rest {
            if input.branching != first.branching {
                return None;
            }
        }
        Some(&first.branching)
    }
}

#[cfg(test)]
mod tests;
