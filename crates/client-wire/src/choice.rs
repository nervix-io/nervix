//! Typed semantic choices for structured client controls.
//!
//! Layer: edges.
//!
//! - **Owns.** The session request and reply shapes for typed choice lookup, their bounded paging,
//!   typed values, and presentation metadata.
//! - **Depends on.** The shared wire codec and vocabulary names, node references, and placement
//!   policy.
//! - **Must not know.** Browser draft state, parser expectations, registry snapshots, or how a
//!   server resolves a choice.

use error_stack::Report;
use flatbuffers::WIPOffset;
use nervix_models::{DomainName, NodeRef, PlacementPolicy, ResourceName};

use crate::{
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_enum},
    common::{WireValueError, decode_node_ref, encode_node_ref},
    wire,
};

/// The semantic question a structured control asks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChoiceTarget {
    DomainPace,
    PlacementPolicy,
    Schema,
}

wire_enum!(ALL_CHOICE_TARGETS: ChoiceTarget => wire::ChoiceTarget {
    DomainPace,
    PlacementPolicy,
    Schema,
});

/// Whether a domain clock advances with wall time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DomainPaceChoice {
    Unpaced,
    Paced,
}

wire_enum!(ALL_DOMAIN_PACE_CHOICES: DomainPaceChoice => wire::DomainPaceChoice {
    Unpaced,
    Paced,
});

wire_enum!(ALL_CHOICE_PLACEMENT_POLICIES: PlacementPolicy => wire::PlacementPolicyChoice {
    RequireColocation,
    PreferColocation,
    Neutral,
    SuggestSeparation,
});

/// A semantic selection. Its variant, rather than its display label, decides what it means.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ChoiceValue {
    DomainPace(DomainPaceChoice),
    PlacementPolicy(PlacementPolicy),
    Domain(DomainName),
    Resource(ResourceName),
    Model(NodeRef),
}

impl ChoiceValue {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<EncodedUnion<wire::ChoiceValue>, Report<WireEncodeError>> {
        let encoded = match self {
            Self::DomainPace(value) => EncodedUnion::new(
                wire::ChoiceValue::DomainPaceVariant,
                wire::DomainPaceVariant::create(
                    encoder.fbb(),
                    &wire::DomainPaceVariantArgs {
                        value: Some((*value).into()),
                    },
                ),
            ),
            Self::PlacementPolicy(value) => EncodedUnion::new(
                wire::ChoiceValue::PlacementPolicyVariant,
                wire::PlacementPolicyVariant::create(
                    encoder.fbb(),
                    &wire::PlacementPolicyVariantArgs {
                        value: Some((*value).into()),
                    },
                ),
            ),
            Self::Domain(domain) => {
                let domain = encoder.text("DomainChoiceReference.domain", domain.as_str())?;
                EncodedUnion::new(
                    wire::ChoiceValue::DomainChoiceReference,
                    wire::DomainChoiceReference::create(
                        encoder.fbb(),
                        &wire::DomainChoiceReferenceArgs {
                            domain: Some(domain),
                        },
                    ),
                )
            }
            Self::Resource(resource) => {
                let resource =
                    encoder.text("ResourceChoiceReference.resource", resource.as_str())?;
                EncodedUnion::new(
                    wire::ChoiceValue::ResourceChoiceReference,
                    wire::ResourceChoiceReference::create(
                        encoder.fbb(),
                        &wire::ResourceChoiceReferenceArgs {
                            resource: Some(resource),
                        },
                    ),
                )
            }
            Self::Model(node) => {
                let node = encode_node_ref(encoder, node)?;
                EncodedUnion::new(
                    wire::ChoiceValue::ModelChoiceReference,
                    wire::ModelChoiceReference::create(
                        encoder.fbb(),
                        &wire::ModelChoiceReferenceArgs { node: Some(node) },
                    ),
                )
            }
        };
        Ok(encoded)
    }

    fn decode_selection(
        decoder: Decoder<'_>,
        selection: wire::ChoiceSelection<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        if let Some(value) = selection.value_as_domain_pace_variant() {
            let value = decoder.required_enumeration("DomainPaceVariant.value", value.value())?;
            return Ok(Self::DomainPace(value));
        }
        if let Some(value) = selection.value_as_placement_policy_variant() {
            let value =
                decoder.required_enumeration("PlacementPolicyVariant.value", value.value())?;
            return Ok(Self::PlacementPolicy(value));
        }
        if let Some(value) = selection.value_as_domain_choice_reference() {
            let domain = decoder.name("DomainChoiceReference.domain", value.domain())?;
            return Ok(Self::Domain(domain));
        }
        if let Some(value) = selection.value_as_resource_choice_reference() {
            let resource = decoder.name("ResourceChoiceReference.resource", value.resource())?;
            return Ok(Self::Resource(resource));
        }
        if let Some(value) = selection.value_as_model_choice_reference() {
            return Ok(Self::Model(decode_node_ref(decoder, value.node())?));
        }
        Err(decoder.unknown_union("ChoiceSelection.value", selection.value_type().0))
    }

    fn decode_choice(
        decoder: Decoder<'_>,
        choice: wire::Choice<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        if let Some(value) = choice.value_as_domain_pace_variant() {
            let value = decoder.required_enumeration("DomainPaceVariant.value", value.value())?;
            return Ok(Self::DomainPace(value));
        }
        if let Some(value) = choice.value_as_placement_policy_variant() {
            let value =
                decoder.required_enumeration("PlacementPolicyVariant.value", value.value())?;
            return Ok(Self::PlacementPolicy(value));
        }
        if let Some(value) = choice.value_as_domain_choice_reference() {
            let domain = decoder.name("DomainChoiceReference.domain", value.domain())?;
            return Ok(Self::Domain(domain));
        }
        if let Some(value) = choice.value_as_resource_choice_reference() {
            let resource = decoder.name("ResourceChoiceReference.resource", value.resource())?;
            return Ok(Self::Resource(resource));
        }
        if let Some(value) = choice.value_as_model_choice_reference() {
            return Ok(Self::Model(decode_node_ref(decoder, value.node())?));
        }
        Err(decoder.unknown_union("Choice.value", choice.value_type().0))
    }
}

/// A typed selection on which another control's choices depend.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChoiceSelection {
    pub value: ChoiceValue,
}

impl ChoiceSelection {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::ChoiceSelection<'fbb>>, Report<WireEncodeError>> {
        let value = self.value.encode(encoder)?;
        Ok(wire::ChoiceSelection::create(
            encoder.fbb(),
            &wire::ChoiceSelectionArgs {
                value_type: value.discriminant,
                value: Some(value.value),
            },
        ))
    }

    fn decode(
        decoder: Decoder<'_>,
        selection: wire::ChoiceSelection<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self {
            value: ChoiceValue::decode_selection(decoder, selection)?,
        })
    }
}

/// A typed choice query with a bounded result page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChoiceLookupRequest {
    target: ChoiceTarget,
    dependencies: Vec<ChoiceSelection>,
    search: String,
    page_size: u16,
    page_cursor: Option<String>,
}

impl ChoiceLookupRequest {
    pub fn new(target: ChoiceTarget, dependencies: Vec<ChoiceSelection>, search: String) -> Self {
        Self {
            target,
            dependencies,
            search,
            page_size: 64,
            page_cursor: None,
        }
    }

    pub fn with_page(
        mut self,
        page_size: u16,
        page_cursor: Option<String>,
    ) -> Result<Self, Report<WireValueError>> {
        if !(1..=100).contains(&page_size) {
            return Err(Report::new(WireValueError::InvalidChoicePageSize {
                size: page_size,
            }));
        }
        self.page_size = page_size;
        self.page_cursor = page_cursor;
        Ok(self)
    }

    pub const fn target(&self) -> ChoiceTarget {
        self.target
    }

    pub fn dependencies(&self) -> &[ChoiceSelection] {
        &self.dependencies
    }

    pub fn search(&self) -> &str {
        &self.search
    }

    pub fn page_size(&self) -> usize {
        usize::from(self.page_size)
    }

    pub fn page_cursor(&self) -> Option<&str> {
        self.page_cursor.as_deref()
    }

    pub(crate) fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::ChoiceLookupRequest<'fbb>>, Report<WireEncodeError>> {
        let dependencies = encoder.table_vector(
            "ChoiceLookupRequest.dependencies",
            &self.dependencies,
            ChoiceSelection::encode,
        )?;
        let search = encoder.text("ChoiceLookupRequest.search", &self.search)?;
        let page_cursor = encoder.optional_text(
            "ChoiceLookupRequest.page_cursor",
            self.page_cursor.as_deref(),
        )?;
        Ok(wire::ChoiceLookupRequest::create(
            encoder.fbb(),
            &wire::ChoiceLookupRequestArgs {
                target: Some(self.target.into()),
                dependencies: Some(dependencies),
                search: Some(search),
                page_size: self.page_size,
                page_cursor,
            },
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        request: wire::ChoiceLookupRequest<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let target =
            decoder.required_enumeration("ChoiceLookupRequest.target", request.target())?;
        let dependencies = decoder.table_vector(
            "ChoiceLookupRequest.dependencies",
            request.dependencies(),
            |selection| ChoiceSelection::decode(decoder, selection),
        )?;
        let search = decoder.text("ChoiceLookupRequest.search", request.search())?;
        if !(1..=100).contains(&request.page_size()) {
            return Err(Report::new(WireDecodeError::InvalidValue {
                field: "ChoiceLookupRequest.page_size",
                kind: "choice page size",
            }));
        }
        let page_cursor =
            decoder.optional_text("ChoiceLookupRequest.page_cursor", request.page_cursor())?;
        Ok(Self {
            target,
            dependencies,
            search,
            page_size: request.page_size(),
            page_cursor,
        })
    }
}

/// Text that presents one choice without carrying its semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChoicePresentation {
    pub label: String,
    pub detail: Option<String>,
    pub group: Option<String>,
}

impl ChoicePresentation {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::ChoicePresentation<'fbb>>, Report<WireEncodeError>> {
        let label = encoder.text("ChoicePresentation.label", &self.label)?;
        let detail = encoder.optional_text("ChoicePresentation.detail", self.detail.as_deref())?;
        let group = encoder.optional_text("ChoicePresentation.group", self.group.as_deref())?;
        Ok(wire::ChoicePresentation::create(
            encoder.fbb(),
            &wire::ChoicePresentationArgs {
                label: Some(label),
                detail,
                group,
            },
        ))
    }

    fn decode(
        decoder: Decoder<'_>,
        presentation: wire::ChoicePresentation<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self {
            label: decoder.text("ChoicePresentation.label", presentation.label())?,
            detail: decoder.optional_text("ChoicePresentation.detail", presentation.detail())?,
            group: decoder.optional_text("ChoicePresentation.group", presentation.group())?,
        })
    }
}

/// One typed value and the text used to present it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Choice {
    pub value: ChoiceValue,
    pub presentation: ChoicePresentation,
}

impl Choice {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::Choice<'fbb>>, Report<WireEncodeError>> {
        let value = self.value.encode(encoder)?;
        let presentation = self.presentation.encode(encoder)?;
        Ok(wire::Choice::create(
            encoder.fbb(),
            &wire::ChoiceArgs {
                value_type: value.discriminant,
                value: Some(value.value),
                presentation: Some(presentation),
            },
        ))
    }

    fn decode(
        decoder: Decoder<'_>,
        choice: wire::Choice<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self {
            value: ChoiceValue::decode_choice(decoder, choice)?,
            presentation: ChoicePresentation::decode(decoder, choice.presentation())?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChoiceStatus {
    Ready,
    MissingContext,
    StaleContext,
    LookupFailed,
}

wire_enum!(ALL_CHOICE_STATUSES: ChoiceStatus => wire::ChoiceStatus {
    Ready,
    MissingContext,
    StaleContext,
    LookupFailed,
});

/// One bounded page of choices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChoiceOutcome {
    pub status: ChoiceStatus,
    pub choices: Vec<Choice>,
    pub page_cursor: Option<String>,
}

impl ChoiceOutcome {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<WireEncodeError>> {
        let choices =
            encoder.table_vector("ChoiceOutcome.choices", &self.choices, Choice::encode)?;
        let page_cursor =
            encoder.optional_text("ChoiceOutcome.page_cursor", self.page_cursor.as_deref())?;
        let outcome = wire::ChoiceOutcome::create(
            encoder.fbb(),
            &wire::ChoiceOutcomeArgs {
                status: Some(self.status.into()),
                choices: Some(choices),
                page_cursor,
            },
        );
        Ok(EncodedUnion::new(wire::ReplyBody::ChoiceOutcome, outcome))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        outcome: wire::ChoiceOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self {
            status: decoder.required_enumeration("ChoiceOutcome.status", outcome.status())?,
            choices: decoder.table_vector(
                "ChoiceOutcome.choices",
                outcome.choices(),
                |choice| Choice::decode(decoder, choice),
            )?,
            page_cursor: decoder
                .optional_text("ChoiceOutcome.page_cursor", outcome.page_cursor())?,
        })
    }
}
