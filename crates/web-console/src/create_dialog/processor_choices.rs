//! Typed semantic questions asked by junction, deduplicator, reorderer, window processor,
//! correlator, reingestor, generator, inferencer and WASM processor controls.
//!
//! - **Owns.** The dependency context sent for compatible input, output, branch, and state
//!   choices, for a correlator's right side and timeout relays, for a generator's materialized
//!   relay, for an inferencer's or WASM processor's resource and version, and for the scoped
//!   fields window, correlator, generator, inferencer and WASM pickers insert, in one browser
//!   processor draft; the controls an open processor form asks about; and applying the
//!   selections of the right side, timeout, materialized relay, binding and field pickers.
//! - **Depends on.** Typed choice wire values and the current processor draft.
//! - **Must not know.** Registry implementation or how a server pages candidates.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use leptos::prelude::{Get as _, GetUntracked, Update as _};
use nervix_client_wire::{ChoiceSelection, ChoiceTarget, ChoiceValue};
use nervix_models::{ModelKind, NodeRef, RelayName};

use super::{
    ChoiceControl, ChoiceQuery, CreateSignals,
    ingestor_route_draft::{InheritDraft, MessageErrorDraft, RouteBranchDraft},
    processor_correlator_draft::{CorrelationSide, TimeoutDraft},
    processor_draft::{NodeBranchDraft, ProcessorDraft, ProcessorFamily, StatePolicyDraft},
    processor_window_draft::WindowDraft,
};

/// What a node-wide branch family asks for before its first input relay can be offered.
struct NodeBranchPrerequisites {
    unselected: &'static str,
    unnamed: &'static str,
}

impl CreateSignals {
    fn processor_model_question(
        self,
        target: ChoiceTarget,
        node: NodeRef,
        missing_domain: &'static str,
        page_size: u16,
    ) -> Result<ChoiceQuery, &'static str> {
        let Some(domain) = self.captured_domain.get_untracked() else {
            return Err(missing_domain);
        };
        Ok(ChoiceQuery {
            target,
            dependencies: vec![
                ChoiceSelection {
                    value: ChoiceValue::Domain(domain),
                },
                ChoiceSelection {
                    value: ChoiceValue::Model(node),
                },
            ],
            page_size,
        })
    }

    fn processor_relay_question(
        self,
        target: ChoiceTarget,
        relay: &RelayName,
        missing_domain: &'static str,
        page_size: u16,
    ) -> Result<ChoiceQuery, &'static str> {
        self.processor_model_question(
            target,
            NodeRef::new(ModelKind::Relay, relay),
            missing_domain,
            page_size,
        )
    }

    /// The question for the first input relay: any relay for a reingestor, and relays with exactly
    /// the node-wide branch for the families that declare one. A correlator asks it for the first
    /// relay of each side.
    fn processor_first_input_question(
        self,
        draft: &ProcessorDraft,
    ) -> Result<ChoiceQuery, &'static str> {
        let prerequisites = match draft.family() {
            ProcessorFamily::Reingestor => {
                return self.domain_question(
                    ChoiceTarget::Relay,
                    "Select a domain before choosing an input relay",
                );
            }
            ProcessorFamily::Junction => NodeBranchPrerequisites {
                unselected: "Choose the junction branch before its input relays",
                unnamed: "Choose the named junction branch before its input relays",
            },
            ProcessorFamily::Deduplicator => NodeBranchPrerequisites {
                unselected: "Choose the deduplicator branch before its input relays",
                unnamed: "Choose the named deduplicator branch before its input relays",
            },
            ProcessorFamily::Reorderer => NodeBranchPrerequisites {
                unselected: "Choose the reorderer branch before its input relays",
                unnamed: "Choose the named reorderer branch before its input relays",
            },
            ProcessorFamily::Window => NodeBranchPrerequisites {
                unselected: "Choose the window processor branch before its input relays",
                unnamed: "Choose the named window processor branch before its input relays",
            },
            ProcessorFamily::Correlator => NodeBranchPrerequisites {
                unselected: "Choose the correlator branch before its left and right relays",
                unnamed: "Choose the named correlator branch before its left and right relays",
            },
            ProcessorFamily::Inferencer => NodeBranchPrerequisites {
                unselected: "Choose the inferencer branch before its input relays",
                unnamed: "Choose the named inferencer branch before its input relays",
            },
            ProcessorFamily::Wasm => NodeBranchPrerequisites {
                unselected: "Choose the WASM processor branch before its input relays",
                unnamed: "Choose the named WASM processor branch before its input relays",
            },
            ProcessorFamily::Generator => {
                return Err("A generator reads the materialized state of one relay, not inputs");
            }
        };
        match &draft.branching {
            NodeBranchDraft::Unselected => Err(prerequisites.unselected),
            NodeBranchDraft::Unbranched => self.domain_question(
                ChoiceTarget::IngestUnbranchedRelay,
                "Select a domain before choosing an input relay",
            ),
            NodeBranchDraft::Branched(_) => {
                let Some(branch) = draft.branching.current_branch() else {
                    return Err(prerequisites.unnamed);
                };
                self.processor_model_question(
                    ChoiceTarget::IngestBranchedRelay,
                    NodeRef::new(ModelKind::Branch, branch),
                    "Select a domain before choosing an input relay",
                    20,
                )
            }
        }
    }

    /// The question a correlator's right side, timeout relay or a scoped field picker asks.
    fn processor_scope_choice_query(
        self,
        draft: &ProcessorDraft,
        control: ChoiceControl,
    ) -> Result<ChoiceQuery, &'static str> {
        let first_right = match draft.correlator() {
            Some(correlator) => correlator.right.current_first(),
            None => None,
        };
        match control {
            ChoiceControl::ProcessorRightInputRelay => {
                let later = match draft.correlator() {
                    Some(correlator) => correlator.right.active > 0,
                    None => false,
                };
                if !later {
                    return self.processor_first_input_question(draft);
                }
                let Some(first) = first_right else {
                    return Err("Choose the first RIGHT FROM relay before another right relay");
                };
                self.processor_relay_question(
                    ChoiceTarget::ProcessorCompatibleInputRelay,
                    first,
                    "Select a domain before choosing a right relay",
                    20,
                )
            }
            ChoiceControl::ProcessorLeftTimeoutRelay => {
                let Some(first) = draft.current_first_input() else {
                    return Err("Choose the first LEFT FROM relay before the left timeout relay");
                };
                self.processor_relay_question(
                    ChoiceTarget::ProcessorCompatibleInputRelay,
                    first,
                    "Select a domain before choosing a timeout relay",
                    20,
                )
            }
            ChoiceControl::ProcessorRightTimeoutRelay => {
                let Some(first) = first_right else {
                    return Err("Choose the first RIGHT FROM relay before the right timeout relay");
                };
                self.processor_relay_question(
                    ChoiceTarget::ProcessorCompatibleInputRelay,
                    first,
                    "Select a domain before choosing a timeout relay",
                    20,
                )
            }
            ChoiceControl::ProcessorAggregateField => {
                let Some(input) = draft.current_first_input() else {
                    return Err("Choose an input relay before its fields");
                };
                if !draft.has_output_assignment() {
                    return Err("Add an output assignment before an aggregate argument");
                }
                if draft
                    .window()
                    .and_then(WindowDraft::aggregate_function)
                    .is_none()
                {
                    return Err(
                        "Enter the aggregate function, such as COUNT, before choosing its input \
                         field",
                    );
                }
                self.processor_relay_question(
                    ChoiceTarget::RelayField,
                    input,
                    "Select a domain before choosing an input field",
                    100,
                )
            }
            ChoiceControl::ProcessorCorrelateLeftField | ChoiceControl::ProcessorRouteLeftField => {
                let Some(first) = draft.current_first_input() else {
                    return Err("Choose the first LEFT FROM relay before its fields");
                };
                self.processor_side_field_question(draft, control, first)
            }
            ChoiceControl::ProcessorCorrelateRightField
            | ChoiceControl::ProcessorRouteRightField => {
                let Some(first) = first_right else {
                    return Err("Choose the first RIGHT FROM relay before its fields");
                };
                self.processor_side_field_question(draft, control, first)
            }
            _ => Err("This control is not a correlator or window choice"),
        }
    }

    /// The fields of one correlator side's first relay. A route's picker inserts into an output
    /// assignment, so it needs one first.
    fn processor_side_field_question(
        self,
        draft: &ProcessorDraft,
        control: ChoiceControl,
        relay: &RelayName,
    ) -> Result<ChoiceQuery, &'static str> {
        let assigns = matches!(
            control,
            ChoiceControl::ProcessorRouteLeftField | ChoiceControl::ProcessorRouteRightField
        );
        if assigns && !draft.has_output_assignment() {
            return Err("Add an output assignment before a field to assign");
        }
        self.processor_relay_question(
            ChoiceTarget::RelayField,
            relay,
            "Select a domain before choosing a field",
            100,
        )
    }

    pub(super) fn processor_choice_query(
        self,
        control: ChoiceControl,
    ) -> Result<ChoiceQuery, &'static str> {
        let Some(signal) = self.active_processor() else {
            return Err("Open a processor form first");
        };
        let draft = signal.get_untracked();
        match control {
            ChoiceControl::ProcessorInputRelay => {
                if draft.from.active > 0 {
                    let Some(input) = draft.current_first_input() else {
                        return Err("Choose the first input relay before another input");
                    };
                    return self.processor_relay_question(
                        ChoiceTarget::ProcessorCompatibleInputRelay,
                        input,
                        "Select a domain before choosing an input relay",
                        20,
                    );
                }
                self.processor_first_input_question(&draft)
            }
            ChoiceControl::ProcessorBranch | ChoiceControl::ProcessorRouteBranch => self
                .domain_question(
                    ChoiceTarget::Branch,
                    "Select a domain before choosing a branch",
                ),
            ChoiceControl::ProcessorStateRelay => {
                let Some(input) = draft.current_first_input() else {
                    return Err("Choose an input relay before materialized state");
                };
                self.processor_relay_question(
                    ChoiceTarget::ProcessorMaterializedRelay,
                    input,
                    "Select a domain before choosing materialized state",
                    20,
                )
            }
            ChoiceControl::ProcessorStateField => {
                let Some(relay) = draft.active_state().and_then(|state| state.current_relay())
                else {
                    return Err("Choose the materialized relay before its default fields");
                };
                self.processor_relay_question(
                    ChoiceTarget::RelayField,
                    relay,
                    "Select a domain before choosing a default field",
                    100,
                )
            }
            ChoiceControl::ProcessorRouteRelay => {
                let Some(route) = draft.active_route() else {
                    return Err("Add a route before choosing an output relay");
                };
                if draft.family().declares_node_branch()
                    || matches!(route.branch, RouteBranchDraft::Preserve)
                {
                    let source = draft.source_relay_or(
                        "Choose an input relay before its output relay",
                        "Choose the materialized relay before its output relay",
                    )?;
                    return self.processor_relay_question(
                        ChoiceTarget::ProcessorInputBranchRelay,
                        source,
                        "Select a domain before choosing an output relay",
                        20,
                    );
                }
                match &route.branch {
                    RouteBranchDraft::Unselected => {
                        Err("Choose the route branch before its output relay")
                    }
                    RouteBranchDraft::Unbranched => self.domain_question(
                        ChoiceTarget::IngestUnbranchedRelay,
                        "Select a domain before choosing an output relay",
                    ),
                    RouteBranchDraft::Branched { .. } => {
                        let Some(branch) = route.branch.current_branch() else {
                            return Err("Choose the named route branch before its output relay");
                        };
                        self.processor_model_question(
                            ChoiceTarget::IngestBranchedRelay,
                            NodeRef::new(ModelKind::Branch, branch),
                            "Select a domain before choosing an output relay",
                            20,
                        )
                    }
                    RouteBranchDraft::Preserve => {
                        Err("Choose an input relay before its output relay")
                    }
                }
            }
            ChoiceControl::ProcessorInputField => {
                let Some(input) = draft.current_first_input() else {
                    return Err("Choose an input relay before inheriting fields");
                };
                self.processor_relay_question(
                    ChoiceTarget::RelayField,
                    input,
                    "Select a domain before choosing an input field",
                    100,
                )
            }
            ChoiceControl::ProcessorOutputField | ChoiceControl::ProcessorErrorField => {
                let Some(route) = draft.active_route() else {
                    return Err("Add a route before choosing a field");
                };
                let relay = if control == ChoiceControl::ProcessorErrorField {
                    route.message_error.current_relay()
                } else {
                    route.current_relay()
                };
                let Some(relay) = relay else {
                    return Err("Choose the relay before its fields");
                };
                self.processor_relay_question(
                    ChoiceTarget::RelayField,
                    relay,
                    "Select a domain before choosing a field",
                    100,
                )
            }
            ChoiceControl::ProcessorBranchField => {
                let Some(branch) = draft
                    .active_route()
                    .and_then(|route| route.branch.current_branch())
                else {
                    return Err("Choose a named route branch before its key fields");
                };
                self.processor_model_question(
                    ChoiceTarget::BranchField,
                    NodeRef::new(ModelKind::Branch, branch),
                    "Select a domain before choosing a branch field",
                    100,
                )
            }
            ChoiceControl::ProcessorErrorRelay => {
                let source = draft.source_relay_or(
                    "Choose an input relay before the error relay",
                    "Choose the materialized relay before the error relay",
                )?;
                self.processor_relay_question(
                    ChoiceTarget::ProcessorInputBranchRelay,
                    source,
                    "Select a domain before choosing an error relay",
                    20,
                )
            }
            ChoiceControl::ProcessorRightInputRelay
            | ChoiceControl::ProcessorLeftTimeoutRelay
            | ChoiceControl::ProcessorRightTimeoutRelay
            | ChoiceControl::ProcessorAggregateField
            | ChoiceControl::ProcessorCorrelateLeftField
            | ChoiceControl::ProcessorCorrelateRightField
            | ChoiceControl::ProcessorRouteLeftField
            | ChoiceControl::ProcessorRouteRightField => {
                self.processor_scope_choice_query(&draft, control)
            }
            ChoiceControl::GeneratorRelay
            | ChoiceControl::GeneratorStateField
            | ChoiceControl::ProcessorResource
            | ChoiceControl::ProcessorResourceVersion
            | ChoiceControl::InferencerInputField
            | ChoiceControl::ProcessorBranchKeyField
            | ChoiceControl::WasmGuestField => self.generated_family_choice_query(&draft, control),
            _ => Err("This control is not a processor choice"),
        }
    }

    /// The question a generator's materialized relay, an inferencer's or WASM processor's
    /// resource and version, or a picker of the fields their routes and mappings read asks.
    fn generated_family_choice_query(
        self,
        draft: &ProcessorDraft,
        control: ChoiceControl,
    ) -> Result<ChoiceQuery, &'static str> {
        match control {
            ChoiceControl::GeneratorRelay => match &draft.branching {
                NodeBranchDraft::Unselected => {
                    Err("Choose the generator branch before its materialized relay")
                }
                NodeBranchDraft::Unbranched => self.domain_question(
                    ChoiceTarget::GeneratorMaterializedRelay,
                    "Select a domain before choosing a materialized relay",
                ),
                NodeBranchDraft::Branched(_) => {
                    let Some(branch) = draft.branching.current_branch() else {
                        return Err(
                            "Choose the named generator branch before its materialized relay"
                        );
                    };
                    self.processor_model_question(
                        ChoiceTarget::GeneratorMaterializedRelay,
                        NodeRef::new(ModelKind::Branch, branch),
                        "Select a domain before choosing a materialized relay",
                        20,
                    )
                }
            },
            ChoiceControl::GeneratorStateField => {
                let relay = match draft.generator() {
                    Some(generator) => generator.current_relay(),
                    None => None,
                };
                let Some(relay) = relay else {
                    return Err("Choose the materialized relay before its state fields");
                };
                if !draft.has_output_assignment() {
                    return Err("Add an output assignment before a field to assign");
                }
                self.processor_relay_question(
                    ChoiceTarget::RelayField,
                    relay,
                    "Select a domain before choosing a state field",
                    100,
                )
            }
            ChoiceControl::ProcessorResource => self.domain_question(
                ChoiceTarget::Resource,
                "Select a domain before choosing a resource",
            ),
            ChoiceControl::ProcessorResourceVersion => {
                let Some(domain) = self.captured_domain.get_untracked() else {
                    return Err("Select a domain before choosing a resource version");
                };
                let resource = match draft.binding() {
                    Some(binding) => binding.pin.current_resource(),
                    None => None,
                };
                let Some(resource) = resource else {
                    return Err("Select a resource to list its completed versions");
                };
                Ok(ChoiceQuery {
                    target: ChoiceTarget::CompletedResourceVersion,
                    dependencies: vec![
                        ChoiceSelection {
                            value: ChoiceValue::Domain(domain),
                        },
                        ChoiceSelection {
                            value: ChoiceValue::Resource(resource.clone()),
                        },
                    ],
                    page_size: 100,
                })
            }
            ChoiceControl::InferencerInputField => {
                let Some(input) = draft.current_first_input() else {
                    return Err("Choose an input relay before its fields");
                };
                let mapping = match draft.inferencer() {
                    Some(inferencer) => inferencer.inputs.active(),
                    None => None,
                };
                if mapping.is_none() {
                    return Err("Add an INPUTS tensor before a field it maps");
                }
                self.processor_relay_question(
                    ChoiceTarget::RelayField,
                    input,
                    "Select a domain before choosing an input field",
                    100,
                )
            }
            ChoiceControl::ProcessorBranchKeyField => {
                let Some(branch) = draft.branching.current_branch() else {
                    return Err("Choose the named branch before its key fields");
                };
                if !draft.has_output_assignment() {
                    return Err("Add an output assignment before a field to assign");
                }
                self.processor_model_question(
                    ChoiceTarget::BranchField,
                    NodeRef::new(ModelKind::Branch, branch),
                    "Select a domain before choosing a branch field",
                    100,
                )
            }
            ChoiceControl::WasmGuestField => {
                let Some(route) = draft.active_route() else {
                    return Err("Add a route before choosing a guest output field");
                };
                let Some(relay) = route.current_relay() else {
                    return Err("Choose the output relay before the guest fields aligned to it");
                };
                if route.assignments.is_empty() {
                    return Err("Add an output assignment before a field to assign");
                }
                self.processor_relay_question(
                    ChoiceTarget::RelayField,
                    relay,
                    "Select a domain before choosing a guest output field",
                    100,
                )
            }
            _ => Err("This control is not a generator, inferencer or WASM processor choice"),
        }
    }

    /// The controls the open processor form asks the session about.
    pub(super) fn processor_open_controls(self) -> Vec<ChoiceControl> {
        let Some(processor) = self.active_processor() else {
            return Vec::new();
        };
        let draft = processor.get_untracked();
        let mut controls = Vec::new();
        if draft.family().reads_inputs() {
            controls.push(ChoiceControl::ProcessorInputRelay);
        }
        controls.push(ChoiceControl::ProcessorRouteRelay);
        if draft.family().declares_node_branch()
            && matches!(draft.branching, NodeBranchDraft::Branched(_))
        {
            controls.push(ChoiceControl::ProcessorBranch);
        }
        if let Some(state) = draft.active_state() {
            controls.push(ChoiceControl::ProcessorStateRelay);
            if matches!(state.policy, StatePolicyDraft::Default(_)) {
                controls.push(ChoiceControl::ProcessorStateField);
            }
        }
        if let Some(correlator) = draft.correlator() {
            controls.push(ChoiceControl::ProcessorRightInputRelay);
            controls.push(ChoiceControl::ProcessorCorrelateLeftField);
            controls.push(ChoiceControl::ProcessorCorrelateRightField);
            controls.push(ChoiceControl::ProcessorRouteLeftField);
            controls.push(ChoiceControl::ProcessorRouteRightField);
            if matches!(correlator.left_timeout, TimeoutDraft::SendTo(_)) {
                controls.push(ChoiceControl::ProcessorLeftTimeoutRelay);
            }
            if matches!(correlator.right_timeout, TimeoutDraft::SendTo(_)) {
                controls.push(ChoiceControl::ProcessorRightTimeoutRelay);
            }
        }
        if draft.window().is_some() {
            controls.push(ChoiceControl::ProcessorAggregateField);
        }
        if draft.generator().is_some() {
            controls.push(ChoiceControl::GeneratorRelay);
            controls.push(ChoiceControl::GeneratorStateField);
        }
        if draft.binding().is_some() {
            controls.push(ChoiceControl::ProcessorResource);
            controls.push(ChoiceControl::ProcessorResourceVersion);
        }
        if draft.inferencer().is_some() {
            controls.push(ChoiceControl::InferencerInputField);
        }
        if draft.wasm().is_some() {
            controls.push(ChoiceControl::WasmGuestField);
        }
        if draft.family().offers_branch_key_references()
            && matches!(draft.branching, NodeBranchDraft::Branched(_))
        {
            controls.push(ChoiceControl::ProcessorBranchKeyField);
        }
        if let Some(route) = draft.active_route() {
            if matches!(
                route.inherit,
                InheritDraft::AllExcept(_) | InheritDraft::Fields(_)
            ) {
                controls.push(ChoiceControl::ProcessorInputField);
            }
            if matches!(route.branch, RouteBranchDraft::Branched { .. }) {
                controls.push(ChoiceControl::ProcessorRouteBranch);
                controls.push(ChoiceControl::ProcessorBranchField);
            }
            controls.push(ChoiceControl::ProcessorOutputField);
            if matches!(route.message_error, MessageErrorDraft::SendTo { .. }) {
                controls.push(ChoiceControl::ProcessorErrorRelay);
                controls.push(ChoiceControl::ProcessorErrorField);
            }
        }
        controls
    }

    /// Applies `value` chosen in a correlator's right side, timeout or a scoped field picker to
    /// the open processor draft. A field is inserted where the picker writes, not held as a
    /// selection.
    pub(super) fn select_processor_scope_choice(self, control: ChoiceControl, value: ChoiceValue) {
        let Some(signal) = self.active_processor() else {
            return;
        };
        signal.update(|draft| match (control, value) {
            (ChoiceControl::ProcessorRightInputRelay, ChoiceValue::Model(node)) => {
                draft.select_right_input(&node);
            }
            (ChoiceControl::ProcessorLeftTimeoutRelay, ChoiceValue::Model(node)) => {
                draft.select_timeout_relay(CorrelationSide::Left, &node);
            }
            (ChoiceControl::ProcessorRightTimeoutRelay, ChoiceValue::Model(node)) => {
                draft.select_timeout_relay(CorrelationSide::Right, &node);
            }
            (ChoiceControl::ProcessorAggregateField, ChoiceValue::Field(field)) => {
                draft.insert_aggregate_argument(field);
            }
            (ChoiceControl::ProcessorCorrelateLeftField, ChoiceValue::Field(field)) => {
                draft.insert_correlation_reference(CorrelationSide::Left, field);
            }
            (ChoiceControl::ProcessorCorrelateRightField, ChoiceValue::Field(field)) => {
                draft.insert_correlation_reference(CorrelationSide::Right, field);
            }
            (ChoiceControl::ProcessorRouteLeftField, ChoiceValue::Field(field)) => {
                draft.insert_route_reference(CorrelationSide::Left, field);
            }
            (ChoiceControl::ProcessorRouteRightField, ChoiceValue::Field(field)) => {
                draft.insert_route_reference(CorrelationSide::Right, field);
            }
            _ => {}
        });
    }

    /// Applies `value` chosen in a generator's materialized-relay control, an inferencer's or WASM
    /// processor's resource or version control, or one of their field pickers, to the open
    /// processor draft. A field is inserted where its picker writes, not held as a selection.
    pub(super) fn select_generated_family_choice(self, control: ChoiceControl, value: ChoiceValue) {
        let Some(signal) = self.active_processor() else {
            return;
        };
        signal.update(|draft| match (control, value) {
            (ChoiceControl::GeneratorRelay, ChoiceValue::Model(node)) => {
                draft.select_generator_relay(&node);
            }
            (ChoiceControl::GeneratorStateField, ChoiceValue::Field(field)) => {
                draft.insert_state_reference(field);
            }
            (ChoiceControl::ProcessorResource, ChoiceValue::Resource(resource)) => {
                if let Some(binding) = draft.binding_mut() {
                    binding.pin.select_resource(resource);
                }
            }
            (ChoiceControl::ProcessorResourceVersion, ChoiceValue::ResourceVersion(version)) => {
                if let Some(binding) = draft.binding_mut() {
                    binding.pin.select_version(version);
                }
            }
            (ChoiceControl::InferencerInputField, ChoiceValue::Field(field)) => {
                if let Some(inferencer) = draft.inferencer_mut() {
                    inferencer.insert_input_field(field);
                }
            }
            (ChoiceControl::ProcessorBranchKeyField, ChoiceValue::Field(field)) => {
                draft.insert_branch_key_reference(field);
            }
            (ChoiceControl::WasmGuestField, ChoiceValue::Field(field)) => {
                draft.insert_generated_reference(field);
            }
            _ => {}
        });
    }

    /// Whether `value` is what a generator's materialized-relay control, or an inferencer's or
    /// WASM processor's resource or version control, holds. A field picker holds no selection.
    pub(super) fn generated_family_selected(
        self,
        control: ChoiceControl,
        value: &ChoiceValue,
    ) -> bool {
        let Some(signal) = self.active_processor() else {
            return false;
        };
        let draft = signal.get();
        match (control, value) {
            (ChoiceControl::GeneratorRelay, ChoiceValue::Model(node)) => match draft.generator() {
                Some(generator) => generator
                    .relay
                    .as_ref()
                    .is_some_and(|selected| selected.selects(ModelKind::Relay, node)),
                None => false,
            },
            (ChoiceControl::ProcessorResource, ChoiceValue::Resource(resource)) => {
                match draft.binding() {
                    Some(binding) => binding.pin.current_resource() == Some(resource),
                    None => false,
                }
            }
            (ChoiceControl::ProcessorResourceVersion, ChoiceValue::ResourceVersion(version)) => {
                match draft.binding() {
                    Some(binding) => binding.pin.version.as_ref().is_some_and(|selected| {
                        selected.is_current() && selected.name() == version
                    }),
                    None => false,
                }
            }
            _ => false,
        }
    }

    /// Whether `value` is what a correlator's right side or timeout control holds. A field picker
    /// holds no selection.
    pub(super) fn processor_scope_selected(
        self,
        control: ChoiceControl,
        value: &ChoiceValue,
    ) -> bool {
        let ChoiceValue::Model(node) = value else {
            return false;
        };
        let Some(signal) = self.active_processor() else {
            return false;
        };
        let draft = signal.get();
        let Some(correlator) = draft.correlator() else {
            return false;
        };
        let relay = match control {
            ChoiceControl::ProcessorRightInputRelay => correlator
                .right
                .active()
                .and_then(|input| input.current_relay()),
            ChoiceControl::ProcessorLeftTimeoutRelay => correlator.left_timeout.current_relay(),
            ChoiceControl::ProcessorRightTimeoutRelay => correlator.right_timeout.current_relay(),
            _ => None,
        };
        match relay {
            Some(relay) => {
                node.kind == ModelKind::Relay && relay.as_str() == node.identifier.as_str()
            }
            None => false,
        }
    }
}
