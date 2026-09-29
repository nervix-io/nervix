//! Typed semantic questions asked by junction and reingestor controls.
//!
//! Layer: edges.
//!
//! - **Owns.** The dependency context sent for compatible input, output, branch, and state
//!   choices in one browser processor draft.
//! - **Depends on.** Typed choice wire values and the current processor draft.
//! - **Must not know.** Registry implementation or how a server pages candidates.

use leptos::prelude::GetUntracked;
use nervix_client_wire::{ChoiceSelection, ChoiceTarget, ChoiceValue};
use nervix_models::{ModelKind, NodeRef, RelayName};

use super::{
    ChoiceControl, ChoiceQuery, CreateSignals,
    ingestor_route_draft::RouteBranchDraft,
    processor_draft::{JunctionBranchDraft, ProcessorFamily},
};

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

    pub(super) fn processor_choice_query(
        self,
        control: ChoiceControl,
    ) -> Result<ChoiceQuery, &'static str> {
        let Some(signal) = self.active_processor() else {
            return Err("Open a junction or reingestor form first");
        };
        let draft = signal.get_untracked();
        match control {
            ChoiceControl::ProcessorInputRelay => {
                if draft.active_input > 0 {
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
                match draft.family {
                    ProcessorFamily::Reingestor => self.domain_question(
                        ChoiceTarget::Relay,
                        "Select a domain before choosing an input relay",
                    ),
                    ProcessorFamily::Junction => match &draft.branching {
                        JunctionBranchDraft::Unselected => {
                            Err("Choose the junction branch before its input relays")
                        }
                        JunctionBranchDraft::Unbranched => self.domain_question(
                            ChoiceTarget::IngestUnbranchedRelay,
                            "Select a domain before choosing an input relay",
                        ),
                        JunctionBranchDraft::Branched(_) => {
                            let Some(branch) = draft.branching.current_branch() else {
                                return Err(
                                    "Choose the named junction branch before its input relays"
                                );
                            };
                            self.processor_model_question(
                                ChoiceTarget::IngestBranchedRelay,
                                NodeRef::new(ModelKind::Branch, branch),
                                "Select a domain before choosing an input relay",
                                20,
                            )
                        }
                    },
                }
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
                if draft.family == ProcessorFamily::Junction
                    || matches!(route.branch, RouteBranchDraft::Preserve)
                {
                    let Some(input) = draft.current_first_input() else {
                        return Err("Choose an input relay before its output relay");
                    };
                    return self.processor_relay_question(
                        ChoiceTarget::ProcessorInputBranchRelay,
                        input,
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
                let Some(input) = draft.current_first_input() else {
                    return Err("Choose an input relay before the error relay");
                };
                self.processor_relay_question(
                    ChoiceTarget::ProcessorInputBranchRelay,
                    input,
                    "Select a domain before choosing an error relay",
                    20,
                )
            }
            _ => Err("This control is not a processor choice"),
        }
    }
}
