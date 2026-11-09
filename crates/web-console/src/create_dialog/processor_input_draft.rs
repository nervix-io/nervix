//! One ordered list of processor input relays until it becomes the input Model of a processor.
//!
//! Layer: edges.
//!
//! - **Owns.** The browser order of one `FROM`, `LEFT FROM` or `RIGHT FROM` list's relays, their
//!   source predicates, the active input, the list's optional collection bounds, how its
//!   refusals name it, and their conversion to current input Models.
//! - **Depends on.** Typed vocabulary, expression parsing, and selected browser references.
//! - **Must not know.** Registry state, runtime input collection, or session transport.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::collections::BTreeSet;

use error_stack::{Report, ResultExt as _};
use nervix_models::{
    InputCollectPolicy, ModelKind, NodeRef, ProcessorInputWhere, ProcessorInputs, RelayName,
};
use nervix_plexus::parse_expression;

use super::{SelectedReference, processor_draft::ProcessorDraftError};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct InputDraft {
    pub(super) relay: Option<SelectedReference<RelayName>>,
    pub(super) where_clause: String,
}

impl InputDraft {
    pub(super) fn select_relay(&mut self, node: &NodeRef) {
        if node.kind == ModelKind::Relay {
            self.relay = Some(SelectedReference::chosen(RelayName::from(&node.identifier)));
        }
    }

    pub(super) fn current_relay(&self) -> Option<&RelayName> {
        self.relay
            .as_ref()
            .and_then(SelectedReference::current_name)
    }

    pub(super) fn invalidate(&mut self) {
        if let Some(relay) = &mut self.relay {
            relay.invalidate();
        }
    }
}

/// Which input list of a processor a draft list is, as its refusals name it: the `FROM` list of
/// most processors, or one side of a correlator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InputList {
    From,
    Left,
    Right,
}

impl InputList {
    /// The list's relays as a refusal names them.
    pub(super) fn relays(self) -> &'static str {
        match self {
            Self::From => "input",
            Self::Left => "LEFT FROM",
            Self::Right => "RIGHT FROM",
        }
    }

    /// One input of the list as a refusal names it.
    pub(super) fn input(self) -> &'static str {
        match self {
            Self::From => "Input",
            Self::Left => "LEFT FROM input",
            Self::Right => "RIGHT FROM input",
        }
    }

    /// The list's collection clause as a refusal names it.
    pub(super) fn collection(self) -> &'static str {
        match self {
            Self::From => "COLLECT FOR",
            Self::Left => "LEFT FROM COLLECT FOR",
            Self::Right => "RIGHT FROM COLLECT FOR",
        }
    }
}

/// The ordered relays one input list reads, the input being edited, and the list's collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct InputListDraft {
    pub(super) relays: Vec<InputDraft>,
    pub(super) active: usize,
    pub(super) collect: Option<InputCollectPolicy>,
}

impl Default for InputListDraft {
    /// One input to choose, as a processor reads at least one relay.
    fn default() -> Self {
        Self {
            relays: vec![InputDraft::default()],
            active: 0,
            collect: None,
        }
    }
}

impl InputListDraft {
    pub(super) fn active(&self) -> Option<&InputDraft> {
        self.relays.get(self.active)
    }

    pub(super) fn active_mut(&mut self) -> Option<&mut InputDraft> {
        self.relays.get_mut(self.active)
    }

    pub(super) fn current_first(&self) -> Option<&RelayName> {
        self.relays.first().and_then(InputDraft::current_relay)
    }

    /// Selects `node` for the active input, and reports whether that changed the first relay.
    ///
    /// The later inputs are offered by the first input's schema and branch, so a changed first
    /// relay requires each of them to be selected again.
    pub(super) fn select(&mut self, node: &NodeRef) -> bool {
        if node.kind != ModelKind::Relay {
            return false;
        }
        let previous = self.current_first().cloned();
        let active = self.active;
        if let Some(input) = self.active_mut() {
            input.select_relay(node);
        }
        let first_changed = active == 0 && self.current_first() != previous.as_ref();
        if first_changed {
            for input in self.relays.iter_mut().skip(1) {
                input.invalidate();
            }
        }
        first_changed
    }

    pub(super) fn invalidate(&mut self) {
        for input in &mut self.relays {
            input.invalidate();
        }
    }

    pub(super) fn add(&mut self) {
        self.relays.push(InputDraft::default());
        self.active = self.relays.len() - 1;
    }

    /// Removes the active input unless it is the only one.
    pub(super) fn remove_active(&mut self) {
        if self.relays.len() > 1 {
            self.relays.remove(self.active);
            self.active = self.active.min(self.relays.len() - 1);
        }
    }

    pub(super) fn move_active(&mut self, up: bool) {
        let other = if up {
            self.active.checked_sub(1)
        } else if self.active + 1 < self.relays.len() {
            Some(self.active + 1)
        } else {
            None
        };
        if let Some(other) = other {
            self.relays.swap(self.active, other);
            self.active = other;
        }
    }

    /// The ordered relays with their predicates and collection, or the first missing, stale,
    /// repeated or unreadable input in list order, named as `list`.
    pub(super) fn build(
        &self,
        list: InputList,
    ) -> error_stack::Result<ProcessorInputs, ProcessorDraftError> {
        if self.relays.is_empty() {
            return Err(Report::new(ProcessorDraftError::Inputs { list }));
        }
        let mut from = Vec::new();
        let mut input_where = Vec::new();
        let mut seen_inputs = BTreeSet::new();
        for (index, input) in self.relays.iter().enumerate() {
            let number = index + 1;
            let relay = match &input.relay {
                Some(relay) => relay.current_name().ok_or_else(|| {
                    Report::new(ProcessorDraftError::InputChanged {
                        list,
                        index: number,
                    })
                })?,
                None => {
                    return Err(Report::new(ProcessorDraftError::Input {
                        list,
                        index: number,
                    }));
                }
            };
            if !seen_inputs.insert(relay.clone()) {
                return Err(Report::new(ProcessorDraftError::DuplicateInput {
                    list,
                    relay: relay.clone(),
                }));
            }
            from.push(relay.clone());
            if !input.where_clause.trim().is_empty() {
                input_where.push(ProcessorInputWhere {
                    relay: relay.clone(),
                    where_clause: parse_expression(input.where_clause.trim()).change_context(
                        ProcessorDraftError::InputWhere {
                            list,
                            index: number,
                        },
                    )?,
                });
            }
        }
        let mut inputs = ProcessorInputs::new(from, input_where);
        if let Some(collect) = &self.collect {
            if collect.collect_for.trim().is_empty() {
                return Err(Report::new(ProcessorDraftError::CollectFor { list }));
            }
            inputs.collect_policy = Some(InputCollectPolicy {
                collect_for: collect.collect_for.trim().to_string(),
                max_batch_size: match collect.max_batch_size.as_deref() {
                    Some(size) if !size.trim().is_empty() => Some(size.trim().to_string()),
                    _ => None,
                },
            });
        }
        Ok(inputs)
    }
}
