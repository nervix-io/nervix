//! Domain-owned placement creation drafts.
//!
//! Layer: edges.
//! - **Owns.** Operator input for a named placement rule — its coverage, its ordered `FROM` and
//!   `TO` members kept visible after the draft moves to another domain, its policy and rank — and
//!   its one-way conversion into the current placement Model.
//! - **Depends on.** Vocabulary names, policies, the placement Model and exact integer parsing.
//! - **Must not know.** Which runtime nodes are placement-eligible, graph validation, scheduling
//!   or browser transport.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::num::NonZeroU64;

use error_stack::{Report, ResultExt as _};
use indexmap::IndexMap;
use meticulous::ResultExt as _;
use nervix_models::{CreatePlacement, ModelName, NodeRef, PlacementName, PlacementPolicy};
use thiserror::Error;

/// What a placement rule covers, as far as the operator has decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) enum PlacementCoverage {
    /// `DOMAIN WIDE`: every eligible hop of the domain.
    #[default]
    DomainWide,
    /// The directed paths from each `FROM` member to each `TO` member.
    Corridor,
}

/// One side of a corridor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CorridorSide {
    From,
    To,
}

impl CorridorSide {
    /// The keyword that introduces this side in `CREATE PLACEMENT`.
    pub(super) fn keyword(self) -> &'static str {
        match self {
            Self::From => "FROM",
            Self::To => "TO",
        }
    }
}

/// Which way a member moves within its side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MemberMove {
    Earlier,
    Later,
}

/// What an operator does to one member of a side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MemberChange {
    Move(MemberMove),
    Remove,
}

/// Whether a selected member still belongs to the draft's captured domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum MemberContext {
    Current,
    /// Selected in a domain the draft has since left: shown, but not used until selected again.
    Changed,
}

/// One side's members in the order the operator chose them. The order is written into the rule,
/// and a name appears at most once, as `CREATE PLACEMENT` keeps one of each.
pub(super) type CorridorMembers = IndexMap<ModelName, MemberContext>;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct PlacementDraft {
    pub name: String,
    pub if_not_exists: bool,
    pub coverage: PlacementCoverage,
    /// The corridor's members. A draft switched to domain-wide coverage keeps them, so switching
    /// back restores the corridor the operator had chosen.
    pub from: CorridorMembers,
    pub to: CorridorMembers,
    pub policy: PlacementPolicy,
    pub rank: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum PlacementDraftError {
    #[error("Placement name is invalid")]
    Name,
    #[error("Rank must be a positive 64-bit integer")]
    Rank,
    #[error("Choose at least one {} member", side.keyword())]
    MemberRequired { side: CorridorSide },
    #[error(
        "{} member {member} belongs to a changed context; remove it or select it again",
        side.keyword()
    )]
    MemberChanged {
        side: CorridorSide,
        member: ModelName,
    },
}

impl PlacementDraft {
    /// A corridor draft that starts from `member`, as a runtime node's graph action opens one.
    pub fn starting_from(member: ModelName) -> Self {
        let mut from = CorridorMembers::new();
        from.insert(member, MemberContext::Current);
        Self {
            coverage: PlacementCoverage::Corridor,
            from,
            ..Self::default()
        }
    }

    pub fn members(&self, side: CorridorSide) -> &CorridorMembers {
        match side {
            CorridorSide::From => &self.from,
            CorridorSide::To => &self.to,
        }
    }

    fn members_mut(&mut self, side: CorridorSide) -> &mut CorridorMembers {
        match side {
            CorridorSide::From => &mut self.from,
            CorridorSide::To => &mut self.to,
        }
    }

    /// Adds the runtime node a member choice selected to the end of `side`. A member already on
    /// that side keeps its place, and one selected in a previous domain becomes current again.
    pub fn add_member(&mut self, side: CorridorSide, node: &NodeRef) {
        self.members_mut(side)
            .insert(node.identifier.clone(), MemberContext::Current);
    }

    /// Whether `node` is a current member of `side`.
    pub fn selects_member(&self, side: CorridorSide, node: &NodeRef) -> bool {
        self.members(side).get(&node.identifier) == Some(&MemberContext::Current)
    }

    /// Moves the member at `index` of `side` one place earlier or later; a member already at
    /// that end stays where it is.
    pub fn move_member(&mut self, side: CorridorSide, index: usize, direction: MemberMove) {
        let members = self.members_mut(side);
        let target = match direction {
            MemberMove::Earlier => index.checked_sub(1),
            MemberMove::Later => index.checked_add(1),
        };
        let Some(target) = target else {
            return;
        };
        if index < members.len() && target < members.len() {
            members.swap_indices(index, target);
        }
    }

    pub fn remove_member(&mut self, side: CorridorSide, index: usize) {
        self.members_mut(side).shift_remove_index(index);
    }

    /// Applies one row action to the member at `index` of `side`.
    pub fn change_member(&mut self, side: CorridorSide, index: usize, change: MemberChange) {
        match change {
            MemberChange::Move(direction) => self.move_member(side, index, direction),
            MemberChange::Remove => self.remove_member(side, index),
        }
    }

    /// Whether a member was selected in a domain the draft has since left.
    pub fn has_changed_members(&self) -> bool {
        self.from
            .values()
            .chain(self.to.values())
            .any(|context| *context == MemberContext::Changed)
    }

    /// Keeps every selected member visible after the draft moved to another domain, but requires
    /// selecting each of them again.
    pub fn invalidate_references(&mut self) {
        for context in self.from.values_mut().chain(self.to.values_mut()) {
            *context = MemberContext::Changed;
        }
    }

    pub fn build(&self) -> Result<CreatePlacement, Report<PlacementDraftError>> {
        let name =
            PlacementName::parse(self.name.trim()).change_context(PlacementDraftError::Name)?;
        let rank = if self.rank.trim().is_empty() {
            None
        } else {
            Some(
                self.rank
                    .trim()
                    .parse::<NonZeroU64>()
                    .change_context(PlacementDraftError::Rank)?,
            )
        };
        match self.coverage {
            PlacementCoverage::DomainWide => {
                Ok(CreatePlacement::domain_wide(name, self.policy, rank))
            }
            PlacementCoverage::Corridor => {
                let from = Self::current_members(&self.from, CorridorSide::From)?;
                let to = Self::current_members(&self.to, CorridorSide::To)?;
                let placement = CreatePlacement::new(name, from, to, self.policy, rank).verified(
                    "both sides were checked non-empty above, the only corridor the vocabulary \
                     refuses",
                );
                Ok(placement)
            }
        }
    }

    /// The names of one side in order, once every member belongs to the draft's domain.
    fn current_members(
        members: &CorridorMembers,
        side: CorridorSide,
    ) -> Result<Vec<ModelName>, Report<PlacementDraftError>> {
        if members.is_empty() {
            return Err(Report::new(PlacementDraftError::MemberRequired { side }));
        }
        let mut names = Vec::with_capacity(members.len());
        for (member, context) in members {
            match context {
                MemberContext::Current => names.push(member.clone()),
                MemberContext::Changed => {
                    return Err(Report::new(PlacementDraftError::MemberChanged {
                        side,
                        member: member.clone(),
                    }));
                }
            }
        }
        Ok(names)
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_models::{
        CreateStatement, Model, ModelKind, PlacementScope, RequestedResourceVersion, Statement,
    };
    use nervix_plexus::client_statement::{ClientStatement, parse_client_statement};

    use super::*;

    fn node(kind: ModelKind, name: &str) -> NodeRef {
        NodeRef::new(
            kind,
            ModelName::parse(name).assured("the test member names are valid"),
        )
    }

    fn corridor(name: &str) -> PlacementDraft {
        PlacementDraft {
            name: name.to_string(),
            coverage: PlacementCoverage::Corridor,
            ..PlacementDraft::default()
        }
    }

    /// The member names of both sides of a built corridor, in their written order.
    #[derive(Debug, PartialEq, Eq)]
    struct CorridorNames<'a> {
        from: Vec<&'a str>,
        to: Vec<&'a str>,
    }

    fn names(placement: &CreatePlacement) -> CorridorNames<'_> {
        let PlacementScope::Corridor { from, to } = &placement.scope else {
            panic!("a corridor draft builds a corridor");
        };
        CorridorNames {
            from: from.iter().map(ModelName::as_str).collect(),
            to: to.iter().map(ModelName::as_str).collect(),
        }
    }

    fn expected<'a>(from: &[&'a str], to: &[&'a str]) -> CorridorNames<'a> {
        CorridorNames {
            from: from.to_vec(),
            to: to.to_vec(),
        }
    }

    #[test]
    fn a_corridor_keeps_the_written_member_order_and_moves_members() {
        let mut draft = corridor("scoring_local");
        draft.add_member(CorridorSide::From, &node(ModelKind::Relay, "input"));
        draft.add_member(CorridorSide::From, &node(ModelKind::Junction, "normalize"));
        draft.add_member(CorridorSide::To, &node(ModelKind::Emitter, "publish"));
        // Selecting a member again keeps its place rather than adding a second one.
        draft.add_member(CorridorSide::From, &node(ModelKind::Relay, "input"));
        draft.policy = PlacementPolicy::SuggestSeparation;
        draft.rank = "3".to_string();

        let built = draft.build().assured("a complete corridor builds");
        assert_eq!(
            names(&built),
            expected(&["input", "normalize"], &["publish"])
        );

        draft.move_member(CorridorSide::From, 1, MemberMove::Earlier);
        draft.move_member(CorridorSide::From, 0, MemberMove::Earlier);
        draft.move_member(CorridorSide::To, 0, MemberMove::Later);
        let moved = draft.build().assured("a reordered corridor builds");
        assert_eq!(
            names(&moved),
            expected(&["normalize", "input"], &["publish"])
        );
        assert_eq!(
            moved
                .to_canonical_plexus()
                .assured("a placement renders canonical Plexus"),
            "CREATE PLACEMENT scoring_local FROM normalize, input TO publish SUGGEST SEPARATION \
             RANK 3;"
        );

        draft.remove_member(CorridorSide::From, 0);
        let removed = draft.build().assured("one FROM member remains");
        assert_eq!(names(&removed), expected(&["input"], &["publish"]));
    }

    #[test]
    fn a_corridor_refuses_an_empty_side_and_a_member_of_a_changed_domain() {
        let mut draft = corridor("refused");
        draft.add_member(CorridorSide::From, &node(ModelKind::Relay, "input"));
        let empty_to = draft.build().expect_err("a corridor needs a TO member");
        assert_eq!(
            *empty_to.current_context(),
            PlacementDraftError::MemberRequired {
                side: CorridorSide::To
            }
        );
        assert_eq!(
            empty_to.current_context().to_string(),
            "Choose at least one TO member"
        );

        draft.add_member(CorridorSide::To, &node(ModelKind::Relay, "output"));
        draft.invalidate_references();
        assert!(!draft.selects_member(CorridorSide::From, &node(ModelKind::Relay, "input")));
        let changed = draft
            .build()
            .expect_err("members of a changed domain must be selected again");
        assert_eq!(
            *changed.current_context(),
            PlacementDraftError::MemberChanged {
                side: CorridorSide::From,
                member: ModelName::parse("input").assured("valid name"),
            }
        );
        assert!(
            changed
                .current_context()
                .to_string()
                .contains("changed context")
        );

        draft.add_member(CorridorSide::From, &node(ModelKind::Relay, "input"));
        draft.add_member(CorridorSide::To, &node(ModelKind::Relay, "output"));
        assert!(draft.selects_member(CorridorSide::From, &node(ModelKind::Relay, "input")));
        assert!(draft.build().is_ok());
    }

    #[test]
    fn domain_wide_coverage_keeps_the_corridor_members_for_later_and_refuses_rank_zero() {
        let mut draft =
            PlacementDraft::starting_from(ModelName::parse("normalize").assured("valid name"));
        assert_eq!(draft.coverage, PlacementCoverage::Corridor);
        draft.name = "everywhere".to_string();
        draft.coverage = PlacementCoverage::DomainWide;
        let domain_wide = draft.build().assured("a domain-wide rule needs no members");
        assert_eq!(domain_wide.scope, PlacementScope::DomainWide);
        assert_eq!(draft.from.len(), 1, "switching coverage keeps the members");

        draft.rank = "0".to_string();
        let rank = draft.build().expect_err("RANK 0 is refused");
        assert_eq!(*rank.current_context(), PlacementDraftError::Rank);
        draft.rank = "18446744073709551616".to_string();
        assert!(draft.build().is_err(), "a rank beyond u64 is refused");
    }

    /// Fills a draft the way an operator does: by name, coverage, the members each side's choice
    /// selected, policy and rank text. A draft keeps a member's name, which is all the rule
    /// writes, so any eligible kind stands in for the choice that selected it.
    fn filled(value: &CreatePlacement) -> PlacementDraft {
        let mut draft = PlacementDraft {
            name: value.name.to_string(),
            policy: value.policy,
            rank: match value.rank {
                Some(rank) => rank.to_string(),
                None => String::new(),
            },
            ..PlacementDraft::default()
        };
        if let PlacementScope::Corridor { from, to } = &value.scope {
            draft.coverage = PlacementCoverage::Corridor;
            for member in from {
                draft.add_member(CorridorSide::From, &NodeRef::new(ModelKind::Relay, member));
            }
            for member in to {
                draft.add_member(CorridorSide::To, &NodeRef::new(ModelKind::Junction, member));
            }
        }
        draft
    }

    #[test]
    fn bolero_placement_drafts_preserve_every_complete_model() {
        bolero::check!()
            .with_iterations(256)
            .with_max_len(1024)
            .for_each(|bytes: &[u8]| {
                let mut arbitrary =
                    nervix_arbitrary::Arbitrary::new(bytes, nervix_arbitrary::Domain::Plexus);
                let value = arbitrary.create_placement();
                let conditional = arbitrary.entropy().flag();
                let mut draft = filled(&value);
                draft.if_not_exists = conditional;
                let actual = draft
                    .build()
                    .assured("every complete current placement is representable by its form");
                assert_eq!(actual, value);
                let statement = Statement::Create(CreateStatement::new(
                    Box::new(Model::<RequestedResourceVersion>::Placement(actual)),
                    conditional,
                ));
                let source = statement
                    .to_canonical_plexus()
                    .assured("every visual placement renders canonical Plexus");
                assert_eq!(
                    parse_client_statement(&source)
                        .assured("canonical placements reparse without losing a clause"),
                    ClientStatement::Server(statement),
                    "{source}"
                );
            });
    }
}
