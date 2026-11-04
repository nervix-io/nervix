//! The in-place changes an `ALTER` makes to a route's construction.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The operations that change one part of a route construction where it stands — its
//!   inheritance, one assignment, one invocation or its output filter — how each applies to a
//!   construction, and why one cannot.
//! - **Depends on.** The route construction Model and the expression vocabulary it holds.
//! - **Must not know.** Which statement or route holds the construction, how a changed
//!   construction is validated against a graph, or how it executes.
//!
//! An ingestor alters the construction of one of its routes and an emitter the construction of
//! its destination; both read the same operations, so a construction changes the same way on
//! either side.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::num::NonZeroU64;

use error_stack::Report;
use meticulous::OptionExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{Assignment, AssignmentTarget, Expression, Inheritance, Invocation, RouteConstruction};

/// One change an `ALTER` makes to a construction where it stands, leaving every other part of the
/// construction as it was.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ConstructionAlteration {
    /// `SET INHERIT …`: what the construction inherits, in place of what it inherited.
    SetInherit { inheritance: Inheritance },
    /// `DROP INHERIT`: the construction inherits nothing.
    DropInherit,
    /// `ADD SET <target> = <expression>`: one assignment after those the construction writes.
    AddSet { assignment: Assignment },
    /// `DROP SET <target>`: the construction no longer writes its assignment to `target`.
    DropSet { target: AssignmentTarget },
    /// `ADD INVOKE <call>`: one invocation after those the construction makes.
    AddInvoke { invocation: Invocation },
    /// `DROP INVOKE <n>`: the construction no longer makes its `n`th invocation, counted from one
    /// in the order it writes them.
    DropInvoke { position: NonZeroU64 },
    /// `SET WHERE <expression>`: the filter the construction applies to what it constructs.
    SetWhere { where_clause: Expression },
    /// `DROP WHERE`: the construction filters nothing.
    DropWhere,
}

/// Why a construction does not take an alteration: the part the alteration drops is not there,
/// or more than one part matches it.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConstructionAlterationError {
    #[error("the construction inherits nothing, so there is no INHERIT to drop")]
    InheritanceNotWritten,
    #[error("the construction writes no assignment to `{target}`")]
    AssignmentNotWritten { target: AssignmentTarget },
    #[error("the construction writes `{target}` more than once, so DROP SET cannot tell which")]
    AssignmentAmbiguous { target: AssignmentTarget },
    #[error("the construction makes {written} invocations, so it has no invocation {position}")]
    InvocationNotWritten {
        position: NonZeroU64,
        written: usize,
    },
    #[error("the construction filters nothing, so there is no WHERE to drop")]
    FilterNotWritten,
}

impl RouteConstruction {
    /// Applies `alteration` to this construction, or leaves the construction as it was and says
    /// why the alteration does not apply.
    pub fn apply_alteration(
        &mut self,
        alteration: &ConstructionAlteration,
    ) -> error_stack::Result<(), ConstructionAlterationError> {
        match alteration {
            ConstructionAlteration::SetInherit { inheritance } => {
                self.inherit = Some(inheritance.clone());
            }
            ConstructionAlteration::DropInherit => {
                if self.inherit.take().is_none() {
                    return Err(Report::new(
                        ConstructionAlterationError::InheritanceNotWritten,
                    ));
                }
            }
            ConstructionAlteration::AddSet { assignment } => {
                self.assignments.push(assignment.clone());
            }
            ConstructionAlteration::DropSet { target } => {
                let index = self.assignment_index(target)?;
                self.assignments.remove(index);
            }
            ConstructionAlteration::AddInvoke { invocation } => {
                self.invocations.push(invocation.clone());
            }
            ConstructionAlteration::DropInvoke { position } => {
                let index = self.invocation_index(*position)?;
                self.invocations.remove(index);
            }
            ConstructionAlteration::SetWhere { where_clause } => {
                self.where_clause = Some(where_clause.clone());
            }
            ConstructionAlteration::DropWhere => {
                if self.where_clause.take().is_none() {
                    return Err(Report::new(ConstructionAlterationError::FilterNotWritten));
                }
            }
        }
        Ok(())
    }

    /// Where the one assignment to `target` stands among those this construction writes.
    ///
    /// The assignments are an ordered sequence a statement writes, so finding one scans what that
    /// statement wrote and nothing larger.
    fn assignment_index(
        &self,
        target: &AssignmentTarget,
    ) -> error_stack::Result<usize, ConstructionAlterationError> {
        let mut found = None;
        for (index, assignment) in self.assignments.iter().enumerate() {
            if assignment.target != *target {
                continue;
            }
            if found.is_some() {
                return Err(Report::new(
                    ConstructionAlterationError::AssignmentAmbiguous {
                        target: target.clone(),
                    },
                ));
            }
            found = Some(index);
        }
        let Some(index) = found else {
            return Err(Report::new(
                ConstructionAlterationError::AssignmentNotWritten {
                    target: target.clone(),
                },
            ));
        };
        Ok(index)
    }

    /// Where the invocation `position` counts to stands among those this construction makes.
    fn invocation_index(
        &self,
        position: NonZeroU64,
    ) -> error_stack::Result<usize, ConstructionAlterationError> {
        let not_written = || {
            Report::new(ConstructionAlterationError::InvocationNotWritten {
                position,
                written: self.invocations.len(),
            })
        };
        // A position too large for an index counts past every invocation a construction can make.
        let Ok(counted) = usize::try_from(position.get()) else {
            return Err(not_written());
        };
        let index = counted
            .checked_sub(1)
            .assured("a position counts from one, so it is at least one");
        if index >= self.invocations.len() {
            return Err(not_written());
        }
        Ok(index)
    }
}

#[cfg(test)]
#[path = "construction_alteration_tests.rs"]
mod tests;
