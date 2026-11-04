//! Construction alterations applied to a construction in place.
//!
//! Layer: test harness.
//! - **Owns.** What each alteration changes in a construction, what it leaves as it was, and why
//!   an alteration that names an absent or ambiguous part is refused.
//! - **Depends on.** The construction alteration and route construction Models.
//! - **Must not know.** The grammar, the registry or the statement that holds a construction.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use meticulous::ResultExt as _;

use super::*;
use crate::{AssignmentTargetScope, BuiltinFunctionName, FieldName, Literal};

fn field(name: &str) -> FieldName {
    FieldName::parse(name).assured("the test name satisfies the field name grammar")
}

fn assignment(target: &str, value: i64) -> Assignment {
    Assignment {
        target: AssignmentTarget::bare(field(target)),
        value: Expression::Literal(Literal::I64(value)),
    }
}

fn invocation(argument: i64) -> Invocation {
    Invocation {
        function: BuiltinFunctionName::parse("write_header")
            .assured("write_header is a builtin function name"),
        arguments: vec![Expression::Literal(Literal::I64(argument))],
    }
}

/// A construction that inherits everything, writes `a` and `b`, filters and invokes twice.
fn written() -> RouteConstruction {
    RouteConstruction {
        inherit: Some(Inheritance::All),
        assignments: vec![assignment("a", 1), assignment("b", 2)],
        where_clause: Some(Expression::Literal(Literal::Bool(true))),
        invocations: vec![invocation(1), invocation(2)],
    }
}

fn altered(
    alteration: ConstructionAlteration,
) -> error_stack::Result<RouteConstruction, ConstructionAlterationError> {
    let mut construction = written();
    construction.apply_alteration(&alteration)?;
    Ok(construction)
}

fn refusal(alteration: ConstructionAlteration) -> ConstructionAlterationError {
    let mut construction = written();
    let report = construction
        .apply_alteration(&alteration)
        .expect_err("the alteration names a part the construction lacks");
    assert_eq!(
        construction,
        written(),
        "a refused alteration changes nothing"
    );
    report.current_context().clone()
}

#[test]
fn set_inherit_replaces_only_the_inheritance() {
    let inheritance = Inheritance::AllExcept(vec![field("secret")]);
    let construction = altered(ConstructionAlteration::SetInherit {
        inheritance: inheritance.clone(),
    })
    .assured("setting an inheritance always applies");
    assert_eq!(
        construction,
        RouteConstruction {
            inherit: Some(inheritance),
            ..written()
        }
    );
}

#[test]
fn add_set_appends_after_the_written_assignments() {
    let construction = altered(ConstructionAlteration::AddSet {
        assignment: assignment("c", 3),
    })
    .assured("adding an assignment always applies");
    assert_eq!(
        construction.assignments,
        vec![assignment("a", 1), assignment("b", 2), assignment("c", 3)]
    );
    assert_eq!(construction.invocations, written().invocations);
}

#[test]
fn drop_set_removes_only_the_named_assignment() {
    let construction = altered(ConstructionAlteration::DropSet {
        target: AssignmentTarget::bare(field("a")),
    })
    .assured("the construction writes a");
    assert_eq!(construction.assignments, vec![assignment("b", 2)]);
}

#[test]
fn drop_set_tells_a_scoped_target_from_the_bare_one() {
    let target = AssignmentTarget {
        scope: AssignmentTargetScope::Output,
        field: field("a"),
    };
    assert_eq!(
        refusal(ConstructionAlteration::DropSet {
            target: target.clone()
        }),
        ConstructionAlterationError::AssignmentNotWritten { target }
    );
}

#[test]
fn drop_set_refuses_a_target_written_twice() {
    let mut construction = written();
    construction.assignments.push(assignment("a", 3));
    let target = AssignmentTarget::bare(field("a"));
    let report = construction
        .apply_alteration(&ConstructionAlteration::DropSet {
            target: target.clone(),
        })
        .expect_err("the construction writes a twice");
    assert_eq!(
        report.current_context(),
        &ConstructionAlterationError::AssignmentAmbiguous { target }
    );
}

#[test]
fn invocations_are_added_last_and_dropped_by_their_position() {
    let added = altered(ConstructionAlteration::AddInvoke {
        invocation: invocation(3),
    })
    .assured("adding an invocation always applies");
    assert_eq!(
        added.invocations,
        vec![invocation(1), invocation(2), invocation(3)]
    );

    let first = NonZeroU64::MIN;
    let dropped = altered(ConstructionAlteration::DropInvoke { position: first })
        .assured("the construction makes a first invocation");
    assert_eq!(dropped.invocations, vec![invocation(2)]);
}

#[test]
fn drop_invoke_refuses_a_position_past_the_written_invocations() {
    let position = NonZeroU64::new(3).assured("three is not zero");
    assert_eq!(
        refusal(ConstructionAlteration::DropInvoke { position }),
        ConstructionAlterationError::InvocationNotWritten {
            position,
            written: 2
        }
    );
    assert_eq!(
        refusal(ConstructionAlteration::DropInvoke {
            position: NonZeroU64::MAX
        }),
        ConstructionAlterationError::InvocationNotWritten {
            position: NonZeroU64::MAX,
            written: 2
        }
    );
}

#[test]
fn where_is_set_and_dropped() {
    let filter = Expression::Literal(Literal::Bool(false));
    let set = altered(ConstructionAlteration::SetWhere {
        where_clause: filter.clone(),
    })
    .assured("setting a filter always applies");
    assert_eq!(set.where_clause, Some(filter));

    let dropped = altered(ConstructionAlteration::DropWhere).assured("the construction filters");
    assert_eq!(dropped.where_clause, None);
    assert_eq!(dropped.assignments, written().assignments);
}

#[test]
fn dropping_an_absent_inheritance_or_filter_is_refused() {
    let mut bare = RouteConstruction::default();
    let inherit = bare
        .apply_alteration(&ConstructionAlteration::DropInherit)
        .expect_err("an empty construction inherits nothing");
    assert_eq!(
        inherit.current_context(),
        &ConstructionAlterationError::InheritanceNotWritten
    );
    let filter = bare
        .apply_alteration(&ConstructionAlteration::DropWhere)
        .expect_err("an empty construction filters nothing");
    assert_eq!(
        filter.current_context(),
        &ConstructionAlterationError::FilterNotWritten
    );
    assert_eq!(bare, RouteConstruction::default());
}
