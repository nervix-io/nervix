//! Native rendering, typed-choice and submission checks for the window processor and correlator
//! forms.
//!
//! Layer: test harness, outside product layer order.
//! - **Owns.** Focused checks of the rendered window and correlator controls, the scoped questions
//!   their pickers ask, the references those pickers insert, and their canonical submissions.
//! - **Depends on.** Processor drafts, typed choices and the create dialog component.
//! - **Must not know.** Browser transport, registry validation or runtime processing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use leptos::prelude::*;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{ChoiceTarget, ChoiceValue};
use nervix_models::{CorrelatorMatchPolicy, DomainName, FieldName, ModelKind, ModelName, NodeRef};

use super::{
    ChoiceControl, CreateDialog, CreateDialogProps, CreateDispatch, CreateKind, CreateSignals,
    ingestor_route_draft::{FlushDraft, MessageErrorDraft},
    processor_correlator_draft::CorrelationSide,
    select_choice, selected_choice,
};

fn node(kind: ModelKind, name: &str) -> NodeRef {
    NodeRef::new(kind, ModelName::parse(name).assured("test name is valid"))
}

fn field(name: &str) -> FieldName {
    FieldName::parse(name).assured("test field is valid")
}

fn orders() -> DomainName {
    DomainName::parse("orders").assured("valid domain")
}

fn render(signals: CreateSignals) -> String {
    let dialog = CreateDialog(
        CreateDialogProps::builder()
            .signals(signals)
            .active_domain(RwSignal::new(Some(orders())))
            .connection_state(RwSignal::new(super::super::ConsoleConnectionState::Waiting))
            .session_generation(RwSignal::new(1))
            .request_tx(RwSignal::new(None))
            .submit(super::ignore_test_submission)
            .build(),
    );
    any_spawner::Executor::poll_local();
    dialog.to_html()
}

/// The dependency the question for `control` names after its domain, as the session receives it.
fn question(signals: CreateSignals, control: ChoiceControl) -> (ChoiceTarget, ChoiceValue) {
    let query = signals
        .choice_query(control)
        .assured("the control has its prerequisites");
    let dependency = query
        .dependencies
        .get(1)
        .assured("a scoped question names its model after the domain")
        .value
        .clone();
    (query.target, dependency)
}

fn missing(signals: CreateSignals, control: ChoiceControl) -> &'static str {
    signals
        .choice_query(control)
        .err()
        .assured("the control lacks a prerequisite")
}

#[test]
fn window_and_correlator_forms_render_their_own_sections_and_omit_what_their_routes_reject() {
    super::super::initialize_test_executor();
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.open(
            CreateKind::WindowProcessor,
            Some(orders()),
            "global-create-button",
        );
        signals.window.update(|draft| {
            draft.choose_unbranched();
            if let Some(window) = draft.window_mut() {
                window.state_size = Some("1MiB".into());
            }
        });
        let window = render(signals);
        for control in [
            "create-processor-input-list",
            "create-processor-filter",
            "create-processor-window",
            "create-window-width-messages",
            "create-window-width-duration",
            "create-window-step-messages",
            "create-window-step-duration",
            "create-window-state-limit",
            "create-window-state-size",
            "create-processor-state",
            "create-processor-route-relay",
            "create-processor-output-fields",
            "create-window-aggregate",
            "create-window-input-fields",
            "create-processor-message-error",
        ] {
            assert!(window.contains(control), "window: {control} must render");
        }
        for control in [
            "create-processor-inherit",
            "create-processor-invocations",
            "create-processor-flush",
            "create-processor-right-input",
            "create-processor-correlation",
            "create-processor-keys",
        ] {
            assert!(
                !window.contains(control),
                "window: {control} must not render"
            );
        }

        signals.open(
            CreateKind::Correlator,
            Some(orders()),
            "global-create-button",
        );
        signals.correlator.update(|draft| {
            draft.choose_unbranched();
            if let Some(correlator) = draft.correlator_mut() {
                correlator.left_timeout.choose_send();
                correlator.right_timeout.choose_send();
            }
        });
        let correlator = render(signals);
        for control in [
            "create-processor-input-list",
            "create-processor-right-input-list",
            "create-processor-right-input",
            "create-processor-right-collect",
            "create-processor-correlation",
            "create-correlate-left-fields",
            "create-correlate-right-fields",
            "create-correlate-where",
            "create-correlator-match",
            "create-correlator-max-time",
            "create-correlator-left-timeout-relay",
            "create-correlator-right-timeout-relay",
            "create-route-left-fields",
            "create-route-right-fields",
            "create-processor-flush",
            "create-processor-message-error",
        ] {
            assert!(
                correlator.contains(control),
                "correlator: {control} must render"
            );
        }
        for control in [
            "create-processor-filter",
            "create-processor-inherit",
            "create-processor-invocations",
            "create-processor-window",
            "create-window-aggregate",
        ] {
            assert!(
                !correlator.contains(control),
                "correlator: {control} must not render"
            );
        }
    });
}

#[test]
fn correlator_choices_ask_for_each_side_and_its_timeout_by_that_side_s_first_relay() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.open(
            CreateKind::Correlator,
            Some(orders()),
            "global-create-button",
        );
        assert_eq!(
            missing(signals, ChoiceControl::ProcessorRightInputRelay),
            "Choose the correlator branch before its left and right relays"
        );
        signals.correlator.update(|draft| draft.choose_branched());
        select_choice(
            signals,
            ChoiceControl::ProcessorBranch,
            ChoiceValue::Model(node(ModelKind::Branch, "by_tenant")),
        );
        for control in [
            ChoiceControl::ProcessorInputRelay,
            ChoiceControl::ProcessorRightInputRelay,
        ] {
            assert_eq!(
                question(signals, control),
                (
                    ChoiceTarget::IngestBranchedRelay,
                    ChoiceValue::Model(node(ModelKind::Branch, "by_tenant"))
                ),
                "{control:?}"
            );
        }
        assert_eq!(
            missing(signals, ChoiceControl::ProcessorLeftTimeoutRelay),
            "Choose the first LEFT FROM relay before the left timeout relay"
        );
        assert_eq!(
            missing(signals, ChoiceControl::ProcessorCorrelateRightField),
            "Choose the first RIGHT FROM relay before its fields"
        );

        select_choice(
            signals,
            ChoiceControl::ProcessorInputRelay,
            ChoiceValue::Model(node(ModelKind::Relay, "orders")),
        );
        select_choice(
            signals,
            ChoiceControl::ProcessorRightInputRelay,
            ChoiceValue::Model(node(ModelKind::Relay, "payments")),
        );
        signals.correlator.update(|draft| {
            if let Some(correlator) = draft.correlator_mut() {
                correlator.right.add();
            }
        });
        assert_eq!(
            question(signals, ChoiceControl::ProcessorRightInputRelay),
            (
                ChoiceTarget::ProcessorCompatibleInputRelay,
                ChoiceValue::Model(node(ModelKind::Relay, "payments"))
            )
        );
        for (control, relay) in [
            (ChoiceControl::ProcessorLeftTimeoutRelay, "orders"),
            (ChoiceControl::ProcessorRightTimeoutRelay, "payments"),
        ] {
            assert_eq!(
                question(signals, control),
                (
                    ChoiceTarget::ProcessorCompatibleInputRelay,
                    ChoiceValue::Model(node(ModelKind::Relay, relay))
                ),
                "{control:?}"
            );
        }
        for (control, relay) in [
            (ChoiceControl::ProcessorCorrelateLeftField, "orders"),
            (ChoiceControl::ProcessorCorrelateRightField, "payments"),
        ] {
            assert_eq!(
                question(signals, control),
                (
                    ChoiceTarget::RelayField,
                    ChoiceValue::Model(node(ModelKind::Relay, relay))
                ),
                "{control:?}"
            );
        }
        for control in [
            ChoiceControl::ProcessorRouteLeftField,
            ChoiceControl::ProcessorRouteRightField,
        ] {
            assert_eq!(
                missing(signals, control),
                "Add an output assignment before a field to assign",
                "{control:?}"
            );
        }
        signals.correlator.update(|draft| {
            if let Some(route) = draft.active_route_mut() {
                route.add_assignment(field("amount"));
            }
        });
        assert_eq!(
            question(signals, ChoiceControl::ProcessorRouteRightField),
            (
                ChoiceTarget::RelayField,
                ChoiceValue::Model(node(ModelKind::Relay, "payments"))
            )
        );
    });
}

#[test]
fn relay_pickers_hold_the_right_input_and_each_side_s_timeout_relay_they_selected() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.open(
            CreateKind::Correlator,
            Some(orders()),
            "global-create-button",
        );
        signals.correlator.update(|draft| draft.choose_unbranched());
        let payments = ChoiceValue::Model(node(ModelKind::Relay, "payments"));
        let unpaid = ChoiceValue::Model(node(ModelKind::Relay, "unpaid"));
        let unclaimed = ChoiceValue::Model(node(ModelKind::Relay, "unclaimed"));
        select_choice(
            signals,
            ChoiceControl::ProcessorRightInputRelay,
            payments.clone(),
        );
        signals.correlator.update(|draft| {
            if let Some(correlator) = draft.correlator_mut() {
                correlator.timeout_mut(CorrelationSide::Left).choose_send();
                correlator.timeout_mut(CorrelationSide::Right).choose_send();
            }
        });
        select_choice(
            signals,
            ChoiceControl::ProcessorLeftTimeoutRelay,
            unpaid.clone(),
        );
        select_choice(
            signals,
            ChoiceControl::ProcessorRightTimeoutRelay,
            unclaimed.clone(),
        );

        let draft = signals.correlator.get_untracked();
        let correlator = draft
            .correlator()
            .assured("a correlator draft holds its correlation");
        let right = correlator.right.current_first().map(|relay| relay.as_str());
        assert_eq!(right, Some("payments"));
        let left_timeout = correlator
            .left_timeout
            .current_relay()
            .map(|relay| relay.as_str());
        assert_eq!(left_timeout, Some("unpaid"));
        let right_timeout = correlator
            .right_timeout
            .current_relay()
            .map(|relay| relay.as_str());
        assert_eq!(right_timeout, Some("unclaimed"));

        // Each relay picker shows only its own selection; a field picker never holds one.
        for (control, held, other) in [
            (ChoiceControl::ProcessorRightInputRelay, &payments, &unpaid),
            (
                ChoiceControl::ProcessorLeftTimeoutRelay,
                &unpaid,
                &unclaimed,
            ),
            (
                ChoiceControl::ProcessorRightTimeoutRelay,
                &unclaimed,
                &payments,
            ),
        ] {
            assert!(selected_choice(signals, control, held), "{control:?}");
            assert!(!selected_choice(signals, control, other), "{control:?}");
        }
        assert!(!selected_choice(
            signals,
            ChoiceControl::ProcessorRightInputRelay,
            &ChoiceValue::Model(node(ModelKind::Schema, "payments")),
        ));
        assert!(!selected_choice(
            signals,
            ChoiceControl::ProcessorCorrelateLeftField,
            &ChoiceValue::Field(field("order_id")),
        ));
    });
}

#[test]
fn scoped_pickers_insert_their_references_where_the_operator_writes() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.open(
            CreateKind::Correlator,
            Some(orders()),
            "global-create-button",
        );
        signals.correlator.update(|draft| draft.choose_unbranched());
        select_choice(
            signals,
            ChoiceControl::ProcessorInputRelay,
            ChoiceValue::Model(node(ModelKind::Relay, "orders")),
        );
        select_choice(
            signals,
            ChoiceControl::ProcessorRightInputRelay,
            ChoiceValue::Model(node(ModelKind::Relay, "payments")),
        );
        select_choice(
            signals,
            ChoiceControl::ProcessorCorrelateLeftField,
            ChoiceValue::Field(field("order_id")),
        );
        signals.correlator.update(|draft| {
            if let Some(correlator) = draft.correlator_mut() {
                correlator.correlate_where.push_str(" =");
            }
        });
        select_choice(
            signals,
            ChoiceControl::ProcessorCorrelateRightField,
            ChoiceValue::Field(field("order_id")),
        );
        signals.correlator.update(|draft| {
            if let Some(route) = draft.active_route_mut() {
                route.add_assignment(field("order_id"));
                route.add_assignment(field("amount"));
            }
        });
        select_choice(
            signals,
            ChoiceControl::ProcessorRouteRightField,
            ChoiceValue::Field(field("amount")),
        );
        let draft = signals.correlator.get_untracked();
        assert_eq!(
            draft
                .correlator()
                .assured("a correlator draft holds its correlation")
                .correlate_where,
            "left.order_id = right.order_id"
        );
        let route = draft.active_route().assured("the route exists");
        assert_eq!(route.assignments[0].expression, "");
        assert_eq!(route.assignments[1].expression, "right.amount");

        signals.open(
            CreateKind::WindowProcessor,
            Some(orders()),
            "global-create-button",
        );
        signals.window.update(|draft| draft.choose_unbranched());
        select_choice(
            signals,
            ChoiceControl::ProcessorInputRelay,
            ChoiceValue::Model(node(ModelKind::Relay, "latencies")),
        );
        assert_eq!(
            missing(signals, ChoiceControl::ProcessorAggregateField),
            "Add an output assignment before an aggregate argument"
        );
        signals.window.update(|draft| {
            if let Some(route) = draft.active_route_mut() {
                route.add_assignment(field("slowest"));
            }
        });
        assert_eq!(
            missing(signals, ChoiceControl::ProcessorAggregateField),
            "Enter the aggregate function, such as COUNT, before choosing its input field"
        );
        signals.window.update(|draft| {
            if let Some(window) = draft.window_mut() {
                window.aggregate = "MAX".into();
            }
        });
        assert_eq!(
            question(signals, ChoiceControl::ProcessorAggregateField),
            (
                ChoiceTarget::RelayField,
                ChoiceValue::Model(node(ModelKind::Relay, "latencies"))
            )
        );
        select_choice(
            signals,
            ChoiceControl::ProcessorAggregateField,
            ChoiceValue::Field(field("latency")),
        );
        signals.window.update(|draft| {
            if let Some(route) = draft.active_route_mut() {
                route.assignments[0].expression.push_str(" -");
            }
            if let Some(window) = draft.window_mut() {
                window.aggregate = "min".into();
            }
        });
        select_choice(
            signals,
            ChoiceControl::ProcessorAggregateField,
            ChoiceValue::Field(field("latency")),
        );
        assert_eq!(
            signals
                .window
                .get_untracked()
                .active_route()
                .assured("the route exists")
                .assignments[0]
                .expression,
            "max(input.latency) - min(input.latency)"
        );
    });
}

#[test]
fn window_and_correlator_submissions_render_their_canonical_statements() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.open(
            CreateKind::WindowProcessor,
            Some(orders()),
            "global-create-button",
        );
        signals.window.update(|draft| {
            draft.name = "latency_windows".into();
            draft.choose_unbranched();
            draft.select_input(&node(ModelKind::Relay, "latencies"));
            if let Some(window) = draft.window_mut() {
                window.width.duration = "5m".into();
                window.step.duration = "1m".into();
                window.state_size = Some("1MiB".into());
            }
            let route = draft.active_route_mut().assured("the route exists");
            route.select_relay(&node(ModelKind::Relay, "latency_summary"));
            route.add_assignment(field("samples"));
            route.assignments[0].expression = "COUNT(input.latency) + 1".into();
            route.where_clause = "output.samples > 0".into();
            route.message_error = MessageErrorDraft::Log;
        });
        let submission = signals.submission().assured("the window draft is complete");
        assert_eq!(submission.kind, CreateKind::WindowProcessor);
        assert!(
            submission.presentation.contains(
                "CREATE WINDOW PROCESSOR latency_windows\n  FROM latencies\n  ATTACHED\n  WIDTH \
                 5m DURATION\n  STEP 1m DURATION\n  MAX STATE SIZE 1048576B\n  UNBRANCHED\n  TO \
                 latency_summary\n    SET samples = count(input.latency) + 1\n    WHERE \
                 output.samples > 0\n    ON MESSAGE ERROR LOG;"
            ),
            "{}",
            submission.presentation
        );
        let CreateDispatch::Command(command) = submission.dispatch else {
            panic!("a window processor is created through the durable command path");
        };
        assert_eq!(command.domain, Some(orders()));

        signals.open(
            CreateKind::Correlator,
            Some(orders()),
            "global-create-button",
        );
        signals.correlator.update(|draft| {
            draft.name = "paid_orders".into();
            draft.choose_unbranched();
            draft.select_input(&node(ModelKind::Relay, "orders"));
            if let Some(correlator) = draft.correlator_mut() {
                correlator.select_right_input(&node(ModelKind::Relay, "payments"));
                correlator.correlate_where = "left.order_id = right.order_id".into();
                correlator.match_policy = Some(CorrelatorMatchPolicy::Latest);
                correlator.max_time = "30s".into();
                correlator.left_timeout.choose_send();
                correlator
                    .left_timeout
                    .select_relay(&node(ModelKind::Relay, "unpaid_orders"));
                correlator.right_timeout.choose_drop();
            }
            let route = draft.active_route_mut().assured("the route exists");
            route.select_relay(&node(ModelKind::Relay, "paid"));
            route.add_assignment(field("amount"));
            route.assignments[0].expression = "right.amount".into();
            route.flush = FlushDraft::Immediate;
            route.message_error = MessageErrorDraft::Log;
        });
        let submission = signals
            .submission()
            .assured("the correlator draft is complete");
        assert_eq!(submission.kind, CreateKind::Correlator);
        assert!(
            submission.presentation.contains(
                "CREATE CORRELATOR paid_orders\n  LEFT FROM orders\n  RIGHT FROM payments\n  \
                 ATTACHED\n  CORRELATE WHERE left.order_id = right.order_id\n  MATCH LATEST\n  \
                 MAX TIME 30s\n  ON CORRELATION TIMEOUT SEND TO unpaid_orders, DROP\n  \
                 UNBRANCHED\n  TO paid\n    SET amount = right.amount\n    FLUSH IMMEDIATE\n    \
                 ON MESSAGE ERROR LOG;"
            ),
            "{}",
            submission.presentation
        );
    });
}

#[test]
fn a_changed_domain_requires_right_inputs_and_timeout_relays_again() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.open(
            CreateKind::Correlator,
            Some(orders()),
            "global-create-button",
        );
        signals.correlator.update(|draft| {
            draft.choose_unbranched();
            draft.select_input(&node(ModelKind::Relay, "orders"));
            if let Some(correlator) = draft.correlator_mut() {
                correlator.select_right_input(&node(ModelKind::Relay, "payments"));
                for side in [CorrelationSide::Left, CorrelationSide::Right] {
                    let timeout = correlator.timeout_mut(side);
                    timeout.choose_send();
                    timeout.select_relay(&node(ModelKind::Relay, "unmatched"));
                }
            }
        });
        signals.change_scope(Some(DomainName::parse("billing").assured("valid domain")));
        let draft = signals.correlator.get_untracked();
        let correlator = draft
            .correlator()
            .assured("a correlator draft holds its correlation");
        assert_eq!(correlator.right.current_first(), None);
        assert_eq!(correlator.left_timeout.current_relay(), None);
        assert_eq!(correlator.right_timeout.current_relay(), None);
        assert_eq!(draft.current_first_input(), None);
    });
}
