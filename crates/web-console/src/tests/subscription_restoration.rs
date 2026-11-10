//! Relay-tab restoration through the production tab and reply owners.

use nervix_client_wire::RowBranch;
use nervix_models::{BranchName, FieldName, ParseAsType, SchemaField};

use super::*;

#[test]
fn restoring_a_tab_carries_the_schema_its_acknowledged_generation_announced() {
    Owner::new().with(|| {
        let mut stream = test_stream();
        stream.schema = RowSchema {
            fields: vec![
                SchemaField {
                    name: FieldName::parse("value").assured("a valid field name"),
                    ty: ParseAsType::I32,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: FieldName::parse("secret").assured("a valid field name"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: true,
                },
            ],
            branch: Some(
                RowBranch::new(
                    BranchName::parse("tenant").assured("a valid branch name"),
                    vec![SchemaField {
                        name: FieldName::parse("tenant_id").assured("a valid key field name"),
                        ty: ParseAsType::String,
                        optional: false,
                        sensitive: false,
                    }],
                )
                .assured("the branch declares a nonempty key"),
            ),
        };
        let schema = stream.schema.clone();
        let signals = subscription_signals(SubscriptionTabState::Open(stream));
        interrupt_subscription_tabs(signals);
        let mut starts = signals.begin_restorations();
        let ConsoleRequest::SubscriptionStart {
            request, origin, ..
        } = starts.remove(0)
        else {
            panic!("the interrupted tab restores through a subscribe request");
        };
        assert_eq!(origin, SubscriptionOrigin::Restoration);
        assert_eq!(request.expected_schema, Some(schema));
    });
}

#[test]
fn a_terminal_restoration_refusal_keeps_the_tabs_rows_and_ends_its_attempts() {
    for refusal in [
        SubscriptionRefusal::DomainNotFound,
        SubscriptionRefusal::RelayNotFound,
        SubscriptionRefusal::RelayChanged,
    ] {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Open(test_stream()));
            signals.subscription_tabs.update(|tabs| {
                tabs[0].lines.push(TermLine::output("{\"value\":1}"));
            });
            interrupt_subscription_tabs(signals);
            assert_eq!(signals.begin_restorations().len(), 1);
            let mut requests =
                SessionRequests::new(Some(nervix_client_wire::SessionLimits::DEFAULT));
            apply_subscribe_outcome(
                signals,
                &mut requests,
                SubscriptionStarted {
                    tab_id: 1,
                    statement: "CREATE SUBSCRIPTION live TO orders;",
                    origin: SubscriptionOrigin::Restoration,
                },
                SubscribeOutcome {
                    disposition: SubscribeDisposition::Refused(refusal),
                    message: "the relay contract cannot be restored".to_string(),
                    diagnostics: Vec::new(),
                },
            );
            signals.subscription_tabs.with_untracked(|tabs| {
                assert!(matches!(tabs[0].state, SubscriptionTabState::Ended));
                assert_eq!(
                    tabs[0].lines.clone().into_lines()[0].line.text,
                    "{\"value\":1}"
                );
                assert!(tabs[0].state.can_resubscribe());
            });
            interrupt_subscription_tabs(signals);
            assert!(signals.begin_restorations().is_empty());
            assert_eq!(signals.active_subscription_tab.get_untracked(), Some(1));
        });
    }
}

#[test]
fn a_restoration_refusal_preserves_the_operators_repl_history() {
    Owner::new().with(|| {
        let signals = subscription_signals(SubscriptionTabState::Open(test_stream()));
        signals.terminal_lines.update(|lines| {
            lines.push(TermLine::output("the operator's own output"));
        });
        interrupt_subscription_tabs(signals);
        assert_eq!(signals.begin_restorations().len(), 1);
        let mut requests = SessionRequests::new(Some(nervix_client_wire::SessionLimits::DEFAULT));
        apply_subscribe_outcome(
            signals,
            &mut requests,
            SubscriptionStarted {
                tab_id: 1,
                statement: "CREATE SUBSCRIPTION live TO orders;",
                origin: SubscriptionOrigin::Restoration,
            },
            SubscribeOutcome {
                disposition: SubscribeDisposition::Refused(SubscriptionRefusal::Unavailable),
                message: "the serving node could not open the subscription now".to_string(),
                diagnostics: Vec::new(),
            },
        );
        let history = signals.terminal_lines.get_untracked().into_lines();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].line.text, "the operator's own output");
    });
}

#[test]
fn a_refused_restoration_waits_before_another_attempt_can_be_issued() {
    Owner::new().with(|| {
        let signals = subscription_signals(SubscriptionTabState::Open(test_stream()));
        interrupt_subscription_tabs(signals);
        assert_eq!(signals.begin_restorations().len(), 1);
        let mut requests = SessionRequests::new(Some(nervix_client_wire::SessionLimits::DEFAULT));
        apply_subscribe_outcome(
            signals,
            &mut requests,
            SubscriptionStarted {
                tab_id: 1,
                statement: "CREATE SUBSCRIPTION live TO orders;",
                origin: SubscriptionOrigin::Restoration,
            },
            SubscribeOutcome {
                disposition: SubscribeDisposition::Refused(SubscriptionRefusal::Unavailable),
                message: "the serving node could not open the subscription now".to_string(),
                diagnostics: Vec::new(),
            },
        );
        assert!(signals.begin_restorations().is_empty());
        assert!(signals.begin_restorations().is_empty());
    });
}

#[test]
fn transient_refusals_back_off_to_the_ceiling_without_issuing_early_or_in_flight() {
    for refusal in [
        SubscriptionRefusal::InTransaction,
        SubscriptionRefusal::InvalidStatement,
        SubscriptionRefusal::NameInUse,
        SubscriptionRefusal::InvalidFilter,
        SubscriptionRefusal::Unavailable,
    ] {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Open(test_stream()));
            interrupt_subscription_tabs(signals);
            assert_eq!(signals.begin_restorations().len(), 1);
            let mut requests =
                SessionRequests::new(Some(nervix_client_wire::SessionLimits::DEFAULT));
            for delay in [1, 2, 4, 5, 5] {
                apply_subscribe_outcome(
                    signals,
                    &mut requests,
                    SubscriptionStarted {
                        tab_id: 1,
                        statement: "CREATE SUBSCRIPTION live TO orders;",
                        origin: SubscriptionOrigin::Restoration,
                    },
                    SubscribeOutcome {
                        disposition: SubscribeDisposition::Refused(refusal),
                        message: "not available on this attempt".to_string(),
                        diagnostics: Vec::new(),
                    },
                );
                signals.subscription_tabs.with_untracked(|tabs| {
                    let SubscriptionTabState::RetryWaiting { wait, .. } = &tabs[0].state else {
                        panic!("a transient refusal retains the desired contract and wait");
                    };
                    assert_eq!(wait.delay, Duration::from_secs(delay));
                    assert_eq!(tabs[0].state.label(), "interrupted");
                    assert!(!tabs[0].state.can_resubscribe());
                });
                assert!(signals.begin_restorations().is_empty());
                for _ in 0..delay {
                    signals.subscription_retry_tick.update(RetryTick::advance);
                    assert!(signals.begin_restorations().is_empty());
                }
                signals.subscription_retry_tick.update(RetryTick::advance);
                assert_eq!(signals.begin_restorations().len(), 1);
                assert!(signals.begin_restorations().is_empty());
            }
            assert!(
                signals
                    .terminal_lines
                    .get_untracked()
                    .into_lines()
                    .is_empty()
            );
        });
    }
}

#[test]
fn closing_a_retry_wait_removes_the_tab_and_releases_its_name() {
    Owner::new().with(|| {
        let signals = subscription_signals(SubscriptionTabState::Restoring(test_restoration()));
        fail_subscription_start(
            signals,
            1,
            SubscriptionOrigin::Restoration,
            None,
            vec![TermLine::error("request cancelled")],
        );
        assert!(signals.begin_subscription_close(1).is_none());
        assert!(signals.subscription_tabs.get_untracked().is_empty());
        assert_eq!(signals.active_subscription_tab.get_untracked(), None);
        for _ in 0..10 {
            signals.subscription_retry_tick.update(RetryTick::advance);
        }
        assert!(signals.begin_restorations().is_empty());
    });
}

#[test]
fn reconnect_retries_a_waiting_contract_immediately_and_resets_its_delay() {
    Owner::new().with(|| {
        let stream = test_stream();
        let schema = stream.schema.clone();
        let signals = subscription_signals(SubscriptionTabState::Open(stream));
        interrupt_subscription_tabs(signals);
        assert_eq!(signals.begin_restorations().len(), 1);
        fail_subscription_start(
            signals,
            1,
            SubscriptionOrigin::Restoration,
            None,
            vec![TermLine::error("request cancelled")],
        );
        assert!(signals.begin_restorations().is_empty());
        interrupt_subscription_tabs(signals);
        let starts = signals.begin_restorations();
        let [ConsoleRequest::SubscriptionStart { request, .. }] = starts.as_slice() else {
            panic!("the replacement session immediately restores the desired tab");
        };
        assert_eq!(request.expected_schema, Some(schema));
        fail_subscription_start(
            signals,
            1,
            SubscriptionOrigin::Restoration,
            None,
            vec![TermLine::error("request cancelled")],
        );
        signals.subscription_tabs.with_untracked(|tabs| {
            let SubscriptionTabState::RetryWaiting { wait, .. } = &tabs[0].state else {
                panic!("an unanswered replacement still keeps its retry policy");
            };
            assert_eq!(wait.delay, SUBSCRIPTION_RETRY_DELAY);
        });
    });
}

#[test]
fn a_late_failed_restoration_of_a_closing_tab_does_not_enter_the_repl() {
    Owner::new().with(|| {
        let signals = subscription_signals(SubscriptionTabState::Restoring(test_restoration()));
        assert!(signals.begin_subscription_close(1).is_none());
        fail_subscription_start(
            signals,
            1,
            SubscriptionOrigin::Restoration,
            Some(SubscriptionRefusal::Unavailable),
            vec![TermLine::error(
                "the serving node could not reopen the subscription",
            )],
        );
        assert!(signals.subscription_tabs.get_untracked().is_empty());
        assert!(
            signals
                .terminal_lines
                .get_untracked()
                .into_lines()
                .is_empty()
        );
    });
}
