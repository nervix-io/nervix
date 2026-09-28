//! Form state and rendering tests for the visual create dialog.
//!
//! Layer: edges.
//!
//! - **Owns.** Direct tests of create form state, choices, and rendering.
//! - **Depends on.** Browser draft Models and presentation components.
//! - **Must not know.** Connector execution or cluster placement.

use futures_channel::mpsc::unbounded;
use leptos::prelude::{GetUntracked as _, Owner, RenderHtml as _, RwSignal, Set as _, Update as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    Choice, ChoiceOutcome, ChoicePresentation, ChoiceStatus, ChoiceTarget, ChoiceValue,
    DomainPaceChoice,
};
use nervix_models::{
    AvroType, DomainName, FieldName, JsonType, MaterializedRelayState, ModelKind, ModelName,
    NodeRef, ParseAsType, PlacementPolicy, RelayName, SchemaName, SubscriptionDeliveryBehavior,
    WireSchemaStrictness,
};

use super::{
    super::ConsoleRequest,
    ChoiceControl, ChoiceGroup, ChoiceGroupProps, ChoiceLoad, ChoiceRequestContext, CreateDialog,
    CreateDialogProps, CreateDispatch, CreateDraftError, CreateKind, CreateMenu, CreateMenuProps,
    CreateProgress, CreateSignals, CreateSubmission, DomainDraft, ResourceDraft, SelectedReference,
    UserDraft, open_form_controls, request_choices,
    schema_draft::{SchemaFieldDraft, SchemaTypeDraft, WireFieldDraft, WireFieldType},
    select_choice, selected_choice,
};

fn domain(name: &str) -> DomainName {
    DomainName::parse(name).assured("the test domain names are valid")
}

fn node(kind: ModelKind, name: &str) -> NodeRef {
    NodeRef::new(
        kind,
        ModelName::parse(name).assured("the test names are valid"),
    )
}

fn command_query(submission: &CreateSubmission) -> &str {
    let CreateDispatch::Command(command) = &submission.dispatch else {
        panic!("the submission runs on the durable command path");
    };
    &command.query
}

fn command_domain(submission: &CreateSubmission) -> Option<&DomainName> {
    let CreateDispatch::Command(command) = &submission.dispatch else {
        panic!("the submission runs on the durable command path");
    };
    command.domain.as_ref()
}

fn outcome(value: ChoiceValue, label: &str) -> ChoiceOutcome {
    ChoiceOutcome {
        status: ChoiceStatus::Ready,
        choices: vec![Choice {
            value,
            presentation: ChoicePresentation {
                label: label.to_string(),
                detail: None,
                group: None,
            },
        }],
        page_cursor: None,
    }
}

#[test]
fn typed_drafts_render_the_canonical_statements_the_dispatcher_sends() {
    let domain_draft = DomainDraft {
        name: "orders".to_string(),
        if_not_exists: true,
        placement: PlacementPolicy::PreferColocation,
        ..DomainDraft::default()
    };
    let submission = domain_draft
        .submission()
        .assured("the domain draft is valid");
    assert_eq!(
        command_query(&submission),
        "CREATE IF NOT EXISTS UNPACED DOMAIN orders PLACEMENT PREFER COLOCATION;"
    );

    let resource = ResourceDraft {
        name: "bundle".to_string(),
        if_not_exists: false,
    };
    let scope = domain("orders");
    let submission = resource
        .submission(Some(scope.clone()))
        .assured("the resource draft and scope are valid");
    assert_eq!(command_query(&submission), "CREATE RESOURCE bundle;");
    assert_eq!(command_domain(&submission), Some(&scope));
}

#[test]
fn credential_presentation_is_masked_while_the_submitted_statement_keeps_the_secret() {
    let user = UserDraft {
        name: "operator".to_string(),
        password: "it's-secret".to_string(),
        if_not_exists: false,
    };
    let submission = user.submission().assured("the user draft is valid");
    assert!(command_query(&submission).contains("it's-secret"));
    assert!(!submission.presentation.contains("it's-secret"));
    assert_eq!(
        submission.presentation,
        "CREATE USER operator WITH PASSWORD '********';"
    );
}

#[test]
fn a_resource_requires_the_scope_captured_when_the_dialog_opened() {
    let resource = ResourceDraft {
        name: "bundle".to_string(),
        if_not_exists: false,
    };
    let error = resource
        .submission(None)
        .expect_err("resource creation needs a selected domain");
    assert_eq!(
        error.current_context(),
        &CreateDraftError::ResourceDomainRequired
    );
}

#[test]
fn independent_choice_controls_discard_older_drafts_and_session_generations() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.open(CreateKind::Domain, None, "trigger");
        let revision = signals.revision.get_untracked();
        let pace_context = ChoiceRequestContext {
            control: ChoiceControl::DomainPace,
            draft_revision: revision,
            session_generation: 7,
            append: false,
        };
        let placement_context = ChoiceRequestContext {
            control: ChoiceControl::PlacementPolicy,
            ..pace_context
        };
        let pace = signals.choices.domain_pace.load;
        let placement = signals.choices.placement_policy.load;

        signals.apply_choice(
            placement_context,
            7,
            outcome(
                ChoiceValue::PlacementPolicy(PlacementPolicy::Neutral),
                "NEUTRAL",
            ),
        );
        signals.apply_choice(
            pace_context,
            7,
            outcome(
                ChoiceValue::DomainPace(DomainPaceChoice::Unpaced),
                "UNPACED",
            ),
        );
        assert!(matches!(
            pace.get_untracked(),
            ChoiceLoad::Ready { choices, .. } if choices[0].presentation.label == "UNPACED"
        ));
        assert!(matches!(
            placement.get_untracked(),
            ChoiceLoad::Ready { choices, .. } if choices[0].presentation.label == "NEUTRAL"
        ));

        signals.edit();
        signals.apply_choice(
            pace_context,
            7,
            outcome(ChoiceValue::DomainPace(DomainPaceChoice::Paced), "PACED"),
        );
        signals.apply_choice(
            ChoiceRequestContext {
                draft_revision: signals.revision.get_untracked(),
                ..placement_context
            },
            8,
            outcome(
                ChoiceValue::PlacementPolicy(PlacementPolicy::RequireColocation),
                "REQUIRE COLOCATION",
            ),
        );
        assert!(matches!(
            pace.get_untracked(),
            ChoiceLoad::Ready { choices, .. } if choices[0].presentation.label == "UNPACED"
        ));
        assert!(matches!(
            placement.get_untracked(),
            ChoiceLoad::Ready { choices, .. } if choices[0].presentation.label == "NEUTRAL"
        ));
    });
}

#[test]
fn a_reply_fills_only_a_control_of_the_open_form() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.open(CreateKind::Relay, Some(domain("orders")), "trigger");
        let context = |control| ChoiceRequestContext {
            control,
            draft_revision: signals.revision.get_untracked(),
            session_generation: 1,
            append: false,
        };
        let relay = ChoiceValue::Model(node(ModelKind::Relay, "orders"));
        signals.apply_choice(
            context(ChoiceControl::SubscriptionRelay),
            1,
            outcome(relay.clone(), "orders"),
        );
        assert_eq!(
            signals.choices.subscription_relay.load.get_untracked(),
            ChoiceLoad::Waiting
        );
        let schema = ChoiceValue::Model(node(ModelKind::Schema, "order_record"));
        signals.apply_choice(
            context(ChoiceControl::RelaySchema),
            1,
            outcome(schema, "order_record"),
        );
        assert!(matches!(
            signals.choices.relay_schema.load.get_untracked(),
            ChoiceLoad::Ready { .. }
        ));

        signals.open(CreateKind::Subscription, Some(domain("orders")), "trigger");
        signals.apply_choice(
            context(ChoiceControl::SubscriptionRelay),
            1,
            outcome(relay, "orders"),
        );
        assert!(matches!(
            signals.choices.subscription_relay.load.get_untracked(),
            ChoiceLoad::Ready { .. }
        ));
    });
}

#[test]
fn a_finished_attempt_requires_an_edit_before_it_can_be_submitted_again() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.open(CreateKind::Resource, None, "trigger");
        let (attempt, revision) = signals.begin_submission(true);
        assert!(signals.progress.get_untracked().blocks_repeat_submit());

        signals.queued_transaction(attempt, revision, 1);
        assert_eq!(
            signals.progress.get_untracked(),
            CreateProgress::QueuedTransaction { position: 1 }
        );
        assert!(signals.progress.get_untracked().blocks_repeat_submit());

        signals.edit();
        assert_eq!(signals.progress.get_untracked(), CreateProgress::Editing);
        assert!(!signals.progress.get_untracked().blocks_repeat_submit());
    });
}

#[test]
fn submission_state_changes_only_for_the_active_attempt() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.open(CreateKind::User, None, "trigger");
        let (attempt, revision) = signals.begin_submission(true);
        assert_eq!(signals.progress.get_untracked().label(), "Submitting");

        assert!(!signals.completed(0, revision));
        signals.queued_reconnect(0, revision);
        assert_eq!(signals.progress.get_untracked(), CreateProgress::Submitting);

        signals.connection_lost();
        assert_eq!(
            signals.progress.get_untracked().label(),
            "Queued until reconnect"
        );
        signals.queued_reconnect(attempt, revision);
        assert!(signals.completed(attempt, revision));
        assert_eq!(signals.progress.get_untracked().label(), "Completed");

        signals.queued_transaction(attempt, revision, 2);
        assert_eq!(
            signals.progress.get_untracked().label(),
            "Queued in transaction · position 2"
        );
    });
}

#[test]
fn choice_outcomes_keep_missing_stale_and_failed_states_distinct() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.open(CreateKind::Domain, None, "trigger");
        let pace = signals.choices.domain_pace.load;
        let revision = signals.revision.get_untracked();
        let context = ChoiceRequestContext {
            control: ChoiceControl::DomainPace,
            draft_revision: revision,
            session_generation: 3,
            append: false,
        };
        let outcome = |status, choices, page_cursor| ChoiceOutcome {
            status,
            choices,
            page_cursor,
        };
        signals.apply_choice(context, 3, outcome(ChoiceStatus::Ready, Vec::new(), None));
        assert_eq!(pace.get_untracked(), ChoiceLoad::Empty);

        let choice = |pace, label: &str| Choice {
            value: ChoiceValue::DomainPace(pace),
            presentation: ChoicePresentation {
                label: label.to_string(),
                detail: None,
                group: None,
            },
        };
        signals.apply_choice(
            context,
            3,
            outcome(
                ChoiceStatus::Ready,
                vec![choice(DomainPaceChoice::Unpaced, "UNPACED")],
                Some("next".to_string()),
            ),
        );
        signals.apply_choice(
            ChoiceRequestContext {
                append: true,
                ..context
            },
            3,
            outcome(
                ChoiceStatus::Ready,
                vec![choice(DomainPaceChoice::Paced, "PACED")],
                None,
            ),
        );
        assert!(matches!(
            pace.get_untracked(),
            ChoiceLoad::Ready { choices, page_cursor: None } if choices.len() == 2
        ));

        for (status, expected) in [
            (
                ChoiceStatus::MissingContext,
                ChoiceLoad::MissingPrerequisite("Choose the fields this control depends on"),
            ),
            (ChoiceStatus::StaleContext, ChoiceLoad::StaleContext),
            (
                ChoiceStatus::LookupFailed,
                ChoiceLoad::Failed("Choices could not be loaded".to_string()),
            ),
        ] {
            signals.apply_choice(context, 3, outcome(status, Vec::new(), None));
            assert_eq!(pace.get_untracked(), expected);
        }
        signals.fail_choice(context, 3, "transport ended".to_string());
        assert_eq!(
            pace.get_untracked(),
            ChoiceLoad::Failed("transport ended".to_string())
        );
        signals.fail_choice(context, 4, "stale transport".to_string());
        assert_eq!(
            pace.get_untracked(),
            ChoiceLoad::Failed("transport ended".to_string())
        );
    });
}

#[test]
fn choice_states_render_hints_and_failures_with_their_own_presentation() {
    super::super::initialize_test_executor();
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        let load = signals.choices.domain_pace.load;
        let request_tx = RwSignal::new(None);
        let session_generation = RwSignal::new(1);
        let render = || {
            ChoiceGroup(
                ChoiceGroupProps::builder()
                    .class_name("create-pace-options")
                    .label("Pace")
                    .control(ChoiceControl::DomainPace)
                    .signals(signals)
                    .request_tx(request_tx)
                    .session_generation(session_generation)
                    .build(),
            )
            .to_html()
        };

        load.set(ChoiceLoad::MissingPrerequisite("Choose a domain"));
        let missing = render();
        assert!(missing.contains("create-choice-missing"));
        assert!(missing.contains("Choose a domain"));

        load.set(ChoiceLoad::StaleContext);
        let stale = render();
        assert!(stale.contains("create-choice-stale"));
        assert!(stale.contains("create-choice-retry"));
        assert!(stale.contains("The form context changed"));

        load.set(ChoiceLoad::Failed(
            "Choices could not be loaded".to_string(),
        ));
        let failed = render();
        assert!(failed.contains("choice-failed"));
        assert!(failed.contains("role=\"alert\""));
        assert!(failed.contains("Choices could not be loaded"));
    });
}

#[test]
fn paced_domain_and_password_validation_report_the_owning_field() {
    let mut draft = DomainDraft {
        name: "orders".to_string(),
        pace: DomainPaceChoice::Paced,
        period: "2s".to_string(),
        skew: "500ms".to_string(),
        ..DomainDraft::default()
    };
    let submission = draft
        .submission()
        .assured("the paced domain draft is valid");
    assert!(command_query(&submission).contains("PACED"));
    assert!(command_query(&submission).contains("PERIOD 2s"));

    draft.period = "soon".to_string();
    assert_eq!(
        draft
            .submission()
            .expect_err("an invalid period is rejected")
            .current_context(),
        &CreateDraftError::Period
    );
    draft.period = "2s".to_string();
    draft.skew = "later".to_string();
    assert_eq!(
        draft
            .submission()
            .expect_err("an invalid skew is rejected")
            .current_context(),
        &CreateDraftError::Skew
    );

    let user = UserDraft {
        name: "operator".to_string(),
        password: String::new(),
        if_not_exists: false,
    };
    assert_eq!(
        user.submission()
            .expect_err("an empty password is rejected")
            .current_context(),
        &CreateDraftError::PasswordRequired
    );
}

#[test]
fn choice_requests_preserve_control_context_and_report_unavailable_channels() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.open(CreateKind::Domain, None, "trigger");
        let pace = signals.choices.domain_pace;
        pace.search.set("wall".to_string());
        let (sender, mut receiver) = unbounded();
        let request_tx = RwSignal::new(Some(sender));
        request_choices(signals, ChoiceControl::DomainPace, request_tx, 8, false);
        let ConsoleRequest::Choice { request, context } = receiver
            .try_recv()
            .assured("the choice channel remains open")
        else {
            panic!("the create dialog sends a typed choice request");
        };
        assert_eq!(request.target(), ChoiceTarget::DomainPace);
        assert_eq!(request.search(), "wall");
        assert!(request.dependencies().is_empty());
        assert_eq!(context.session_generation, 8);
        assert!(!context.append);

        pace.load.set(ChoiceLoad::Ready {
            choices: Vec::new(),
            page_cursor: Some("page-two".to_string()),
        });
        request_choices(signals, ChoiceControl::DomainPace, request_tx, 8, true);
        let ConsoleRequest::Choice { request, context } = receiver
            .try_recv()
            .assured("the choice channel remains open")
        else {
            panic!("the create dialog sends a typed choice request");
        };
        assert_eq!(request.page_cursor(), Some("page-two"));
        assert!(context.append);

        request_choices(
            signals,
            ChoiceControl::PlacementPolicy,
            request_tx,
            8,
            false,
        );
        let ConsoleRequest::Choice { request, .. } = receiver
            .try_recv()
            .assured("the choice channel remains open")
        else {
            panic!("the create dialog sends a typed choice request");
        };
        assert_eq!(request.dependencies().len(), 1);

        let unavailable = RwSignal::new(None);
        request_choices(signals, ChoiceControl::DomainPace, unavailable, 8, false);
        assert_eq!(
            pace.load.get_untracked(),
            ChoiceLoad::Failed("The session is not available".to_string())
        );

        let (closed_sender, closed_receiver) = unbounded();
        drop(closed_receiver);
        request_choices(
            signals,
            ChoiceControl::DomainPace,
            RwSignal::new(Some(closed_sender)),
            8,
            false,
        );
        assert_eq!(
            pace.load.get_untracked(),
            ChoiceLoad::Failed("The session channel is closed".to_string())
        );

        pace.load.set(ChoiceLoad::Empty);
        request_choices(signals, ChoiceControl::DomainPace, request_tx, 8, true);
        assert!(receiver.try_recv().is_err());
    });
}

#[test]
fn typed_choice_selection_updates_only_the_draft_its_control_edits() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        select_choice(
            signals,
            ChoiceControl::DomainPace,
            ChoiceValue::DomainPace(DomainPaceChoice::Paced),
        );
        select_choice(
            signals,
            ChoiceControl::PlacementPolicy,
            ChoiceValue::PlacementPolicy(PlacementPolicy::RequireColocation),
        );
        assert_eq!(signals.domain.get_untracked().pace, DomainPaceChoice::Paced);
        assert_eq!(
            signals.domain.get_untracked().placement,
            PlacementPolicy::RequireColocation
        );
        select_choice(
            signals,
            ChoiceControl::DomainPace,
            ChoiceValue::Domain(domain("orders")),
        );
        assert_eq!(signals.domain.get_untracked().pace, DomainPaceChoice::Paced);

        let schema = ChoiceValue::Model(node(ModelKind::Schema, "order_record"));
        select_choice(signals, ChoiceControl::RelaySchema, schema.clone());
        assert!(selected_choice(
            signals,
            ChoiceControl::RelaySchema,
            &schema
        ));
        assert!(!selected_choice(
            signals,
            ChoiceControl::BranchSchema,
            &schema
        ));
        assert!(signals.structured.get_untracked().branch.schema.is_none());

        let branch = ChoiceValue::Model(node(ModelKind::Branch, "by_tenant"));
        select_choice(signals, ChoiceControl::RelayBranch, branch.clone());
        assert!(selected_choice(
            signals,
            ChoiceControl::RelayBranch,
            &branch
        ));

        let relay = ChoiceValue::Model(node(ModelKind::Relay, "orders"));
        select_choice(signals, ChoiceControl::SubscriptionRelay, relay.clone());
        assert!(selected_choice(
            signals,
            ChoiceControl::SubscriptionRelay,
            &relay
        ));

        let field = ChoiceValue::Field(FieldName::parse("amount").assured("valid field"));
        select_choice(signals, ChoiceControl::SubscriptionField, field.clone());
        select_choice(signals, ChoiceControl::SubscriptionField, field.clone());
        assert_eq!(
            signals.subscription.get_untracked().filter,
            "input.amount input.amount"
        );
        assert!(!selected_choice(
            signals,
            ChoiceControl::SubscriptionField,
            &field
        ));
        select_choice(signals, ChoiceControl::RelaySchema, field);
        assert!(selected_choice(
            signals,
            ChoiceControl::RelaySchema,
            &schema
        ));
    });
}

#[test]
fn branch_scope_change_keeps_the_draft_and_requires_reselecting_its_schema() {
    Owner::new().with(|| {
        let first = domain("first");
        let second = domain("second");
        let schema = SchemaName::parse("tenant_key").assured("valid schema");
        let signals = CreateSignals::new();

        signals.change_scope(Some(first.clone()));
        assert_eq!(signals.captured_domain.get_untracked(), None);
        signals.open(CreateKind::Domain, None, "trigger");
        signals.change_scope(Some(first.clone()));
        assert_eq!(signals.captured_domain.get_untracked(), None);

        signals.open(CreateKind::Branch, Some(first.clone()), "trigger");
        signals.structured.update(|drafts| {
            drafts.branch.name = "by_tenant".to_string();
            drafts.branch.schema = Some(SelectedReference::chosen(schema.clone()));
            drafts.branch.ttl = "5m".to_string();
        });
        signals.change_scope(Some(first.clone()));
        assert!(
            signals
                .structured
                .get_untracked()
                .branch
                .schema
                .assured("the schema stays selected")
                .is_current()
        );

        signals.change_scope(Some(second.clone()));
        let retained = signals.structured.get_untracked().branch;
        let retained_schema = retained.schema.assured("the schema stays selected");
        assert_eq!(retained_schema.name(), &schema);
        assert!(!retained_schema.is_current());
        assert_eq!(retained.ttl, "5m");
        assert_eq!(
            signals.captured_domain.get_untracked(),
            Some(second.clone())
        );

        signals.open(CreateKind::Branch, Some(first), "trigger");
        assert_eq!(signals.captured_domain.get_untracked(), Some(second));
        assert_eq!(signals.structured.get_untracked().branch.name, "by_tenant");
    });
}

#[test]
fn relay_and_subscription_scope_changes_invalidate_their_references() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.open(CreateKind::Relay, Some(domain("first")), "trigger");
        signals.relay.update(|draft| {
            draft.select_schema(&node(ModelKind::Schema, "order_record"));
            draft.select_branch(&node(ModelKind::Branch, "by_tenant"));
        });
        signals.change_scope(Some(domain("second")));
        let relay = signals.relay.get_untracked();
        assert!(!relay.selects_schema(&node(ModelKind::Schema, "order_record")));
        assert!(!relay.selects_branch(&node(ModelKind::Branch, "by_tenant")));

        signals.open_subscription(
            domain("first"),
            RelayName::parse("orders").assured("valid relay"),
            "trigger",
        );
        signals.change_scope(Some(domain("first")));
        assert!(
            signals
                .subscription
                .get_untracked()
                .current_relay()
                .is_some()
        );
        signals.change_scope(Some(domain("second")));
        assert!(
            signals
                .subscription
                .get_untracked()
                .current_relay()
                .is_none()
        );
        assert_eq!(
            signals
                .submission()
                .expect_err("the relay must be selected again")
                .current_context()
                .to_string(),
            "The selected relay belongs to a changed context; select it again"
        );
    });
}

#[test]
fn branch_schema_requests_bind_the_captured_domain_and_page_cursor() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        let branch_schema = signals.choices.branch_schema;
        let (sender, mut receiver) = unbounded();
        let request_tx = RwSignal::new(Some(sender));
        signals.open(CreateKind::Branch, None, "trigger");
        request_choices(signals, ChoiceControl::BranchSchema, request_tx, 9, false);
        assert_eq!(
            branch_schema.load.get_untracked(),
            ChoiceLoad::MissingPrerequisite("Select a domain before choosing a schema")
        );
        assert!(receiver.try_recv().is_err());

        let scope = domain("orders");
        signals.change_scope(Some(scope.clone()));
        branch_schema.search.set("tenant".to_string());
        request_choices(signals, ChoiceControl::BranchSchema, request_tx, 9, false);
        let ConsoleRequest::Choice { request, context } = receiver
            .try_recv()
            .assured("the schema picker requests a typed page")
        else {
            panic!("the schema picker must send a choice request");
        };
        assert_eq!(request.target(), ChoiceTarget::Schema);
        assert_eq!(request.search(), "tenant");
        assert_eq!(request.page_size(), 20);
        assert_eq!(request.dependencies()[0].value, ChoiceValue::Domain(scope));
        assert_eq!(context.control, ChoiceControl::BranchSchema);

        branch_schema.load.set(ChoiceLoad::Ready {
            choices: Vec::new(),
            page_cursor: Some("next-page".to_string()),
        });
        request_choices(signals, ChoiceControl::BranchSchema, request_tx, 9, true);
        let ConsoleRequest::Choice { request, context } = receiver
            .try_recv()
            .assured("a page cursor requests the next schema page")
        else {
            panic!("the schema picker must send a choice request");
        };
        assert_eq!(request.page_cursor(), Some("next-page"));
        assert!(context.append);

        branch_schema.load.set(ChoiceLoad::Empty);
        request_choices(signals, ChoiceControl::BranchSchema, request_tx, 9, true);
        assert!(receiver.try_recv().is_err());
    });
}

#[test]
fn relay_and_subscription_controls_ask_typed_questions_of_the_captured_domain() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        let (sender, mut receiver) = unbounded();
        let request_tx = RwSignal::new(Some(sender));
        let mut next_request = || {
            let ConsoleRequest::Choice { request, context } = receiver
                .try_recv()
                .assured("the control sends a typed choice request")
            else {
                panic!("a control sends a choice request");
            };
            (request, context)
        };

        signals.open(CreateKind::Subscription, None, "trigger");
        for (control, reason) in [
            (
                ChoiceControl::RelaySchema,
                "Select a domain before choosing a schema",
            ),
            (
                ChoiceControl::RelayBranch,
                "Select a domain before choosing a branch",
            ),
            (
                ChoiceControl::SubscriptionRelay,
                "Select a domain before choosing a relay",
            ),
            (
                ChoiceControl::SubscriptionField,
                "Select a domain before choosing a relay",
            ),
        ] {
            request_choices(signals, control, request_tx, 2, false);
            assert_eq!(
                signals.choices.of(control).load.get_untracked(),
                ChoiceLoad::MissingPrerequisite(reason)
            );
        }

        let scope = domain("orders");
        signals.change_scope(Some(scope.clone()));
        request_choices(
            signals,
            ChoiceControl::SubscriptionField,
            request_tx,
            2,
            false,
        );
        assert_eq!(
            signals.choices.subscription_field.load.get_untracked(),
            ChoiceLoad::MissingPrerequisite("Select a relay to list the fields of its records")
        );
        for (control, target) in [
            (ChoiceControl::RelaySchema, ChoiceTarget::Schema),
            (ChoiceControl::RelayBranch, ChoiceTarget::Branch),
            (ChoiceControl::SubscriptionRelay, ChoiceTarget::Relay),
        ] {
            request_choices(signals, control, request_tx, 2, false);
            let (request, context) = next_request();
            assert_eq!(request.target(), target);
            assert_eq!(request.page_size(), 20);
            assert_eq!(
                request.dependencies()[0].value,
                ChoiceValue::Domain(scope.clone())
            );
            assert_eq!(context.control, control);
        }

        signals.subscription.update(|draft| {
            draft.select_relay(&node(ModelKind::Relay, "orders"));
        });
        request_choices(
            signals,
            ChoiceControl::SubscriptionField,
            request_tx,
            2,
            false,
        );
        let (request, _) = next_request();
        assert_eq!(request.target(), ChoiceTarget::RelayField);
        assert_eq!(request.page_size(), 100);
        assert_eq!(
            request.dependencies()[1].value,
            ChoiceValue::Model(node(ModelKind::Relay, "orders"))
        );
    });
}

#[test]
fn branch_schema_selection_is_exact_and_does_not_clear_an_invalid_reference() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        let choice = ChoiceValue::Model(node(ModelKind::Schema, "tenant_key"));
        assert!(!selected_choice(
            signals,
            ChoiceControl::BranchSchema,
            &choice
        ));
        select_choice(signals, ChoiceControl::BranchSchema, choice.clone());
        assert!(selected_choice(
            signals,
            ChoiceControl::BranchSchema,
            &choice
        ));

        signals
            .structured
            .update(|drafts| drafts.branch.invalidate_references());
        assert!(!selected_choice(
            signals,
            ChoiceControl::BranchSchema,
            &choice
        ));
        assert_eq!(
            signals
                .structured
                .get_untracked()
                .branch
                .schema
                .assured("the schema stays selected")
                .name(),
            &SchemaName::parse("tenant_key").assured("valid schema")
        );

        select_choice(
            signals,
            ChoiceControl::BranchSchema,
            ChoiceValue::Model(node(ModelKind::Relay, "tenant_key")),
        );
        assert!(!selected_choice(
            signals,
            ChoiceControl::BranchSchema,
            &choice
        ));
    });
}

#[test]
fn a_contextual_subscription_opens_a_fresh_named_draft_for_its_relay() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        assert_eq!(
            signals.subscription.get_untracked().name,
            "web_console_subscription_1"
        );
        signals.subscription.update(|draft| {
            draft.filter = "input.amount > 1".to_string();
        });
        let scope = domain("orders");
        let relay = RelayName::parse("orders").assured("valid relay");
        signals.open_subscription(scope.clone(), relay.clone(), "graph-search");
        assert_eq!(signals.open.get_untracked(), Some(CreateKind::Subscription));
        assert_eq!(signals.captured_domain.get_untracked(), Some(scope.clone()));
        let draft = signals.subscription.get_untracked();
        assert_eq!(draft.name, "web_console_subscription_2");
        assert_eq!(draft.current_relay(), Some(&relay));
        assert!(draft.filter.is_empty());
        assert_eq!(draft.delivery, SubscriptionDeliveryBehavior::Blocking);

        let submission = signals
            .submission()
            .assured("a contextual draft is complete");
        let CreateDispatch::Subscription(dispatch) = submission.dispatch else {
            panic!("a subscription opens under the subscription lifecycle");
        };
        assert_eq!(dispatch.domain, scope);
        assert_eq!(
            dispatch.statement,
            "CREATE SUBSCRIPTION web_console_subscription_2 TO orders;"
        );
        assert_eq!(submission.presentation, dispatch.statement);

        signals.open_subscription(scope, relay, "graph-search");
        assert_eq!(
            signals.subscription.get_untracked().name,
            "web_console_subscription_3"
        );
    });
}

#[test]
fn domain_scoped_forms_need_their_captured_domain_and_run_where_they_belong() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        signals.relay.update(|draft| {
            draft.name = "orders".to_string();
            draft.select_schema(&node(ModelKind::Schema, "order_record"));
            draft.choose_unbranched();
            draft.materialized_state = Some(MaterializedRelayState::LastByTimestamp);
            draft.if_not_exists = true;
        });
        signals.open(CreateKind::Relay, None, "trigger");
        assert_eq!(
            signals
                .submission()
                .expect_err("a relay needs a domain")
                .current_context(),
            &CreateDraftError::ScopedDomainRequired
        );
        signals.open(CreateKind::Subscription, None, "trigger");
        assert_eq!(
            signals
                .submission()
                .expect_err("a subscription needs a domain")
                .current_context(),
            &CreateDraftError::ScopedDomainRequired
        );

        let scope = domain("orders");
        signals.open(CreateKind::Relay, Some(scope.clone()), "trigger");
        assert_eq!(signals.captured_domain.get_untracked(), None);
        signals.change_scope(Some(scope.clone()));
        signals.relay.update(|draft| {
            draft.select_schema(&node(ModelKind::Schema, "order_record"));
        });
        let submission = signals.submission().assured("the relay draft is complete");
        assert_eq!(
            command_query(&submission),
            "CREATE IF NOT EXISTS RELAY orders SCHEMA order_record UNBRANCHED CAPACITY 1 WITH \
             MATERIALIZED STATE LAST BY TIMESTAMP;"
        );
        assert_eq!(command_domain(&submission), Some(&scope));

        signals
            .relay
            .update(|draft| draft.capacity = "none".to_string());
        assert_eq!(
            signals
                .submission()
                .expect_err("capacity must be a count")
                .current_context()
                .to_string(),
            "Capacity must be a positive integer"
        );
        assert!(CreateKind::Relay.takes_if_not_exists());
        assert!(!CreateKind::Subscription.takes_if_not_exists());
        assert_eq!(
            ChoiceControl::SubscriptionField.form(),
            CreateKind::Subscription
        );
        assert_eq!(ChoiceControl::RelayBranch.form(), CreateKind::Relay);
    });
}

#[test]
fn each_form_asks_only_the_questions_its_draft_needs() {
    Owner::new().with(|| {
        let signals = CreateSignals::new();
        assert_eq!(
            open_form_controls(signals, CreateKind::Domain),
            [ChoiceControl::DomainPace, ChoiceControl::PlacementPolicy]
        );
        assert_eq!(
            open_form_controls(signals, CreateKind::Branch),
            [ChoiceControl::BranchSchema]
        );
        assert_eq!(
            open_form_controls(signals, CreateKind::Relay),
            [ChoiceControl::RelaySchema]
        );
        signals.relay.update(|draft| draft.choose_branched());
        assert_eq!(
            open_form_controls(signals, CreateKind::Relay),
            [ChoiceControl::RelaySchema, ChoiceControl::RelayBranch]
        );
        assert_eq!(
            open_form_controls(signals, CreateKind::Subscription),
            [
                ChoiceControl::SubscriptionRelay,
                ChoiceControl::SubscriptionField
            ]
        );
        for kind in [
            CreateKind::User,
            CreateKind::Resource,
            CreateKind::Schema,
            CreateKind::WireJsonSchema,
        ] {
            assert!(open_form_controls(signals, kind).is_empty());
        }
    });
}

#[test]
fn creation_components_render_each_typed_draft_and_accessible_status() {
    super::super::initialize_test_executor();
    Owner::new().with(|| {
        let scope = domain("orders");
        let active_domain = RwSignal::new(Some(scope.clone()));
        let connection_state = RwSignal::new(super::super::ConsoleConnectionState::Waiting);
        let generation = RwSignal::new(4);
        let request_tx = RwSignal::new(None);
        let signals = CreateSignals::new();

        let menu = CreateMenu(
            CreateMenuProps::builder()
                .signals(signals)
                .active_domain(active_domain)
                .build(),
        );
        let menu_markup = menu.to_html();
        assert!(menu_markup.contains("global-create-button"));
        assert!(menu_markup.contains("Create"));
        assert!(menu_markup.contains("Resource"));
        assert!(menu_markup.contains("data-create-kind=\"relay\""));
        assert!(menu_markup.contains("data-create-kind=\"subscription\""));

        signals.domain.update(|draft| {
            draft.name = "analytics".to_string();
            draft.if_not_exists = true;
            draft.placement = PlacementPolicy::PreferColocation;
        });
        signals.open(CreateKind::Domain, None, "global-create-button");
        let render = || {
            let dialog = CreateDialog(
                CreateDialogProps::builder()
                    .signals(signals)
                    .active_domain(active_domain)
                    .connection_state(connection_state)
                    .session_generation(generation)
                    .request_tx(request_tx)
                    .submit(|_, _, _| {})
                    .build(),
            );
            any_spawner::Executor::poll_local();
            if signals.open.get_untracked() == Some(CreateKind::Domain) {
                signals.choices.domain_pace.load.set(ChoiceLoad::Ready {
                    choices: vec![Choice {
                        value: ChoiceValue::DomainPace(DomainPaceChoice::Unpaced),
                        presentation: ChoicePresentation {
                            label: "UNPACED".to_string(),
                            detail: Some("No domain clock".to_string()),
                            group: None,
                        },
                    }],
                    page_cursor: Some("more-pace".to_string()),
                });
                signals
                    .choices
                    .placement_policy
                    .load
                    .set(ChoiceLoad::Ready {
                        choices: vec![Choice {
                            value: ChoiceValue::PlacementPolicy(PlacementPolicy::PreferColocation),
                            presentation: ChoicePresentation {
                                label: "PREFER COLOCATION".to_string(),
                                detail: None,
                                group: None,
                            },
                        }],
                        page_cursor: None,
                    });
            }
            if signals.open.get_untracked() == Some(CreateKind::Subscription) {
                signals
                    .choices
                    .subscription_field
                    .load
                    .set(ChoiceLoad::Ready {
                        choices: vec![Choice {
                            value: ChoiceValue::Field(
                                FieldName::parse("amount").assured("valid field"),
                            ),
                            presentation: ChoicePresentation {
                                label: "amount".to_string(),
                                detail: Some("I64 OPTIONAL".to_string()),
                                group: None,
                            },
                        }],
                        page_cursor: None,
                    });
            }
            dialog.to_html()
        };
        let domain_markup = render();
        assert!(domain_markup.contains("role=\"dialog\""));
        assert!(domain_markup.contains("Create domain"));
        assert!(domain_markup.contains(
            "CREATE IF NOT EXISTS UNPACED DOMAIN analytics PLACEMENT PREFER COLOCATION;"
        ));
        assert!(domain_markup.contains("UNPACED"));
        assert!(domain_markup.contains("Load more"));

        signals.user.update(|draft| {
            draft.name = "operator".to_string();
            draft.password = "never-render-me".to_string();
        });
        signals.open(CreateKind::User, None, "global-create-button");
        let user_markup = render();
        assert!(user_markup.contains("Create user"));
        assert!(user_markup.contains("********"));
        assert!(!user_markup.contains("never-render-me"));

        signals
            .resource
            .update(|draft| draft.name = "bundle".to_string());
        signals.open(
            CreateKind::Resource,
            Some(scope.clone()),
            "sidebar-create-resource",
        );
        let (attempt, revision) = signals.begin_submission(false);
        let resource_markup = render();
        assert!(resource_markup.contains("Create resource"));
        assert!(resource_markup.contains("Queued until reconnect"));
        assert!(resource_markup.contains("CREATE RESOURCE bundle;"));

        signals.failed(attempt, revision, "resource already exists".to_string());
        let failed_markup = render();
        assert!(failed_markup.contains("Failed"));
        assert!(failed_markup.contains("resource already exists"));

        signals.structured.update(|drafts| {
            drafts.schema.name = "visual_record".to_string();
            drafts.schema.fields.push(SchemaFieldDraft {
                name: "tenant".to_string(),
                ty: SchemaTypeDraft {
                    scalar: Some(ParseAsType::U32),
                    ..SchemaTypeDraft::default()
                },
                optional: true,
                sensitive: true,
            });
        });
        signals.open(
            CreateKind::Schema,
            Some(scope.clone()),
            "global-create-button",
        );
        let schema_markup = render();
        assert!(schema_markup.contains("Create schema"));
        assert!(schema_markup.contains("tenant U32 OPTIONAL SENSITIVE"));
        assert!(schema_markup.contains("Wrap in fixed array"));

        for (kind, field_type, format_name) in [
            (
                CreateKind::WireJsonSchema,
                WireFieldType::Json(JsonType::String),
                "JSON",
            ),
            (
                CreateKind::WireCborSchema,
                WireFieldType::Json(JsonType::String),
                "CBOR",
            ),
            (
                CreateKind::WireAvroSchema,
                WireFieldType::Avro(AvroType::String),
                "AVRO",
            ),
        ] {
            signals.structured.update(|drafts| {
                let wire = drafts.wire_mut(kind.wire_format().assured("a wire kind has a format"));
                wire.name = "visual_wire".to_string();
                wire.mode = Some(WireSchemaStrictness::Loose);
                wire.fields.push(WireFieldDraft {
                    name: "payload".to_string(),
                    ty: Some(field_type),
                    optional: true,
                });
            });
            signals.open(kind, Some(scope.clone()), "global-create-button");
            let wire_markup = render();
            assert!(wire_markup.contains(&format!("CREATE WIRE {format_name} SCHEMA visual_wire")));
            assert!(wire_markup.contains("payload STRING OPTIONAL"));
        }

        signals.structured.update(|drafts| {
            drafts.branch.name = "by_tenant".to_string();
            drafts.branch.schema = Some(SelectedReference::chosen(
                SchemaName::parse("visual_record").assured("valid name"),
            ));
            drafts.branch.ttl = "5m".to_string();
        });
        signals.open(
            CreateKind::Branch,
            Some(scope.clone()),
            "global-create-button",
        );
        let branch_markup = render();
        assert!(branch_markup.contains("Create branch"));
        assert!(branch_markup.contains("Selected schema: visual_record"));
        assert!(branch_markup.contains("CREATE BRANCH by_tenant SCHEMA visual_record TTL 5m;"));

        signals.relay.update(|draft| {
            draft.name = "visual_orders".to_string();
            draft.select_schema(&node(ModelKind::Schema, "visual_record"));
            draft.select_branch(&node(ModelKind::Branch, "by_tenant"));
            draft.capacity = "4".to_string();
        });
        signals.open(
            CreateKind::Relay,
            Some(scope.clone()),
            "global-create-button",
        );
        let relay_markup = render();
        assert!(relay_markup.contains("Create relay"));
        assert!(relay_markup.contains("Selected schema: visual_record"));
        assert!(relay_markup.contains("Selected branch: by_tenant"));
        assert!(relay_markup.contains("LAST BY TIMESTAMP"));
        assert!(relay_markup.contains(
            "CREATE RELAY visual_orders SCHEMA visual_record BRANCHED BY by_tenant CAPACITY 4;"
        ));
        signals.change_scope(Some(domain("elsewhere")));
        let changed_markup = render();
        assert!(changed_markup.contains("domain changed. Select a schema again."));
        assert!(changed_markup.contains("domain changed. Select a branch again."));

        signals.open_subscription(
            scope,
            RelayName::parse("visual_orders").assured("valid relay"),
            "graph-search",
        );
        signals.subscription.update(|draft| {
            draft.filter = "input.amount=1".to_string();
            draft.delivery = SubscriptionDeliveryBehavior::Dropping;
            draft.sampled = true;
            draft.sample_rate = "0.5".to_string();
        });
        let subscription_markup = render();
        assert!(subscription_markup.contains("Create subscription"));
        assert!(subscription_markup.contains("Selected relay: visual_orders"));
        assert!(subscription_markup.contains("Reads as WHERE input.amount = 1"));
        assert!(subscription_markup.contains("I64 OPTIONAL"));
        assert!(!subscription_markup.contains("create-if-not-exists"));
        assert!(
            subscription_markup.contains(
                "TO visual_orders DROPPING BATCH SAMPLE RATE 0.5 WHERE input.amount = 1;"
            )
        );
        signals
            .subscription
            .update(|draft| draft.filter = "input.amount =".to_string());
        let invalid_markup = render();
        assert!(invalid_markup.contains("create-filter-reading invalid"));
    });
}
