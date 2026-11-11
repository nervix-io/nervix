//! Relay subscription publication and retained-branch visibility tests.
//!
//! Layer: test harness.
//! - **Owns.** Subscriber teardown, attachment and concrete-branch inspection regressions.
//! - **Depends on.** The production relay boundary and its shared native fixtures.
//! - **Must not know.** External transport drivers or client presentation behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use super::*;

fn notification_schedule(domain: &DomainName, relay: Option<bool>) -> DomainSchedule {
    let schema = named::<SchemaName>("notification");
    let mut nodes = Vec::new();
    if let Some(sensitive) = relay {
        nodes.push(scheduled_model(nervix_models::Model::Schema(
            CreateSchema {
                name: schema.clone(),
                fields: vec![nervix_models::SchemaField {
                    name: named("user_id"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive,
                }],
            },
        )));
        nodes.push(scheduled_model(nervix_models::Model::Relay(CreateRelay {
            name: named("notifications"),
            schema,
            buffer: nonzero!(2usize),
            branching: RelayBranching::unbranched(),
            materialized_state: None,
        })));
    }
    DomainSchedule::new(domain.clone(), nodes, Vec::new())
}

fn notification_definition(sensitive: bool) -> RelaySubscriptionDefinition {
    let schema = compile_schema(&CreateSchema {
        name: named("notification"),
        fields: vec![nervix_models::SchemaField {
            name: named("user_id"),
            ty: ParseAsType::I64,
            optional: false,
            sensitive,
        }],
    });
    RelaySubscriptionDefinition::new(Arc::new(schema), ResolvedBranching::unbranched())
}

#[nervix_primitives::test]
async fn relay_rebuilds_end_subscribers_only_when_the_relay_rows_change() {
    let runtime = Runtime::default();
    let domain = domain("default");
    runtime.sync_domains(&BTreeMap::from([(
        domain.clone(),
        unpaced_domain_state(domain.as_str()),
    )]));
    let node = ClusterNodeName::parse("node-1").expect("valid name");
    let relay = named::<RelayName>("notifications");
    let rebuild = |schedule: DomainSchedule| {
        let runtime = runtime.clone();
        let domain = domain.clone();
        let node = node.clone();
        async move {
            runtime
                .rebuild_domain_from_schedule(&node, &domain, Some(schedule), true)
                .await
                .expect("the notification schedule builds");
        }
    };

    rebuild(notification_schedule(&domain, Some(false))).await;
    let mut public = runtime
        .subscribe_stream(&domain, &relay, &notification_definition(false))
        .await
        .expect("a subscriber describing the declared rows attaches");

    rebuild(notification_schedule(&domain, Some(false))).await;
    assert!(
        matches!(public.try_recv(), RelayTryRecv::Empty),
        "a rebuild that keeps the relay's rows keeps its subscribers"
    );

    rebuild(notification_schedule(&domain, Some(true))).await;
    assert!(
        timeout(Duration::from_secs(5), public.recv())
            .await
            .expect("a closed subscriber is woken")
            .is_none(),
        "a field that became sensitive ends every subscriber that announced it as public"
    );
    let refused = runtime
        .subscribe_stream(&domain, &relay, &notification_definition(false))
        .await
        .expect_err("the relay no longer has the public definition");
    assert!(matches!(
        refused.current_context(),
        RuntimeError::RelayRedefined { .. }
    ));
    let mut sensitive = runtime
        .subscribe_stream(&domain, &relay, &notification_definition(true))
        .await
        .expect("a subscriber describing the current rows attaches");

    rebuild(notification_schedule(&domain, None)).await;
    assert!(
        timeout(Duration::from_secs(5), sensitive.recv())
            .await
            .expect("a withdrawn subscriber is woken")
            .is_none(),
        "removing the relay ends its subscribers"
    );
    let refused = runtime
        .subscribe_stream(&domain, &relay, &notification_definition(true))
        .await
        .expect_err("the relay is gone");
    assert!(matches!(
        refused.current_context(),
        RuntimeError::RelayNotInstantiated { .. }
    ));
}

#[nervix_primitives::test]
async fn a_subscriber_cannot_attach_to_a_relay_this_node_does_not_declare() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let relay = named::<RelayName>("notifications");
    let not_instantiated = "relay 'notifications' in domain 'default' is not instantiated";

    let unrouted = runtime
        .subscribe_stream(&domain, &relay, &notification_definition(false))
        .await
        .expect_err("a domain without an execution routes no relay");
    assert_eq!(format!("{unrouted:#}"), not_instantiated);

    let mut routing = DomainRoutingSnapshot::default();
    routing
        .relay_services
        .insert(relay.clone(), test_relay_boundary_services());
    install_test_domain_execution(&runtime, &domain, Vec::new(), routing);
    let undeclared = runtime
        .subscribe_stream(&domain, &relay, &notification_definition(false))
        .await
        .expect_err("a relay boundary that declares no definition admits no subscriber");
    assert_eq!(format!("{undeclared:#}"), not_instantiated);
}

#[nervix_primitives::test]
async fn the_console_lists_the_concrete_branches_the_relay_owner_holds() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    let relay = named::<RelayName>("orders");
    let services = test_relay_boundary_services();
    let owner_task = runtime
        .spawn_relay_owner_task(
            &domain,
            &relay,
            Unit::SINGLE,
            services.clone(),
            RelayRetention::default(),
        )
        .expect("a whole relay schedules its single unit");
    let beta = string_branch_key("tenant", "beta");
    let acme = string_branch_key("tenant", "acme");
    fan_out_through_owner(&services, &beta).await;
    fan_out_through_owner(&services, &acme).await;
    install_test_domain_execution(
        &runtime,
        &domain,
        Vec::new(),
        DomainRoutingSnapshot {
            relay_services: HashMap::from_iter([(relay.clone(), services.clone())]),
            ..DomainRoutingSnapshot::default()
        },
    );

    let listed = runtime
        .dataflow_relay_branch_statistics(&domain, &relay)
        .into_iter()
        .map(|statistics| statistics.branch)
        .collect::<Vec<_>>();

    let mut expected = [&acme, &beta]
        .into_iter()
        .map(|key| {
            key.as_ref()
                .expect("the test keys are concrete")
                .fingerprint()
                .to_string()
        })
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(
        listed, expected,
        "branches are listed by the fingerprints of their keys, in fingerprint order"
    );
    assert!(
        runtime
            .dataflow_relay_branch_statistics(&domain, &named("absent"))
            .is_empty()
    );
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[nervix_primitives::test]
async fn an_unscheduled_materialized_record_is_visible_only_while_its_owner_holds_the_branch() {
    let runtime = Runtime::default();
    let domain = domain("default");
    let relay = named::<RelayName>("notifications");
    let untracked = named::<RelayName>("untracked");
    publish_state_identity(&runtime, &domain, ModelKind::Relay, &relay);
    publish_state_identity(&runtime, &domain, ModelKind::Relay, &untracked);
    let presence = runtime
        .relay_branch_presence(&domain, &relay, Unit::SINGLE)
        .expect("the relay's state identity is published");
    let routing = DomainRoutingSnapshot {
        relay_services: [(relay.clone(), services_reporting_to(presence.clone()))]
            .into_iter()
            .collect(),
        ..DomainRoutingSnapshot::default()
    };
    let placement_of = |relay: &RelayName| {
        runtime
            .state_placement(
                &domain,
                RuntimeStateKind::MaterializedRelay,
                ModelKind::Relay,
                relay,
                None,
            )
            .expect("the relay's state identity is published")
    };
    let acme = string_branch_key("tenant", "acme");
    let beta = string_branch_key("tenant", "beta");
    let mut owner = OwnedBranches::<BranchKey, ()>::claim(presence);
    owner
        .admit(
            acme.as_ref(),
            Timestamp::from_unix_nanos(1),
            None,
            |_, _| Ok::<(), std::convert::Infallible>(()),
        )
        .expect("the test constructor cannot fail");

    let placement = placement_of(&relay);
    assert!(runtime.materialized_stream_key_is_visible(Some(&routing), &placement, &acme));
    assert!(!runtime.materialized_stream_key_is_visible(Some(&routing), &placement, &beta));
    assert!(
        runtime.materialized_stream_key_is_visible(
            Some(&routing),
            &placement_of(&untracked),
            &beta
        ),
        "a relay without a presence restricts nothing"
    );
    drop(owner);
    assert!(!runtime.materialized_stream_key_is_visible(Some(&routing), &placement, &acme));
}

#[nervix_primitives::test]
async fn a_payload_for_a_relay_this_node_has_not_instantiated_names_its_domain_and_relay() {
    let runtime = Runtime::default();
    let relay = named::<RelayName>("incoming");
    let installed = domain("installed");
    install_unpaced_test_domain(&runtime, &installed);
    let batch_ipc = test_schema(&[("value", ParseAsType::I64)])
        .batch_from_test_rows([[("value".to_string(), RuntimeValue::I64(1))]])
        .expect("batch should build")
        .encode_arrow_ipc(
            runtime.executor(),
            nervix_execution::QueueAdmission::RefuseWhenFull,
        )
        .await
        .expect("batch ipc should serialize");
    let payload = |kind, target: &DomainName| RelayPayload {
        ack_timing: nervix_models::RemoteAckTiming::default(),
        decoded_bytes: 0,
        delivery: RelayDelivery {
            channel_incarnation: [1; 16],
            sequence: 0,
        },
        kind,
        domain: target.clone(),
        relay: relay.clone(),
        unit: Unit::SINGLE,
        key: None,
        batch_ipc: batch_ipc.clone(),
        metadata: Vec::new(),
        acks: Vec::new(),
        admission: None,
    };

    let absent = domain("absent");
    let stream = match runtime
        .handle_remote_stream_payload_with_owner_ingress(
            payload(RelayPayloadKind::Ingress, &absent),
            true,
        )
        .await
    {
        Ok(()) => panic!("a stream payload needs its domain on this node"),
        Err(error) => error,
    };
    assert_eq!(
        stream.to_string(),
        "relay 'incoming' in domain 'absent' is not instantiated"
    );
    for target in [&absent, &installed] {
        let fanout = match runtime
            .handle_remote_subscription_payload(payload(
                RelayPayloadKind::SubscriptionFanout,
                target,
            ))
            .await
        {
            Ok(()) => panic!("a subscription payload needs its relay instantiated"),
            Err(error) => error,
        };
        assert_eq!(
            fanout.to_string(),
            format!(
                "relay 'incoming' in domain '{}' is not instantiated",
                target.as_str()
            )
        );
    }

    let passive = DomainRoutingSnapshot {
        passive_only: true,
        ..DomainRoutingSnapshot::default()
    };
    let without_schema = DomainRoutingSnapshot {
        relay_services: HashMap::from_iter([(relay.clone(), test_relay_boundary_services())]),
        ..DomainRoutingSnapshot::default()
    };
    for routing in [passive, DomainRoutingSnapshot::default(), without_schema] {
        let target = match runtime.remote_stream_target(&routing, &installed, &relay) {
            Ok(_) => panic!("a relay this node does not run is not a remote stream target"),
            Err(error) => error,
        };
        assert_eq!(
            target.to_string(),
            "relay 'incoming' in domain 'installed' is not instantiated"
        );
    }
}
