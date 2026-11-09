//! Public cluster-identity observations through Plexus and fresh-cluster lifecycle fixtures.
//!
//! Layer: test harness.
//! - **Owns.** Saving and comparing the public identity, and replacing a stopped fixture cluster.
//! - **Depends on.** The scenario's session client and the real in-process cluster lifecycle.
//! - **Must not know.** Private keys, identity storage, bootstrap proposals or signing internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the public cluster-identity scenario fixture")
)]

use cucumber::{given, then, when};
use meticulous::{OptionExt as _, ResultExt as _};
use uuid::{Uuid, Variant, Version};

use super::{ScenarioWorld, expand_placeholders, given_cluster_is_started};

#[given("the bootstrap node will propose competing cluster identities")]
fn request_competing_proposals(world: &mut ScenarioWorld) {
    world
        .fault_injection
        .request_competing_cluster_identity_proposals(
            nervix_models::ClusterNodeName::parse("node-1")
                .assured("the bootstrap fixture uses node-1"),
        );
}

#[then("both competing bootstrap identity proposals completed")]
fn competing_proposals_completed(world: &mut ScenarioWorld) {
    let node = nervix_models::ClusterNodeName::parse("node-1")
        .assured("the bootstrap fixture uses node-1");
    assert!(
        world
            .fault_injection
            .competing_cluster_identity_proposals_completed(&node)
    );
}

#[then(expr = "the inspected backup has the source identity saved as {string}")]
fn inspected_source_identity(world: &mut ScenarioWorld, name: String) {
    let expected = expand_placeholders(world, &format!("{{{{{name}_uid}}}}"));
    let output = world
        .last_cli_output
        .as_ref()
        .verified("the preceding archive inspection ran the CLI");
    assert!(output.status.success(), "the archive inspection succeeded");
    let inspected: serde_json::Value = serde_json::from_slice(&output.stdout)
        .assured("the CLI returned a JSON archive description");
    assert_eq!(inspected["cluster_uid"].as_str(), Some(expected.as_str()));
}

#[then(expr = "the public cluster identity is saved as {string}")]
fn save_public_identity(world: &mut ScenarioWorld, name: String) {
    let output = world
        .last_command_output
        .as_ref()
        .verified("the preceding public DESCRIBE command produced output");
    for (field, suffix) in [
        ("cluster uid: ", "uid"),
        ("cluster key: ", "key"),
        ("cluster created: ", "created"),
    ] {
        let Some(value) = output.lines().find_map(|line| line.strip_prefix(field)) else {
            panic!("DESCRIBE CLUSTER lacks {field:?}: {output}");
        };
        match suffix {
            "uid" => {
                let uid = Uuid::parse_str(value).assured("the public cluster UID is a UUID");
                assert_eq!(uid.get_version(), Some(Version::Random));
                assert_eq!(uid.get_variant(), Variant::RFC4122);
                assert_eq!(uid.to_string(), value);
            }
            "key" => {
                assert_eq!(
                    value.len(),
                    43,
                    "a SHA-256 base64url thumbprint has 43 characters"
                );
                assert!(
                    value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
                );
            }
            "created" => {
                chrono::DateTime::parse_from_rfc3339(value)
                    .assured("the public creation instant is RFC 3339");
            }
            _ => unreachable!("the local table has exactly three identity fields"),
        }
        world
            .placeholders
            .insert(format!("{name}_{suffix}"), value.to_string());
    }
}

#[then(expr = "every node eventually reports the public cluster identity saved as {string}")]
async fn every_node_reports_saved_identity(world: &mut ScenarioWorld, name: String) {
    let uid = expand_placeholders(world, &format!("{{{{{name}_uid}}}}"));
    let key = expand_placeholders(world, &format!("{{{{{name}_key}}}}"));
    let created = expand_placeholders(world, &format!("{{{{{name}_created}}}}"));
    let expected = [
        format!("cluster uid: {uid}"),
        format!("cluster key: {key}"),
        format!("cluster created: {created}"),
    ];
    for node in world.cluster().node_ids() {
        nervix_primitives::task::consume_budget().await;
        world
            .wait_for_cluster_description(&node, |output| {
                expected
                    .iter()
                    .all(|line| output.lines().any(|actual| actual == line))
            })
            .await;
    }
}

#[when(expr = "the stopped cluster is replaced with a fresh {int} node cluster")]
async fn replace_stopped_cluster(world: &mut ScenarioWorld, nodes: usize) {
    world.forget_session_observations();
    let mut preceding = world
        .cluster
        .take()
        .verified("the scenario started a source cluster");
    preceding
        .shutdown()
        .await
        .assured("the source cluster stops before its fixture is released");
    drop(preceding);
    world.fault_injection = Default::default();
    world.cluster_config.temp_dir = None;
    world.cluster_config.node_name_prefix = "clone".to_string();
    given_cluster_is_started(world, nodes).await;
}

#[then(expr = "the public cluster identity differs from the identity saved as {string}")]
fn identity_differs(world: &mut ScenarioWorld, name: String) {
    let previous_uid = expand_placeholders(world, &format!("{{{{{name}_uid}}}}"));
    let previous_key = expand_placeholders(world, &format!("{{{{{name}_key}}}}"));
    save_public_identity(world, "clone".to_string());
    assert_ne!(world.placeholders["clone_uid"], previous_uid);
    assert_ne!(world.placeholders["clone_key"], previous_key);
}
