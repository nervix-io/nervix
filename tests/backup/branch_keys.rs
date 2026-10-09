//! Steps that copy an archive with altered branch keys, to probe how a restore checks each archived
//! key against the branching its restored entity declares.
//!
//! Layer: test harness.
//! - **Owns.** Copies of an archive whose branch lifecycle entries, branch state descriptors or
//!   materialized record identities under one state path hold keys of another shape.
//! - **Depends on.** The archive format's records, reader and writer.
//! - **Must not know.** How the server checks, converts or installs an archived branch key.

use cucumber::given;
use nervix_backup::{
    ArchiveRecord, BranchLifecycleRecord, DeduplicatorStateDescriptor,
    MaterializedIdentitiesRecord, StateField, StateValue, WasmStateDescriptor,
    WindowStateDescriptor,
};

use super::{
    restore::{copy_of_archive, write_archive},
    *,
};

/// How a step alters every branch key it finds under its state path.
enum KeyAlteration {
    /// The field takes another name.
    Rename { field: String, name: String },
    /// The field is taken out of the key.
    Remove { field: String },
    /// A string field the branch schema does not declare joins the key.
    Add { field: String },
    /// The field holds a string in place of its archived value.
    Retype { field: String },
    /// The key is taken away, so the state reads as unbranched.
    Unbranch,
}

impl KeyAlteration {
    fn alter(&self, key: &mut Option<Vec<StateField>>) {
        let Some(fields) = key.as_mut() else {
            panic!("every key a scenario alters names a concrete branch");
        };
        match self {
            Self::Rename { field, name } => Self::named(fields, field).name = name.clone(),
            Self::Remove { field } => {
                let before = fields.len();
                fields.retain(|archived| archived.name != *field);
                assert_ne!(fields.len(), before, "the key holds field '{field}'");
            }
            Self::Add { field } => fields.push(StateField {
                name: field.clone(),
                value: StateValue::String("added".to_string()),
            }),
            Self::Retype { field } => {
                Self::named(fields, field).value = StateValue::String("retyped".to_string());
            }
            Self::Unbranch => {
                *key = None;
                return;
            }
        }
        // An archived key lists its fields in name order, which the archive reader requires of
        // every key, so only the restore's check against the branch declaration can refuse it.
        fields.sort_by(|left, right| left.name.cmp(&right.name));
    }

    /// The field of `fields` named `field`. A scenario's branch keys hold a few fields each, which
    /// bounds the walk.
    fn named<'key>(fields: &'key mut [StateField], field: &str) -> &'key mut StateField {
        let Some(archived) = fields.iter_mut().find(|archived| archived.name == field) else {
            panic!("the key holds field '{field}'");
        };
        archived
    }
}

/// Copies archive `source` to `target` with every branch key the sections under
/// `domains/<domain>/state/<under>/` hold altered by `alteration`, each altered section carrying its
/// own digest in the rewritten manifest.
fn copy_with_altered_keys(
    world: &mut ScenarioWorld,
    source: &str,
    target: &str,
    under: &str,
    alteration: &KeyAlteration,
) {
    let copy = copy_of_archive(&archive_path(world, source));
    let under = format!("/state/{}/", expand_placeholders(world, under));
    let mut replaced = BTreeMap::new();
    let mut altered = 0_usize;
    for (path, bytes) in &copy.sections {
        if !path.contains(&under) {
            continue;
        }
        let encoded = if path.ends_with("/branches.rkyv") {
            let mut lifecycle =
                BranchLifecycleRecord::decode(path, bytes).expect("the archived lifecycle decodes");
            for entry in &mut lifecycle.branches {
                alteration.alter(&mut entry.key);
                altered += 1;
            }
            lifecycle.encode()
        } else if path.ends_with("/identities.rkyv") {
            let mut identities = MaterializedIdentitiesRecord::decode(path, bytes)
                .expect("the archived record identities decode");
            for identity in &mut identities.identities {
                alteration.alter(&mut identity.branch);
                altered += 1;
            }
            identities.encode()
        } else if path.ends_with("/descriptor.rkyv") && path.contains("/state/deduplicator/") {
            let mut descriptor = DeduplicatorStateDescriptor::decode(path, bytes)
                .expect("the archived deduplicator descriptor decodes");
            alteration.alter(&mut descriptor.branch);
            altered += 1;
            descriptor.encode()
        } else if path.ends_with("/descriptor.rkyv") && path.contains("/state/window_processor/") {
            let mut descriptor = WindowStateDescriptor::decode(path, bytes)
                .expect("the archived window descriptor decodes");
            alteration.alter(&mut descriptor.branch);
            altered += 1;
            descriptor.encode()
        } else if path.ends_with("/descriptor.rkyv") && path.contains("/state/wasm_processor/") {
            let mut descriptor = WasmStateDescriptor::decode(path, bytes)
                .expect("the archived WASM descriptor decodes");
            alteration.alter(&mut descriptor.branch);
            altered += 1;
            descriptor.encode()
        } else {
            continue;
        };
        replaced.insert(
            path.clone(),
            encoded.expect("an altered record of the current shape encodes"),
        );
    }
    assert_ne!(altered, 0, "the archive holds branch keys under '{under}'");
    write_archive(&copy, &replaced, &archive_path(world, target));
}

#[given(
    expr = "backup archive {string} is copied to {string} with field {string} renamed to {string} \
            in every branch key under {string}"
)]
fn given_branch_key_field_renamed(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    field: String,
    name: String,
    under: String,
) {
    copy_with_altered_keys(
        world,
        &source,
        &target,
        &under,
        &KeyAlteration::Rename { field, name },
    );
}

#[given(
    expr = "backup archive {string} is copied to {string} with field {string} removed from every \
            branch key under {string}"
)]
fn given_branch_key_field_removed(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    field: String,
    under: String,
) {
    copy_with_altered_keys(
        world,
        &source,
        &target,
        &under,
        &KeyAlteration::Remove { field },
    );
}

#[given(
    expr = "backup archive {string} is copied to {string} with field {string} added to every \
            branch key under {string}"
)]
fn given_branch_key_field_added(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    field: String,
    under: String,
) {
    copy_with_altered_keys(
        world,
        &source,
        &target,
        &under,
        &KeyAlteration::Add { field },
    );
}

#[given(
    expr = "backup archive {string} is copied to {string} with field {string} holding a string in \
            every branch key under {string}"
)]
fn given_branch_key_field_retyped(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    field: String,
    under: String,
) {
    copy_with_altered_keys(
        world,
        &source,
        &target,
        &under,
        &KeyAlteration::Retype { field },
    );
}

#[given(
    expr = "backup archive {string} is copied to {string} with every branch key under {string} \
            unbranched"
)]
fn given_branch_keys_unbranched(
    world: &mut ScenarioWorld,
    source: String,
    target: String,
    under: String,
) {
    copy_with_altered_keys(world, &source, &target, &under, &KeyAlteration::Unbranch);
}
