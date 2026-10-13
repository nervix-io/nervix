//! Public checks of large guest save capture, interruption, and resumed restore bytes.
//!
//! Layer: test harness.
//! - **Owns.** Arming a guest-section interruption and comparing the raw guest blobs of two
//!   verified public archives.
//! - **Depends on.** The backup scenario's archive reader and fault injection.
//! - **Must not know.** The node's checkpoint or staged-section representation.

use nervix_models::ModelName;

use super::{archive_path, restore::copy_of_archive, scenario_domain, *};

#[given(
    expr = "the next guest save capture of {string} in domain {string} is interrupted after {int} \
            chunks"
)]
fn given_guest_save_capture_is_interrupted(
    world: &mut ScenarioWorld,
    entity: String,
    domain: String,
    chunks: u64,
) {
    let domain = scenario_domain(world, &domain);
    let entity = ModelName::parse(&entity).assured("the scenario names a valid processor");
    world
        .fault_injection
        .interrupt_guest_save_capture(domain, entity, chunks);
}

#[then(expr = "backup archives {string} and {string} have identical guest saves above 32 MiB")]
fn then_large_guest_saves_match(world: &mut ScenarioWorld, source: String, restored: String) {
    fn saves(path: &std::path::Path) -> BTreeMap<String, Vec<u8>> {
        let copy = copy_of_archive(path);
        let saves = copy
            .sections
            .into_iter()
            .filter_map(|(path, bytes)| {
                let (_, relative) = path.split_once("/state/wasm_processor/")?;
                path.ends_with("/guest.bin")
                    .then(|| (relative.to_string(), bytes))
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(saves.len(), 2, "both guest branches have archived saves");
        assert!(
            saves.values().all(|bytes| bytes.len() > 32 * 1024 * 1024),
            "each saved guest exceeds the default bulk budget"
        );
        saves
    }
    assert_eq!(
        saves(&archive_path(world, &source)),
        saves(&archive_path(world, &restored)),
        "the restored guest saves are byte-for-byte identical"
    );
}
