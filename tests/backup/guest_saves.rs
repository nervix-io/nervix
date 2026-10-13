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

/// The guest saves an archive holds, keyed by processor and branch fingerprint.
fn archived_guest_saves(path: &std::path::Path) -> BTreeMap<String, Vec<u8>> {
    let copy = copy_of_archive(path);
    let mut saves = BTreeMap::new();
    for (section, bytes) in copy.sections {
        let Some((_, relative)) = section.split_once("/state/wasm_processor/") else {
            continue;
        };
        if !section.ends_with("/guest.bin") {
            continue;
        }
        saves.insert(relative.to_string(), bytes);
    }
    assert_eq!(saves.len(), 2, "both guest branches have archived saves");
    for (entry, bytes) in &saves {
        assert!(
            bytes.len() > 32 * 1024 * 1024,
            "the saved guest {entry} exceeds the default bulk budget"
        );
    }
    saves
}

/// How many bytes from the first differing offset a mismatch reports.
const GUEST_SAVE_DIFFERENCE_WINDOW: usize = 16;

/// One archived guest save, described without its bytes.
struct GuestSaveSummary {
    length: usize,
    blake3: String,
}

impl GuestSaveSummary {
    fn of(bytes: &[u8]) -> Self {
        Self {
            length: bytes.len(),
            blake3: blake3::hash(bytes).to_hex().to_string(),
        }
    }
}

/// Where two archives disagree about one branch's guest save, bounded however large the saves
/// are: a 40 MiB save printed whole buries the difference in hundreds of megabytes of output.
struct GuestSaveDifference {
    entry: String,
    source: Option<GuestSaveSummary>,
    restored: Option<GuestSaveSummary>,
    first_differing_offset: Option<usize>,
    source_bytes_at_offset: Vec<u8>,
    restored_bytes_at_offset: Vec<u8>,
}

impl GuestSaveDifference {
    fn between(
        source: &BTreeMap<String, Vec<u8>>,
        restored: &BTreeMap<String, Vec<u8>>,
    ) -> Vec<Self> {
        let entries = source
            .keys()
            .chain(restored.keys())
            .collect::<BTreeSet<_>>();
        let mut differences = Vec::new();
        for entry in entries {
            let source_bytes = source.get(entry);
            let restored_bytes = restored.get(entry);
            if source_bytes == restored_bytes {
                continue;
            }
            differences.push(Self::of(entry, source_bytes, restored_bytes));
        }
        differences
    }

    fn of(entry: &str, source: Option<&Vec<u8>>, restored: Option<&Vec<u8>>) -> Self {
        let mut difference = Self {
            entry: entry.to_string(),
            source: source.map(|bytes| GuestSaveSummary::of(bytes)),
            restored: restored.map(|bytes| GuestSaveSummary::of(bytes)),
            first_differing_offset: None,
            source_bytes_at_offset: Vec::new(),
            restored_bytes_at_offset: Vec::new(),
        };
        let (Some(source), Some(restored)) = (source, restored) else {
            return difference;
        };
        let mismatch = source
            .iter()
            .zip(restored.iter())
            .position(|(source_byte, restored_byte)| source_byte != restored_byte);
        // Saves that agree on their common prefix differ first where the shorter one ends.
        let offset = match mismatch {
            Some(offset) => offset,
            None => source.len().min(restored.len()),
        };
        difference.first_differing_offset = Some(offset);
        difference.source_bytes_at_offset = Self::window(source, offset);
        difference.restored_bytes_at_offset = Self::window(restored, offset);
        difference
    }

    fn window(bytes: &[u8], offset: usize) -> Vec<u8> {
        let tail = bytes.get(offset..).unwrap_or_default();
        tail.iter()
            .take(GUEST_SAVE_DIFFERENCE_WINDOW)
            .copied()
            .collect()
    }
}

impl std::fmt::Display for GuestSaveSummary {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} bytes, blake3 {}", self.length, self.blake3)
    }
}

impl std::fmt::Display for GuestSaveDifference {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: source ", self.entry)?;
        match &self.source {
            Some(summary) => write!(formatter, "{summary}")?,
            None => write!(formatter, "absent")?,
        }
        write!(formatter, ", restored ")?;
        match &self.restored {
            Some(summary) => write!(formatter, "{summary}")?,
            None => write!(formatter, "absent")?,
        }
        if let Some(offset) = self.first_differing_offset {
            write!(
                formatter,
                ", first differing offset {offset}: source {:02x?}, restored {:02x?}",
                self.source_bytes_at_offset, self.restored_bytes_at_offset
            )?;
        }
        Ok(())
    }
}

#[then(expr = "backup archives {string} and {string} have identical guest saves above 32 MiB")]
fn then_large_guest_saves_match(world: &mut ScenarioWorld, source: String, restored: String) {
    let source_saves = archived_guest_saves(&archive_path(world, &source));
    let restored_saves = archived_guest_saves(&archive_path(world, &restored));
    let differences = GuestSaveDifference::between(&source_saves, &restored_saves);
    let described = differences
        .iter()
        .map(GuestSaveDifference::to_string)
        .collect::<Vec<_>>();
    assert!(
        differences.is_empty(),
        "the restored guest saves are byte-for-byte identical; {source} and {restored} differ in: \
         {}",
        described.join("; ")
    );
}
