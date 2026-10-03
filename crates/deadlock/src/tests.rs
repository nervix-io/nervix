use std::{
    num::{NonZeroU64, NonZeroUsize},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::Entropy;
use nervix_primitives::deadlock::{
    Access, ActiveCycle, BlockedAttempt, BlockedThread, BoundedText, Finding, LockKind, LockSite,
    MAX_CYCLE_THREADS, MAX_TEXT_BYTES, SourceSite, TrackedLockId, TrackedThreadId, WaitedLock,
};

use crate::{
    DeadlockEvidence, EvidenceDirectory, EvidenceError, EvidenceOutOfBounds, MAX_FINDINGS,
    ProcessRecord, render_finding,
    wire::{
        CycleWire, EvidenceWire, FindingWire, ProcessWire, TextWire, ThreadWire, WaitedLockWire,
        decode_wire, encode_wire,
    },
};

fn id(number: u64) -> NonZeroU64 {
    NonZeroU64::new(number).assured("tests number threads and locks from one")
}

fn at(nanos: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(nanos)
}

fn site(file: &str, line: u32) -> SourceSite {
    SourceSite {
        file: BoundedText::new(file),
        line,
        column: 9,
    }
}

fn process() -> ProcessRecord {
    ProcessRecord {
        id: 4_242,
        program: Some(BoundedText::new("nervix-server")),
        started_at: at(1_700_000_000_000_000_000),
    }
}

fn blocked(thread: u64, lock: u64, name: Option<&str>) -> BlockedThread {
    BlockedThread {
        thread: TrackedThreadId::new(id(thread)),
        name: name.map(BoundedText::new),
        waits_for: Some(WaitedLock {
            id: TrackedLockId::new(id(lock)),
            site: Some(LockSite {
                kind: LockKind::Mutex,
                constructed_at: site("src/runtime/state_store.rs", 425),
            }),
        }),
        attempt: Some(BlockedAttempt {
            access: Access::Exclusive,
            at: site("src/runtime/state_store.rs", 1_100),
        }),
    }
}

fn two_thread_cycle() -> ActiveCycle {
    let unrecorded = BlockedThread {
        thread: TrackedThreadId::new(id(5)),
        name: None,
        waits_for: Some(WaitedLock {
            id: TrackedLockId::new(id(17)),
            site: None,
        }),
        attempt: None,
    };
    ActiveCycle::new(
        at(1_700_000_000_500_000_000),
        vec![blocked(3, 12, Some("tokio-runtime-worker")), unrecorded],
        0,
    )
    .assured("two threads are within the bounds")
}

fn evidence_with_every_variant() -> DeadlockEvidence {
    let unnamed_lock = BlockedThread {
        thread: TrackedThreadId::new(id(9)),
        name: Some(BoundedText::new(&"ü".repeat(MAX_TEXT_BYTES))),
        waits_for: None,
        attempt: None,
    };
    let reader = BlockedThread {
        attempt: Some(BlockedAttempt {
            access: Access::Shared,
            at: site("crates/consensus/src/storage_fault.rs", 140),
        }),
        ..blocked(11, 13, Some("reader"))
    };
    let mut threads = vec![reader; MAX_CYCLE_THREADS];
    threads[0] = unnamed_lock;
    let longest = ActiveCycle::new(at(u64::MAX), threads, 7).assured("the bound allows omitting");
    DeadlockEvidence::new(
        process(),
        vec![
            Finding::ActiveCycle(two_thread_cycle()),
            Finding::ActiveCycle(longest),
            Finding::Overflow { lost: id(2) },
        ],
    )
    .assured("three findings are within the bound")
}

#[test]
fn evidence_decodes_from_its_own_encoding_to_the_complete_original() {
    for evidence in [
        evidence_with_every_variant(),
        DeadlockEvidence::started(ProcessRecord {
            program: None,
            ..process()
        }),
    ] {
        let encoded = evidence.encode().assured("valid evidence encodes");
        assert_eq!(
            DeadlockEvidence::decode(&encoded).assured("its own encoding decodes"),
            evidence
        );
    }
}

#[test]
fn a_header_of_other_bytes_is_refused_before_the_payload_is_read() {
    let encoded = DeadlockEvidence::started(process())
        .encode()
        .assured("valid evidence encodes");
    let mut foreign = encoded.clone();
    foreign[0] = b'X';
    let mut other_kind = encoded.clone();
    other_kind[8] = 2;
    let mut other_version = encoded.clone();
    other_version[10] = 9;
    let cases = [
        (foreign, EvidenceError::ForeignMagic),
        (encoded[..9].to_vec(), EvidenceError::ForeignMagic),
        (encoded[..11].to_vec(), EvidenceError::ForeignMagic),
        (other_kind, EvidenceError::ForeignKind { found: 2 }),
        (
            other_version,
            EvidenceError::UnsupportedVersion {
                found: 9,
                supported: 1,
            },
        ),
    ];
    for (bytes, expected) in cases {
        let refused = DeadlockEvidence::decode(&bytes);
        let Err(refusal) = refused else {
            panic!("other bytes decoded as evidence");
        };
        assert_eq!(*refusal.current_context(), expected);
    }
}

#[test]
fn evidence_larger_than_its_bound_is_refused_unread() {
    let limit = usize::try_from(crate::wire::MAX_EVIDENCE_BYTES).assured("the bound fits usize");
    let oversized = vec![0_u8; limit.checked_add(1).assured("the bound is small")];
    let refused = DeadlockEvidence::decode(&oversized);
    let Err(refusal) = refused else {
        panic!("oversized bytes decoded");
    };
    assert!(matches!(
        refusal.current_context(),
        EvidenceError::TooLarge { .. }
    ));
}

#[test]
fn a_finding_past_the_bound_is_refused() {
    let mut evidence = DeadlockEvidence::started(process());
    for lost in 1..=MAX_FINDINGS {
        let lost = u64::try_from(lost).assured("the bound fits u64");
        evidence = evidence
            .with_finding(Finding::Overflow { lost: id(lost) })
            .assured("findings up to the bound are recorded");
    }
    let refused = evidence.with_finding(Finding::Overflow { lost: id(1) });
    assert_eq!(
        refused,
        Err(EvidenceOutOfBounds::TooManyFindings {
            findings: MAX_FINDINGS
        })
    );
}

#[test]
fn a_cycle_renders_every_thread_with_its_lock_and_where_it_waits() {
    let rendered = render_finding(&Finding::ActiveCycle(two_thread_cycle()));
    let lines: Vec<&str> = rendered.lines().collect();
    assert_eq!(
        lines,
        [
            "nervix deadlock detector: active deadlock among 2 threads, detected at \
             2023-11-14T22:13:20.500000000Z",
            "  thread 3 (tokio-runtime-worker) waits for lock 12, a mutex constructed at \
             src/runtime/state_store.rs:425:9; it asked for exclusive access at \
             src/runtime/state_store.rs:1100:9",
            "  thread 5 (unnamed) waits for lock 17, whose construction was not recorded; where \
             it asked for the lock was not recorded",
            "  each thread waits for a lock the next one holds, and the last for one the first \
             holds",
        ]
    );
}

#[test]
fn a_cycle_of_one_omitted_threads_and_lost_findings_say_so() {
    let one = ActiveCycle::new(at(0), vec![blocked(1, 2, None)], 0).assured("within bounds");
    let rendered = render_finding(&Finding::ActiveCycle(one));
    assert!(
        rendered.contains("active deadlock among 1 thread,"),
        "{rendered}"
    );
    assert!(
        rendered.ends_with("  the thread waits for a lock it holds itself\n"),
        "{rendered}"
    );

    let threads = vec![blocked(1, 2, Some("worker")); MAX_CYCLE_THREADS];
    let long = ActiveCycle::new(at(0), threads, 3).assured("within bounds");
    let rendered = render_finding(&Finding::ActiveCycle(long));
    assert!(
        rendered.contains("active deadlock among 67 threads"),
        "{rendered}"
    );
    assert!(
        rendered.contains("  ... and 3 more threads, beyond the 64 a finding describes\n"),
        "{rendered}"
    );
    assert_eq!(rendered.lines().count(), MAX_CYCLE_THREADS + 3);

    let mut detectorless = blocked(4, 5, Some("worker"));
    detectorless.waits_for = None;
    let unnamed = ActiveCycle::new(at(0), vec![detectorless], 0).assured("within bounds");
    let rendered = render_finding(&Finding::ActiveCycle(unnamed));
    assert!(
        rendered.contains("waits for a lock the detector did not name"),
        "{rendered}"
    );

    let rendered = render_finding(&Finding::Overflow { lost: id(4) });
    assert_eq!(
        rendered,
        "nervix deadlock detector: 4 more findings were made while the hand-off to the recorder \
         was full; they are not described\n"
    );
}

#[test]
fn a_text_past_the_bound_keeps_its_leading_characters_and_says_how_long_it_was() {
    let long = "✓".repeat(MAX_TEXT_BYTES);
    let kept = BoundedText::new(&long);
    assert!(kept.is_truncated());
    assert!(kept.as_str().len() <= MAX_TEXT_BYTES);
    assert!(kept.as_str().len() > MAX_TEXT_BYTES - 3);
    assert_eq!(
        kept.original_bytes(),
        u64::try_from(long.len()).assured("fits")
    );
    assert!(
        kept.to_string()
            .ends_with(&format!("... (cut from {} bytes)", long.len()))
    );
    let short = BoundedText::new("worker");
    assert!(!short.is_truncated());
    assert_eq!(short.to_string(), "worker");
}

#[test]
fn the_directory_records_each_process_in_its_own_file_and_replaces_it_whole() {
    let root = tempfile::tempdir().assured("a temporary directory can be created");
    let directory = EvidenceDirectory::new(root.path());
    let started = DeadlockEvidence::started(process());
    let file = directory
        .record(&started)
        .assured("the directory is writable");
    assert_eq!(
        directory.files().assured("listable"),
        std::slice::from_ref(&file)
    );
    std::fs::write(root.path().join("notes.txt"), b"not evidence").assured("writable");
    std::fs::write(root.path().join("deadlock-1.partial"), b"partial").assured("writable");

    let found = started
        .with_finding(Finding::ActiveCycle(two_thread_cycle()))
        .assured("one finding is within the bound");
    let replaced = directory
        .record(&found)
        .assured("the directory is writable");
    assert_eq!(replaced, file);
    assert_eq!(
        directory.read_all().assured("readable"),
        std::slice::from_ref(&found)
    );

    let other = DeadlockEvidence::started(ProcessRecord { id: 7, ..process() });
    directory
        .record(&other)
        .assured("the directory is writable");
    let mut all = directory.read_all().assured("readable");
    all.sort_by_key(|evidence| evidence.process().id);
    assert_eq!(all, [other, found]);
}

#[test]
fn a_directory_that_cannot_hold_evidence_fails_with_the_path() {
    let root = tempfile::tempdir().assured("a temporary directory can be created");
    let missing = EvidenceDirectory::new(root.path().join("missing"));
    let refused = missing.record(&DeadlockEvidence::started(process()));
    let Err(refusal) = refused else {
        panic!("evidence was written into a directory that does not exist");
    };
    assert!(matches!(
        refusal.current_context(),
        EvidenceError::Write { .. }
    ));
    let listed = missing.files();
    let Err(refusal) = listed else {
        panic!("a directory that does not exist was listed");
    };
    assert!(matches!(
        refusal.current_context(),
        EvidenceError::List { .. }
    ));

    let corrupt = EvidenceDirectory::new(root.path());
    std::fs::write(root.path().join("deadlock-1-2.rkyv"), b"NVXDLEVD").assured("writable");
    let read = corrupt.read_all();
    let Err(refusal) = read else {
        panic!("corrupt evidence was read");
    };
    assert!(matches!(
        refusal.current_context(),
        EvidenceError::Read { .. }
    ));
}

#[test]
fn an_out_of_bounds_value_says_which_bound_it_broke() {
    use nervix_primitives::deadlock::{CycleOutOfBounds, TextOutOfBounds};

    let cases = [
        (
            EvidenceOutOfBounds::TooManyFindings { findings: 17 },
            "17 findings, more than the 16 evidence holds",
        ),
        (
            EvidenceOutOfBounds::Text(TextOutOfBounds::Kept { bytes: 513 }),
            "a kept text of 513 bytes is longer than 512",
        ),
        (
            EvidenceOutOfBounds::Cycle(CycleOutOfBounds::Empty),
            "a cycle has no threads",
        ),
        (
            EvidenceOutOfBounds::ZeroIdentity,
            "a thread or lock numbered zero",
        ),
        (
            EvidenceOutOfBounds::NothingLost,
            "an overflow that lost no finding",
        ),
    ];
    for (bounds, expected) in cases {
        assert_eq!(bounds.to_string(), expected);
        assert_eq!(
            EvidenceError::OutOfBounds(bounds).to_string(),
            format!("the evidence describes a value outside its bounds: {expected}")
        );
    }
}

#[test]
fn a_directory_names_its_path_and_lists_only_evidence_files() {
    use std::os::unix::ffi::OsStrExt as _;

    let root = tempfile::tempdir().assured("a temporary directory can be created");
    let directory = EvidenceDirectory::new(root.path());
    assert_eq!(directory.path(), root.path());
    let foreign = std::ffi::OsStr::from_bytes(b"deadlock-\xff.rkyv");
    std::fs::write(root.path().join(foreign), b"not UTF-8").assured("writable");
    assert!(directory.files().assured("listable").is_empty());
}

#[test]
fn a_time_before_the_epoch_cannot_be_recorded() {
    let early = DeadlockEvidence::started(ProcessRecord {
        started_at: UNIX_EPOCH - Duration::from_secs(1),
        ..process()
    });
    let refused = early.encode();
    let Err(refusal) = refused else {
        panic!("a time before the epoch was encoded");
    };
    assert_eq!(*refusal.current_context(), EvidenceError::TimeOutOfRange);
}

/// Evidence built from the choices `entropy` makes: every value it holds is one a recording
/// process could hold, reaching every variant and the edges of every bound.
fn generated_evidence(entropy: &mut Entropy<'_>) -> DeadlockEvidence {
    let mut findings = Vec::new();
    for _ in 0..entropy.count(MAX_FINDINGS) {
        findings.push(generated_finding(entropy));
    }
    let process = ProcessRecord {
        id: u32::try_from(entropy.up_to(u64::from(u32::MAX))).assured("drawn below u32::MAX"),
        program: generated_optional_text(entropy),
        started_at: at(entropy.any_u64()),
    };
    DeadlockEvidence::new(process, findings).assured("at most the bound of findings is drawn")
}

fn generated_finding(entropy: &mut Entropy<'_>) -> Finding {
    if entropy.flag() {
        return Finding::Overflow {
            lost: generated_id(entropy),
        };
    }
    let count = entropy
        .positive_count(NonZeroUsize::new(MAX_CYCLE_THREADS).assured("the bound is nonzero"));
    let mut threads = Vec::with_capacity(count);
    for _ in 0..count {
        threads.push(generated_thread(entropy));
    }
    let omitted = if count == MAX_CYCLE_THREADS {
        entropy.any_u64()
    } else {
        0
    };
    Finding::ActiveCycle(
        ActiveCycle::new(at(entropy.any_u64()), threads, omitted)
            .assured("the drawn cycle is within its bounds"),
    )
}

fn generated_thread(entropy: &mut Entropy<'_>) -> BlockedThread {
    let waits_for = if entropy.flag() {
        let site = if entropy.flag() {
            Some(LockSite {
                kind: entropy.pick([LockKind::Mutex, LockKind::RwLock, LockKind::CondvarState]),
                constructed_at: generated_site(entropy),
            })
        } else {
            None
        };
        Some(WaitedLock {
            id: TrackedLockId::new(generated_id(entropy)),
            site,
        })
    } else {
        None
    };
    let attempt = if entropy.flag() {
        Some(BlockedAttempt {
            access: entropy.pick([Access::Exclusive, Access::Shared]),
            at: generated_site(entropy),
        })
    } else {
        None
    };
    BlockedThread {
        thread: TrackedThreadId::new(generated_id(entropy)),
        name: generated_optional_text(entropy),
        waits_for,
        attempt,
    }
}

fn generated_site(entropy: &mut Entropy<'_>) -> SourceSite {
    SourceSite {
        file: generated_text(entropy),
        line: u32::try_from(entropy.up_to(u64::from(u32::MAX))).assured("drawn below u32::MAX"),
        column: u32::try_from(entropy.up_to(u64::from(u32::MAX))).assured("drawn below u32::MAX"),
    }
}

fn generated_id(entropy: &mut Entropy<'_>) -> NonZeroU64 {
    id(entropy.boundary_biased(1..=u64::MAX))
}

fn generated_optional_text(entropy: &mut Entropy<'_>) -> Option<BoundedText> {
    if entropy.flag() {
        Some(generated_text(entropy))
    } else {
        None
    }
}

/// A text of up to a little past the bound, of characters one to four bytes long, so a cut can
/// land inside a character.
fn generated_text(entropy: &mut Entropy<'_>) -> BoundedText {
    let length = entropy.count(MAX_TEXT_BYTES.checked_add(8).assured("the bound is small"));
    let mut text = String::new();
    while text.len() < length {
        text.push(entropy.pick(['a', '/', 'é', '✓', '🦀']));
    }
    BoundedText::new(&text)
}

#[test]
fn bolero_deadlock_evidence_round_trips() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4_096)
        .for_each(|bytes: &[u8]| {
            let evidence = generated_evidence(&mut Entropy::new(bytes));
            let encoded = evidence.encode().assured("generated evidence encodes");
            let decoded = DeadlockEvidence::decode(&encoded).assured("its own encoding decodes");
            assert_eq!(decoded, evidence);
        });
}

#[test]
fn bolero_malformed_deadlock_evidence_is_refused_or_round_trips() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1_024)
        .for_each(|bytes: &[u8]| {
            let mut headed = b"NVXDLEVD\x01\x00\x01\x00".to_vec();
            headed.extend_from_slice(bytes);
            for candidate in [bytes, headed.as_slice()] {
                // Whatever decodes is evidence within every bound, and encodes back to itself.
                if let Ok(evidence) = DeadlockEvidence::decode(candidate) {
                    let encoded = evidence.encode().assured("decoded evidence encodes");
                    let decoded =
                        DeadlockEvidence::decode(&encoded).assured("its own encoding decodes");
                    assert_eq!(decoded, evidence);
                }
            }
        });
}

/// A wire shape that breaks exactly one bound of `evidence`, chosen by `entropy`.
fn out_of_bounds_wire(entropy: &mut Entropy<'_>, evidence: &DeadlockEvidence) -> EvidenceWire {
    let mut wire = EvidenceWire::try_from(evidence).assured("generated evidence converts");
    let thread = ThreadWire {
        thread: 1,
        name: None,
        waits_for: None,
        attempt: None,
    };
    let cycle = CycleWire {
        detected_at_unix_nanos: 0,
        threads: vec![thread.clone()],
        omitted_threads: 0,
    };
    match entropy.byte() % 9 {
        0 => {
            let past = MAX_FINDINGS.checked_add(1).assured("the bound is small");
            wire.findings = vec![FindingWire::Overflow { lost: 1 }; past];
        }
        1 => wire.findings.push(FindingWire::ActiveCycle(CycleWire {
            threads: Vec::new(),
            ..cycle
        })),
        2 => {
            let past = MAX_CYCLE_THREADS
                .checked_add(1)
                .assured("the bound is small");
            wire.findings.push(FindingWire::ActiveCycle(CycleWire {
                threads: vec![thread; past],
                ..cycle
            }));
        }
        3 => wire.findings.push(FindingWire::ActiveCycle(CycleWire {
            omitted_threads: entropy.boundary_biased(1..=u64::MAX),
            ..cycle
        })),
        4 => wire.findings.push(FindingWire::ActiveCycle(CycleWire {
            threads: vec![ThreadWire {
                thread: 0,
                ..thread
            }],
            ..cycle
        })),
        5 => wire.findings.push(FindingWire::ActiveCycle(CycleWire {
            threads: vec![ThreadWire {
                waits_for: Some(WaitedLockWire { id: 0, site: None }),
                ..thread
            }],
            ..cycle
        })),
        6 => wire.findings.push(FindingWire::Overflow { lost: 0 }),
        7 => {
            let past = MAX_TEXT_BYTES.checked_add(1).assured("the bound is small");
            wire.process = ProcessWire {
                program: Some(TextWire {
                    text: "a".repeat(past),
                    original_bytes: u64::try_from(past).assured("fits"),
                }),
                ..wire.process
            };
        }
        _ => {
            let kept = entropy.count(MAX_TEXT_BYTES - 4);
            let original = u64::try_from(kept).assured("fits");
            let program = if entropy.flag() {
                // Cut further than the bound requires.
                TextWire {
                    text: "a".repeat(kept),
                    original_bytes: original.checked_add(1).assured("small"),
                }
            } else {
                // Longer than the text it claims to be cut from.
                TextWire {
                    text: "a".repeat(kept.checked_add(1).assured("small")),
                    original_bytes: original,
                }
            };
            wire.process = ProcessWire {
                program: Some(program),
                ..wire.process
            };
        }
    }
    wire
}

#[test]
fn bolero_deadlock_evidence_outside_its_bounds_is_refused() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1_024)
        .for_each(|bytes: &[u8]| {
            let mut entropy = Entropy::new(bytes);
            let evidence = DeadlockEvidence::new(process(), Vec::new())
                .assured("evidence without findings is within the bounds");
            let wire = out_of_bounds_wire(&mut entropy, &evidence);
            let encoded = encode_wire(&wire).assured("a bounded number of wire values encodes");
            assert_eq!(
                decode_wire(&encoded).assured("the wire shape itself is valid"),
                wire
            );
            let refused = DeadlockEvidence::decode(&encoded);
            let Err(refusal) = refused else {
                panic!("evidence outside its bounds decoded: {wire:?}");
            };
            assert!(
                matches!(refusal.current_context(), EvidenceError::OutOfBounds(_)),
                "{refusal:?}"
            );
        });
}
