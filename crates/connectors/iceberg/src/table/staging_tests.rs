//! A staging file reads back exactly the staged batches its appends recorded, from any of them.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::Cancelled;
use nervix_primitives::sync::StdArc;

use super::{AppendTarget, Appended, Staged, StagingError, append, create, read_span};

/// The mapped columns every fixture batch carries.
fn schema() -> StdArc<Schema> {
    StdArc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("action", DataType::Utf8, true),
    ]))
}

/// One staged batch of `user_ids` with their `actions`.
fn batch_of(
    schema: &StdArc<Schema>,
    user_ids: Vec<i64>,
    actions: Vec<Option<String>>,
) -> RecordBatch {
    RecordBatch::try_new(
        StdArc::clone(schema),
        vec![
            StdArc::new(Int64Array::from(user_ids)),
            StdArc::new(StringArray::from(actions)),
        ],
    )
    .assured("the columns match the fixture schema")
}

/// One staged batch holding `user_ids`, each row's action naming its user and every third one
/// left null.
fn batch(schema: &StdArc<Schema>, user_ids: &[i64]) -> RecordBatch {
    let mut actions = Vec::with_capacity(user_ids.len());
    for user_id in user_ids {
        if user_id % 3 == 0 {
            actions.push(None);
        } else {
            actions.push(Some(format!("action-{user_id}")));
        }
    }
    batch_of(schema, user_ids.to_vec(), actions)
}

/// A `before_batch` that lets every batch through.
fn every_batch() -> Result<(), Cancelled> {
    Ok(())
}

/// A `before_batch` that lets `allowed` batches through and refuses the next.
struct RefuseAfter {
    allowed: usize,
}

impl RefuseAfter {
    fn next(&mut self) -> Result<(), Cancelled> {
        let Some(left) = self.allowed.checked_sub(1) else {
            return Err(Cancelled);
        };
        self.allowed = left;
        Ok(())
    }
}

/// The length of the file at `path`.
fn file_length(path: &std::path::Path) -> u64 {
    std::fs::metadata(path)
        .assured("the staging file exists")
        .len()
}

/// Every contiguous span of `ranges`, read back from `path`, holds exactly the matching run of
/// `staged`.
fn assert_every_span_reads_back(
    path: &std::path::Path,
    schema_end: u64,
    schema: &Schema,
    ranges: &[std::ops::Range<u64>],
    staged: &[RecordBatch],
) {
    for start in 0..ranges.len() {
        for stop in start + 1..=ranges.len() {
            let span = ranges[start].start..ranges[stop - 1].end;
            let read = read_span(path, schema_end, span, stop - start, schema)
                .assured("every span of staged batches reads back");
            assert_eq!(read, staged[start..stop].to_vec());
        }
    }
}

#[test]
fn appends_continue_one_file_and_every_span_reads_back_its_batches() {
    let directory = tempfile::tempdir().assured("the staging directory is created");
    let path = directory.path().join("staging-1.arrow");
    let schema = schema();
    let batches = [
        batch(&schema, &[1, 2, 3]),
        batch(&schema, &[4]),
        batch(&schema, &[5, 6]),
    ];

    let schema_end = create(&path, &schema).assured("the staging file is created");
    let first = append(&path, schema_end, &schema, &batches[..2], every_batch)
        .assured("the first append writes its batches");
    let second = append(&path, first.end, &schema, &batches[2..], every_batch)
        .assured("the second append continues the file");

    let mut ranges = first.ranges.clone();
    ranges.extend(second.ranges.iter().cloned());
    assert_eq!(ranges[0].start, schema_end);
    for pair in ranges.windows(2) {
        assert_eq!(
            pair[0].end, pair[1].start,
            "appended batches are contiguous"
        );
    }
    assert_eq!(
        file_length(&path),
        second.end,
        "the file ends after its last staged batch"
    );
    assert_every_span_reads_back(&path, schema_end, &schema, &ranges, &batches);
}

#[test]
fn a_failed_append_is_cut_back_off_and_the_next_one_continues_from_the_recorded_end() {
    let directory = tempfile::tempdir().assured("the staging directory is created");
    let path = directory.path().join("staging-1.arrow");
    let schema = schema();
    let schema_end = create(&path, &schema).assured("the staging file is created");
    let kept = append(
        &path,
        schema_end,
        &schema,
        &[batch(&schema, &[1, 2])],
        every_batch,
    )
    .assured("the first append writes its batch");

    // The second append writes one batch and is refused before its next one.
    let mut refusal = RefuseAfter { allowed: 1 };
    let refused = append(
        &path,
        kept.end,
        &schema,
        &[batch(&schema, &[3]), batch(&schema, &[4])],
        || refusal.next(),
    )
    .err()
    .assured("a refused batch fails the append");

    assert_eq!(
        refused.current_context(),
        &StagingError::Append { path: path.clone() }
    );
    assert_eq!(refused.downcast_ref::<Cancelled>(), Some(&Cancelled));
    assert_eq!(
        file_length(&path),
        kept.end,
        "the failed append's batch is cut back off"
    );
    let resumed = append(
        &path,
        kept.end,
        &schema,
        &[batch(&schema, &[5])],
        every_batch,
    )
    .assured("the next append continues from the recorded end");
    assert_eq!(
        read_span(&path, schema_end, schema_end..resumed.end, 2, &schema)
            .assured("both staged batches read back"),
        vec![batch(&schema, &[1, 2]), batch(&schema, &[5])]
    );
}

#[test]
fn an_append_cuts_off_what_follows_the_recorded_end_before_it_writes() {
    let directory = tempfile::tempdir().assured("the staging directory is created");
    let path = directory.path().join("staging-1.arrow");
    let schema = schema();
    let schema_end = create(&path, &schema).assured("the staging file is created");
    let recorded = append(
        &path,
        schema_end,
        &schema,
        &[batch(&schema, &[1])],
        every_batch,
    )
    .assured("the first append writes its batch");
    // An append its emitter stopped waiting for wrote a batch no staged batch records.
    let unrecorded = append(
        &path,
        recorded.end,
        &schema,
        &[batch(&schema, &[2])],
        every_batch,
    )
    .assured("the unrecorded append writes its batch");
    assert!(unrecorded.end > recorded.end);

    let next = append(
        &path,
        recorded.end,
        &schema,
        &[batch(&schema, &[3])],
        every_batch,
    )
    .assured("the next append starts at the recorded end");

    assert_eq!(next.ranges[0].start, recorded.end);
    assert_eq!(file_length(&path), next.end);
    assert_eq!(
        read_span(&path, schema_end, schema_end..next.end, 2, &schema)
            .assured("the recorded batches read back"),
        vec![batch(&schema, &[1]), batch(&schema, &[3])]
    );
}

#[test]
fn a_span_holding_another_count_of_batches_than_recorded_is_refused() {
    let directory = tempfile::tempdir().assured("the staging directory is created");
    let path = directory.path().join("staging-1.arrow");
    let schema = schema();
    let schema_end = create(&path, &schema).assured("the staging file is created");
    let Appended { end, .. } = append(
        &path,
        schema_end,
        &schema,
        &[batch(&schema, &[1]), batch(&schema, &[2])],
        every_batch,
    )
    .assured("the append writes both batches");

    let refused = read_span(&path, schema_end, schema_end..end, 1, &schema)
        .err()
        .assured("a span of two batches recorded as one is refused");

    assert_eq!(
        refused.current_context(),
        &StagingError::BatchCount {
            path: path.clone(),
            expected: 1,
            found: 2,
        }
    );
}

#[test]
fn a_staging_file_read_with_another_schema_is_refused() {
    let directory = tempfile::tempdir().assured("the staging directory is created");
    let path = directory.path().join("staging-1.arrow");
    let schema = schema();
    let schema_end = create(&path, &schema).assured("the staging file is created");
    let Appended { end, .. } = append(
        &path,
        schema_end,
        &schema,
        &[batch(&schema, &[1])],
        every_batch,
    )
    .assured("the append writes its batch");
    let other = Schema::new(vec![Field::new("user_id", DataType::Int64, false)]);

    let refused = read_span(&path, schema_end, schema_end..end, 1, &other)
        .err()
        .assured("a staging file read with another schema is refused");

    assert_eq!(
        refused.current_context(),
        &StagingError::Schema { path: path.clone() }
    );
}

#[test]
fn a_new_staging_file_whose_first_append_is_refused_is_removed_again() {
    let directory = tempfile::tempdir().assured("the staging directory is created");
    let path = directory.path().join("staging-1.arrow");
    let schema = schema();
    let target = AppendTarget::New { path: path.clone() };

    let refused = target
        .append(&schema, &[batch(&schema, &[1])], || Err(Cancelled))
        .err()
        .assured("a refused first batch fails the append");

    assert_eq!(
        refused.current_context(),
        &StagingError::Append { path: path.clone() }
    );
    assert!(
        !path.exists(),
        "the file the failed append created is removed"
    );

    let Staged {
        schema_end,
        appended,
    } = target
        .append(&schema, &[batch(&schema, &[2])], every_batch)
        .assured("the next append creates the file again");
    assert_eq!(
        read_span(&path, schema_end, schema_end..appended.end, 1, &schema)
            .assured("the staged batch reads back"),
        vec![batch(&schema, &[2])]
    );
}

#[test]
fn a_staging_file_is_never_created_over_an_existing_file() {
    let directory = tempfile::tempdir().assured("the staging directory is created");
    let path = directory.path().join("staging-1.arrow");
    std::fs::write(&path, b"not a staging file").assured("the fixture file is written");
    let schema = schema();

    let refused = AppendTarget::New { path: path.clone() }
        .append(&schema, &[batch(&schema, &[1])], every_batch)
        .err()
        .assured("a path that names a file is refused");

    assert_eq!(
        refused.current_context(),
        &StagingError::Create { path: path.clone() }
    );
    assert_eq!(
        std::fs::read(&path).assured("the existing file is left in place"),
        b"not a staging file"
    );
}

/// One generated carrier: the action of each of its rows, and whether the append that carries it
/// ends after it.
#[derive(Debug, bolero::TypeGenerator)]
struct GeneratedCarrier {
    actions: Vec<Option<Vec<u8>>>,
    closes_append: bool,
}

/// Every span of staged batches reads back exactly the batches appended there, whichever appends
/// wrote them, and a refused append leaves the file as its earlier appends recorded it.
#[test]
fn bolero_staging_files_read_back_every_span_of_their_staged_batches() {
    let generator = bolero::generator::produce_with::<Vec<GeneratedCarrier>>().len(1_usize..=6);
    bolero::check!()
        .with_iterations(128)
        .with_max_len(1_024)
        .with_generator(generator)
        .for_each(|carriers| {
            let schema = schema();
            let directory = tempfile::tempdir().assured("the staging directory is created");
            let path = directory.path().join("staging-1.arrow");
            let schema_end = create(&path, &schema).assured("the staging file is created");
            let mut staged = Vec::new();
            let mut ranges = Vec::new();
            let mut end = schema_end;
            let mut pending = Vec::new();
            let mut next_user_id = 0_i64;
            for carrier in carriers {
                let mut user_ids = Vec::with_capacity(carrier.actions.len());
                let mut actions = Vec::with_capacity(carrier.actions.len());
                for generated in &carrier.actions {
                    next_user_id = next_user_id
                        .checked_add(1)
                        .assured("at most a thousand generated rows");
                    user_ids.push(next_user_id);
                    let Some(bytes) = generated else {
                        actions.push(None);
                        continue;
                    };
                    actions.push(Some(String::from_utf8_lossy(bytes).into_owned()));
                }
                pending.push(batch_of(&schema, user_ids, actions));
                if carrier.closes_append {
                    let appended = append(&path, end, &schema, &pending, every_batch)
                        .assured("an append of generated batches is written");
                    end = appended.end;
                    ranges.extend(appended.ranges);
                    staged.append(&mut pending);
                }
            }
            // A refused append of what is left changes nothing the earlier appends recorded.
            if !pending.is_empty() {
                let mut refusal = RefuseAfter {
                    allowed: pending.len() - 1,
                };
                let refused = append(&path, end, &schema, &pending, || refusal.next())
                    .err()
                    .assured("an append refused before its last batch fails");
                assert_eq!(
                    refused.current_context(),
                    &StagingError::Append { path: path.clone() }
                );
            }
            assert_eq!(file_length(&path), end);
            assert_every_span_reads_back(&path, schema_end, &schema, &ranges, &staged);
        });
}
