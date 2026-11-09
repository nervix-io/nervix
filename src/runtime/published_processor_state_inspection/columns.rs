//! Typed Arrow sections for selected keys, window rows and branch metadata.
//!
//! Layer: data plane.
//! - **Owns.** Typed Arrow sections for selected keys, window rows and branch metadata.
//! - **Depends on.** Published processor snapshots, typed execution plans and bulk admission.
//! - **Must not know.** Models, Plexus, sessions or presentation formats.

use super::*;

impl PublishedPage {
    pub(super) fn sections(
        &self,
        plan: &ReadPlan,
        cancellation: &Cancellation,
    ) -> ReadResult<Vec<ProcessorStateSectionColumns>> {
        cancellation
            .check()
            .change_context(ProcessorStatePageError::Admission)?;
        match plan {
            ReadPlan::Keys { fields, .. } => {
                let mut payload = Vec::with_capacity(fields.len());
                if !self.keys.is_empty() {
                    for (column, field) in fields.iter().enumerate() {
                        cancellation
                            .check()
                            .change_context(ProcessorStatePageError::Admission)?;
                        if self
                            .keys
                            .iter()
                            .any(|row| row.key.parts().len() != fields.len())
                        {
                            return Err(Report::new(ProcessorStatePageError::Columns));
                        }
                        payload.push(
                            ReorderKeyPart::typed_column(
                                field.data_type(),
                                self.keys.iter().map(|row| &row.key.parts()[column]),
                                self.keys.len(),
                            )
                            .change_context(ProcessorStatePageError::Columns)?,
                        );
                    }
                }
                Ok(vec![ProcessorStateSectionColumns {
                    section: ProcessorStateSection::Keys,
                    rows: self.counts.rows,
                    payload,
                    appended: vec![timestamp_column(self.keys.iter().map(|row| row.seen_at))],
                    key_fields: fields.clone(),
                }])
            }
            ReadPlan::Window { arguments, .. } => {
                let payload = payload_columns(self.windows.iter().map(|row| &row.entry.record))?;
                let mut appended: Vec<arrow_array::ArrayRef> = vec![
                    StdArc::new(arrow_array::UInt64Array::from_iter_values(
                        self.windows.iter().map(|row| row.entry.sequence),
                    )),
                    timestamp_column(self.windows.iter().map(|row| row.entry.timestamp)),
                ];
                appended.extend(payload_columns(
                    self.windows.iter().map(|row| &row.entry.arguments),
                )?);
                Ok(vec![
                    ProcessorStateSectionColumns {
                        section: ProcessorStateSection::Rows,
                        rows: self.counts.rows,
                        payload,
                        appended,
                        key_fields: arguments.fields().to_vec(),
                    },
                    header_columns(&self.headers),
                    structure_columns(&self.structures),
                ])
            }
        }
    }
}

fn header_columns(rows: &[HeaderRow]) -> ProcessorStateSectionColumns {
    use arrow_array::UInt64Array;
    let payload: Vec<arrow_array::ArrayRef> = if rows.is_empty() {
        Vec::new()
    } else {
        vec![
            StdArc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.revision),
            )),
            StdArc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.rows),
            )),
            StdArc::new(UInt64Array::from_iter(rows.iter().map(|row| row.first))),
            StdArc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.next),
            )),
            StdArc::new(UInt64Array::from_iter(
                rows.iter().map(|row| row.incarnation),
            )),
            StdArc::new(
                TimestampNanosecondArray::from_iter(
                    rows.iter().map(|row| row.due.map(Timestamp::unix_nanos)),
                )
                .with_timezone("+00:00"),
            ),
            StdArc::new(UInt64Array::from_iter_values(
                rows.iter().map(|row| row.bytes),
            )),
        ]
    };
    ProcessorStateSectionColumns {
        section: ProcessorStateSection::Header,
        rows: u64::try_from(rows.len()).assured("a bounded header count fits u64"),
        payload,
        appended: Vec::new(),
        key_fields: Vec::new(),
    }
}

fn structure_columns(rows: &[StructureRow]) -> ProcessorStateSectionColumns {
    let payload: Vec<arrow_array::ArrayRef> = if rows.is_empty() {
        Vec::new()
    } else {
        vec![
            StdArc::new(UInt32Array::from_iter_values(
                rows.iter().map(|row| row.index),
            )),
            StdArc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.kind.as_str()),
            )),
            StdArc::new(arrow_array::UInt64Array::from_iter_values(
                rows.iter().map(|row| row.count),
            )),
        ]
    };
    ProcessorStateSectionColumns {
        section: ProcessorStateSection::Accumulators,
        rows: u64::try_from(rows.len()).assured("bounded structure counts fit u64"),
        payload,
        appended: Vec::new(),
        key_fields: Vec::new(),
    }
}
