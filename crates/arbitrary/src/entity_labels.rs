//! Bounded complete metadata maps from the shared entropy cursor.
//!
//! Layer: test harness.
//! - **Owns.** Valid exact keys, empty/Unicode values and complete map operands.
//! - **Depends on.** Vocabulary constructors and the existing deterministic generator.
//! - **Must not know.** Registry policy, parsing, persistence or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns bounded metadata generation")
)]

use meticulous::ResultExt as _;
use nervix_models::{EntityLabels, LabelAssignment, LabelKey};

use crate::Arbitrary;

impl Arbitrary<'_> {
    pub fn entity_labels(&mut self) -> EntityLabels {
        let count = usize::from(self.entropy.byte() % 8);
        let mut entries = Vec::with_capacity(count);
        for index in 0..count {
            let key = LabelKey::try_from(format!("team/key-{index}"))
                .assured("the bounded index is spelled with valid ASCII label characters");
            let value = match self.entropy.byte() % 6 {
                0 => String::new(),
                1 => " 'quoted' \"value\" ".to_owned(),
                2 => "Δ🙂\n\r\0".to_owned(),
                3 => "$s$verbatim$s$".to_owned(),
                4 => "Mixed Case / path".to_owned(),
                _ => self.entropy.byte().to_string(),
            };
            entries.push(LabelAssignment { key, value });
        }
        EntityLabels::try_from(entries).assured("each generated key has a distinct index")
    }
}
