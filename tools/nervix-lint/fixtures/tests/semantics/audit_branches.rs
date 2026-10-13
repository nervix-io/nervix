//! Audit fixture: statements every branch of one exhaustive conditional repeats.
#![cfg_attr(
    nervix_lint,
    nervix::context(lifecycle, reason = "the audit calibration runs these examples once")
)]

pub struct Ledger {
    entries: Vec<u64>,
    total: u64,
}

impl Ledger {
    /// Both branches record the value the same way.
    pub fn record(&mut self, value: u64, large: bool) {
        if large {
            self.entries.push(value);
            self.total += value;
        } else {
            self.entries.push(value);
            self.total += 1;
        }
    }

    /// Every arm of the match records the same value.
    pub fn classify(&mut self, value: u64) {
        match value {
            0 => {
                self.entries.push(value);
            }
            1..=9 => {
                self.entries.push(value);
                self.total += 1;
            }
            _ => self.entries.push(value),
        }
    }

    /// A conditional without an else is not exhaustive; distinct statements and exits are not
    /// repeated work.
    pub fn distinct(&mut self, value: u64) -> u64 {
        if value == 0 {
            self.entries.push(value);
        }
        if value > 10 {
            self.entries.push(value);
            return self.total;
        } else {
            self.entries.clear();
            return self.total;
        }
    }
}

pub enum Column {
    Narrow(Vec<u8>),
    Wide(Vec<u64>),
}

/// Arms that spell one call over differently typed bindings dispatch by type.
pub fn width(column: &Column) -> usize {
    match column {
        Column::Narrow(values) => values.len(),
        Column::Wide(values) => values.len(),
    }
}

/// Constructing the same value in every arm is not repeated work.
pub fn accepted(column: &Column) -> Option<usize> {
    match column {
        Column::Narrow(_) => Some(1),
        Column::Wide(_) => Some(1),
    }
}
