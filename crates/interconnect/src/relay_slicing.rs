//! The position of contiguous relay pieces under changing receiver bounds.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Shrinking an unadmitted range and advancing a confirmed prefix exactly once.
//! - **Depends on.** Typed receive bounds and integer row counts.
//! - **Must not know.** Arrow, acknowledgement storage, graph policy or runtime tasks.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the relay re-slice cursor")
)]

use meticulous::OptionExt as _;
use nervix_models::PeerReceiveBounds;

/// A refusal never advances the cursor. Only confirmed admission advances its prefix.
#[doc(hidden)]
pub struct RelaySliceCursor {
    rows: usize,
    admitted: usize,
    length: usize,
    bounds: Option<PeerReceiveBounds>,
}

impl RelaySliceCursor {
    pub fn new(rows: usize) -> Self {
        Self {
            rows,
            admitted: 0,
            length: rows,
            bounds: None,
        }
    }
    pub fn admitted_rows(&self) -> usize {
        self.admitted
    }
    pub fn total_rows(&self) -> usize {
        self.rows
    }
    pub fn part_rows(&self) -> usize {
        self.length
    }
    pub fn receive_bounds(&self) -> Option<PeerReceiveBounds> {
        self.bounds
    }
    pub fn refuse(&mut self, bounds: PeerReceiveBounds) {
        self.bounds = Some(match self.bounds {
            Some(current) => current.intersection(bounds),
            None => bounds,
        });
    }
    pub fn shrink(&mut self) -> bool {
        if self.length <= 1 {
            return false;
        }
        self.length /= 2;
        true
    }
    pub fn admit(&mut self) {
        self.admitted = self
            .admitted
            .checked_add(self.length)
            .assured("the selected range is within this batch");
        self.length = self.length.min(
            self.rows
                .checked_sub(self.admitted)
                .assured("admitted rows never exceed the batch"),
        );
    }
}
