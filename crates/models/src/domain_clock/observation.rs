//! A domain clock as one node has it installed, in the form a client observes it.
//!
//! Layer: vocabulary.
//! - **Owns.** The observed START generation and installation state of a domain clock, and the
//!   text a client displays for them.
//! - **Depends on.** Validated clock periods, skews, mappings and timestamps.
//! - **Must not know.** How a node installs, publishes or delivers the clock, or any transport.

use std::fmt;

use super::{DomainClockPeriod, DomainClockSkew, DomainClockState};

/// A domain clock as one node has it installed: the START generation it belongs to and the state
/// of that generation's installation.
///
/// The states mirror a node's installation states. Every state a node publishes for a domain it
/// holds has one variant here; a domain the node no longer holds has none, because no clock is
/// left to observe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainClockObservation {
    /// The number of `START`s the domain has committed; zero before its first.
    pub generation: u64,
    pub state: DomainClockObservedState,
}

/// The installation state of one domain clock generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainClockObservedState {
    /// The generation is stopped and runs no domain work.
    Stopped,
    /// The generation is paced and running, and the node lacks its committed mapping or an
    /// assigned clock authority, so its clock cannot be read.
    Uninstalled,
    /// The generation reads actual UTC.
    Unpaced,
    /// The generation projects UTC through its committed mapping.
    Paced(PacedDomainClock),
}

/// The committed clock of a paced generation: its tick period, the admission skew, and the mapping
/// from UTC to logical time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PacedDomainClock {
    pub period: DomainClockPeriod,
    pub skew: DomainClockSkew,
    pub mapping: DomainClockState,
}

impl fmt::Display for DomainClockObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "generation {}, ", self.generation)?;
        match &self.state {
            DomainClockObservedState::Stopped => formatter.write_str("stopped"),
            DomainClockObservedState::Uninstalled => formatter.write_str("uninstalled"),
            DomainClockObservedState::Unpaced => formatter.write_str("unpaced"),
            DomainClockObservedState::Paced(paced) => write!(
                formatter,
                "paced: period {}, skew {}, logical origin {}, UTC anchor {}, time rate {}",
                paced.period,
                paced.skew,
                paced.mapping.logical_start().to_rfc3339(),
                paced.mapping.wall_started_at().to_rfc3339(),
                paced.mapping.time_rate(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use meticulous::ResultExt as _;

    use super::*;
    use crate::{DomainTimeRate, Timestamp};

    fn observation(state: DomainClockObservedState) -> DomainClockObservation {
        DomainClockObservation {
            generation: 3,
            state,
        }
    }

    #[test]
    fn every_state_displays_its_generation_and_state() {
        assert_eq!(
            observation(DomainClockObservedState::Stopped).to_string(),
            "generation 3, stopped"
        );
        assert_eq!(
            observation(DomainClockObservedState::Uninstalled).to_string(),
            "generation 3, uninstalled"
        );
        assert_eq!(
            observation(DomainClockObservedState::Unpaced).to_string(),
            "generation 3, unpaced"
        );
        let paced = PacedDomainClock {
            period: DomainClockPeriod::try_from(Duration::from_millis(100))
                .assured("one hundred milliseconds is a valid period"),
            skew: DomainClockSkew::try_from(Duration::from_millis(10))
                .assured("ten milliseconds is a valid skew"),
            mapping: DomainClockState::new(
                Timestamp::from_unix_nanos(1_500_000_001),
                "2030-01-01T00:00:00Z"
                    .parse()
                    .assured("the fixture origin is RFC 3339"),
                DomainTimeRate::try_from(2.5).assured("the fixture rate is positive and finite"),
            ),
        };
        assert_eq!(
            observation(DomainClockObservedState::Paced(paced)).to_string(),
            "generation 3, paced: period 100ms, skew 10ms, logical origin 2030-01-01T00:00:00Z, \
             UTC anchor 1970-01-01T00:00:01.500000001Z, time rate 2.5"
        );
    }
}
