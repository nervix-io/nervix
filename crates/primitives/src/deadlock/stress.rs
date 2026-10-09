//! The bounded scheduling disturbance a `deloxide-stress` build compiles into its tracked locks.
//!
//! Deloxide's random preemption delays a thread that already holds a tracked lock before it asks
//! for another one, with a fixed probability and for a delay drawn between two bounds, and can
//! yield a thread after it releases one. A wait-for cycle needs two threads that each hold a lock
//! the other wants at the same moment; widening the window between one acquisition and the next
//! makes the lifecycle paths that take nested locks meet that moment far more often than an idle
//! schedule does. Disturbance never decides a finding: the detector reports exactly the cycles it
//! would report without it.
//!
//! A configuration is bounded by construction: a probability in millionths of at most one, delays
//! of whole microseconds between one microsecond and [`MAX_STRESS_DELAY`], the shortest no longer
//! than the longest. Deloxide draws its preemptions and delays from an entropy source it seeds
//! itself, so a configuration reproduces the disturbance's distribution, not one schedule.
//!
//! Deloxide's component-based strategy is not offered: it appends every nested acquisition to a
//! list it never trims and scans that list on each one, so its memory and its cost per acquisition
//! grow with the life of the process.

use std::{fmt, num::NonZeroU32, time::Duration};

/// The probability scale of a configuration: a preemption probability of one is this many
/// millionths.
pub const PREEMPTION_SCALE: u32 = 1_000_000;

/// The longest delay a configuration may inject before one acquisition. Product physical deadlines
/// are measured in hundreds of milliseconds and longer; a delay below this bound holds one runtime
/// worker for a fraction of the shortest of them.
pub const MAX_STRESS_DELAY: Duration = Duration::from_millis(2);

/// Deloxide's random preemption, as one diagnostic process applies it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StressConfiguration {
    preemptions_per_million: NonZeroU32,
    shortest_delay: Duration,
    longest_delay: Duration,
    yield_after_release: bool,
}

/// Why a configuration is not one [`StressConfiguration::new`] accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StressOutOfBounds {
    /// The probability is above one.
    Probability { per_million: u32 },
    /// A delay is zero, longer than [`MAX_STRESS_DELAY`], or not a whole number of microseconds.
    Delay { delay: Duration },
    /// The shortest delay is longer than the longest.
    Inverted {
        shortest: Duration,
        longest: Duration,
    },
}

impl fmt::Display for StressOutOfBounds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Probability { per_million } => write!(
                f,
                "a preemption probability of {per_million} millionths is above one"
            ),
            Self::Delay { delay } => write!(
                f,
                "a stress delay of {delay:?} is not a whole number of microseconds between 1µs \
                 and {MAX_STRESS_DELAY:?}"
            ),
            Self::Inverted { shortest, longest } => write!(
                f,
                "the shortest stress delay {shortest:?} is longer than the longest {longest:?}"
            ),
        }
    }
}

impl std::error::Error for StressOutOfBounds {}

impl StressConfiguration {
    /// The configuration of the diagnostic lane's `deloxide-stress` selection: one in twenty of the
    /// acquisitions a thread makes while it holds a tracked lock waits between 20µs and 200µs
    /// first. A release does not yield: a yield after every release of every tracked lock, beside
    /// a quarter of nested acquisitions delayed by up to a millisecond, made stressed nodes miss
    /// their product deadlines.
    pub const LANE: Self = match Self::new(
        match NonZeroU32::new(PREEMPTION_SCALE / 20) {
            Some(per_million) => per_million,
            None => panic!("a twentieth of the scale is nonzero"),
        },
        Duration::from_micros(20),
        Duration::from_micros(200),
        false,
    ) {
        Ok(configuration) => configuration,
        Err(_) => panic!("the lane's stress configuration is within its bounds"),
    };

    /// A configuration that preempts `preemptions_per_million` of the nested acquisitions for a
    /// delay between `shortest_delay` and `longest_delay`, and yields after every release when
    /// `yield_after_release` is set.
    pub const fn new(
        preemptions_per_million: NonZeroU32,
        shortest_delay: Duration,
        longest_delay: Duration,
        yield_after_release: bool,
    ) -> Result<Self, StressOutOfBounds> {
        if preemptions_per_million.get() > PREEMPTION_SCALE {
            return Err(StressOutOfBounds::Probability {
                per_million: preemptions_per_million.get(),
            });
        }
        if !Self::is_delay(shortest_delay) {
            return Err(StressOutOfBounds::Delay {
                delay: shortest_delay,
            });
        }
        if !Self::is_delay(longest_delay) {
            return Err(StressOutOfBounds::Delay {
                delay: longest_delay,
            });
        }
        if shortest_delay.as_micros() > longest_delay.as_micros() {
            return Err(StressOutOfBounds::Inverted {
                shortest: shortest_delay,
                longest: longest_delay,
            });
        }
        Ok(Self {
            preemptions_per_million,
            shortest_delay,
            longest_delay,
            yield_after_release,
        })
    }

    const fn is_delay(delay: Duration) -> bool {
        let whole_microseconds = delay.subsec_nanos().is_multiple_of(1_000);
        let positive = !delay.is_zero();
        let bounded = delay.as_nanos() <= MAX_STRESS_DELAY.as_nanos();
        whole_microseconds && positive && bounded
    }

    /// How many of a million nested acquisitions wait first.
    pub const fn preemptions_per_million(self) -> NonZeroU32 {
        self.preemptions_per_million
    }

    pub const fn shortest_delay(self) -> Duration {
        self.shortest_delay
    }

    pub const fn longest_delay(self) -> Duration {
        self.longest_delay
    }

    pub const fn yield_after_release(self) -> bool {
        self.yield_after_release
    }
}
