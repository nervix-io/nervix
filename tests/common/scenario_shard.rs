//! The share of the scenario suite that one shard of a sharded run executes.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The `--shard` option, the order in which a sharded run numbers the suite's scenario
//!   runs, and which of those runs each shard keeps.
//! - **Depends on.** The features Cucumber's parser read, with their outline examples expanded.
//! - **Must not know.** What a scenario does, how a run is admitted to its run slots, or which CI
//!   job runs a shard.
//!
//! # How a sharded run divides the suite
//!
//! Every shard reads the same feature files. It numbers each scenario run the parser produced, an
//! outline's example row counting as one run, in the order of its feature file's path and then of
//! the run's line, and keeps the runs whose number leaves the shard's own remainder when divided by
//! the number of shards. Each run therefore belongs to exactly one shard, and the shards agree on
//! the division without exchanging anything.
//!
//! Consecutive runs go to different shards, so a feature's runs are dealt across the shards rather
//! than kept together. A whole feature cannot be the unit. The coordinated WASM state-reset feature
//! runs one scenario at a time, and in Check run 41010408001 its chain took 48.8 minutes of the
//! 85.5-minute suite, longer than a third of it. Dealt across the shards, each shard holds about its
//! share of that chain, of the web console features that run two at a time, and of every feature
//! of long-running scenarios.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::{NonZeroUsize, ParseIntError},
    path::PathBuf,
    str::FromStr,
};

use cucumber::gherkin::{Feature, Scenario};
use meticulous::OptionExt as _;

/// The environment variable a run sets to execute one shard of the suite, as `--shard` does.
pub(crate) const SCENARIO_SHARD_ENV: &str = "NERVIX_TEST_SCENARIO_SHARD";

/// Which shard of a sharded run this run executes, when it executes only one.
#[derive(Clone, Copy, Debug, clap::Args)]
pub(crate) struct ScenarioShardArgs {
    /// Execute one shard's share of the scenario runs, written `INDEX/COUNT` with the index counted
    /// from one. The shards of one count divide the runs between them, each run to exactly one.
    #[arg(long = "shard", env = SCENARIO_SHARD_ENV, value_name = "INDEX/COUNT")]
    shard: Option<ScenarioShard>,
}

impl ScenarioShardArgs {
    /// The shard this run executes, or `None` when it executes every run it selects.
    pub(crate) fn shard(self) -> Option<ScenarioShard> {
        self.shard
    }
}

/// One shard of a run divided into `count` shards: the shard numbered `index`, counted from one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ScenarioShard {
    index: NonZeroUsize,
    count: NonZeroUsize,
}

/// Why a shard's text names no shard.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum ScenarioShardError {
    #[error("a shard is written INDEX/COUNT, such as 1/3")]
    Form,
    #[error("{text:?} is not a whole number")]
    Number {
        text: String,
        #[source]
        source: ParseIntError,
    },
    #[error("shards are counted from one")]
    Zero,
    #[error("a run of {count} shards has no shard {index}")]
    BeyondCount {
        index: NonZeroUsize,
        count: NonZeroUsize,
    },
}

impl ScenarioShard {
    /// The shard numbered `index` of `count` shards.
    pub(crate) fn new(
        index: NonZeroUsize,
        count: NonZeroUsize,
    ) -> Result<Self, ScenarioShardError> {
        if index > count {
            return Err(ScenarioShardError::BeyondCount { index, count });
        }
        Ok(Self { index, count })
    }

    /// Keeps only this shard's runs of `features` and reports how many it kept of those it read.
    ///
    /// A feature the parser could not read is left as it is, so every shard reports it.
    pub(crate) fn keep_own_runs<E>(self, features: &mut [Result<Feature, E>]) -> ShardSelection {
        let mut positions = BTreeSet::new();
        let mut read = 0_usize;
        for feature in features.iter().flatten() {
            for scenario in Self::scenarios_of(feature) {
                positions.insert(RunPosition::of(feature, scenario));
                read = read
                    .checked_add(1)
                    .assured("a suite holds far fewer than usize::MAX scenario runs");
            }
        }
        let mut own_lines: BTreeMap<Option<PathBuf>, BTreeSet<usize>> = BTreeMap::new();
        for (number, position) in positions.into_iter().enumerate() {
            if self.owns(number) {
                own_lines
                    .entry(position.path)
                    .or_default()
                    .insert(position.line);
            }
        }
        let mut kept = 0_usize;
        for feature in features.iter_mut().flatten() {
            let lines = own_lines.remove(&feature.path).unwrap_or_default();
            feature
                .scenarios
                .retain(|scenario| lines.contains(&scenario.position.line));
            for rule in &mut feature.rules {
                rule.scenarios
                    .retain(|scenario| lines.contains(&scenario.position.line));
            }
            let kept_in_feature = Self::scenarios_of(feature).count();
            kept = kept
                .checked_add(kept_in_feature)
                .assured("a shard keeps no more runs than the suite holds");
        }
        ShardSelection {
            shard: self,
            kept,
            read,
        }
    }

    /// Every scenario run of `feature`, outside its rules and inside them.
    fn scenarios_of(feature: &Feature) -> impl Iterator<Item = &Scenario> {
        let in_rules = feature.rules.iter().flat_map(|rule| &rule.scenarios);
        feature.scenarios.iter().chain(in_rules)
    }

    /// Whether the run numbered `number`, counted from zero, belongs to this shard.
    fn owns(self, number: usize) -> bool {
        let remainder = number % self.count.get();
        let own_remainder = self
            .index
            .get()
            .checked_sub(1)
            .assured("a shard index counts from one");
        remainder == own_remainder
    }

    /// The shard number one part of a shard's text writes.
    fn number(text: &str) -> Result<NonZeroUsize, ScenarioShardError> {
        let number: usize = text.parse().map_err(|source| ScenarioShardError::Number {
            text: text.to_string(),
            source,
        })?;
        NonZeroUsize::new(number).ok_or(ScenarioShardError::Zero)
    }
}

impl FromStr for ScenarioShard {
    type Err = ScenarioShardError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let Some((index, count)) = text.split_once('/') else {
            return Err(ScenarioShardError::Form);
        };
        let index = Self::number(index)?;
        let count = Self::number(count)?;
        Self::new(index, count)
    }
}

impl fmt::Display for ScenarioShard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} of {}", self.index, self.count)
    }
}

/// Where one scenario run is written: its feature file and its line, an outline example's line
/// being the one Cucumber gives its row.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RunPosition {
    path: Option<PathBuf>,
    line: usize,
}

impl RunPosition {
    fn of(feature: &Feature, scenario: &Scenario) -> Self {
        Self {
            path: feature.path.clone(),
            line: scenario.position.line,
        }
    }
}

/// The runs one shard kept out of the runs the parser read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ShardSelection {
    pub(crate) shard: ScenarioShard,
    pub(crate) kept: usize,
    pub(crate) read: usize,
}

impl fmt::Display for ShardSelection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "shard {}: {} of the {} scenario runs read",
            self.shard, self.kept, self.read
        )
    }
}
