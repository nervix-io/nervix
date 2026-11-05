//! Layer: engines and infrastructure.
//! - **Owns.** Allowance generations and conversion between injected monotonic offsets and UTC.
//! - **Depends on.** Typed durations, Chrono and validated allowance declarations.
//! - **Must not know.** Persistence, system UTC, domains, clocks or graph policy.

use std::{collections::BTreeSet, time::Duration};

use chrono::{DateTime, TimeDelta, Utc};
use error_stack::Report;

use crate::{AdmissionError, Clause, KeyFingerprint, KeyedOptions, Quota};

pub(crate) struct RestorePlan {
    pub(crate) declarations: Vec<Clause>,
    pub(crate) maximum_age: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyGeneration {
    pub key: KeyFingerprint,
    pub remaining: Duration,
    pub age: Duration,
    pub ordinal: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClauseGeneration {
    Direct {
        quota: Quota,
        remaining: Option<Duration>,
    },
    Keyed {
        quota: Quota,
        options: KeyedOptions,
        keys: Vec<KeyGeneration>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Generation {
    pub clauses: Vec<ClauseGeneration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UtcKeyGeneration {
    pub key: KeyFingerprint,
    pub horizon: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub ordinal: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UtcClauseGeneration {
    Direct {
        quota: Quota,
        horizon: Option<DateTime<Utc>>,
    },
    Keyed {
        quota: Quota,
        options: KeyedOptions,
        keys: Vec<UtcKeyGeneration>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UtcGeneration {
    pub clauses: Vec<UtcClauseGeneration>,
}

fn delta(value: Duration) -> Result<TimeDelta, Report<AdmissionError>> {
    TimeDelta::from_std(value).map_err(|_| Report::new(AdmissionError::ClockRange))
}

impl Generation {
    pub(crate) fn prepare(&self) -> Result<RestorePlan, Report<AdmissionError>> {
        let mut declarations = Vec::with_capacity(self.clauses.len());
        let mut maximum_age = Duration::ZERO;
        for clause in &self.clauses {
            match clause {
                ClauseGeneration::Direct { quota, remaining } => {
                    if let Some(remaining) = remaining
                        && (remaining.is_zero() || *remaining > quota.horizon)
                    {
                        return Err(Report::new(AdmissionError::GenerationState));
                    }
                    declarations.push(Clause::direct(*quota));
                }
                ClauseGeneration::Keyed {
                    quota,
                    options,
                    keys,
                } => {
                    if keys.len() > options.max_keys {
                        return Err(Report::new(AdmissionError::GenerationState));
                    }
                    let mut identities = BTreeSet::new();
                    let mut previous: Option<&KeyGeneration> = None;
                    for key in keys {
                        if key.remaining.is_zero()
                            || key.remaining > quota.horizon
                            || key.age >= options.ttl
                            || key.ordinal == u64::MAX
                            || !identities.insert(key.key)
                        {
                            return Err(Report::new(AdmissionError::GenerationState));
                        }
                        if let Some(previous) = previous
                            && (previous.ordinal >= key.ordinal || previous.age < key.age)
                        {
                            return Err(Report::new(AdmissionError::GenerationState));
                        }
                        previous = Some(key);
                        maximum_age = maximum_age.max(key.age);
                    }
                    declarations.push(Clause::keyed(*quota, *options));
                }
            }
        }
        Ok(RestorePlan {
            declarations,
            maximum_age,
        })
    }

    pub fn to_utc(&self, utc_now: DateTime<Utc>) -> Result<UtcGeneration, Report<AdmissionError>> {
        self.prepare()?;
        let mut clauses = Vec::with_capacity(self.clauses.len());
        for clause in &self.clauses {
            let converted = match clause {
                ClauseGeneration::Direct { quota, remaining } => {
                    let horizon = match remaining {
                        Some(remaining) => Some(
                            utc_now
                                .checked_add_signed(delta(*remaining)?)
                                .ok_or_else(|| Report::new(AdmissionError::ClockRange))?,
                        ),
                        None => None,
                    };
                    UtcClauseGeneration::Direct {
                        quota: *quota,
                        horizon,
                    }
                }
                ClauseGeneration::Keyed {
                    quota,
                    options,
                    keys,
                } => {
                    let mut converted = Vec::with_capacity(keys.len());
                    for key in keys {
                        let horizon = utc_now
                            .checked_add_signed(delta(key.remaining)?)
                            .ok_or_else(|| Report::new(AdmissionError::ClockRange))?;
                        let last_seen = utc_now
                            .checked_sub_signed(delta(key.age)?)
                            .ok_or_else(|| Report::new(AdmissionError::ClockRange))?;
                        converted.push(UtcKeyGeneration {
                            key: key.key,
                            horizon,
                            last_seen,
                            ordinal: key.ordinal,
                        });
                    }
                    UtcClauseGeneration::Keyed {
                        quota: *quota,
                        options: *options,
                        keys: converted,
                    }
                }
            };
            clauses.push(converted);
        }
        Ok(UtcGeneration { clauses })
    }
}

impl UtcGeneration {
    pub fn from_utc(&self, utc_now: DateTime<Utc>) -> Result<Generation, Report<AdmissionError>> {
        let mut clauses = Vec::with_capacity(self.clauses.len());
        for clause in &self.clauses {
            let converted = match clause {
                UtcClauseGeneration::Direct { quota, horizon } => {
                    let remaining = match horizon {
                        Some(horizon) if *horizon > utc_now => Some(
                            horizon
                                .signed_duration_since(utc_now)
                                .to_std()
                                .map_err(|_| Report::new(AdmissionError::ClockRange))?,
                        ),
                        _ => None,
                    };
                    ClauseGeneration::Direct {
                        quota: *quota,
                        remaining,
                    }
                }
                UtcClauseGeneration::Keyed {
                    quota,
                    options,
                    keys,
                } => {
                    let mut converted = Vec::with_capacity(keys.len());
                    for key in keys {
                        if key.horizon <= utc_now {
                            continue;
                        }
                        let age = utc_now
                            .signed_duration_since(key.last_seen)
                            .to_std()
                            .map_err(|_| Report::new(AdmissionError::ClockRegressed))?;
                        if age >= options.ttl {
                            continue;
                        }
                        let remaining = key
                            .horizon
                            .signed_duration_since(utc_now)
                            .to_std()
                            .map_err(|_| Report::new(AdmissionError::ClockRange))?;
                        converted.push(KeyGeneration {
                            key: key.key,
                            remaining,
                            age,
                            ordinal: key.ordinal,
                        });
                    }
                    ClauseGeneration::Keyed {
                        quota: *quota,
                        options: *options,
                        keys: converted,
                    }
                }
            };
            clauses.push(converted);
        }
        let generation = Generation { clauses };
        generation.prepare()?;
        Ok(generation)
    }
}
