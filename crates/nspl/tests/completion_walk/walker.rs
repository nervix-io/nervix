//! The walk itself: a level-synchronous breadth-first sweep of the completion graph.
//!
//! Every level is evaluated by a bounded pool of workers doing nothing but pure `suggest`/`parse`
//! calls, and every decision — dedup, budget, finding emission, what the next level contains — is
//! made afterwards on one thread over the results in index order. That split is what keeps the
//! report byte-identical at any `--jobs`.
//!
//! Breadth-first is also the useful order: findings surface by how few keystrokes it takes to reach
//! them, so a run cut short by a budget has still covered the most reachable grammar.

use std::{
    hash::{DefaultHasher, Hash, Hasher},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::atomic::{AtomicUsize, Ordering},
};

use ahash_compile_time::{HashSet, HashSetExt};

use crate::{
    grammar::{Grammar, ParseOutcome},
    report::{CONTEXT_LABELS, Finding, FindingKind, Report, SHAPE_LABELS},
    suggestion::{Materialized, Materializer, SuggestionClass},
};

/// Headroom for the recursive descent the parser performs over synthesized inputs.
const WORKER_STACK_SIZE: usize = 16 * 1024 * 1024;

/// How many characters of a keyword are typed when probing the prefix filter.
const PREFIX_PROBE_LEN: usize = 2;

/// Bounds that keep an infinite graph finite. All deterministic, so two runs agree exactly.
#[derive(Debug, Clone, Copy)]
pub struct Budget {
    pub max_depth: usize,
    pub max_label_repeats: usize,
    pub max_states: usize,
    pub max_frontier: usize,
    pub signature_window: usize,
}

/// One node of the walk: the statement built so far, plus how the last step got here.
#[derive(Debug, Clone)]
pub struct WalkState {
    pub input: String,
    pub path: Vec<String>,
    /// Whether the last step injected a canned free-form body.
    free_form: bool,
}

impl WalkState {
    pub fn seed(input: String) -> Self {
        Self {
            path: Vec::new(),
            input,
            free_form: false,
        }
    }

    pub fn extend(&self, label: &str, materialized: &Materialized) -> Self {
        // Whitespace is not a token, so a single space is always a safe join.
        let input = if self.input.is_empty() {
            materialized.text.clone()
        } else {
            format!("{} {}", self.input, materialized.text)
        };

        let mut path = self.path.clone();
        path.push(label.to_string());

        Self {
            input,
            path,
            free_form: materialized.free_form,
        }
    }

    fn label_repeats(&self, label: &str) -> usize {
        self.path.iter().filter(|applied| *applied == label).count()
    }

    fn context(&self) -> Vec<String> {
        self.path
            .iter()
            .rev()
            .take(CONTEXT_LABELS)
            .rev()
            .cloned()
            .collect()
    }

    /// Identity of the grammar position this state stands at. Two states offering the same
    /// continuations after the same recent labels are the same position as far as completion is
    /// concerned, and expanding both only re-walks the same edges.
    ///
    /// The statement shape is part of the identity because that abstraction is otherwise too
    /// coarse: `TO ref:relay` looks identical in a junction and in an emitter, yet finishing them
    /// needs different work, so collapsing the two leaves one family unexplored and looking
    /// unreachable.
    pub fn position_signature(&self, suggestions: &[String], window: usize) -> u64 {
        let mut hasher = DefaultHasher::new();
        suggestions.hash(&mut hasher);
        for label in self.path.iter().take(SHAPE_LABELS) {
            label.hash(&mut hasher);
        }
        for label in self.path.iter().rev().take(window) {
            label.hash(&mut hasher);
        }
        // Depth matters because the walk is breadth-first: without it, a shorter path claims the
        // position first and the longer one is skipped. That is exactly backwards when the longer
        // path is the one carrying the clause needed to finish, as `ENCODE USING` is for an
        // emitter, and the family then looks unreachable when it is only unexplored.
        self.path.len().hash(&mut hasher);
        hasher.finish()
    }
}

/// What one worker produced for one state.
enum Evaluated {
    Panicked {
        message: String,
    },
    Parsed {
        suggestions: Vec<String>,
        outcome: ParseOutcome,
        prefix_probe: Option<PrefixProbe>,
    },
}

/// The result of typing the first characters of a suggestion and asking again.
struct PrefixProbe {
    label: String,
    typed: String,
    offered: Vec<String>,
}

pub struct Walker {
    grammar: Grammar,
    materializer: Materializer,
    budget: Budget,
    jobs: usize,
    visited: HashSet<String>,
    positions: HashSet<u64>,
    report: Report,
    /// States left unexpanded by `--max-depth`, and suggestions skipped by `--max-label-repeats`.
    /// Both bound coverage, so both are reported rather than quietly applied.
    depth_limited: usize,
    repeat_limited: usize,
}

impl Walker {
    pub fn new(grammar: Grammar, budget: Budget, jobs: usize) -> Self {
        Self {
            grammar,
            materializer: Materializer::new(),
            budget,
            jobs: jobs.max(1),
            visited: HashSet::new(),
            positions: HashSet::new(),
            report: Report::default(),
            depth_limited: 0,
            repeat_limited: 0,
        }
    }

    pub fn run(mut self, seed: &str) -> Report {
        let mut frontier = vec![WalkState::seed(seed.to_string())];
        self.visited.insert(seed.to_string());

        while !frontier.is_empty() {
            if self.report.stats.states_evaluated >= self.budget.max_states {
                self.report.note_truncation(format!(
                    "state budget {} reached with {} states still queued",
                    self.budget.max_states,
                    frontier.len()
                ));
                break;
            }

            self.report.stats.levels += 1;
            let evaluated = self.evaluate_level(&frontier);
            frontier = self.apply_level(frontier, evaluated);

            if frontier.len() > self.budget.max_frontier {
                self.report.note_truncation(format!(
                    "level {} narrowed from {} to {} states by --max-frontier",
                    self.report.stats.levels,
                    frontier.len(),
                    self.budget.max_frontier
                ));
                frontier.truncate(self.budget.max_frontier);
            }
        }

        if self.depth_limited > 0 {
            self.report.note_truncation(format!(
                "{} state(s) left unexpanded at the --max-depth {} limit",
                self.depth_limited, self.budget.max_depth
            ));
        }
        if self.repeat_limited > 0 {
            self.report.note_truncation(format!(
                "{} suggestion(s) skipped by --max-label-repeats {}",
                self.repeat_limited, self.budget.max_label_repeats
            ));
        }

        self.report
    }

    /// Evaluate a whole level in parallel. Workers decide nothing; they only fill in results, which
    /// are put back into level order before anything looks at them.
    fn evaluate_level(&self, level: &[WalkState]) -> Vec<Evaluated> {
        let cursor = AtomicUsize::new(0);

        let parts = std::thread::scope(|scope| {
            let handles = (0..self.jobs)
                .map(|_| {
                    std::thread::Builder::new()
                        .stack_size(WORKER_STACK_SIZE)
                        .spawn_scoped(scope, || {
                            let mut evaluated = Vec::new();
                            loop {
                                let index = cursor.fetch_add(1, Ordering::Relaxed);
                                let Some(state) = level.get(index) else {
                                    break;
                                };
                                evaluated.push((index, self.evaluate(state)));
                            }
                            evaluated
                        })
                        .expect("evaluation worker must spawn")
                })
                .collect::<Vec<_>>();

            handles
                .into_iter()
                .map(|handle| handle.join().expect("evaluation worker must not panic"))
                .collect::<Vec<_>>()
        });

        let mut evaluated = parts.into_iter().flatten().collect::<Vec<_>>();
        evaluated.sort_by_key(|(index, _)| *index);
        evaluated
            .into_iter()
            .map(|(_, evaluated)| evaluated)
            .collect()
    }

    fn evaluate(&self, state: &WalkState) -> Evaluated {
        let evaluated = catch_unwind(AssertUnwindSafe(|| {
            let suggestions = self.grammar.suggest(&state.input);
            let outcome = self.grammar.parse(&state.input);
            let prefix_probe = self.probe_prefix(&state.input, &suggestions);
            (suggestions, outcome, prefix_probe)
        }));

        match evaluated {
            Ok((suggestions, outcome, prefix_probe)) => Evaluated::Parsed {
                suggestions,
                outcome,
                prefix_probe,
            },
            Err(payload) => Evaluated::Panicked {
                message: panic_message(payload.as_ref()),
            },
        }
    }

    /// Type the first characters of one keyword suggestion and check the suggestion survives. This
    /// is the path the server autocomplete depends on, and it costs a whole extra grammar build, so
    /// only the first eligible suggestion of each state is probed.
    fn probe_prefix(&self, input: &str, suggestions: &[String]) -> Option<PrefixProbe> {
        let label = suggestions.iter().find(|label| {
            SuggestionClass::classify(label) == Some(SuggestionClass::Keyword)
                && label.split(' ').next().is_some_and(|word| {
                    word.len() > PREFIX_PROBE_LEN && word.is_char_boundary(PREFIX_PROBE_LEN)
                })
        })?;

        let typed = label.get(..PREFIX_PROBE_LEN)?.to_string();

        Some(PrefixProbe {
            label: label.clone(),
            offered: self.grammar.suggest_typed(input, &typed),
            typed,
        })
    }

    fn apply_level(&mut self, level: Vec<WalkState>, evaluated: Vec<Evaluated>) -> Vec<WalkState> {
        let mut next = Vec::new();

        for (state, evaluated) in level.into_iter().zip(evaluated) {
            self.report.stats.states_evaluated += 1;
            self.report.stats.deepest_path = self.report.stats.deepest_path.max(state.path.len());

            let (suggestions, outcome, prefix_probe) = match evaluated {
                Evaluated::Panicked { message } => {
                    self.push(
                        &state,
                        FindingKind::Panic,
                        state.path.last().cloned(),
                        Vec::new(),
                        message,
                    );
                    continue;
                }
                Evaluated::Parsed {
                    suggestions,
                    outcome,
                    prefix_probe,
                } => (suggestions, outcome, prefix_probe),
            };

            if !self.record_edge(&state, &outcome) {
                continue;
            }

            self.report.note_reached(&state.path);
            if outcome == ParseOutcome::Accepted {
                self.report.note_completed(&state.path);
            }

            self.check_invariants(&state, &suggestions, prefix_probe.as_ref());

            if suggestions.is_empty() {
                // A complete statement with nothing more to offer is the normal way a branch ends:
                // end of input is not representable as a suggestion, so completion goes quiet.
                let Some(message) = outcome.message() else {
                    continue;
                };

                self.push(
                    &state,
                    FindingKind::DeadEnd,
                    None,
                    Vec::new(),
                    format!("completion offers nothing; parser {message}"),
                );
                continue;
            }

            if state.path.len() >= self.budget.max_depth {
                self.depth_limited += 1;
                continue;
            }

            let signature = state.position_signature(&suggestions, self.budget.signature_window);
            if !self.positions.insert(signature) {
                continue;
            }

            self.report.stats.states_expanded += 1;
            self.expand(&state, &suggestions, &mut next);
        }

        next
    }

    /// Judge the step that produced this state. Returns whether the walk may look any further.
    fn record_edge(&mut self, state: &WalkState, outcome: &ParseOutcome) -> bool {
        let kind = match outcome {
            ParseOutcome::Accepted | ParseOutcome::Incomplete { .. } => return true,
            ParseOutcome::Rejected { .. } if state.free_form => FindingKind::CannedBodyRejected,
            ParseOutcome::Rejected { .. } => FindingKind::RejectedSuggestion,
            ParseOutcome::LexFailure { .. } => FindingKind::LexFailure,
        };

        let detail = outcome.message().unwrap_or_default().to_string();
        self.push(state, kind, state.path.last().cloned(), Vec::new(), detail);
        false
    }

    fn check_invariants(
        &mut self,
        state: &WalkState,
        suggestions: &[String],
        prefix_probe: Option<&PrefixProbe>,
    ) {
        if let Some(pair) = suggestions.windows(2).find(|pair| pair[0] == pair[1]) {
            self.push(
                state,
                FindingKind::DuplicateSuggestion,
                Some(pair[0].clone()),
                suggestions.to_vec(),
                "the same suggestion was offered twice".to_string(),
            );
        }

        if let Some(pair) = suggestions.windows(2).find(|pair| pair[0] > pair[1]) {
            self.push(
                state,
                FindingKind::UnsortedSuggestions,
                Some(pair[1].clone()),
                suggestions.to_vec(),
                format!("{} was offered after {}", pair[1], pair[0]),
            );
        }

        if let Some(probe) = prefix_probe
            && !probe.offered.contains(&probe.label)
        {
            self.push(
                state,
                FindingKind::PrefixFilterDrop,
                Some(probe.label.clone()),
                probe.offered.clone(),
                format!("typing '{}' dropped the suggestion", probe.typed),
            );
        }
    }

    fn expand(&mut self, state: &WalkState, suggestions: &[String], next: &mut Vec<WalkState>) {
        for label in suggestions {
            let materialized = match self
                .materializer
                .materialize(label, state.label_repeats(label))
            {
                Ok(materialized) => materialized,
                Err(error) => {
                    self.push(
                        state,
                        FindingKind::UnknownLabel,
                        Some(label.clone()),
                        suggestions.to_vec(),
                        error.to_string(),
                    );
                    continue;
                }
            };

            if state.label_repeats(label) >= self.budget.max_label_repeats {
                self.repeat_limited += 1;
                continue;
            }

            // A free-form region repeats, so its label is offered again straight after it is
            // filled. The canned bodies are whole expressions, not fragments that concatenate, so
            // applying one twice in a row builds `1 1` and blames the grammar for the tool.
            if materialized.free_form && state.path.last().map(String::as_str) == Some(label) {
                continue;
            }

            if materialized.free_form {
                self.report.note_degenerate_region(label, &state.context());
            }

            let child = state.extend(label, &materialized);
            if self.visited.insert(child.input.clone()) {
                next.push(child);
            }
        }
    }

    fn push(
        &mut self,
        state: &WalkState,
        kind: FindingKind,
        label: Option<String>,
        suggestions: Vec<String>,
        detail: String,
    ) {
        self.report.push(Finding::new(
            kind,
            label,
            state.input.clone(),
            state.path.clone(),
            suggestions,
            detail,
        ));
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "parser panicked with a non-string payload".to_string()
    }
}
