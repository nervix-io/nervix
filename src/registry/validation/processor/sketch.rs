//! Validation of bounded, epoch-aligned window sketch state.
//!
//! Layer: decisions.
//!
//! - **Owns.** Requiring a finite per-branch allowance, a finite branch count, duration panes,
//!   and enough bytes for every configured sketch pane.
//! - **Depends on.** Window models, branch declarations and the VM's sketch reservation contract.
//! - **Must not know.** Runtime tasks, snapshots or transport.

use error_stack::Report;
use nervix_models::{
    CreateWindowProcessor, DomainName, ModelIndex, ModelName, RelayName, WindowStateLimit,
};
use nervix_vm::window::{WindowPaneLayout, WindowSketchConfig};

use crate::registry::{error::RegistryError, validation::branching::branch_model};

pub(super) fn validate(
    domain: &DomainName,
    identifier: &ModelName,
    models: &ModelIndex,
    processor: &CreateWindowProcessor,
    route: &RelayName,
    demands: &[WindowSketchConfig],
) -> Result<(), Report<RegistryError>> {
    let limit = match processor.state_limit {
        WindowStateLimit::MaxBytes(limit) => limit.get(),
        WindowStateLimit::Unbounded => {
            return Err(Report::new(RegistryError::WindowSketchMissingStateLimit {
                domain: domain.clone(),
                processor: identifier.clone(),
                route: route.clone(),
            }));
        }
    };
    let layout = match (
        processor.width.duration.as_deref(),
        processor.step.duration.as_deref(),
    ) {
        (Some(width), Some(step)) => {
            let width = humantime::parse_duration(width).ok();
            let step = humantime::parse_duration(step).ok();
            match (width, step) {
                (Some(width), Some(step)) => WindowPaneLayout::for_width_and_step(width, step),
                _ => None,
            }
        }
        _ => None,
    };
    let Some(layout) = layout else {
        return Err(Report::new(RegistryError::WindowSketchRequiresTimePanes {
            domain: domain.clone(),
            processor: identifier.clone(),
            route: route.clone(),
        }));
    };
    if let Some(branch) = processor.branched_by.branch() {
        let branch = branch_model(domain, identifier, models, branch)?;
        if branch.eviction.is_none() {
            return Err(Report::new(
                RegistryError::WindowSketchRequiresBranchLimit {
                    domain: domain.clone(),
                    processor: identifier.clone(),
                    route: route.clone(),
                },
            ));
        }
    }
    let mut sketch_bytes = 0_u128;
    for demand in demands {
        let Some(total) = sketch_bytes.checked_add(demand.reserved_bytes()) else {
            return Err(overflow(domain, identifier, route));
        };
        sketch_bytes = total;
    }
    let Some(panes) = layout.maximum_panes.checked_add(1) else {
        return Err(overflow(domain, identifier, route));
    };
    let Some(sketch_bytes) = sketch_bytes.checked_mul(u128::from(panes)) else {
        return Err(overflow(domain, identifier, route));
    };
    let Some(required) = sketch_bytes.checked_add(1024) else {
        return Err(overflow(domain, identifier, route));
    };
    if required > u128::from(limit) {
        return Err(Report::new(RegistryError::WindowSketchStateBudget {
            domain: domain.clone(),
            processor: identifier.clone(),
            route: route.clone(),
            panes: layout.maximum_panes,
            required,
            limit,
        }));
    }
    Ok(())
}

fn overflow(
    domain: &DomainName,
    identifier: &ModelName,
    route: &RelayName,
) -> Report<RegistryError> {
    Report::new(RegistryError::WindowSketchBudgetOverflow {
        domain: domain.clone(),
        processor: identifier.clone(),
        route: route.clone(),
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use meticulous::ResultExt as _;
    use nervix_recovery::Reported as _;

    use crate::registry::{
        storage::Registry,
        test_fixtures::{example_graph_models, temp_db_path},
    };

    #[test]
    fn sketch_window_requires_time_panes_and_an_explicit_sufficient_state_budget() {
        for (width, step, limit, expected) in [
            (
                "WIDTH 2s DURATION",
                "STEP 1s DURATION",
                "",
                Some("requires MAX STATE SIZE"),
            ),
            (
                "WIDTH 2 MESSAGES",
                "STEP 1 MESSAGES",
                "MAX STATE SIZE 1MiB",
                Some("requires duration WIDTH and STEP"),
            ),
            (
                "WIDTH 2s DURATION",
                "STEP 1s DURATION",
                "MAX STATE SIZE 100B",
                Some("above MAX STATE SIZE"),
            ),
            (
                "WIDTH 2s DURATION",
                "STEP 1s DURATION",
                "MAX STATE SIZE 1MiB",
                None,
            ),
        ] {
            let nspl = format!(
                r#"
                CREATE SCHEMA sketch_input (value I64);
                CREATE SCHEMA sketch_output (distinct_values I64);
                CREATE RELAY sketch_in SCHEMA sketch_input UNBRANCHED;
                CREATE RELAY sketch_out SCHEMA sketch_output UNBRANCHED;
                CREATE WINDOW PROCESSOR sketch_window FROM sketch_in
                  {width} {step} {limit} UNBRANCHED
                  TO sketch_out SET distinct_values = APPROX_COUNT_DISTINCT(input.value, 10)
                    ON MESSAGE ERROR LOG;
            "#
            );
            let (domain, models) = example_graph_models("sketch window state budget", &nspl);
            let path = temp_db_path();
            let registry = Registry::open(&path)
                .assured("the test fixture supplies a writable temporary registry path");
            let applied = registry.apply_batch(&domain, models);
            if let Some(expected) = expected {
                let error = applied.expect_err("invalid sketch state plan must be rejected");
                assert!(error.to_string().contains(expected), "{error:#}");
            } else {
                applied.assured("the test case has sufficient budget and valid duration panes");
            }
            fs::remove_dir_all(path).reported("remove sketch registry test directory");
        }
    }

    #[test]
    fn branched_sketch_window_requires_a_branch_instance_limit() {
        let (domain, models) = example_graph_models(
            "sketch branch limit",
            r#"
            CREATE SCHEMA sketch_input (tenant STRING, value I64);
            CREATE SCHEMA sketch_output (tenant STRING, distinct_values I64);
            CREATE SCHEMA sketch_key (tenant STRING);
            CREATE BRANCH sketch_branch SCHEMA sketch_key TTL 5m;
            CREATE RELAY sketch_in SCHEMA sketch_input BRANCHED BY sketch_branch;
            CREATE RELAY sketch_out SCHEMA sketch_output BRANCHED BY sketch_branch;
            CREATE WINDOW PROCESSOR sketch_window FROM sketch_in
              WIDTH 2s DURATION STEP 1s DURATION MAX STATE SIZE 1MiB
              BRANCHED BY sketch_branch
              TO sketch_out SET tenant = FIRST(input.tenant),
                distinct_values = APPROX_COUNT_DISTINCT(input.value, 10)
                ON MESSAGE ERROR LOG;
        "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path)
            .assured("the test fixture supplies a writable temporary registry path");
        let error = registry
            .apply_batch(&domain, models)
            .expect_err("a branched sketch requires MAX INSTANCES");
        assert!(
            error
                .to_string()
                .contains("requires its branch to declare MAX INSTANCES"),
            "{error:#}"
        );
        fs::remove_dir_all(path).reported("remove sketch registry test directory");
    }
}
