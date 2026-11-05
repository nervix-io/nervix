//! Public-library scenarios for the engine, before graph policy consumes it.

use std::{num::NonZeroU32, time::Duration};

use chrono::{TimeDelta, TimeZone as _, Utc};
use cucumber::{World, given, then, when};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::time::{Instant, sleep_until};
use nervix_rate_limit::{Admission, Charge, Clause, Generation, Limiter, Quota};

#[derive(Debug)]
struct Unit {
    limiter: Limiter,
    first: Option<Instant>,
    saved: Option<Generation>,
    refusal: Option<Admission>,
}

#[derive(Debug, Default, World)]
struct EngineWorld {
    units: Vec<Unit>,
}

fn charges() -> [Charge; 2] {
    [Charge::Direct(NonZeroU32::new(1).assured("one payload")); 2]
}

#[given(expr = "{int} independent execution-unit limiters with 20 and 40 millisecond clauses")]
fn units(world: &mut EngineWorld, count: usize) {
    for _ in 0..count {
        let now = Instant::now();
        let clauses = [20, 40]
            .into_iter()
            .map(|millis| {
                Clause::direct(
                    Quota::new(1, Duration::from_millis(millis), 1).assured("positive test quota"),
                )
            })
            .collect();
        world.units.push(Unit {
            limiter: Limiter::new(clauses, now),
            first: None,
            saved: None,
            refusal: None,
        });
    }
}

#[when("each unit admits its first whole payload")]
fn first(world: &mut EngineWorld) {
    for unit in &mut world.units {
        let now = Instant::now();
        assert_eq!(
            unit.limiter
                .admit_at(&charges(), now)
                .assured("whole payload"),
            Admission::Admitted
        );
        unit.first = Some(now);
        unit.saved = Some(unit.limiter.generation(now).assured("same captured clock"));
    }
}

#[when("each unit checks another payload at its captured first-admission instant")]
fn second(world: &mut EngineWorld) {
    for unit in &mut world.units {
        unit.refusal = Some(
            unit.limiter
                .admit_at(&charges(), unit.first.assured("first payload was admitted"))
                .assured("same payload"),
        );
    }
}

#[then("every refusal preserves the complete allowance generation")]
fn refunded(world: &mut EngineWorld) {
    for unit in &world.units {
        let first = unit.first.assured("first payload was admitted");
        assert_eq!(
            unit.refusal,
            Some(Admission::WaitUntil(first + Duration::from_millis(40)))
        );
        assert_eq!(
            Some(
                unit.limiter
                    .generation(first)
                    .assured("same captured clock")
            ),
            unit.saved
        );
    }
}

#[then("each unit admits its next payload at least 40 milliseconds after its first")]
async fn waited(world: &mut EngineWorld) {
    for unit in &mut world.units {
        nervix_primitives::task::consume_budget().await;
        let Some(Admission::WaitUntil(deadline)) = unit.refusal else {
            panic!("the payload must wait for its latest clause");
        };
        sleep_until(deadline).await;
        let admitted = Instant::now();
        assert_eq!(
            unit.limiter
                .admit_at(&charges(), admitted)
                .assured("same payload"),
            Admission::Admitted
        );
        assert!(
            admitted.duration_since(unit.first.assured("first admission"))
                >= Duration::from_millis(40)
        );
    }
}

#[when("its generation is restored after 10 milliseconds of injected UTC")]
fn restored(world: &mut EngineWorld) {
    let utc = Utc
        .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .single()
        .assured("valid fixture UTC");
    for unit in &mut world.units {
        let generation = unit
            .saved
            .as_ref()
            .assured("first generation")
            .to_utc(utc)
            .assured("UTC fits");
        let now = Instant::now();
        let elapsed = generation
            .from_utc(utc + TimeDelta::milliseconds(10))
            .assured("positive downtime");
        unit.limiter = Limiter::from_generation(elapsed, now).assured("valid generation");
        unit.first = Some(now);
    }
}

#[then("each restored unit still owes exactly 30 milliseconds before the next whole payload")]
fn restored_debt(world: &mut EngineWorld) {
    for unit in &mut world.units {
        let now = unit.first.assured("restoration anchor");
        assert_eq!(
            unit.limiter
                .admit_at(&charges(), now)
                .assured("same payload"),
            Admission::WaitUntil(now + Duration::from_millis(30))
        );
        assert_eq!(
            unit.limiter
                .admit_at(&charges(), now + Duration::from_millis(30))
                .assured("refilled payload"),
            Admission::Admitted
        );
    }
}

#[nervix_primitives::main(flavor = "current_thread")]
async fn main() {
    EngineWorld::cucumber()
        .run_and_exit("tests/features/admission.feature")
        .await;
}
