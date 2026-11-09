//! Layer: test harness.
//! - **Owns.** Bounded same-host timing of the actual buffered emitter flush and its row oracle.
//! - **Depends on.** The opaque runtime benchmark driver and selected monotonic time.
//! - **Must not know.** Metric implementation, connector internals or live cluster placement.

use std::io::Write as _;

use clap::Parser;
use error_stack::ResultExt as _;
use nervix_primitives::time::Instant;
use nervix_server::runtime::admitted_work_benchmark::EmitterFlushBenchmark;

#[derive(Parser)]
struct Arguments {
    // Cargo supplies this control flag to benchmarks with a custom harness.
    #[arg(long = "bench", hide = true)]
    _bench: bool,
    #[arg(long, default_value_t = 32, value_parser = clap::value_parser!(u16).range(1..=1_024))]
    rows: u16,
    #[arg(long, default_value_t = 1_000, value_parser = clap::value_parser!(u32).range(1..=10_000))]
    iterations: u32,
    #[arg(long, default_value_t = 9, value_parser = clap::value_parser!(u8).range(1..=30))]
    trials: u8,
    #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u16).range(0..=100))]
    warmup: u16,
}

#[derive(serde::Serialize)]
struct Measurement {
    trial: u8,
    rows_per_flush: u16,
    iterations: u32,
    elapsed_nanos: u128,
}

#[derive(Debug, thiserror::Error)]
enum MeasurementError {
    #[error("cannot encode the flush measurement")]
    Encode,
    #[error("cannot write the flush measurement")]
    Write,
}

fn main() -> error_stack::Result<(), MeasurementError> {
    let args = Arguments::parse();
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    for trial in 0..args.trials {
        let mut driver = EmitterFlushBenchmark::new(usize::from(args.rows));
        for _ in 0..args.warmup {
            assert_eq!(driver.flush(), u64::from(args.rows));
        }
        let started = Instant::now();
        for _ in 0..args.iterations {
            assert_eq!(driver.flush(), u64::from(args.rows));
        }
        let measurement = Measurement {
            trial,
            rows_per_flush: args.rows,
            iterations: args.iterations,
            elapsed_nanos: started.elapsed().as_nanos(),
        };
        serde_json::to_writer(&mut output, &measurement)
            .change_context(MeasurementError::Encode)?;
        writeln!(output).change_context(MeasurementError::Write)?;
    }
    Ok(())
}
