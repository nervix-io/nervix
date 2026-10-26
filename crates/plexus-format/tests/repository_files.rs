//! Formatting guarantees checked against every Plexus file the repository ships.

use nervix_plexus::client_statement::parse_client_statements;
use nervix_plexus_format::format_source;

/// Every tracked `.plexus` file, embedded so the guarantees are checked at build time.
const FILES: &[(&str, &str)] = &[
    ("iot", include_str!("../../../examples/iot/iot.plexus")),
    (
        "datalake",
        include_str!("../../../examples/datalake/datalake.plexus"),
    ),
    (
        "nats_factory_windows",
        include_str!("../../../examples/nats-factory-windows/nats_factory_windows.plexus"),
    ),
    (
        "wasm_dual",
        include_str!("../../../examples/wasm-processors/wasm-dual.plexus"),
    ),
    (
        "binance_websocket",
        include_str!("../../../examples/binance-websocket/binance_websocket.plexus"),
    ),
    (
        "onnx_batched",
        include_str!("../../../examples/onnx-inference/batched.plexus"),
    ),
    (
        "onnx_per_message",
        include_str!("../../../examples/onnx-inference/per-message.plexus"),
    ),
    (
        "paced_simulation",
        include_str!("../../../examples/paced-simulation/paced_simulation.plexus"),
    ),
    (
        "quickstart",
        include_str!("../../../scripts/console-screenshots/quickstart.plexus"),
    ),
];

#[test]
fn every_repository_file_is_already_formatted() {
    for (name, source) in FILES {
        let formatted = format_source(source).unwrap_or_else(|error| panic!("{name}: {error:?}"));
        assert_eq!(
            formatted, *source,
            "{name} is not formatted; run `just plexus-fmt`"
        );
    }
}

#[test]
fn formatting_every_repository_file_is_idempotent() {
    for (name, source) in FILES {
        let once = format_source(source).unwrap_or_else(|error| panic!("{name}: {error:?}"));
        let twice = format_source(&once).unwrap_or_else(|error| panic!("{name}: {error:?}"));
        assert_eq!(once, twice, "{name} kept changing when formatted twice");
    }
}

#[test]
fn formatting_every_repository_file_preserves_its_statements() {
    for (name, source) in FILES {
        let formatted = format_source(source).unwrap_or_else(|error| panic!("{name}: {error:?}"));
        let before = parse_client_statements(source).unwrap_or_else(|e| panic!("{name}: {e:?}"));
        let after = parse_client_statements(&formatted).unwrap_or_else(|e| panic!("{name}: {e:?}"));
        assert_eq!(before, after, "{name} changed meaning when formatted");
    }
}

#[test]
fn formatting_every_repository_file_keeps_its_comments() {
    for (name, source) in FILES {
        let formatted = format_source(source).unwrap_or_else(|error| panic!("{name}: {error:?}"));
        let before = comment_lines(source);
        let after = comment_lines(&formatted);
        assert_eq!(before, after, "{name} lost or altered a comment");
    }
}

/// The whole-line comments of `source`, in order, ignoring indentation.
fn comment_lines(source: &str) -> Vec<&str> {
    source
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("//"))
        .collect()
}
