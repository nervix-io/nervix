//! Application argument and startup tests.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Assertions about parsed arguments and the application they configure.
//! - **Depends on.** The application module under test and the session test fixtures.
//! - **Must not know.** Production ownership beyond the parent module under test.

use std::path::PathBuf;

use session_service::{current_word_prefix, word_start};
use test_fixtures::{test_addr, test_args, try_test_args};

use super::*;

#[test]
fn args_parse_observability_listen_addr() {
    let args = test_args(&["--observability-listen-addr", "127.0.0.1:19090"]);
    let app = Application::try_from(args).expect("args should parse");
    assert_eq!(app.observability_listen_addr, test_addr(19090));
}

#[test]
fn args_default_to_the_application_shutdown_timeouts() {
    let args = test_args(&[]);

    assert_eq!(args.drain_timeout, shutdown::DEFAULT_DRAIN_TIMEOUT);
    assert_eq!(args.shutdown_timeout, shutdown::DEFAULT_SHUTDOWN_TIMEOUT);
}

#[test]
fn args_parse_shutdown_timeout() {
    let args = test_args(&["--shutdown-timeout", "2m"]);

    assert_eq!(args.shutdown_timeout, Duration::from_secs(120));
}

#[test]
fn args_parse_temp_dir() {
    let args = test_args(&["--temp-dir", "/tmp/nervix-temp"]);
    let app = Application::try_from(args).expect("args should parse");
    assert_eq!(app.temp_dir, PathBuf::from("/tmp/nervix-temp"));
}

#[test]
fn args_parse_resource_store_limits() {
    let args = test_args(&[
        "--resource-max-archive-bytes",
        "8MiB",
        "--resource-max-extracted-bytes",
        "32MiB",
        "--resource-max-file-count",
        "2048",
    ]);
    let app = Application::try_from(args).expect("args should parse");

    assert_eq!(app.resource_store_limits.max_archive_bytes, 8 * 1024 * 1024);
    assert_eq!(
        app.resource_store_limits.max_extracted_bytes,
        32 * 1024 * 1024
    );
    assert_eq!(app.resource_store_limits.max_file_count, 2048);
}

#[test]
fn args_parse_web_console_listen_addr() {
    let args = test_args(&[
        "--web-console-listen-addr",
        "127.0.0.1:17420",
        "--web-console-https-listen-addr",
        "127.0.0.1:17443",
        "--web-console-tls-cert",
        "tls/dev/node.pem",
        "--web-console-tls-key",
        "tls/dev/node-key.pem",
    ]);
    let app = Application::try_from(args).expect("args should parse");
    assert_eq!(app.web_console_listen_addr, test_addr(17420));
    assert_eq!(app.web_console_https_listen_addr, Some(test_addr(17443)));
    assert_eq!(
        app.web_console_tls_cert,
        Some(PathBuf::from("tls/dev/node.pem"))
    );
    assert_eq!(
        app.web_console_tls_key,
        Some(PathBuf::from("tls/dev/node-key.pem"))
    );
}

#[test]
fn parse_and_text_encoding_helpers_roundtrip() {
    assert_eq!(current_word_prefix("CREATE SCHE", 11), "sche");
    assert_eq!(word_start("CREATE SCHE", 11), 7);
    assert_eq!(
        parse_human_duration("1500ms").expect("valid duration"),
        Duration::from_millis(1500)
    );
    assert_eq!(
        parse_human_bytes("1.5MiB").expect("valid bytes"),
        ubyte::ByteUnit::Mebibyte(1) + ubyte::ByteUnit::Kibibyte(512)
    );
    assert_eq!(parse_trace_sample_ratio("0.25").expect("valid ratio"), 0.25);
    assert!(parse_trace_sample_ratio("1.25").is_err());

    let bytes = vec![0xde, 0xad, 0xbe, 0xef];
    let hex = encode_hex(&bytes);
    assert_eq!(hex, "deadbeef");

    assert!(parse_human_duration("oops").is_err());
    assert!(parse_human_bytes("oops").is_err());
}

#[test]
fn server_args_parse_memory_pressure_options() {
    let args = test_args(&[
        "--memory-high-watermark",
        "2MiB",
        "--memory-low-watermark",
        "1MiB",
        "--memory-pressure-check-interval",
        "250ms",
        "--memory-pressure-resume-jitter",
        "500ms",
    ]);
    let app = Application::try_from(args).expect("args should parse");
    let memory_pressure = app.memory_pressure.expect("memory pressure configured");

    assert_eq!(memory_pressure.high_watermark, ubyte::ByteUnit::Mebibyte(2));
    assert_eq!(memory_pressure.low_watermark, ubyte::ByteUnit::Mebibyte(1));
    assert_eq!(memory_pressure.check_interval, Duration::from_millis(250));
    assert_eq!(memory_pressure.resume_jitter, Duration::from_millis(500));
}

#[test]
fn server_args_reject_incomplete_memory_pressure_watermarks() {
    let args = test_args(&["--memory-high-watermark", "2MiB"]);

    let error = Application::try_from(args).expect_err("low watermark is required");
    assert!(format!("{error:?}").contains("memory high watermark requires"));
}

#[test]
fn server_args_parse_opentelemetry_options() {
    let args = test_args(&[
        "--otel-enabled",
        "--otel-otlp-endpoint",
        "http://collector:4317",
        "--otel-service-name",
        "nervix-test",
        "--otel-trace-sample-ratio",
        "0.5",
    ]);

    assert!(args.otel_enabled);
    assert_eq!(args.otel_otlp_endpoint, "http://collector:4317");
    assert_eq!(args.otel_service_name, "nervix-test");
    assert_eq!(args.otel_trace_sample_ratio, 0.5);
}

#[test]
fn server_args_do_not_require_opentelemetry_options() {
    let args = test_args(&[]);

    assert!(!args.otel_enabled);
    assert_eq!(args.otel_otlp_endpoint, "http://127.0.0.1:4317");
    assert_eq!(args.otel_service_name, "nervix");
    assert_eq!(args.otel_trace_sample_ratio, 1.0);
}

#[test]
fn server_args_only_require_opentelemetry_enable_flag_to_enable_export() {
    let args = test_args(&["--otel-enabled"]);

    assert!(args.otel_enabled);
    assert_eq!(args.otel_otlp_endpoint, "http://127.0.0.1:4317");
    assert_eq!(args.otel_service_name, "nervix");
    assert_eq!(args.otel_trace_sample_ratio, 1.0);
}

#[test]
fn server_args_reject_invalid_opentelemetry_sample_ratio() {
    let result = try_test_args(&["--otel-trace-sample-ratio", "2.0"]);

    assert!(result.is_err());
}
