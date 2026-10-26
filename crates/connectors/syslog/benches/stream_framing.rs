//! Criterion throughput of RFC 6587 stream framing.
//!
//! A connection stream of RFC 5424 messages in one framing is read into the production decoder in
//! the connection's 8 KiB reads, so frames of every size end, and are split, at read boundaries.
//! Payload sizes range from typical log lines to frames several reads long.

use std::num::NonZeroUsize;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector_syslog::frame_stream;

/// The bytes a connection reads at a time.
const READ_SIZE: usize = 8_192;

/// The stream bytes each measurement frames.
const STREAM_BYTES: usize = 1 << 20;

/// One RFC 5424 message whose `MSG` pads it to `payload_bytes`.
fn message(payload_bytes: usize, sequence: usize) -> String {
    let header = format!("<34>1 2003-10-11T22:14:15.003Z edge-1 orders {sequence} ID47 - ");
    // A header already longer than the size wanted gets no padding at all.
    let padding = payload_bytes.saturating_sub(header.len());
    format!("{header}{}", "m".repeat(padding))
}

/// About a mebibyte of frames of one framing, each holding `payload_bytes`.
fn stream(octet_counted: bool, payload_bytes: usize) -> Vec<u8> {
    let mut stream = Vec::with_capacity(STREAM_BYTES + payload_bytes + 16);
    let mut sequence = 0;
    while stream.len() < STREAM_BYTES {
        let message = message(payload_bytes, sequence);
        if octet_counted {
            stream.extend_from_slice(format!("{} {message}", message.len()).as_bytes());
        } else {
            stream.extend_from_slice(message.as_bytes());
            stream.extend_from_slice(b"\r\n");
        }
        sequence += 1;
    }
    stream
}

fn framing(criterion: &mut Criterion) {
    let max_message_size = NonZeroUsize::new(64 * 1024).assured("the limit is a nonzero constant");
    for (octet_counted, name) in [(true, "octet_counted"), (false, "non_transparent")] {
        let mut group = criterion.benchmark_group(format!("syslog_stream_framing_{name}"));
        for payload_bytes in [128_usize, 1_024, 16_384] {
            let stream = stream(octet_counted, payload_bytes);
            let bytes = u64::try_from(stream.len()).assured("a mebibyte fits u64");
            group.throughput(Throughput::Bytes(bytes));
            group.bench_with_input(
                BenchmarkId::from_parameter(payload_bytes),
                &stream,
                |bencher, stream| {
                    bencher.iter(|| {
                        frame_stream(black_box(stream), READ_SIZE, max_message_size)
                            .assured("the benchmark stream holds only complete frames")
                    });
                },
            );
        }
        group.finish();
    }
}

criterion_group!(stream_framing, framing);
criterion_main!(stream_framing);
