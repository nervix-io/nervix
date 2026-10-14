//! The exports the OpenTelemetry Collector's debug exporter printed, read back from the Collector's
//! followed output.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** How the debug exporter prints the body of one export it received: the line that
//!   starts the body and names its signal, the lines that start its records, and the line that
//!   ends it.
//! - **Depends on.** The text of the Collector's followed output, and `strum`'s derives.
//! - **Must not know.** Nervix, the scenarios, or what the text an export carries means.
//!
//! # One export, one body
//!
//! For each export it receives, the debug exporter logs two entries: a summary that counts the
//! export's records, such as `Metrics` with its data points, and then the export's body, at the
//! detailed verbosity the suite's Collector configuration sets. The Collector's logger prints each
//! entry whole, but nothing keeps the two entries together, so an entry of a concurrent export can
//! print between a summary and its body. A summary therefore does not say which body it counts, and
//! an export is read from its body alone.
//!
//! A body's first line holds the entry's time and level and then the export's first resource,
//! which names the signal, such as `ResourceMetrics #0`. Each record starts a line of its own, such
//! as `LogRecord #1`, `Span #0` or `NumberDataPoints #1`. The entry ends with the logger's fields,
//! which the logger prints after the body's last line, on a line that starts with a tab. A body
//! whose fields have not been printed yet is still arriving, so it is not read until they are.

use meticulous::OptionExt as _;
use strum::{Display, EnumProperty, EnumString};

/// A signal the Collector receives, named as the steps that read its exports name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, EnumString, EnumProperty)]
#[strum(serialize_all = "lowercase")]
pub enum CollectorSignal {
    #[strum(props(first_resource = "ResourceLog #0", record_noun = "log records"))]
    Logs,
    #[strum(props(first_resource = "ResourceSpans #0", record_noun = "spans"))]
    Traces,
    #[strum(props(first_resource = "ResourceMetrics #0", record_noun = "data points"))]
    Metrics,
}

impl CollectorSignal {
    /// The message that starts the body of every export of this signal: its first resource.
    fn first_resource(self) -> &'static str {
        self.get_str("first_resource")
            .assured("the strum property is declared on every variant of this enum")
    }

    /// What the records of this signal are called, as in `2 data points`.
    pub fn record_noun(self) -> &'static str {
        self.get_str("record_noun")
            .assured("the strum property is declared on every variant of this enum")
    }

    /// Whether `line` starts the body of an export of this signal: the entry's time, its level and
    /// the export's first resource, separated by tabs.
    fn starts_body(self, line: &str) -> bool {
        let Some((_time, message)) = line.split_once("\tinfo\t") else {
            return false;
        };
        message == self.first_resource()
    }

    /// Whether `line` starts one record of an export of this signal, such as `LogRecord #1`.
    fn starts_record(self, line: &str) -> bool {
        let Some((kind, index)) = line.split_once(" #") else {
            return false;
        };
        let numbered = !index.is_empty() && index.bytes().all(|byte| byte.is_ascii_digit());
        if !numbered {
            return false;
        }
        match self {
            Self::Logs => kind == "LogRecord",
            Self::Traces => kind == "Span",
            // The data points of every metric type: numbers, histograms, exponential histograms
            // and summaries.
            Self::Metrics => kind.ends_with("DataPoints"),
        }
    }

    /// The exports of this signal whose bodies `printed` holds whole, in the order the Collector
    /// printed them.
    pub fn exports(self, printed: &str) -> Vec<CollectorExport> {
        let mut exports = Vec::new();
        let mut lines = printed.lines();
        while let Some(line) = lines.next() {
            if !self.starts_body(line) {
                continue;
            }
            let mut export = CollectorExport::starting(line);
            let mut whole = false;
            for line in lines.by_ref() {
                // The logger's fields end the entry, after the body's last line.
                if line.starts_with('\t') {
                    whole = true;
                    break;
                }
                export.read(self, line);
            }
            if !whole {
                // The rest of this body has not been printed yet, and nothing printed after it.
                break;
            }
            exports.push(export);
        }
        exports
    }
}

/// One export the debug exporter printed: the body of one request the Collector received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectorExport {
    /// The body's lines as printed, each ending with a line feed.
    body: String,
    /// How many records the body holds.
    records: usize,
}

impl CollectorExport {
    fn starting(first_line: &str) -> Self {
        let mut export = Self {
            body: String::new(),
            records: 0,
        };
        export.append(first_line);
        export
    }

    /// Reads the next line of the body, and counts it when it starts a record of `signal`.
    fn read(&mut self, signal: CollectorSignal, line: &str) {
        if signal.starts_record(line) {
            self.records = self
                .records
                .checked_add(1)
                .assured("a body holds fewer records than it has lines");
        }
        self.append(line);
    }

    fn append(&mut self, line: &str) {
        self.body.push_str(line);
        self.body.push('\n');
    }

    /// How many records the export holds.
    pub fn records(&self) -> usize {
        self.records
    }

    /// Where `text` first occurs in the export's body.
    pub fn position(&self, text: &str) -> Option<usize> {
        self.body.find(text)
    }
}

#[cfg(test)]
mod tests {
    use super::{CollectorExport, CollectorSignal};

    const TIME: &str = "2026-10-13T20:46:22.511Z";

    /// The fields the Collector's logger prints with each entry of the debug exporter, as version
    /// 0.159.0 prints them, followed by `counts` for a summary.
    fn fields(signal: CollectorSignal, counts: &str) -> String {
        format!(
            "{{\"resource\": {{\"service.instance.id\": \"3f18c443-1615-4d69-a1bb-5026165ab430\", \
             \"service.name\": \"otelcol\", \"service.version\": \"0.159.0\"}}, \
             \"otelcol.component.id\": \"debug\", \"otelcol.component.kind\": \"exporter\", \
             \"otelcol.signal\": \"{signal}\"{counts}}}"
        )
    }

    /// The summary entry the debug exporter prints for an export of `signal`.
    fn summary(signal: CollectorSignal, records: usize) -> String {
        let (heading, counts) = match signal {
            CollectorSignal::Logs => (
                "Logs",
                format!(", \"resource logs\": 1, \"log records\": {records}"),
            ),
            CollectorSignal::Traces => (
                "Traces",
                format!(", \"resource spans\": 1, \"spans\": {records}"),
            ),
            CollectorSignal::Metrics => (
                "Metrics",
                format!(", \"resource metrics\": 1, \"metrics\": 1, \"data points\": {records}"),
            ),
        };
        format!("{TIME}\tinfo\t{heading}\t{}\n", fields(signal, &counts))
    }

    /// The body entry the debug exporter prints for an export of `signal` whose message is `lines`.
    fn body(signal: CollectorSignal, lines: &[String]) -> String {
        let mut printed = format!("{TIME}\tinfo\t");
        for line in lines {
            printed.push_str(line);
            printed.push('\n');
        }
        printed.push('\t');
        printed.push_str(&fields(signal, ""));
        printed.push('\n');
        printed
    }

    /// The lines that open a body of `signal`: its resource and scope.
    fn resource(signal: CollectorSignal) -> Vec<String> {
        let (resource, scope) = match signal {
            CollectorSignal::Logs => ("ResourceLog #0", "ScopeLogs"),
            CollectorSignal::Traces => ("ResourceSpans #0", "ScopeSpans"),
            CollectorSignal::Metrics => ("ResourceMetrics #0", "ScopeMetrics"),
        };
        vec![
            resource.to_string(),
            "Resource SchemaURL: ".to_string(),
            "Resource attributes:".to_string(),
            "     -> service.name: Str(nervix-cucumber)".to_string(),
            format!("{scope} #0"),
            format!("{scope} SchemaURL: "),
            "InstrumentationScope batch-test 1.0".to_string(),
        ]
    }

    /// A logs body with one log record per entry of `bodies`.
    fn logs_body(bodies: &[&str]) -> String {
        let mut lines = resource(CollectorSignal::Logs);
        for (index, text) in bodies.iter().enumerate() {
            lines.push(format!("LogRecord #{index}"));
            lines.push("ObservedTimestamp: 1970-01-01 00:00:00 +0000 UTC".to_string());
            lines.push("Timestamp: 2026-10-13 20:46:22.489129753 +0000 UTC".to_string());
            lines.push("SeverityText: ".to_string());
            lines.push("SeverityNumber: Unspecified(0)".to_string());
            lines.push(format!("Body: Str({text})"));
            lines.push("Attributes:".to_string());
            lines.push(format!("     -> event.seq: Int({index})"));
            lines.push("Trace ID: ".to_string());
            lines.push("Span ID: ".to_string());
            lines.push("Flags: 0".to_string());
        }
        body(CollectorSignal::Logs, &lines)
    }

    /// A traces body with one span per entry of `names`, each with one event and one link.
    fn traces_body(names: &[&str]) -> String {
        let mut lines = resource(CollectorSignal::Traces);
        for (index, name) in names.iter().enumerate() {
            lines.push(format!("Span #{index}"));
            lines.push("    Trace ID       : 00112233445566778899aabbccddeeff".to_string());
            lines.push("    Parent ID      : ".to_string());
            lines.push("    ID             : 0011223344556677".to_string());
            lines.push(format!("    Name           : {name}"));
            lines.push("    Kind           : Internal".to_string());
            lines.push("    Status code    : Ok".to_string());
            lines.push("Attributes:".to_string());
            lines.push(format!("     -> event.marker: Str({name})"));
            lines.push("Events:".to_string());
            lines.push("SpanEvent #0".to_string());
            lines.push("     -> Name: checkpoint".to_string());
            lines.push("Links:".to_string());
            lines.push("SpanLink #0".to_string());
            lines.push("     -> Trace ID: 00112233445566778899aabbccddeeff".to_string());
        }
        body(CollectorSignal::Traces, &lines)
    }

    /// A metrics body with one sum whose data points carry `markers`, one each.
    fn metrics_body(markers: &[&str]) -> String {
        let mut lines = resource(CollectorSignal::Metrics);
        lines.push("Metric #0".to_string());
        lines.push("Descriptor:".to_string());
        lines.push("     -> Name: nervix.test.batch.count".to_string());
        lines.push("     -> DataType: Sum".to_string());
        for (index, marker) in markers.iter().enumerate() {
            lines.push(format!("NumberDataPoints #{index}"));
            lines.push("Data point attributes:".to_string());
            lines.push(format!("     -> event.marker: Str({marker})"));
            lines.push(format!("Value: {index}"));
        }
        body(CollectorSignal::Metrics, &lines)
    }

    /// The members an export carries, in the order its body names them.
    fn carried<'member>(export: &CollectorExport, members: &[&'member str]) -> Vec<&'member str> {
        let mut carried = Vec::new();
        for member in members {
            if let Some(position) = export.position(member) {
                carried.push((position, *member));
            }
        }
        carried.sort_unstable();
        carried.into_iter().map(|(_, member)| member).collect()
    }

    #[test]
    fn exports_are_read_from_their_bodies_when_summaries_of_concurrent_exports_print_between() {
        let mut printed = summary(CollectorSignal::Metrics, 2);
        printed.push_str(&summary(CollectorSignal::Metrics, 1));
        printed.push_str(&metrics_body(&["first", "second"]));
        printed.push_str(&metrics_body(&["third"]));
        let exports = CollectorSignal::Metrics.exports(&printed);
        let members = ["first", "second", "third"];
        assert_eq!(exports.len(), 2);
        assert_eq!(exports[0].records(), 2);
        assert_eq!(carried(&exports[0], &members), ["first", "second"]);
        assert_eq!(exports[1].records(), 1);
        assert_eq!(carried(&exports[1], &members), ["third"]);
    }

    #[test]
    fn an_export_whose_summary_the_logger_did_not_print_is_read_from_its_body() {
        let mut printed = summary(CollectorSignal::Metrics, 1);
        printed.push_str(&metrics_body(&["unrelated"]));
        printed.push_str(&metrics_body(&["first", "second"]));
        printed.push_str(&metrics_body(&["third"]));
        let exports = CollectorSignal::Metrics.exports(&printed);
        let records = exports
            .iter()
            .map(CollectorExport::records)
            .collect::<Vec<_>>();
        assert_eq!(records, [1, 2, 1]);
        assert_eq!(
            carried(&exports[1], &["first", "second", "third"]),
            ["first", "second"]
        );
        assert_eq!(
            carried(&exports[2], &["first", "second", "third"]),
            ["third"]
        );
    }

    #[test]
    fn the_bodies_of_other_signals_are_not_part_of_an_export() {
        let members = ["first", "second", "third"];
        let mut printed = summary(CollectorSignal::Metrics, 1);
        printed.push_str(&metrics_body(&["unrelated"]));
        printed.push_str(&summary(CollectorSignal::Logs, 2));
        printed.push_str(&logs_body(&["first", "second"]));
        printed.push_str(&summary(CollectorSignal::Traces, 1));
        printed.push_str(&traces_body(&["third"]));
        let metrics = CollectorSignal::Metrics.exports(&printed);
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].records(), 1);
        assert_eq!(carried(&metrics[0], &members), Vec::<&str>::new());
        let logs = CollectorSignal::Logs.exports(&printed);
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].records(), 2);
        assert_eq!(carried(&logs[0], &members), ["first", "second"]);
        let traces = CollectorSignal::Traces.exports(&printed);
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].records(), 1);
        assert_eq!(carried(&traces[0], &members), ["third"]);
    }

    #[test]
    fn entries_other_than_bodies_are_not_exports() {
        let mut printed = format!(
            "{TIME}\tinfo\tservice@v0.159.0/service.go:259\tStarting otelcol...\t{{\"Version\": \
             \"0.159.0\"}}\n"
        );
        printed.push_str(&format!(
            "{TIME}\twarn\tgrpc@v1.75.0/server.go:893\tconnection closed by the peer\n"
        ));
        printed.push_str(&summary(CollectorSignal::Metrics, 1));
        printed.push_str(&metrics_body(&["first"]));
        let exports = CollectorSignal::Metrics.exports(&printed);
        assert_eq!(exports.len(), 1);
        assert_eq!(exports[0].records(), 1);
        assert!(exports[0].position("first").is_some());
    }

    #[test]
    fn a_body_is_read_once_the_logger_printed_its_fields() {
        let whole = logs_body(&["first", "second"]);
        let fields_start = whole
            .rfind("\n\t")
            .expect("a body ends with the logger's fields");
        let (arriving, rest) = whole.split_at(fields_start + 1);
        assert_eq!(CollectorSignal::Logs.exports(arriving), Vec::new());
        let mut printed = arriving.to_string();
        printed.push_str(&rest[..2]);
        let exports = CollectorSignal::Logs.exports(&printed);
        assert_eq!(exports.len(), 1);
        assert_eq!(exports[0].records(), 2);
    }

    #[test]
    fn records_are_counted_by_the_lines_that_start_them() {
        let traces = traces_body(&["first", "second", "third"]);
        let exports = CollectorSignal::Traces.exports(&traces);
        assert_eq!(exports.len(), 1);
        assert_eq!(exports[0].records(), 3, "events and links are not spans");

        let mut lines = resource(CollectorSignal::Metrics);
        let kinds = [
            "NumberDataPoints",
            "HistogramDataPoints",
            "ExponentialHistogramDataPoints",
            "SummaryDataPoints",
        ];
        for (index, kind) in kinds.iter().enumerate() {
            lines.push(format!("Metric #{index}"));
            lines.push(format!("{kind} #0"));
            lines.push(format!("{kind} #1"));
            lines.push("Exemplars:".to_string());
            lines.push("Exemplar #0".to_string());
        }
        let metrics = body(CollectorSignal::Metrics, &lines);
        let exports = CollectorSignal::Metrics.exports(&metrics);
        assert_eq!(exports.len(), 1);
        assert_eq!(
            exports[0].records(),
            8,
            "every data point, and no metric or exemplar"
        );
    }

    #[test]
    fn a_record_name_inside_a_value_does_not_start_a_record() {
        let logs = logs_body(&["LogRecord #7", "Span #1 NumberDataPoints #2"]);
        let exports = CollectorSignal::Logs.exports(&logs);
        assert_eq!(exports.len(), 1);
        assert_eq!(exports[0].records(), 2);
        let traces = traces_body(&["Span #4"]);
        assert_eq!(CollectorSignal::Traces.exports(&traces)[0].records(), 1);
    }

    #[test]
    fn a_record_line_needs_a_record_number() {
        let mut lines = resource(CollectorSignal::Logs);
        lines.push("LogRecord #0".to_string());
        lines.push("LogRecord #".to_string());
        lines.push("LogRecord #1a".to_string());
        let logs = body(CollectorSignal::Logs, &lines);
        assert_eq!(CollectorSignal::Logs.exports(&logs)[0].records(), 1);
    }

    #[test]
    fn steps_name_signals_by_their_lowercase_names() {
        let signals = [
            ("logs", CollectorSignal::Logs, "log records"),
            ("traces", CollectorSignal::Traces, "spans"),
            ("metrics", CollectorSignal::Metrics, "data points"),
        ];
        for (name, signal, record_noun) in signals {
            assert_eq!(name.parse::<CollectorSignal>(), Ok(signal));
            assert_eq!(signal.to_string(), name);
            assert_eq!(signal.record_noun(), record_noun);
        }
        assert!("spans".parse::<CollectorSignal>().is_err());
    }
}
