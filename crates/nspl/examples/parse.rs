use std::{path::PathBuf, process::ExitCode};

use ariadne::{Color, Label, Report, ReportKind, Source};
use nervix_nspl::{
    schema::{Diagnostic, ParseFromSourceError},
    statement::{parse_statement, suggest_statement},
};
use rustyline::{
    Context, Editor, Helper,
    completion::{Completer, Pair},
    error::ReadlineError,
    highlight::Highlighter,
    hint::Hinter,
    history::DefaultHistory,
    validate::Validator,
};

#[derive(Debug, Clone, Default)]
struct NsplHelper {
    buffer_prefix: String,
}

impl Helper for NsplHelper {}
impl Highlighter for NsplHelper {}
impl Validator for NsplHelper {}
impl Hinter for NsplHelper {
    type Hint = String;
}

impl Completer for NsplHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let start = word_start(line, pos);
        let combined = format!("{}{}", self.buffer_prefix, &line[..pos]);
        let pairs = suggest_statement(&combined, combined.len())
            .into_iter()
            .map(|w| Pair {
                display: w.to_string(),
                replacement: w.to_string(),
            })
            .collect::<Vec<_>>();

        Ok((start, pairs))
    }
}

fn word_start(line: &str, pos: usize) -> usize {
    let is_word = |c: char| c.is_ascii_alphanumeric() || c == '_';
    line[..pos]
        .char_indices()
        .rev()
        .find(|(_, c)| !is_word(*c))
        .map(|(idx, c)| idx + c.len_utf8())
        .unwrap_or(0)
}

fn print_diagnostics(kind: &str, source_id: &str, source: &str, diagnostics: &[Diagnostic]) {
    for diagnostic in diagnostics {
        let report = Report::build(ReportKind::Error, (source_id, diagnostic.span.clone()))
            .with_message(format!("{kind} error"))
            .with_label(
                Label::new((source_id, diagnostic.span.clone()))
                    .with_message(diagnostic.message.clone())
                    .with_color(Color::Red),
            )
            .finish();

        if let Err(err) = report.eprint((source_id, Source::from(source))) {
            eprintln!("failed to render diagnostic: {err}");
        }
    }
}

fn print_parse_result(input: &str) {
    match parse_statement(input) {
        Ok(parsed) => {
            println!("{parsed:#?}");
        }
        Err(ParseFromSourceError::Lex {
            source,
            diagnostics,
        }) => {
            print_diagnostics("lex", "repl", &source, &diagnostics);
        }
        Err(ParseFromSourceError::Parse {
            source,
            diagnostics,
        }) => {
            print_diagnostics("parse", "repl", &source, &diagnostics);
        }
    }
}

fn history_path() -> PathBuf {
    PathBuf::from(".nervix_history")
}

fn main() -> ExitCode {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if !args.is_empty() {
        eprintln!("this example is interactive and does not accept CLI args");
        return ExitCode::FAILURE;
    }

    let mut rl: Editor<NsplHelper, DefaultHistory> = match Editor::new() {
        Ok(editor) => editor,
        Err(err) => {
            eprintln!("failed to initialize line editor: {err}");
            return ExitCode::FAILURE;
        }
    };
    rl.set_helper(Some(NsplHelper::default()));

    let hist_path = history_path();
    let _ = rl.load_history(&hist_path);

    println!("NSPL interactive parser");
    println!("Supported now:");
    println!("  CREATE SCHEMA <name> (<field defs>) [;]");
    println!("  CREATE STRICT|LOOSE WIRE JSON|CBOR|AVRO SCHEMA <name> (<field defs>) [;]");
    println!(
        "  CREATE CODEC <name> FROM WIRE JSON|CBOR|AVRO SCHEMA <wire_schema> TO SCHEMA <schema> \
         [;]"
    );
    println!(
        "  CREATE CODEC <name> FROM JSON|YAML|TOML|XML|CBOR TO SCHEMA <schema> WITH JAQ \
         TRANSFORMATION '...' [;]"
    );
    println!(
        "  CREATE CODEC <name> FROM PROTOBUF USING RESOURCE <resource> [VERSION <n>] CONFIG \
         {{'file' = '<path.proto>', 'include' = '.'}} MESSAGE '<package.Message>' TO SCHEMA \
         <schema> WITH JAQ TRANSFORMATION '...' [;]"
    );
    println!("  CREATE RELAY <name> SCHEMA <schema> [CAPACITY <n>] [;]");
    println!(
        "  CREATE VHOST <name> <hostname>, <hostname>, ... [WITH TLS <resource> [VERSION <n>]] [;]"
    );
    println!("  CREATE ENDPOINT <name> ON <vhost> PATH '/path' TYPE WEBSOCKETS|HTTP [;]");
    println!(
        "  CREATE CLIENT <name> TYPE \
         KAFKA|PULSAR|HTTP|PROMETHEUS|RABBITMQ|REDIS|MQTT|NATS|ZEROMQ|SQS|S3|GCS|AZURE_BLOB|\
         ICEBERG_REST|WEBSOCKETS CONFIG {{ 'k' = 'v', ... }} [;]"
    );
    println!(
        "  CREATE INGESTOR <name> [FILTER WHERE <expr>] TO <relay> ... DECODE USING <codec> ... \
         FROM HTTP|KAFKA|PULSAR|PROMETHEUS|RABBITMQ|REDIS|MQTT|NATS|ZEROMQ|SQS|ENDPOINT|WEBSOCKETS \
         ... [ ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;]"
    );
    println!(
        "  CREATE EMITTER <name> FROM <s> ENCODE USING <codec> TO \
         KAFKA|PULSAR|RABBITMQ|REDIS|MQTT|NATS|ZEROMQ|SQS ... [ ON MESSAGE ERROR LOG ON GENERAL \
         ERROR LOG;]"
    );
    println!("  SHOW CREATE SCHEMA|CODEC|CLIENT|VHOST|ENDPOINT|INGESTOR|RELAY|EMITTER <name> [;]");
    println!("  SHOW CLUSTER STATUS [;]");
    println!("History: Up/Down. Completion: Tab. Type 'exit' to quit.");

    let mut buffer = String::new();

    loop {
        let prompt = if buffer.is_empty() {
            "nervix> "
        } else {
            "....> "
        };
        if let Some(helper) = rl.helper_mut() {
            helper.buffer_prefix = buffer.clone();
        }

        let line = match rl.readline(prompt) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                if !buffer.trim().is_empty() {
                    print_parse_result(&buffer);
                }
                break;
            }
            Err(err) => {
                eprintln!("failed to read input: {err}");
                return ExitCode::FAILURE;
            }
        };

        let trimmed = line.trim();
        if buffer.is_empty()
            && (trimmed.eq_ignore_ascii_case("exit") || trimmed.eq_ignore_ascii_case("quit"))
        {
            break;
        }

        if !trimmed.is_empty() {
            let _ = rl.add_history_entry(line.as_str());
        }

        if trimmed.is_empty() {
            continue;
        }

        buffer.push_str(&line);
        buffer.push('\n');

        if trimmed.ends_with(';') {
            print_parse_result(&buffer);
            buffer.clear();
        }
    }

    let _ = rl.save_history(&hist_path);
    println!("bye");
    ExitCode::SUCCESS
}
