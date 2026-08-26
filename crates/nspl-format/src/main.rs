//! Command-line entry point for the NSPL formatter.

use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::ExitCode,
};

use clap::Parser;
use ignore::WalkBuilder;
use nervix_nspl_format::{FormatError, diagnostics, format_source};

/// The name standard input reports as, in both listings and diagnostics.
const STDIN_ORIGIN: &str = "<stdin>";

#[derive(Parser, Debug)]
#[command(name = "nervix-nspl-format")]
#[command(about = "Format NSPL configuration files")]
struct Args {
    /// NSPL files or directories to format; a directory is searched recursively for `.nspl`
    /// files, and `-` reads standard input and writes standard output
    #[arg(required = true, value_name = "PATH")]
    paths: Vec<PathBuf>,
    /// Report files that are not formatted instead of rewriting them
    #[arg(long, conflicts_with = "stdout")]
    check: bool,
    /// Write the formatted result to standard output instead of the file
    #[arg(long)]
    stdout: bool,
}

/// What happened to one input.
enum Outcome {
    /// The input was already formatted, or was rewritten.
    Settled,
    /// `--check` found the input is not formatted.
    WouldReformat,
    /// The input could not be lexed or parsed.
    Rejected,
    /// The input could not be read or written.
    Unusable,
    /// The formatter could not render the input, which is a defect.
    Defective,
}

impl Outcome {
    /// The exit code this outcome alone would produce.
    fn code(&self) -> u8 {
        match self {
            Self::Settled => 0,
            Self::WouldReformat => 1,
            Self::Rejected => 3,
            Self::Unusable => 4,
            Self::Defective => 5,
        }
    }
}

fn main() -> ExitCode {
    let args = Args::parse();

    let reads_stdin = args.paths.iter().any(|path| path.as_os_str() == "-");
    if reads_stdin && args.paths.len() > 1 {
        eprintln!("nervix-nspl-format: `-` cannot be combined with other paths");
        return ExitCode::from(2);
    }

    if reads_stdin {
        return ExitCode::from(format_stdin(args.check).code());
    }

    let (inputs, mut worst) = collect_inputs(&args.paths);
    let mut unformatted = Vec::new();

    for path in &inputs {
        let outcome = format_path(path, &args, &mut unformatted);
        worst = worst.max(outcome.code());
    }

    if args.check && !unformatted.is_empty() {
        eprintln!(
            "{} of {} files are not formatted; rerun without --check to rewrite them",
            unformatted.len(),
            inputs.len()
        );
    }

    ExitCode::from(worst)
}

/// Expands the arguments into the files to format, with the worst outcome seen while searching.
///
/// A path naming a file is taken as given, whatever its extension: naming a file is an explicit
/// request to format it. A path naming a directory is searched recursively for `.nspl` files.
fn collect_inputs(paths: &[PathBuf]) -> (Vec<PathBuf>, u8) {
    let mut inputs = Vec::new();
    let mut worst = 0u8;

    for path in paths {
        if !path.is_dir() {
            inputs.push(path.clone());
            continue;
        }
        match nspl_files_in(path) {
            Ok(found) => inputs.extend(found),
            Err(error) => {
                eprintln!(
                    "nervix-nspl-format: cannot search {}: {error}",
                    path.display()
                );
                worst = worst.max(Outcome::Unusable.code());
            }
        }
    }

    (inputs, worst)
}

/// Every `.nspl` file under `root`, in a stable order.
///
/// Ignore rules and hidden directories are honored, so a search never descends into build output,
/// vendored trees, or anything the repository has excluded. Symbolic links are not followed, so a
/// link cycle cannot trap the search and no file is formatted twice.
fn nspl_files_in(root: &Path) -> Result<Vec<PathBuf>, ignore::Error> {
    let mut found = Vec::new();

    for entry in WalkBuilder::new(root)
        .hidden(true)
        .follow_links(false)
        .build()
    {
        let entry = entry?;
        if !entry.file_type().is_some_and(|kind| kind.is_file()) {
            continue;
        }
        if entry.path().extension().is_some_and(|ext| ext == "nspl") {
            found.push(entry.into_path());
        }
    }

    // A stable order keeps `--check` output reproducible across runs and machines.
    found.sort();
    Ok(found)
}

fn format_stdin(check: bool) -> Outcome {
    let mut input = String::new();
    if let Err(error) = std::io::stdin().read_to_string(&mut input) {
        eprintln!("nervix-nspl-format: cannot read standard input: {error}");
        return Outcome::Unusable;
    }

    match format_source(&input) {
        Ok(formatted) if check => {
            if formatted == input {
                Outcome::Settled
            } else {
                println!("{STDIN_ORIGIN}");
                Outcome::WouldReformat
            }
        }
        Ok(formatted) => {
            print!("{formatted}");
            let _ = std::io::stdout().flush();
            Outcome::Settled
        }
        Err(error) => {
            report(STDIN_ORIGIN, &error);
            outcome_for(&error)
        }
    }
}

fn format_path(path: &Path, args: &Args, unformatted: &mut Vec<PathBuf>) -> Outcome {
    let origin = path.display().to_string();

    let input = match fs::read(path) {
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(text) => text,
            Err(_) => {
                eprintln!("nervix-nspl-format: {origin} is not valid UTF-8");
                return Outcome::Unusable;
            }
        },
        Err(error) => {
            eprintln!("nervix-nspl-format: cannot read {origin}: {error}");
            return Outcome::Unusable;
        }
    };

    let formatted = match format_source(&input) {
        Ok(formatted) => formatted,
        Err(error) => {
            report(&origin, &error);
            return outcome_for(&error);
        }
    };

    if args.stdout {
        print!("{formatted}");
        let _ = std::io::stdout().flush();
        return Outcome::Settled;
    }

    if formatted == input {
        return Outcome::Settled;
    }

    if args.check {
        println!("{origin}");
        unformatted.push(path.to_path_buf());
        return Outcome::WouldReformat;
    }

    match write_atomically(path, &formatted) {
        Ok(()) => Outcome::Settled,
        Err(error) => {
            eprintln!("nervix-nspl-format: cannot write {origin}: {error}");
            Outcome::Unusable
        }
    }
}

/// Replaces `path` with `contents` without ever leaving a partly written file behind.
fn write_atomically(path: &Path, contents: &str) -> std::io::Result<()> {
    let directory = path.parent().unwrap_or_else(|| Path::new("."));
    let mut file = tempfile::NamedTempFile::new_in(directory)?;
    file.write_all(contents.as_bytes())?;
    file.flush()?;

    // A temporary file is created private to its owner, so the target's own permissions are
    // carried over before it takes the target's place.
    if let Ok(metadata) = fs::metadata(path) {
        let _ = file.as_file().set_permissions(metadata.permissions());
    }

    file.persist(path)?;
    Ok(())
}

fn report(origin: &str, error: &FormatError) {
    match error {
        FormatError::Parse(parse) => diagnostics::report(origin, parse),
        other => eprintln!("nervix-nspl-format: {origin}: {other}"),
    }
}

fn outcome_for(error: &FormatError) -> Outcome {
    match error {
        FormatError::Parse(_) => Outcome::Rejected,
        FormatError::Render { .. } | FormatError::Verification { .. } => Outcome::Defective,
    }
}
