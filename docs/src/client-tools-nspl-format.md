# NSPL Formatter

`nervix-nspl-format` puts NSPL files into their canonical shape. It is an offline tool: it never
opens a session, contacts a cluster, or executes anything it reads. It parses a file, renders every
statement canonically, writes the result back, and can instead report which files would change.

The shape it writes is the same one `SHOW CREATE` prints, so text copied between a file and a
session is already formatted.

## Installing

The formatter is installed alongside the other tools; see
[Cargo Install From GitHub](installation-cargo.md).

## Using It

```bash
nervix-nspl-format pipeline.nspl              # format one file in place
nervix-nspl-format .                          # format every NSPL file beneath here
nervix-nspl-format --check .                  # report instead of rewriting
nervix-nspl-format --stdout pipeline.nspl     # write to standard output
cat pipeline.nspl | nervix-nspl-format -      # read standard input
```

| Argument | Behavior |
| --- | --- |
| `<PATH>...` | NSPL files or directories. `-` reads standard input and writes standard output. |
| `--check` | Print the path of every file that is not formatted, and change nothing. |
| `--stdout` | Write the formatted result to standard output instead of the file. |

`--check` and `--stdout` cannot be combined, and `-` cannot be combined with other paths.

## Searching Directories

A path naming a directory is searched recursively for `.nspl` files. The search honors your
`.gitignore` and skips hidden directories, so it never descends into build output or vendored
trees, and it does not follow symbolic links. Files are formatted in a stable order, so `--check`
output is reproducible.

A path naming a file is formatted whatever its extension — naming a file is an explicit request.
Only the recursive search filters by extension, which is why a template such as `graph.nspl.upon`
is never picked up by `nervix-nspl-format .`.

There is no built-in diff. To see what would change:

```bash
nervix-nspl-format --stdout pipeline.nspl | diff -u pipeline.nspl -
```

## Exit Codes

| Code | Meaning |
| --- | --- |
| 0 | Every file was handled, or under `--check` every file was already formatted. |
| 1 | `--check` only: at least one file is not formatted. |
| 2 | The arguments were not usable. |
| 3 | A file could not be parsed. The parse error is printed with the offending span. |
| 4 | A file could not be read or written, or was not UTF-8. |
| 5 | A file could not be rendered. This is a defect in the formatter; please report it. |

A file that fails never blocks the others: every failure is reported and every other file is still
formatted. A file that fails is left exactly as it was.

## What It Guarantees

- **Meaning is preserved.** Reparsing the output yields exactly the statements that went in. The
  formatter checks this itself before writing, and refuses to write if it ever fails.
- **Formatting is idempotent.** Formatting an already formatted file changes nothing, byte for byte,
  and the file is not rewritten at all — so modification times do not churn.
- **Comments survive.** A comment between two statements stays between them, and a comment above a
  statement stays above it. A file header and a comment trailing the last statement are kept.
- **Writes are atomic.** The result is written to a temporary file in the same directory and renamed
  over the target, so an interrupted run never leaves a partly written file.

## What It Changes

Formatting a hand-written file for the first time normalizes more than layout:

- Keywords are uppercased, including type names, and identifiers are lowercased — NSPL lowercases
  identifiers when it parses them, so this is the name the cluster already uses.
- String literals prefer single quotes; a value containing an apostrophe uses double quotes, and a
  value containing a newline or both quote styles is dollar-quoted.
- Values that were left implicit are written out, such as a relay's `CAPACITY` and an ingestor's
  `INSTANCES`.
- Redundant parentheses are dropped from expressions, keeping only those that change grouping.

Durations and byte sizes keep the spelling you wrote: `250ms` stays `250ms`, and `1MiB` stays
`1MiB`.

## Comments Inside A Statement

A comment between two statements has one obvious place to go. A comment *inside* a statement does
not, so the formatter does not guess: a statement containing a comment in its body is written back
exactly as you wrote it, and the statements around it are still formatted.

That statement keeps its original layout until the comment is moved out of it. This is deliberate —
the alternative is relocating a comment away from the clause it explains.
