from __future__ import annotations

import io
import subprocess
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.check_typed_errors import main


def write_repository(root: Path, sources: dict[str, str]) -> None:
    """Stage `sources` in a git repository, because the check reads tracked files only."""

    for path, body in sources.items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body, encoding="utf-8")
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=root, check=True)
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)


def run(root: Path) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        status = main(["--root", str(root)])
    return status, out.getvalue(), err.getvalue()


class TypedErrorRuleTests(unittest.TestCase):
    def test_a_string_error_fails_and_names_the_rule_and_every_site(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(
                root,
                {
                    "src/lib.rs": """
struct Envelope {
    result: Result<Vec<u8>, String>,
}

fn parse(
    value: &str,
) -> Result<
    Vec<u8>,
    String,
> {
    Ok(value.as_bytes().to_vec())
}

fn parse_all(values: &[&str]) -> Vec<u8> {
    values.iter().map(|value| parse(value)).collect::<Result<Vec<_>, String>>();
    Vec::new()
}
""",
                },
            )

            status, report, errors = run(root)

            self.assertEqual(status, 1)
            # One stream, so a CI log cannot print the rule below the sites it explains.
            self.assertEqual(errors, "")
            self.assertTrue(report.startswith("error[typed-errors]: "), report)
            self.assertIn("Typed error conversions", report)
            self.assertIn("  src/lib.rs:3: result: Result<Vec<u8>, String>,\n", report)
            self.assertIn("  src/lib.rs:8: ) -> Result<\n", report)
            self.assertIn("  src/lib.rs:16: values.iter()", report)

    def test_typed_errors_comments_literals_and_tests_pass(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_repository(
                root,
                {
                    "src/lib.rs": """
// A comment may name Result<(), String> without holding one.
const DOCUMENTED: &str = "Result<(), String>";

fn typed(value: &str) -> error_stack::Result<Vec<String>, ParseError> {
    Ok(vec![value.to_owned()])
}

#[cfg(test)]
mod tests {
    fn helper() -> Result<(), String> {
        Ok(())
    }
}
""",
                    "src/lookup_tests.rs": "fn helper() -> Result<(), String> { Ok(()) }\n",
                    "tests/scenarios.rs": "fn step() -> Result<(), String> { Ok(()) }\n",
                },
            )

            status, report, errors = run(root)

            self.assertEqual(status, 0)
            self.assertEqual(report, "")
            self.assertEqual(errors, "")


if __name__ == "__main__":
    unittest.main()
