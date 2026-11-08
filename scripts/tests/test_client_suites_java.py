"""Resolve the JVM suite against current Java declarations using the prepared source analyzer."""

from __future__ import annotations

import os
import tempfile
import shutil
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.client_suites import GUIDE_SECTIONS, INVENTORY, Host, analyze_java, validate

ROOT = Path(__file__).resolve().parents[2]


class JavaSurfaceTests(unittest.TestCase):
    def analyze(self, source: str, suite: str):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "api").mkdir()
            (root / "suite/boundary").mkdir(parents=True)
            (root / "api/Api.java").write_text(source)
            (root / "suite/boundary/Usage.java").write_text(suite)
            host = Host("java", "fixture", "api/Api.java", "", "suite", "guide.md")
            environment = {
                "NERVIX_CLIENT_SUITE_JAVA_CHECKER": os.environ.get(
                    "NERVIX_CLIENT_SUITE_JAVA_CHECKER", str(ROOT / "target/client-suites/java")
                ),
                "NERVIX_CLIENT_CONFORMANCE_DIR": os.environ.get(
                    "NERVIX_CLIENT_CONFORMANCE_DIR", str(ROOT / "target/client-conformance")
                ),
            }
            with patch.dict(os.environ, environment):
                return analyze_java(root, host)

    def test_validation_holds_resolved_members_and_tests_to_the_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "api").mkdir()
            (root / "suite/boundary").mkdir(parents=True)
            (root / "tests").mkdir()
            (root / "api/Api.java").write_text("""
package fixture;
public class Api { public void read() {} public void finish() {} }
""")
            usage = root / "suite/boundary/Usage.java"
            source = """
package fixture;
import org.junit.jupiter.api.Test;
class Usage { @Test void usesTheApi() { var api = new Api(); api.read(); CALL_FINISH } }
"""
            usage.write_text(source.replace("CALL_FINISH", ""))
            inventory = root / INVENTORY
            entry = '\n[[test]]\nhost = "java"\nidentity = "fixture.Usage#usesTheApi"\ntier = "boundary"\n'
            inventory.write_text(entry)
            (root / "guide.md").write_text("# Guide\n" + "\n".join(
                f"## {section}\nAn application example.\n" for section in GUIDE_SECTIONS
            ))
            host = Host("java", "fixture", "api/Api.java", "", "suite", "guide.md")
            environment = {
                "NERVIX_CLIENT_SUITE_JAVA_CHECKER": os.environ.get(
                    "NERVIX_CLIENT_SUITE_JAVA_CHECKER", str(ROOT / "target/client-suites/java")
                ),
                "NERVIX_CLIENT_CONFORMANCE_DIR": os.environ.get(
                    "NERVIX_CLIENT_CONFORMANCE_DIR", str(ROOT / "target/client-conformance")
                ),
            }
            with patch.dict(os.environ, environment):
                missing = validate(root, [host])
                self.assertTrue(any("fixture.Api#finish()" in problem for problem in missing))
                usage.write_text(source.replace("CALL_FINISH", "api.finish();"))
                self.assertEqual(validate(root, [host]), [])
                inventory.write_text(entry.replace("usesTheApi", "anotherTest"))
                drift = validate(root, [host])
                self.assertTrue(any("does not register" in problem for problem in drift))
                self.assertTrue(any("which the suite lacks" in problem for problem in drift))

    def test_resolves_the_receiver_overload_constructor_record_and_enum(self):
        result = self.analyze("""
package fixture;
public final class Api {
    public Api() {}
    public int read() { return 1; }
    public int reference() { return 3; }
    public int read(int value) { return value; }
    public record Point(int coordinate) {}
    public enum Mode { FAST, SLOW }
    public static class Other { public int read(int value) { return value; } }
}
""", """
package fixture;
import org.junit.jupiter.api.Test;
class Usage {
    @Test void readsItsOwnMember() {
        var api = new Api();
        api.read();
        java.util.function.IntSupplier referenced = api::reference;
        var other = new Api.Other();
        other.read(1);
        var point = new Api.Point(7);
        point.coordinate();
        var mode = Api.Mode.FAST;
        String text = "Api.read(int) Api.Mode.SLOW";
        // api.read(1); Api.Mode.SLOW;
    }
}
""")
        self.assertEqual(result.problems, [])
        self.assertIn("fixture.Api#<init>()", result.references)
        self.assertIn("fixture.Api#read()", result.references)
        self.assertIn("fixture.Api#reference()", result.references)
        self.assertNotIn("fixture.Api#read(int)", result.references)
        self.assertIn("fixture.Api.Other#read(int)", result.references)
        self.assertIn("fixture.Api.Point#coordinate()", result.references)
        self.assertIn("fixture.Api.Point#<init>(int)", result.references)
        self.assertIn("fixture.Api.Mode#FAST", result.references)
        self.assertNotIn("fixture.Api.Mode#SLOW", result.references)
        self.assertEqual(result.tests, {"fixture.Usage#readsItsOwnMember": "boundary"})

    def test_unused_imports_and_public_methods_of_private_owners_do_not_count(self):
        result = self.analyze("""
package fixture;
public final class Api {
    public static int unused() { return 1; }
    private static class Private { public int hidden() { return 1; } }
}
""", """
package fixture;
import static fixture.Api.unused;
import org.junit.jupiter.api.Test;
class Usage { @Test void leavesItUnused() {} }
""")
        self.assertIn("fixture.Api#unused()", result.surface - result.references)
        self.assertFalse(any("Private" in value for value in result.surface))

    def test_cached_launcher_is_verified_before_it_is_reused(self):
        prepared = Path(os.environ.get("NERVIX_CLIENT_CONFORMANCE_DIR", ROOT / "target/client-conformance"))
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / "junit-platform-console-standalone.jar"
            shutil.copyfile(prepared / "java/junit-platform-console-standalone.jar", artifact)
            command = ["bash", str(ROOT / "scripts/download_junit.sh"), directory]
            accepted = subprocess.run(command, capture_output=True, text=True, check=False)
            self.assertEqual(accepted.returncode, 0, accepted.stderr)
            self.assertEqual(accepted.stdout.strip(), str(artifact))
            artifact.write_bytes(b"invalid cached launcher")
            refused = subprocess.run(command, capture_output=True, text=True, check=False)
            self.assertNotEqual(refused.returncode, 0)
            self.assertIn("SHA-256 mismatch", refused.stderr)
            self.assertEqual(artifact.read_bytes(), b"invalid cached launcher")

    def test_missing_and_invalid_junit_tests_fail_closed(self):
        result = self.analyze("package fixture; public class Api {}", """
package fixture;
import org.junit.jupiter.params.ParameterizedTest;
class Usage { @ParameterizedTest void repeated(int value) {} }
""")
        self.assertTrue(any("stable" in problem for problem in result.problems))
        self.assertTrue(any("no JUnit tests" in problem for problem in result.problems))
        broken = self.analyze("package fixture; public class Api {}", """
package fixture;
import org.junit.jupiter.api.Test;
class Usage { @Test void broken() { new Api().missing(); } }
""")
        self.assertTrue(any("source analysis failed" in problem for problem in broken.problems))


if __name__ == "__main__":
    unittest.main()
