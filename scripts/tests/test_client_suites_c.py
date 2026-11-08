"""Compiler-resolved C/C++ header references and named suite inventory controls."""

from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from scripts import client_suites
from scripts.client_suites_c import NativeSuite


HEADER = """
#ifndef NERVIX_CLIENT_H
#define NERVIX_CLIENT_H
typedef struct nx_handle nx_handle;
typedef enum nx_mode { NX_READY = 1, NX_WAITING = 2 } nx_mode;
nx_handle *nx_open(nx_mode mode);
void nx_close(nx_handle *handle);
#endif
"""

SOURCE = """
#include "nervix_client.h"
struct suite_test { const char *tier; const char *name; void (*run)(void); };
// ANCHOR: example
static void test_handles(void) {
    nx_handle *handle = nx_open(NX_READY);
    nx_mode mode = NX_WAITING;
    (void)mode;
    nx_close(handle);
}
// ANCHOR_END: example
static const struct suite_test suite_tests[] = {
    {"boundary", "test_handles", test_handles},
};
int main(void) { suite_tests[0].run(); return 0; }
"""


class NativeSuiteTests(unittest.TestCase):
    def check_source(self, source: str, language: str = "c"):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        header = root / "crates/client-ffi/include/nervix_client.h"
        header.parent.mkdir(parents=True)
        header.write_text(HEADER)
        suite = root / f"tests/client_conformance/{language}/suite"
        suite.mkdir(parents=True)
        suffix = "cpp" if language == "cpp" else "c"
        (suite / f"main.{suffix}").write_text(source)
        self.suite = NativeSuite(root, language)
        return self.suite.inspect()

    def test_c_references_the_declared_header_and_registers_named_tests(self):
        report = self.check_source(SOURCE)
        self.assertEqual(report.problems, [])
        self.assertEqual(report.missing, set())
        self.assertEqual(report.tests, {"test_handles": "boundary"})
        self.assertEqual(report.surface,
                         {"nx_handle", "nx_mode", "NX_READY", "NX_WAITING", "nx_open", "nx_close"})

    def test_cpp_resolves_calls_through_a_custom_unique_ptr_deleter(self):
        source = SOURCE.replace('#include "nervix_client.h"',
                                '#include "nervix_client.h"\n#include <memory>')
        source = source.replace("nx_close(handle);", "std::unique_ptr<nx_handle, decltype(&nx_close)> owned(handle, nx_close);")
        report = self.check_source(source, "cpp")
        self.assertEqual(report.problems, [])
        self.assertEqual(report.missing, set())

    def test_comments_and_strings_do_not_count_as_references(self):
        source = SOURCE.replace("nx_close(handle);", '(void)handle; /* nx_close */ (void)"nx_close";')
        self.assertEqual(self.check_source(source).missing, {"nx_close"})

    def test_a_local_function_pointer_with_the_same_name_is_not_the_header_function(self):
        source = SOURCE.replace("nx_close(handle);", "void (*nx_close)(nx_handle *) = 0; nx_close(handle);")
        self.assertEqual(self.check_source(source).missing, {"nx_close"})

    def test_a_shadowed_enum_constant_is_not_the_header_enumerator(self):
        source = SOURCE.replace("nx_mode mode = NX_WAITING;", "nx_mode unaffected = NX_READY; (void)unaffected; int NX_WAITING = 0; int mode = NX_WAITING;")
        self.assertEqual(self.check_source(source).missing, {"NX_WAITING"})

    def test_a_missing_type_is_reported(self):
        source = SOURCE.replace("nx_mode mode = NX_WAITING;", "int mode = NX_WAITING;")
        self.assertIn("nx_mode", self.check_source(source).missing)

    def test_a_local_shadow_type_does_not_reference_the_header_type(self):
        source = SOURCE.replace("nx_handle *handle = nx_open(NX_READY);",
                                "void *handle = nx_open(NX_READY); "
                                "{ typedef int nx_handle; nx_handle *fake = 0; (void)fake; }")
        self.assertEqual(self.check_source(source).missing, {"nx_handle"})

    def test_a_nested_shadow_does_not_hide_a_later_real_type(self):
        source = SOURCE.replace("nx_handle *handle = nx_open(NX_READY);",
                                "{ typedef int nx_handle; nx_handle fake = 0; (void)fake; } "
                                "nx_handle *handle = nx_open(NX_READY);")
        self.assertEqual(self.check_source(source).missing, set())

    def test_a_same_name_cpp_function_in_another_namespace_does_not_count(self):
        source = SOURCE.replace('struct suite_test',
                                'namespace other { void nx_close(nx_handle *) {} }\nstruct suite_test', 1)
        source = source.replace("nx_close(handle);", "other::nx_close(handle);")
        report = self.check_source(source, "cpp")
        self.assertEqual(report.problems, [])
        self.assertEqual(report.missing, {"nx_close"})

    def test_decltype_of_a_shadowed_pointer_does_not_reference_the_header_function(self):
        source = SOURCE.replace("nx_close(handle);",
                                "void (*nx_close)(nx_handle *) = nullptr; "
                                "decltype(nx_close) alias = nx_close; (void)alias; (void)handle;")
        self.assertEqual(self.check_source(source, "cpp").missing, {"nx_close"})

    def test_unregistered_function_is_detected(self):
        source = SOURCE + "\nstatic void test_missing(void) {}\n"
        self.assertTrue(any("test_missing" in p for p in self.check_source(source).problems))

    def test_table_identity_must_match_the_registered_function(self):
        source = SOURCE.replace('"test_handles", test_handles', '"another_test", test_handles')
        self.assertTrue(any("identity" in p for p in self.check_source(source).problems))

    def test_duplicate_registration_is_rejected(self):
        source = SOURCE.replace('{"boundary", "test_handles", test_handles},',
                                '{"boundary", "test_handles", test_handles},\n'
                                '    {"boundary", "test_handles", test_handles},')
        self.assertTrue(any("twice" in p for p in self.check_source(source).problems))

    def test_invalid_tier_is_rejected(self):
        self.assertTrue(any("tier" in p for p in self.check_source(SOURCE.replace('"boundary"', '"other"')).problems))

    def test_syntax_errors_are_not_zero_missing_members(self):
        report = self.check_source(SOURCE + "\nthis is not C;\n")
        self.assertTrue(any("compiler" in p for p in report.problems))

    def test_an_empty_table_is_not_a_suite(self):
        source = SOURCE.replace('{"boundary", "test_handles", test_handles},', "")
        self.assertTrue(self.check_source(source).problems)

    def test_inventory_checks_missing_extra_and_misclassified_tests(self):
        tests = self.check_source(SOURCE).tests
        host = client_suites.HOSTS["c"]
        self.assertEqual(client_suites.compare_inventory(host, {"test_handles": "boundary"}, tests), [])
        self.assertTrue(any("does not register c test test_handles" in p
                            for p in client_suites.compare_inventory(host, {}, tests)))
        extra = {"test_handles": "boundary", "test_missing": "boundary"}
        self.assertTrue(any("test_missing, which the suite lacks" in p
                            for p in client_suites.compare_inventory(host, extra, tests)))
        self.assertTrue(any("as cluster, but it is boundary" in p
                            for p in client_suites.compare_inventory(host, {"test_handles": "cluster"}, tests)))

    def test_header_references_cannot_be_replaced_by_suite_definitions(self):
        self.assertTrue(any("defines binding function" in p for p in
                            self.check_source(SOURCE + "\nvoid nx_close(nx_handle *handle) { (void)handle; }\n").problems))

    def test_the_validation_command_checks_both_hosts_and_their_combined_guide(self):
        self.check_source(SOURCE)
        root = self.suite.root
        cpp = NativeSuite(root, "cpp")
        cpp.directory.mkdir(parents=True)
        cpp.source.write_text(SOURCE)
        guide = root / "docs/src/client-library-c.md"
        guide.parent.mkdir(parents=True)
        sections = []
        for section in client_suites.GUIDE_SECTIONS:
            sections.append(f"## {section}\n\nProse.\n")
            if section in {"Connect", "Lifetimes And Memory"}:
                host = "c" if section == "Connect" else "cpp"
                extension = "c" if host == "c" else "cpp"
                sections.append(f"```{host}\n{{{{#include ../../tests/client_conformance/{host}/suite/main.{extension}:example}}}}\n```\n")
        guide.write_text("\n".join(sections))
        (root / client_suites.INVENTORY).write_text("\n".join(
            f'[[test]]\nhost = "{host}"\ntier = "boundary"\nidentity = "test_handles"\n'
            for host in ("c", "cpp")
        ))
        command = ["--root", str(root), "validate", "--host", "c", "--host", "cpp"]
        self.assertEqual(client_suites.main(command), 0)
        cpp.source.write_text(SOURCE.replace("nx_close(handle);", "(void)handle;"))
        guide.write_text(guide.read_text().replace("../../tests/client_conformance/c/suite/main.c", "elsewhere.c"))
        diagnostic = io.StringIO()
        with contextlib.redirect_stderr(diagnostic):
            self.assertEqual(client_suites.main(command), 1)
        self.assertIn("cpp suite never references header member nx_close", diagnostic.getvalue())
        self.assertIn("is not a source", diagnostic.getvalue())


if __name__ == "__main__":
    unittest.main()
