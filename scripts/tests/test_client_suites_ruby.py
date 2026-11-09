"""Native registrations and parsed Ruby references, independent of the extension or a cluster."""

from pathlib import Path
import tempfile
import subprocess
import unittest

from scripts import client_suites as suites
from scripts.client_suites_ruby import analyze, read_surface


class RubySurfaceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.sources = self.root / "crates/client-ruby/src"
        self.suite = self.root / "tests/client_conformance/ruby/suite"
        self.sources.mkdir(parents=True)
        (self.suite / "boundary").mkdir(parents=True)
        self.native("""
            fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
                let fields = crate::class(ruby, module, "Fields")?;
                fields.define_singleton_method("new", function!(Fields::new, 0))?;
                fields.define_method("schema", method!(Fields::schema, 0))?;
                Ok(())
            }
        """)

    def native(self, text):
        (self.sources / "lib.rs").write_text(text)

    def ruby(self, body):
        (self.suite / "boundary/sample_test.rb").write_text("""
            require 'minitest'
            module Sample
              NX = Nervix::Client
              class Boundary < Minitest::Test; end
              class Cases < Boundary
                def test_current
                  %s
                end
              end
            end
        """ % body)

    def analysis(self):
        return analyze(self.root, "crates/client-ruby/src",
                       "tests/client_conformance/ruby/suite", "Nervix::Client")

    def test_constructor_and_actual_calls_reference_the_complete_surface(self):
        self.ruby("fields = NX::Fields.new; fields.schema")
        result = self.analysis()
        self.assertEqual([], result.problems)
        self.assertEqual(set(), result.surface - result.references)
        self.assertEqual({"Sample::Cases#test_current": "boundary"}, result.tests)

    def test_an_unreferenced_method_is_missing(self):
        self.ruby("NX::Fields.new")
        self.assertEqual({"Nervix::Client::Fields#schema"},
                         self.analysis().surface - self.analysis().references)

    def test_comments_strings_and_untyped_same_named_calls_do_not_count(self):
        self.ruby("""
          # NX::Fields.new.schema
          text = "NX::Fields.new.schema"
          unrelated = Object.new
          unrelated.schema
        """)
        references = self.analysis().references
        self.assertNotIn("Nervix::Client::Fields", references)
        self.assertNotIn("Nervix::Client::Fields#schema", references)

    def test_reassignment_withdraws_the_receiver_type(self):
        self.ruby("value = NX::Fields.new; value = Object.new; value.schema")
        self.assertNotIn("Nervix::Client::Fields#schema", self.analysis().references)

    def test_an_explicit_runtime_class_assertion_qualifies_a_receiver(self):
        self.ruby("value = helper; assert_instance_of NX::Fields, value; value.schema")
        self.assertIn("Nervix::Client::Fields#schema", self.analysis().references)

    def test_implicit_fixture_methods_can_be_asserted_as_native_receivers(self):
        self.ruby("assert_instance_of NX::Fields, fields; fields.schema")
        self.assertIn("Nervix::Client::Fields#schema", self.analysis().references)

    def test_an_alias_of_another_namespace_does_not_credit_native_members(self):
        self.ruby("NX = Object; fields = NX::Fields.new; fields.schema")
        result = self.analysis()
        self.assertTrue(result.problems)

    def test_block_receivers_keep_the_preceding_type(self):
        self.ruby("fields = NX::Fields.new; [1].each { fields.schema }")
        self.assertIn("Nervix::Client::Fields#schema", self.analysis().references)

    def test_registration_comments_and_test_only_files_do_not_add_members(self):
        with (self.sources / "lib.rs").open("a") as output:
            output.write('// fields.define_method("fiction", method!(Fields::fiction, 0))?;')
        (self.sources / "tests.rs").write_text('fields.define_method("test_case", method!(Fields::test_case, 0))?;')
        (self.sources / "tests").mkdir()
        (self.sources / "tests/server.rs").write_text('fields.define_method("fixture", method!(Fields::fixture, 0))?;')
        result = read_surface(self.root, "crates/client-ruby/src", "Nervix::Client")
        self.assertEqual({"Nervix", "Nervix::Client", "Nervix::Client::Fields",
                          "Nervix::Client::Fields.new", "Nervix::Client::Fields#schema"}, result.surface)

    def test_the_class_helper_and_dynamic_lookups_are_not_declarations(self):
        with (self.sources / "lib.rs").open("a") as output:
            output.write('''
              fn class(ruby: &Ruby, module: RModule, name: &str) {
                let class = module.define_class(name, ruby.class_object())?;
                class
              }
              fn lookup(module: RModule, name: &str) {
                let found = module.const_get(name)?;
              }
            ''')
        self.ruby("NX::Fields.new.schema")
        result = self.analysis()
        self.assertEqual([], result.problems)
        self.assertEqual(set(), result.surface - result.references)

    def test_enum_macro_declarations_define_their_exact_constant_set(self):
        self.native("""
          macro_rules! host_enum {
            ($name:ident { $($ruby:literal),+ }) => {
              fn register(module: RModule) {
                let values = module.define_module(stringify!($name))?;
                $(values.const_set($ruby, 1)?;)+
              }
            };
          }
          host_enum! { FailureKind from FailureKind {
            Cancelled = 7 as "CANCELLED",
            Deadline = 6 as "DEADLINE",
          } }
        """)
        result = read_surface(self.root, "crates/client-ruby/src", "Nervix::Client")
        self.assertEqual([], result.problems)
        self.assertEqual({"Nervix", "Nervix::Client", "Nervix::Client::FailureKind",
                          "Nervix::Client::FailureKind::CANCELLED",
                          "Nervix::Client::FailureKind::DEADLINE"}, result.surface)

    def test_error_array_and_inherited_attributes_have_one_owner(self):
        self.native("""
          fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
            let base = module.define_error("ClientError", ruby.exception_standard_error())?;
            for name in ["CancelledError", "DeadlineError"] {
              module.define_error(name, base)?;
            }
            base.define_attr("kind", Attr::Read)?;
            Ok(())
          }
        """)
        self.ruby("""
          failure = assert_raises(NX::CancelledError) { operation }
          failure.kind
          NX::DeadlineError
        """)
        result = self.analysis()
        self.assertEqual([], result.problems)
        self.assertIn("Nervix::Client::ClientError#kind", result.references)
        self.assertIn("Nervix::Client::DeadlineError", result.surface)

    def test_duplicate_test_names_fail_discovery(self):
        self.ruby("NX::Fields.new.schema")
        (self.suite / "boundary/duplicate_test.rb").write_text("""
          module Sample
            class Cases < Boundary
              def test_current; end
            end
          end
        """)
        self.assertTrue(any("twice" in problem for problem in self.analysis().problems))

    def test_malformed_ruby_is_a_finding(self):
        self.ruby("def unfinished")
        self.assertTrue(self.analysis().problems)

    def test_a_dynamic_registration_is_a_finding(self):
        self.native("""
          fn register(ruby: &Ruby, module: RModule) {
            let fields = crate::class(ruby, module, "Fields")?;
            fields.define_method(name, method!(Fields::schema, 0))?;
          }
        """)
        result = read_surface(self.root, "crates/client-ruby/src", "Nervix::Client")
        self.assertTrue(any("literal name" in problem for problem in result.problems))

    def test_a_dynamic_class_or_unknown_parent_is_a_finding(self):
        for declaration, expected in (
            ('let fields = crate::class(ruby, module, name)?;', 'literal name'),
            ('let fields = module.define_class(name, ruby.class_object())?;', 'literal name'),
            ('let fields = unknown.define_class("Fields", ruby.class_object())?;', 'resolved parent'),
        ):
            with self.subTest(declaration=declaration):
                self.native('fn register(ruby: &Ruby, module: RModule) {' + declaration + '}')
                result = read_surface(self.root, "crates/client-ruby/src", "Nervix::Client")
                self.assertTrue(any(expected in problem for problem in result.problems))

    def test_validation_combines_the_surface_inventory_and_guide(self):
        inventory = self.root / suites.INVENTORY
        entry = ('[[test]]\nhost = "ruby"\nidentity = "Sample::Cases#test_current"\n'
                 'tier = "boundary"\n')
        inventory.write_text(entry)
        guide = self.root / "docs/src/client-library-ruby.md"
        guide.parent.mkdir(parents=True)
        guide.write_text("# Ruby guide\n" + "\n".join(f"## {section}\n" for section in suites.GUIDE_SECTIONS))
        host = suites.HOSTS["ruby"]
        self.ruby("NX::Fields.new")
        self.assertTrue(any("Fields#schema" in problem for problem in suites.validate(self.root, [host])))
        self.ruby("NX::Fields.new.schema")
        self.assertEqual([], suites.validate(self.root, [host]))
        inventory.write_text(entry.replace('test_current', 'test_other'))
        drift = suites.validate(self.root, [host])
        self.assertTrue(any("does not register" in problem for problem in drift))
        self.assertTrue(any("which the suite lacks" in problem for problem in drift))
        inventory.write_text(entry)
        guide.write_text("## Connect\n")
        self.assertTrue(any("sections" in problem for problem in suites.validate(self.root, [host])))

    def test_missing_and_extra_inventory_entries_are_reported(self):
        self.ruby("NX::Fields.new.schema")
        result = self.analysis()
        host = suites.HOSTS["ruby"]
        problems = suites.compare_inventory(host, {"Sample::Cases#test_other": "cluster"}, result.tests)
        self.assertEqual(2, len(problems))
        self.assertTrue(any("test_current" in problem for problem in problems))
        self.assertTrue(any("test_other" in problem for problem in problems))

    def test_guide_sections_and_include_anchors_are_required(self):
        guide = self.root / "docs/src/client-library-ruby.md"
        guide.parent.mkdir(parents=True)
        headings = "\n".join(f"## {section}\n" for section in suites.GUIDE_SECTIONS)
        fence = chr(96) * 3
        guide.write_text(headings + f"\n{fence}ruby\n" +
                         "{{#include ../../tests/client_conformance/ruby/suite/boundary/sample_test.rb:example}}\n" +
                         fence + "\n")
        self.ruby("# ANCHOR: example\n NX::Fields.new.schema\n # ANCHOR_END: example")
        self.assertEqual([], suites.check_guide(self.root, suites.HOSTS["ruby"]))
        self.ruby("NX::Fields.new.schema")
        self.assertTrue(any("no anchor" in problem for problem in suites.check_guide(self.root, suites.HOSTS["ruby"])))
        guide.write_text("## Connect\n")
        self.assertTrue(any("sections" in problem for problem in suites.check_guide(self.root, suites.HOSTS["ruby"])))


class RubyReporterTests(unittest.TestCase):
    def test_results_are_complete_lines_and_follow_the_actual_teardown_verdict(self):
        reporter = Path(__file__).resolve().parents[2] / "tests/client_conformance/ruby/suite/results.rb"
        program = """
          class Passing < Minitest::Test
            def test_pass; assert true; end
          end
          class FailedTeardown < Minitest::Test
            def test_body_passes; assert true; end
            def teardown; flunk 'the teardown failed'; end
          end
          class Skipped < Minitest::Test
            def test_skip; skip 'not qualification'; end
          end
          exit(Minitest.run(['--seed', '1']) ? 0 : 1)
        """
        result = subprocess.run(["ruby", "-r", str(reporter), "-e", program],
                                capture_output=True, text=True, check=False)
        self.assertEqual(1, result.returncode, result.stderr)
        lines = [line for line in result.stdout.splitlines() if line.startswith(suites.RESULT_PREFIX)]
        self.assertEqual({
            "NERVIX-SUITE passed Passing#test_pass",
            "NERVIX-SUITE failed FailedTeardown#test_body_passes",
            "NERVIX-SUITE skipped Skipped#test_skip",
        }, set(lines))


if __name__ == "__main__":
    unittest.main()
