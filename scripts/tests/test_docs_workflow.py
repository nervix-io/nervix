from __future__ import annotations

import re
import tomllib
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from scripts.build_book import MDBOOK_VERSION, SCREENSHOTS_DIR
from scripts.publish_docs_alias import update_alias


class DocsWorkflowTests(unittest.TestCase):
    def test_nspl_agent_skill_is_a_top_level_book_section(self) -> None:
        summary = Path("docs/src/SUMMARY.md").read_text()

        self.assertIn("- [NSPL Agent Skill](./nspl-agent-skill.md)", summary.splitlines())
        self.assertNotIn(
            "  - [NSPL Agent Skill](./nspl-agent-skill.md)",
            summary.splitlines(),
        )

    def test_client_tools_is_a_top_level_book_section(self) -> None:
        summary = Path("docs/src/SUMMARY.md").read_text().splitlines()

        self.assertIn("- [Client Tools](./client-tools.md)", summary)
        self.assertIn("  - [Command Line Client](./client-tools-cli.md)", summary)
        self.assertIn("  - [Web Console](./client-tools-web-console.md)", summary)

    @staticmethod
    def captured_screenshots() -> set[str]:
        capture = Path("scripts/console-screenshots/capture.mjs").read_text()
        return set(re.findall(r"\"(console-[a-z-]+\.png)\"", capture))

    @staticmethod
    def referenced_screenshots() -> dict[str, str]:
        referenced = {}
        for page in sorted(Path("docs/src").glob("*.md")):
            for target in re.findall(r"!\[[^]]*]\(images/([^)]+)\)", page.read_text()):
                referenced.setdefault(target, page.name)
        return referenced

    def test_every_referenced_screenshot_is_captured_by_the_scenario(self) -> None:
        # The images are build output, so a chapter can only reference one the
        # capture scenario actually produces.
        captured = self.captured_screenshots()
        referenced = self.referenced_screenshots()

        self.assertTrue(referenced, "no documentation page references a screenshot")
        for name, page in sorted(referenced.items()):
            self.assertIn(name, captured, f"{page} references uncaptured screenshot {name}")

    def test_no_captured_screenshot_is_orphaned(self) -> None:
        referenced = self.referenced_screenshots()

        for name in sorted(self.captured_screenshots()):
            self.assertIn(name, referenced, f"{name} is captured but no chapter shows it")

    def test_screenshot_output_directory_matches_the_capture_recipe(self) -> None:
        # build_book.py stages the images from wherever the recipe told the tool
        # to write them, so both sides must resolve the same directory.
        justfile = Path("justfile").read_text()

        self.assertIn('--output "{{ cargo_target_dir }}/docs-screenshots"', justfile)
        self.assertEqual(SCREENSHOTS_DIR, Path("target/docs-screenshots").resolve())

    def test_screenshots_are_build_output_not_repository_content(self) -> None:
        self.assertFalse(
            Path("docs/src/images").exists(),
            "screenshots are captured into target/ by `just book`, not committed",
        )

    def test_console_screenshots_are_captured_by_a_standalone_tool(self) -> None:
        # Documentation artifacts are produced by a build tool driving the real
        # binaries, not by running a test.
        justfile = Path("justfile").read_text()

        self.assertIn("node scripts/console-screenshots/capture.mjs", justfile)
        self.assertNotIn("docs_screenshots.feature", justfile)
        self.assertFalse(Path("tests/features/web-console/docs_screenshots.feature").exists())

    def test_capture_tool_drives_the_shipped_binaries(self) -> None:
        capture = Path("scripts/console-screenshots/capture.mjs").read_text()

        self.assertIn("NERVIX_WEB_CONSOLE_LISTEN_ADDR", capture)
        self.assertIn("/readyz", capture)
        self.assertNotIn("cargo test", capture)

    def test_building_the_book_recaptures_the_console_screenshots(self) -> None:
        # A published book must never show an older console than it documents.
        justfile = Path("justfile").read_text()

        self.assertIn('book version="": test-docs docs-screenshots', justfile)

    def test_book_job_can_build_the_console_and_the_binaries(self) -> None:
        # The book build embeds the console into nervix-server and renders the
        # reference chapter from nervix-cli, so it needs the workspace toolchain.
        workflow = Path(".github/workflows/docker-build.yaml").read_text()
        _, build_book = workflow.split("\n  build-book:", maxsplit=1)

        self.assertIn("wasm32-unknown-unknown", build_book)
        self.assertIn("trunk", build_book)
        self.assertIn("protobuf-compiler", build_book)

    def test_cli_reference_is_a_generated_chapter_under_the_command_line_client(self) -> None:
        # The file is rendered into the staged source at build time, so it is
        # listed in SUMMARY.md but deliberately absent from the repository.
        summary = Path("docs/src/SUMMARY.md").read_text().splitlines()

        self.assertIn("    - [nervix-cli Reference](./nervix-cli-reference.md)", summary)
        self.assertFalse(Path("docs/src/nervix-cli-reference.md").exists())

    def test_cli_page_points_at_the_generated_reference(self) -> None:
        page = Path("docs/src/client-tools-cli.md").read_text()

        self.assertIn("(nervix-cli-reference.md)", page)

    def test_book_job_installs_locked_python_dependencies_with_uv(self) -> None:
        workflow = Path(".github/workflows/docker-build.yaml").read_text()
        _, build_book = workflow.split("\n  build-book:", maxsplit=1)

        self.assertNotIn("rclone", build_book)
        self.assertIn("astral-sh/setup-uv@", build_book)
        self.assertIn("uv sync --locked", build_book)

    def test_uv_project_declares_boto3_runtime_dependency(self) -> None:
        project = tomllib.loads(Path("pyproject.toml").read_text())

        self.assertIn("boto3>=1.40,<2", project["project"]["dependencies"])
        self.assertFalse(project["tool"]["uv"]["package"])

    def test_docs_commands_run_in_the_locked_uv_environment(self) -> None:
        justfile = Path("justfile").read_text()

        self.assertIn(
            "uv run --locked python scripts/upload_book_to_r2.py",
            justfile,
        )
        self.assertIn(
            "uv run --locked python scripts/publish_docs_alias.py",
            justfile,
        )

    def test_docs_ci_builds_and_publishes_nervix_pdf(self) -> None:
        workflow = Path(".github/workflows/docker-build.yaml").read_text()
        _, build_book = workflow.split("\n  build-book:", maxsplit=1)
        justfile = Path("justfile").read_text()

        self.assertIn("Install Pandoc", build_book)
        self.assertIn("pdf_url=", build_book)
        self.assertIn("steps.docs.outputs.pdf_url", build_book)
        self.assertIn("fonts-noto-cjk", build_book)
        self.assertIn("fonts-noto-core", build_book)
        self.assertIn("lmodern", build_book)
        self.assertIn("texlive-fonts-recommended", build_book)
        self.assertIn("texlive-latex-extra", build_book)
        self.assertIn("texlive-lang-chinese", build_book)
        self.assertIn("texlive-lang-cjk", build_book)
        self.assertIn("texlive-xetex", build_book)
        self.assertIn("just book-pdf", justfile)
        self.assertIn('output_path="docs/book/nervix.pdf"', justfile)
        self.assertIn("python3 scripts/render_pdf_title.py", justfile)
        self.assertIn('--version "{{ version }}"', justfile)
        self.assertIn('--include-before-body="${tmp_title}"', justfile)
        self.assertIn("--pdf-engine=xelatex", justfile)
        self.assertIn(
            "--include-in-header=docs/theme/nervix-pdf-header.tex",
            justfile,
        )
        self.assertIn("--variable=graphics", justfile)
        self.assertIn("--toc \\", justfile)
        self.assertIn("--toc-depth=2", justfile)
        self.assertIn("--variable=geometry:margin=0.8in", justfile)
        self.assertIn("--variable=linestretch:1.08", justfile)

    def test_ci_installs_the_mdbook_version_pinned_in_the_build_script(self) -> None:
        workflow = Path(".github/workflows/docker-build.yaml").read_text()
        _, build_book = workflow.split("\n  build-book:", maxsplit=1)

        # The pin lives in build_book.py alone; CI resolves it instead of
        # repeating a literal that could drift from local builds.
        self.assertIn("from scripts.build_book import MDBOOK_VERSION", build_book)
        self.assertIn("mdbook-version: ${{ steps.mdbook.outputs.version }}", build_book)
        self.assertNotIn(f'mdbook-version: "{MDBOOK_VERSION}"', build_book)

    def test_docs_ci_handles_pushes_without_pull_request_context(self) -> None:
        workflow = Path(".github/workflows/docker-build.yaml").read_text()
        _, build_book = workflow.split("\n  build-book:", maxsplit=1)
        docs_target, comment_step = build_book.split(
            "\n      - name: Comment docs link on PR",
            maxsplit=1,
        )

        self.assertIn(
            'if [[ "${{ github.event_name }}" == "pull_request" ]]; then',
            docs_target,
        )
        self.assertIn(
            'docs_target="main-${{ github.sha }}"',
            docs_target,
        )
        self.assertIn(
            'just publish-book "${{ steps.docs.outputs.target }}"',
            docs_target,
        )
        self.assertTrue(
            comment_step.lstrip().startswith(
                "if: github.event_name == 'pull_request'\n"
            )
        )

    def test_pdf_title_page_contains_canonical_product_metadata(self) -> None:
        title_page = Path("docs/theme/nervix-pdf-title.tex").read_text()

        self.assertIn("Nervix", title_page)
        self.assertIn("https://docs.nervix.io/", title_page)
        self.assertIn("https://github.com/nervix-io/nervix", title_page)
        self.assertIn("Copyright 2026 Emergentix, Inc.", title_page)
        self.assertIn("FCL-1.0-ALv2", title_page)
        self.assertIn(
            r"Documentation version: \texttt{@@NERVIX_DOCUMENTATION_VERSION@@}",
            title_page,
        )
        self.assertIn(r"\hyperref[license]", title_page)
        self.assertNotIn(
            "https://github.com/nervix-io/nervix/blob/main/LICENSE.md",
            title_page,
        )
        self.assertIn(
            r"\includegraphics[width=1.2in]{docs/theme/nervix-pdf-mark.pdf}",
            title_page,
        )
        self.assertTrue(Path("docs/theme/nervix-pdf-mark.pdf").is_file())

    def test_pdf_chapter_uses_local_license_link(self) -> None:
        agent_skill = Path("docs/src/nspl-agent-skill.md").read_text()

        self.assertIn("[FCL-1.0-ALv2 license](license.md)", agent_skill)
        self.assertNotIn(
            "https://github.com/nervix-io/nervix/blob/main/LICENSE.md",
            agent_skill,
        )

    def test_pdf_code_blocks_wrap_at_print_width(self) -> None:
        pdf_header = Path("docs/theme/nervix-pdf-header.tex").read_text()

        self.assertIn(r"\usepackage{fvextra}", pdf_header)
        self.assertIn(r"\RecustomVerbatimEnvironment{Highlighting}", pdf_header)
        self.assertIn("breaklines=true", pdf_header)
        self.assertIn("breakanywhere=true", pdf_header)
        self.assertIn("breaknonspaceingroup=true", pdf_header)
        self.assertIn("NervixCodeBackground", pdf_header)
        self.assertIn("NervixCodeBorder", pdf_header)
        self.assertIn(r"]{Shaded}", pdf_header)

    def test_pdf_blockquotes_are_visually_distinct(self) -> None:
        pdf_header = Path("docs/theme/nervix-pdf-header.tex").read_text()

        self.assertIn(r"\usepackage{mdframed}", pdf_header)
        self.assertIn("NervixQuoteBackground", pdf_header)
        self.assertIn("NervixQuoteRule", pdf_header)
        self.assertIn(r"\renewmdenv", pdf_header)
        self.assertIn("leftmargin=1.5em", pdf_header)
        self.assertIn(r"font=\itshape", pdf_header)

    def test_docs_publisher_does_not_shell_out_for_storage_operations(self) -> None:
        publisher = Path("scripts/publish_docs_alias.py").read_text()

        self.assertNotIn("import subprocess", publisher)
        self.assertNotIn("wrangler", publisher)
        self.assertNotIn("NamedTemporaryFile", publisher)

    def test_alias_is_written_through_the_direct_s3_client(self) -> None:
        client = Mock()
        with patch(
            "scripts.publish_docs_alias.upload_book_to_r2.put_object"
        ) as put_object:
            update_alias(
                client=client,
                bucket="nervix-docs",
                alias="snapshot",
                target="pr-43-abc/",
            )

        put_object.assert_called_once_with(
            client=client,
            bucket="nervix-docs",
            object_key="meta/snapshot.txt",
            payload=b"pr-43-abc\n",
            content_type="text/plain; charset=utf-8",
        )


if __name__ == "__main__":
    unittest.main()
