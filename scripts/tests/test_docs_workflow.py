from __future__ import annotations

import tomllib
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from scripts.build_book import MDBOOK_VERSION
from scripts.publish_docs_alias import update_alias


class DocsWorkflowTests(unittest.TestCase):
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
        self.assertIn("--pdf-engine=xelatex", justfile)
        self.assertIn(
            "--include-before-body=docs/theme/nervix-pdf-title.tex",
            justfile,
        )
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
            r"\includegraphics[width=1.2in]{docs/theme/nervix-pdf-mark.pdf}",
            title_page,
        )
        self.assertTrue(Path("docs/theme/nervix-pdf-mark.pdf").is_file())

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
