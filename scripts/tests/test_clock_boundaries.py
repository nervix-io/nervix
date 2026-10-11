"""Clock-source scope remains precise when test modules declare source contracts."""

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from scripts.check_clock_boundaries import product_source


class ProductClockSourceTests(unittest.TestCase):
    def source(self, text: str) -> str:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "source.rs"
            path.write_text(text)
            return product_source(path)

    def test_annotated_fixtures_keep_product_calls_and_line_coordinates(self) -> None:
        text = '''fn first() { Timestamp::now(); }
#[cfg(test)]
#[cfg_attr(nervix_lint, nervix::layer(harness, reason="clock fixtures"))]
mod tests { fn fixture() { Timestamp::now(); } }
fn last() { Timestamp::now(); }
'''
        source = self.source(text)
        self.assertEqual(source.count("Timestamp::now()"), 2)
        self.assertEqual(source.splitlines()[0], text.splitlines()[0])
        self.assertEqual(source.splitlines()[4], text.splitlines()[4])

    def test_comments_and_literals_cannot_change_product_scope(self) -> None:
        text = '''const TEXT: &str = r##"#[cfg(test)] mod fixtures {}"##;
// #[cfg(test)] mod fixtures {}
#[cfg_attr(nervix_lint, nervix::layer(harness, reason="reviewed boundary"))]
mod owner { fn product() { Timestamp::now(); } }
'''
        self.assertEqual(self.source(text), text)

    def test_other_cfg_branches_keep_product_clock_calls(self) -> None:
        text = '''#[cfg(feature="testing")]
mod injection { fn product() { Timestamp::now(); } }
'''
        self.assertEqual(self.source(text), text)
