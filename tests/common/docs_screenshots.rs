//! The web console image set published with the Client Tools documentation.
//!
//! Regenerating a screenshot must produce an image that is comparable with the one already
//! committed, so the browser geometry, the animation state of the rendered graph, and the capture
//! settings all belong to one owner rather than to individual scenario steps.

use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use playwright_rs::{
    Animations, BrowserContextOptions, Caret, Page, Scale, ScreenshotOptions, ScreenshotType,
    Viewport,
};

/// Freezes every SVG timeline in the console at a fixed offset. `Animations::Disabled` covers CSS
/// animations and transitions but not SMIL, and the graph draws its traffic pulses with
/// `<animateMotion>`, so an unfrozen capture places each pulse wherever the clock happened to be.
const FREEZE_SVG_TIMELINES: &str = "\
    (offset) => {
        for (const svg of document.querySelectorAll('svg')) {
            if (typeof svg.pauseAnimations === 'function') {
                svg.pauseAnimations();
                svg.setCurrentTime(offset);
            }
        }
    }";

/// Reduces the rendered graph to a comparable string. Polled until two consecutive readings match
/// so a capture never lands mid-relayout or during the chart's entry animation.
const READ_GRAPH_LAYOUT_SIGNATURE: &str = "\
    () => {
        const chart = document.querySelector('#execution-graph-chart');
        const renders = chart ? chart.getAttribute('data-render-count') : 'none';
        const items = Array.from(document.querySelectorAll('.graph-hit-layer > *')).map((item) => {
            const box = item.getBoundingClientRect();
            const label = item.getAttribute('data-label') ?? '';
            return `${label}@${Math.round(box.x)},${Math.round(box.y)}`;
        });
        return `renders=${renders}|${items.join('|')}`;
    }";

pub(crate) struct DocumentationScreenshots {
    directory: PathBuf,
}

impl DocumentationScreenshots {
    /// Where the captured images are written. `just docs-screenshots` points this at
    /// `docs/src/images`; every other run writes under the ignored build directory so an ordinary
    /// suite run never dirties the working tree.
    const DIRECTORY_ENV: &'static str = "NERVIX_DOCS_SCREENSHOT_DIR";
    const DEFAULT_DIRECTORY: &'static str = "target/docs-screenshots";
    /// Wide enough that the sidebar plus a zoomed-out execution graph fit without the graph being
    /// clipped, and the aspect ratio the published pages are laid out for.
    const VIEWPORT: Viewport = Viewport {
        width: 1920,
        height: 1200,
    };
    /// Captured at device pixels so the images stay legible when a reader opens one full size.
    const DEVICE_SCALE_FACTOR: f64 = 2.0;
    /// Half of the graph's 2.7s pulse period: every edge shares that duration, so freezing here
    /// puts every pulse at the midpoint of its own path.
    const PULSE_FREEZE_OFFSET_SECONDS: f64 = 1.35;
    /// The sidebar cluster counters are placeholders wired to constants rather than to cluster
    /// state; publishing them would document numbers that are not true of any deployment.
    const SUPPRESSED_PLACEHOLDERS: &'static str = ".cluster-block { display: none; }";
    const SETTLE_TIMEOUT: Duration = Duration::from_secs(10);
    const SETTLE_INTERVAL: Duration = Duration::from_millis(150);

    pub(crate) fn from_environment() -> Result<Self, String> {
        let directory = PathBuf::from(
            std::env::var(Self::DIRECTORY_ENV)
                .unwrap_or_else(|_| Self::DEFAULT_DIRECTORY.to_string()),
        );
        std::fs::create_dir_all(&directory).map_err(|error| {
            format!(
                "documentation screenshot directory '{}' could not be created: {error}",
                directory.display()
            )
        })?;
        Ok(Self { directory })
    }

    /// Browser geometry the published images are captured with. Only the documentation flow opens
    /// a context with these; other console scenarios keep Playwright's defaults so their geometry
    /// assertions stay anchored to the viewport they were written against.
    pub(crate) fn browser_context_options() -> BrowserContextOptions {
        BrowserContextOptions::builder()
            .viewport(Self::VIEWPORT)
            .device_scale_factor(Self::DEVICE_SCALE_FACTOR)
            .build()
    }

    pub(crate) async fn capture_page(&self, page: &Page, name: &str) -> Result<(), String> {
        self.settle(page).await?;
        let bytes = page
            .screenshot(Self::capture_options())
            .await
            .map_err(|error| format!("documentation screenshot '{name}' failed: {error}"))?;
        self.write(name, &bytes)
    }

    pub(crate) async fn capture_selector(
        &self,
        page: &Page,
        selector: &str,
        name: &str,
    ) -> Result<(), String> {
        self.settle(page).await?;
        let bytes = page
            .locator(selector)
            .screenshot(Self::capture_options())
            .await
            .map_err(|error| {
                format!("documentation screenshot '{name}' of '{selector}' failed: {error}")
            })?;
        self.write(name, &bytes)
    }

    /// Pixel dimensions of a captured image, read back from the PNG header.
    pub(crate) fn dimensions(&self, name: &str) -> Result<(u32, u32), String> {
        let path = self.directory.join(name);
        let bytes = std::fs::read(&path)
            .map_err(|error| format!("screenshot '{}' is unreadable: {error}", path.display()))?;
        const SIGNATURE: [u8; 8] = [0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
        const IHDR_END: usize = 24;
        if bytes.len() < IHDR_END || bytes[..SIGNATURE.len()] != SIGNATURE {
            return Err(format!(
                "screenshot '{}' is not a PNG image ({} bytes)",
                path.display(),
                bytes.len()
            ));
        }
        let dimension = |offset: usize| {
            u32::from_be_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ])
        };
        Ok((dimension(16), dimension(20)))
    }

    fn capture_options() -> ScreenshotOptions {
        ScreenshotOptions::builder()
            .screenshot_type(ScreenshotType::Png)
            .animations(Animations::Disabled)
            .caret(Caret::Hide)
            // Without this the capture is downsampled back to CSS pixels and the device scale
            // factor above buys nothing.
            .scale(Scale::Device)
            .style(Self::SUPPRESSED_PLACEHOLDERS)
            .build()
    }

    /// Freezes the graph's SVG timelines and waits for its layout to stop moving.
    async fn settle(&self, page: &Page) -> Result<(), String> {
        page.evaluate::<f64, ()>(
            FREEZE_SVG_TIMELINES,
            Some(&Self::PULSE_FREEZE_OFFSET_SECONDS),
        )
        .await
        .map_err(|error| format!("graph animations could not be frozen: {error}"))?;

        let deadline = Instant::now() + Self::SETTLE_TIMEOUT;
        let mut previous: Option<String> = None;
        loop {
            tokio::task::consume_budget().await;
            let signature = page
                .evaluate::<(), String>(READ_GRAPH_LAYOUT_SIGNATURE, None)
                .await
                .map_err(|error| format!("graph layout signature is unreadable: {error}"))?;
            if previous.as_ref() == Some(&signature) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "graph layout did not settle within {:?}. last signature: {signature}",
                    Self::SETTLE_TIMEOUT
                ));
            }
            previous = Some(signature);
            tokio::time::sleep(Self::SETTLE_INTERVAL).await;
        }
    }

    fn write(&self, name: &str, bytes: &[u8]) -> Result<(), String> {
        let path = self.directory.join(name);
        std::fs::write(&path, bytes).map_err(|error| {
            format!(
                "screenshot '{}' could not be written: {error}",
                path.display()
            )
        })
    }
}
