//! Framing a graph in the console's viewport: which zoom and pan show a region of the canvas.
//!
//! Every graph the console draws shares these rules, so the fit control, search framing and edge
//! focus behave the same on the live execution graph and on a transaction's impact.

use crate::graph::layout::Rect;

/// A region of a graph's canvas, in canvas pixels.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GraphBounds {
    pub left: f64,
    pub top: f64,
    pub right: f64,
    pub bottom: f64,
}

impl GraphBounds {
    pub fn from_point(x: i32, y: i32) -> Self {
        let x = f64::from(x);
        let y = f64::from(y);
        Self {
            left: x,
            top: y,
            right: x,
            bottom: y,
        }
    }

    pub fn from_rect(rect: Rect) -> Self {
        Self {
            left: f64::from(rect.x),
            top: f64::from(rect.y),
            right: f64::from(rect.right()),
            bottom: f64::from(rect.bottom()),
        }
    }

    /// The whole of a canvas of the given size.
    pub fn canvas(width: i32, height: i32) -> Self {
        Self::from_rect(Rect {
            x: 0,
            y: 0,
            width,
            height,
        })
    }

    pub fn include_point(&mut self, x: f64, y: f64) {
        self.left = self.left.min(x);
        self.top = self.top.min(y);
        self.right = self.right.max(x);
        self.bottom = self.bottom.max(y);
    }

    pub fn include_bounds(&mut self, bounds: Self) {
        self.include_point(bounds.left, bounds.top);
        self.include_point(bounds.right, bounds.bottom);
    }

    /// Grow `bounds` to hold `next`, starting from nothing when there is nothing yet.
    pub fn include(bounds: &mut Option<Self>, next: Self) {
        match bounds {
            Some(bounds) => bounds.include_bounds(next),
            None => *bounds = Some(next),
        }
    }

    pub fn width(self) -> f64 {
        (self.right - self.left).max(1.0)
    }

    pub fn height(self) -> f64 {
        (self.bottom - self.top).max(1.0)
    }

    pub fn center(self) -> (f64, f64) {
        (
            (self.left + self.right) / 2.0,
            (self.top + self.bottom) / 2.0,
        )
    }
}

/// A width and a height in pixels: the stage a graph is shown on, or the canvas it is drawn on.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Extent {
    pub width: f64,
    pub height: f64,
}

/// How a graph's canvas sits on the stage: scaled by `zoom` about the canvas centre, then moved by
/// the pan.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Viewport {
    pub zoom: f64,
    pub pan_x: f64,
    pub pan_y: f64,
}

impl Viewport {
    /// The zoom range the stage allows, shared by the buttons, the wheel and the fit control.
    pub const MIN_ZOOM: f64 = 0.25;
    pub const MAX_ZOOM: f64 = 3.0;
    /// One press of a zoom button.
    pub const ZOOM_STEP: f64 = 0.1;
    /// Fitting never enlarges: a small graph is shown at its natural size, centred.
    pub const FIT_MAX_ZOOM: f64 = 1.0;
    /// Clearance kept around the graph when framing it.
    const FIT_PADDING: f64 = 48.0;

    /// The zoom and pan that centre `bounds` of a canvas on the stage, as large as fits without
    /// exceeding `max_zoom`. A stage that has not been laid out yet frames nothing.
    pub fn framing(
        stage: Extent,
        canvas: Extent,
        bounds: GraphBounds,
        max_zoom: f64,
    ) -> Option<Self> {
        if stage.width <= 1.0 || stage.height <= 1.0 {
            return None;
        }
        let available_width = (stage.width - Self::FIT_PADDING * 2.0).max(stage.width * 0.4);
        let available_height = (stage.height - Self::FIT_PADDING * 2.0).max(stage.height * 0.4);
        let zoom = (available_width / bounds.width())
            .min(available_height / bounds.height())
            .clamp(Self::MIN_ZOOM, max_zoom);
        let (center_x, center_y) = bounds.center();
        // The canvas starts centred on the stage and scales about its own centre.
        let base_x = (stage.width - canvas.width) / 2.0;
        let base_y = (stage.height - canvas.height) / 2.0;
        let origin_x = canvas.width / 2.0;
        let origin_y = canvas.height / 2.0;
        Some(Self {
            zoom,
            pan_x: stage.width / 2.0 - base_x - zoom * center_x - (1.0 - zoom) * origin_x,
            pan_y: stage.height / 2.0 - base_y - zoom * center_y - (1.0 - zoom) * origin_y,
        })
    }

    /// Where a canvas point lands on the stage under this viewport.
    pub fn stage_point(self, stage: Extent, canvas: Extent, x: f64, y: f64) -> (f64, f64) {
        let base_x = (stage.width - canvas.width) / 2.0;
        let base_y = (stage.height - canvas.height) / 2.0;
        let origin_x = canvas.width / 2.0;
        let origin_y = canvas.height / 2.0;
        (
            base_x + self.pan_x + origin_x + self.zoom * (x - origin_x),
            base_y + self.pan_y + origin_y + self.zoom * (y - origin_y),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STAGE: Extent = Extent {
        width: 1200.0,
        height: 800.0,
    };

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-6,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn framing_centres_the_bounds_on_the_stage() {
        let canvas = Extent {
            width: 3000.0,
            height: 2000.0,
        };
        let bounds = GraphBounds::from_rect(Rect {
            x: 2000,
            y: 1200,
            width: 400,
            height: 200,
        });
        let viewport = Viewport::framing(STAGE, canvas, bounds, Viewport::MAX_ZOOM)
            .expect("a laid-out stage frames the bounds");
        let (center_x, center_y) = bounds.center();
        let (stage_x, stage_y) = viewport.stage_point(STAGE, canvas, center_x, center_y);
        assert_close(stage_x, STAGE.width / 2.0);
        assert_close(stage_y, STAGE.height / 2.0);
    }

    #[test]
    fn fitting_never_enlarges_and_never_shrinks_past_the_zoom_range() {
        let small = Extent {
            width: 300.0,
            height: 200.0,
        };
        let fit = Viewport::framing(
            STAGE,
            small,
            GraphBounds::canvas(300, 200),
            Viewport::FIT_MAX_ZOOM,
        )
        .expect("a laid-out stage frames the canvas");
        assert_close(fit.zoom, Viewport::FIT_MAX_ZOOM);

        let huge = Extent {
            width: 100_000.0,
            height: 100_000.0,
        };
        let fit = Viewport::framing(
            STAGE,
            huge,
            GraphBounds::canvas(100_000, 100_000),
            Viewport::FIT_MAX_ZOOM,
        )
        .expect("a laid-out stage frames the canvas");
        assert_close(fit.zoom, Viewport::MIN_ZOOM);
    }

    #[test]
    fn a_stage_that_is_not_laid_out_frames_nothing() {
        let collapsed = Extent {
            width: 0.0,
            height: 800.0,
        };
        assert_eq!(
            Viewport::framing(
                collapsed,
                STAGE,
                GraphBounds::canvas(100, 100),
                Viewport::FIT_MAX_ZOOM
            ),
            None
        );
    }

    #[test]
    fn bounds_grow_to_hold_everything_included() {
        let mut bounds = None;
        GraphBounds::include(&mut bounds, GraphBounds::from_point(10, 20));
        GraphBounds::include(
            &mut bounds,
            GraphBounds::from_rect(Rect {
                x: -5,
                y: 30,
                width: 10,
                height: 10,
            }),
        );
        let bounds = bounds.expect("two regions were included");
        assert_close(bounds.left, -5.0);
        assert_close(bounds.top, 20.0);
        assert_close(bounds.right, 10.0);
        assert_close(bounds.bottom, 40.0);
    }
}
