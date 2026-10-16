//! Exact references for current window moments and histogram placement.
//!
//! Layer: test harness.
//! - **Owns.** Bounded paired runs, independent null selection and numerical error oracles.
//! - **Depends on.** Production typed run kernels and exact floating addition partials.
//! - **Must not know.** Arrow, branch ownership, scheduling or aggregate output construction.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns bounded kernel reference properties")
)]

use meticulous::ResultExt as _;
use nervix_approx_into::ApproxInto as _;

use super::{
    RunValidity, bucket_indices, co_moments, moments,
    tests::{bit, bitmap_of, exact_partials, partials_value},
};

#[derive(Debug, bolero::TypeGenerator)]
struct MomentRun {
    #[generator(bolero::generator::produce_with::<Vec<[u64; 2]>>().len(0_usize..=128))]
    values: Vec<[u64; 2]>,
    first_bits: [u8; 32],
    second_bits: [u8; 32],
    first_offset: u8,
    second_offset: u8,
    mode: u8,
    layout: u8,
}

struct Reference {
    count: u64,
    mean: f64,
    squares: f64,
}

impl Reference {
    fn new(values: &[f64]) -> Self {
        let count = u64::try_from(values.len()).assured("at most 128 values");
        if count == 0 {
            return Self {
                count,
                mean: 0.0,
                squares: 0.0,
            };
        }
        let sum = partials_value(&exact_partials(values.iter().copied()));
        let mean = sum / count.approx_into::<f64>();
        let mut terms = Vec::new();
        for value in values {
            let delta = value - mean;
            let square = delta * delta;
            terms.push(square);
            terms.push(delta.mul_add(delta, -square));
        }
        let squares = partials_value(&exact_partials(terms));
        Self {
            count,
            mean,
            squares,
        }
    }

    fn mean_bound(&self, values: &[f64]) -> f64 {
        let magnitudes: f64 = values.iter().map(|value| value.abs()).sum();
        let n = self.count.approx_into::<f64>().max(1.0);
        // Includes compensated addition and division rounding, against the exact sum.
        4.0 * f64::EPSILON * self.mean.abs()
            + (2.0 * (n + 16.0) * f64::EPSILON).powi(2) * magnitudes / n
    }

    fn check(&self, count: u64, mean: f64, squares: f64, values: &[f64]) {
        assert_eq!(count, self.count);
        let mean_bound = self.mean_bound(values);
        let n = self.count.approx_into::<f64>().max(1.0);
        close(mean, self.mean, mean_bound);
        // A two-pass centered fold: subtraction, products, lane additions and the mean error.
        let square_bound =
            16.0 * (n + 16.0) * f64::EPSILON * self.squares + 4.0 * n * mean_bound.powi(2);
        close(squares, self.squares, square_bound);
    }
}

fn close(actual: f64, expected: f64, bound: f64) {
    assert!(
        actual.is_finite(),
        "expected finite {expected:e}, got {actual:e}"
    );
    let error = partials_value(&exact_partials([actual, -expected])).abs();
    assert!(
        error <= bound,
        "{actual:e} differs from exact reference {expected:e} by {error:e}, bound {bound:e}"
    );
}

impl MomentRun {
    fn bounded(raw: u64) -> f64 {
        let exponent = i32::try_from((raw >> 52) % 401).assured("below 401") - 200;
        let significand = (raw & ((1_u64 << 52) - 1)) | (1_u64 << 52);
        let magnitude = significand.approx_into::<f64>() * 2_f64.powi(exponent - 52);
        if raw >> 63 == 0 {
            magnitude
        } else {
            -magnitude
        }
    }

    fn check(&self) {
        let first_offset = usize::from(self.first_offset % 64);
        let second_offset = usize::from(self.second_offset % 64);
        let first_bits = bitmap_of(&self.first_bits, first_offset + self.values.len());
        let second_bits = bitmap_of(&self.second_bits, second_offset + self.values.len());
        let first_validity = RunValidity::new(Some(&first_bits), first_offset);
        let second_validity = RunValidity::new(Some(&second_bits), second_offset);
        let overflow = self.mode.is_multiple_of(4);
        let huge = match self.mode % 3 {
            0 => 1e308,
            1 => f64::MAX,
            _ => -1e308,
        };
        let mut first = Vec::new();
        let mut second = Vec::new();
        let mut kept = Vec::new();
        let mut pairs = Vec::new();
        for (index, raw) in self.values.iter().enumerate() {
            let a = if overflow {
                huge
            } else {
                Self::bounded(raw[0])
            };
            let b = Self::bounded(raw[1]);
            first.push(a);
            second.push(b);
            if bit(&first_bits, first_offset + index) {
                kept.push(a);
                if bit(&second_bits, second_offset + index) {
                    pairs.push([a, b]);
                }
            }
        }
        let actual = moments(&first, first_validity, |value| value);
        let paired = co_moments(
            &first,
            first_validity,
            |value| value,
            &second,
            second_validity,
            |value| value,
        );
        let a: Vec<_> = pairs.iter().map(|pair| pair[0]).collect();
        let b: Vec<_> = pairs.iter().map(|pair| pair[1]).collect();
        let second_reference = Reference::new(&b);
        second_reference.check(paired.count, paired.second_mean, paired.second_squares, &b);
        if overflow {
            assert_eq!(
                actual.count,
                u64::try_from(kept.len()).assured("bounded run")
            );
            assert_eq!(
                paired.count,
                u64::try_from(pairs.len()).assured("bounded run")
            );
            let expected = if kept.is_empty() { 0.0 } else { huge };
            assert_eq!(actual.mean, expected);
            let expected = if pairs.is_empty() { 0.0 } else { huge };
            assert_eq!(paired.first_mean, expected);
            assert_eq!(actual.squares, 0.0);
            assert_eq!(paired.first_squares, 0.0);
            assert_eq!(paired.cross_products, 0.0);
        } else {
            Reference::new(&kept).check(actual.count, actual.mean, actual.squares, &kept);
            let first_reference = Reference::new(&a);
            first_reference.check(paired.count, paired.first_mean, paired.first_squares, &a);
            let mut products = Vec::new();
            let mut magnitudes = 0.0;
            for pair in &pairs {
                let x = pair[0] - first_reference.mean;
                let y = pair[1] - second_reference.mean;
                let product = x * y;
                products.push(product);
                products.push(x.mul_add(y, -product));
                magnitudes += product.abs();
            }
            let expected = partials_value(&exact_partials(products));
            let n = paired.count.approx_into::<f64>();
            let bound = 32.0 * (n + 16.0) * f64::EPSILON * magnitudes
                + 4.0 * n * first_reference.mean_bound(&a) * second_reference.mean_bound(&b);
            close(paired.cross_products, expected, bound);
        }
        self.check_buckets();
    }

    fn check_buckets(&self) {
        let buckets = usize::from(self.layout % 16) + 1;
        let min = -f64::from(self.layout);
        let width = 2_f64.powi(i32::from(self.layout % 9) - 4);
        let max = min + buckets.approx_into::<f64>() * width;
        let mut values = vec![min - width, min, max, max + width];
        let mut expected = vec![Some(0), Some(0), Some(buckets - 1), Some(buckets - 1)];
        for index in 1..buckets {
            let edge = min + index.approx_into::<f64>() * width;
            values.extend([edge - width / 4.0, edge, edge + width / 4.0]);
            expected.extend([Some(index - 1), Some(index), Some(index)]);
        }
        let offset = usize::from(self.first_offset % 64);
        let bits = bitmap_of(&self.first_bits, offset + values.len());
        for (index, bucket) in expected.iter_mut().enumerate() {
            if !bit(&bits, offset + index) {
                *bucket = None;
            }
        }
        assert_eq!(
            bucket_indices(
                &values,
                RunValidity::new(Some(&bits), offset),
                |value| value,
                min,
                max,
                width,
                buckets
            ),
            expected
        );
    }
}

#[test]
fn bolero_window_moments_and_buckets_match_exact_references() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .with_type::<MomentRun>()
        .for_each(MomentRun::check);
}

#[test]
fn identical_overflow_scale_values_have_zero_centered_moments() {
    MomentRun {
        values: vec![[0; 2]; 16],
        first_bits: [255; 32],
        second_bits: [255; 32],
        first_offset: 3,
        second_offset: 7,
        mode: 0,
        layout: 4,
    }
    .check();
}
