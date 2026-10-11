#![cfg_attr(nervix_lint, nervix::layer(data_plane, reason="typed execution interface"))]
pub fn forward<T: Iterator>(value: <T as Iterator>::Item) -> <T as Iterator>::Item { value }
