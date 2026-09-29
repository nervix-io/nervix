//! Streams over the channels of [`crate::sync`], selected for the build's execution mode.
//!
//! A stream wrapper adapts a channel endpoint, so it comes from the same backend as the channel: a
//! Shuttle build's receiver is wrapped by Shuttle's wrapper. The combinators and constructors come
//! with it, so a stream and the channel it reads cannot end up on different backends.

#[cfg(feature = "shuttle")]
pub use shuttle_tokio_stream::{Stream, StreamExt, iter, pending, wrappers};
#[cfg(not(feature = "shuttle"))]
pub use tokio_stream::{Stream, StreamExt, iter, pending, wrappers};
