//! Exploration of the production keeper of a command pool's minimum against connections closing
//! while it reads its pool.
//!
//! Layer: test harness.
//! - **Owns.** A scripted pool whose connections the checks close and the keeper restores, and the
//!   invariant that no closure leaves the pool below its minimum.
//! - **Depends on.** The production keeper, modeled tasks and channels, and the canonical Shuttle
//!   runner.
//! - **Must not know.** Sockets, wall time, `bb8`, or a Redis server.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns pool minimum interleaving evidence")
)]

use std::num::NonZeroU32;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_model_harness::shuttle::check_random_and_pct;
use nervix_primitives::{
    sync::{Arc, watch},
    task::{spawn, yield_now},
};

use super::*;

/// A pool of idle connections. A check closes them and the keeper restores them, and every change
/// is published for the check to wait on.
#[derive(Clone)]
struct ScriptedPool {
    established: Arc<watch::Sender<EstablishedConnections>>,
}

impl ScriptedPool {
    fn holding(total: u32) -> Self {
        let (established, _) = watch::channel(EstablishedConnections { total, idle: total });
        Self {
            established: Arc::new(established),
        }
    }

    /// Close one idle connection, as the pool does when it retires one.
    fn close_one(&self) {
        self.established.send_modify(|established| {
            established.total = established
                .total
                .checked_sub(1)
                .assured("a check closes only connections the pool holds");
            established.idle = established
                .idle
                .checked_sub(1)
                .assured("the scripted pool's connections are all idle");
        });
    }

    /// Wait until the pool holds `total` connections again.
    async fn holds(&self, total: u32) {
        let mut established = self.established.subscribe();
        established
            .wait_for(|established| established.total == total)
            .await
            .assured("the check holds the pool's sender for as long as it waits");
    }
}

impl KeptPool for ScriptedPool {
    fn established(&self) -> EstablishedConnections {
        *self.established.borrow()
    }

    async fn restore(&self, restoration: Restoration) -> Result<(), Report<PoolMinimumError>> {
        // A scheduling point inside the restoration, as the checkouts a real one awaits.
        yield_now().await;
        // Every checkout took an idle connection or one the pool opened for it, and all of them
        // returned idle.
        self.established.send_modify(|established| {
            let total = established.total.max(restoration.checkouts.get());
            *established = EstablishedConnections { total, idle: total };
        });
        Ok(())
    }
}

/// Keep a pool of `minimum` connections while `closers` tasks each close one of them, and wait
/// until the pool holds its minimum again once every closer has finished.
fn keep_while_connections_close(minimum: u32, closers: u32) {
    shuttle::future::block_on(async move {
        let minimum = NonZeroU32::new(minimum).assured("the checks keep a positive minimum");
        let pool = ScriptedPool::holding(minimum.get());
        let closures = ConnectionClosures::default();
        let keeper = PoolMinimum::keep(pool.clone(), minimum, closures.clone());
        let mut closing = Vec::new();
        for _ in 0..closers {
            let pool = pool.clone();
            let closures = closures.clone();
            closing.push(spawn(async move {
                pool.close_one();
                closures.closed();
            }));
        }
        for closer in closing {
            closer
                .await
                .assured("a panicking model task fails its execution");
        }
        // A closure the keeper slept through leaves the pool short, and this wait never ends.
        pool.holds(minimum.get()).await;
        drop(keeper);
    });
}

#[test]
fn shuttle_a_connection_closing_while_the_keeper_reads_its_pool_is_restored() {
    check_random_and_pct(|| keep_while_connections_close(2, 1));
}

#[test]
fn shuttle_connections_closing_together_are_all_restored() {
    check_random_and_pct(|| keep_while_connections_close(3, 2));
}
