//! Compiler fixture outside the product layer order.
//! Owns: paired examples distinguished by their resolved APIs.
//! Depends on: ordinary collections, I/O and the selected primitive boundary.
//! Must not know: the implementation of the detector or its policy classifications.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "compiler API calibration invokes these examples during installation"
    )
)]

use std::{
    collections::{BTreeMap, HashMap},
    io::{Read, Write},
    ops::Deref,
};

use indexmap::IndexMap;
pub use nervix_primitives::expect_lint;
use nervix_primitives::{
    collections::DashMap,
    sync::{
        Arc,
        blocking::{Mutex, MutexGuard, RwLock},
    },
};

pub fn owned_collections(
    hash: &mut HashMap<u32, u32>,
    tree: &mut BTreeMap<u32, u32>,
    index: &mut IndexMap<u32, u32>,
    json: &mut serde_json::Map<String, serde_json::Value>,
) {
    hash.entry(1).or_insert(2);
    tree.entry(1).or_insert(2);
    index.entry(1).or_insert(2);
    json.entry("key").or_insert(serde_json::Value::Null);
}

pub fn io_operations(reader: &mut impl Read, writer: &mut impl Write) -> std::io::Result<()> {
    let mut bytes = [0; 4];
    reader.read(&mut bytes)?;
    writer.write(&bytes)?;
    Ok(())
}

/// Ordinary methods are selected using their real receiver type.
///
/// ```
/// let mut collection = nervix_lint_fixtures::CustomCollection;
/// collection.entry(1);
/// ```
///
/// ```compile_fail
/// let mut collection = 1_u32;
/// collection.entry(1);
/// ```
pub struct CustomCollection;

impl CustomCollection {
    pub fn entry(&mut self, _key: u32) {}
    pub fn read(&self) {}
    pub fn write(&mut self) {}
    pub fn lock(&self) {}
}

pub fn custom_methods(collection: &mut CustomCollection) {
    collection.entry(1);
    collection.read();
    collection.write();
    collection.lock();
}

pub fn locked_collection(shared: &Mutex<HashMap<u32, u32>>) {
    let mut guard = shared.lock();
    guard.entry(1).or_insert(2);
}

pub type MapAlias = DashMap<u32, u32>;

pub struct MapWrapper(pub MapAlias);

impl Deref for MapWrapper {
    type Target = MapAlias;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn shared_map_operations(map: &MapAlias) {
    drop(map.get(&1));
    drop(map.get_mut(&1));
    map.contains_key(&1);
    drop(map.entry(1));
    map.insert(1, 2);
    map.remove(&1);
    drop(map.iter());
    drop(map.iter_mut());
    drop(map.try_get(&1));
    drop(map.try_get_mut(&1));
    drop(map.try_entry(1));
}

pub fn borrowed_iteration(map: &MapAlias) {
    drop(map.into_iter());
}

#[cfg(not(feature = "shuttle"))]
pub fn owned_iteration(map: MapAlias) {
    drop(map.into_iter());
}

#[cfg(not(feature = "shuttle"))]
pub fn reserve_shared_map(map: &mut MapAlias) {
    drop(map.try_reserve(1));
}

include!(concat!(env!("OUT_DIR"), "/acquisition.rs"));

pub fn aliases_ufcs_and_deref(map: &Arc<MapWrapper>) {
    #[cfg(not(feature = "shuttle"))]
    drop(MapAlias::get(map, &1));
    // Shuttle's primitive adapter exposes borrowed operations through Deref; associated
    // functions on that adapter do not inherit the backend's methods.
    #[cfg(feature = "shuttle")]
    drop((**map).get(&1));
    drop(map.get(&1));
}

pub struct LockWrapper(Mutex<u32>);

impl LockWrapper {
    pub fn access(&self) -> MutexGuard<'_, u32> {
        self.0.lock()
    }
}

pub fn locking_helper(wrapper: &LockWrapper) {
    drop(wrapper.access());
}

pub fn callbacks_and_expansion(map: &MapAlias) {
    let callback = || drop(map.get(&1));
    callback();
    nervix_lint_fixture_macros::pass_authored_tokens! { drop(map.get(&2)); }
}

pub fn read_write_lock(lock: &RwLock<u32>) {
    drop(RwLock::read(lock));
    drop(lock.write());
    drop(lock.try_read());
    drop(lock.try_write());
}

pub async fn asynchronous_lock(lock: &nervix_primitives::sync::Mutex<u32>) {
    drop(lock.lock().await);
    drop(lock.try_lock());
}
