//! Finite synchronization APIs identified by resolved definition and normalized receiver.
//! This is rule code; it contains no product-source classifications.

use crate::Acquisition;

pub fn acquisition(receiver: &str, defining_crate: &str, operation: &str) -> Option<Acquisition> {
    // The resolved lock_api operation acquires the selected raw mutex, including parking_lot and Shuttle backends. Access through get_mut or a held guard adds no acquisition.
    if matches!(
        receiver,
        "lock_api::mutex::Mutex" | "lock_api::remutex::ReentrantMutex"
    ) && matches!(defining_crate, "lock_api")
    {
        return match operation {
            "lock" | "lock_arc" => Some(Acquisition::Exclusive),
            "try_lock" | "try_lock_for" | "try_lock_until" | "try_lock_arc"
            | "try_lock_arc_for" | "try_lock_arc_until" => Some(Acquisition::TryExclusive),
            _ => None,
        };
    }
    // Read and write guards acquire the selected raw RwLock. Upgradable guards reserve exclusive upgrade ownership. Try calls still attempt synchronization.
    if matches!(receiver, "lock_api::rwlock::RwLock") && matches!(defining_crate, "lock_api") {
        return match operation {
            "read" | "read_arc" => Some(Acquisition::Shared),
            "write" | "write_arc" | "upgradable_read" | "upgradable_read_arc" => {
                Some(Acquisition::Exclusive)
            }
            "try_read" | "try_read_arc" | "try_read_for" | "try_read_until" => {
                Some(Acquisition::TryShared)
            }
            "try_write"
            | "try_write_arc"
            | "try_write_for"
            | "try_write_until"
            | "try_upgradable_read"
            | "try_upgradable_read_for"
            | "try_upgradable_read_until"
            | "try_upgradable_read_arc"
            | "try_upgradable_read_arc_for"
            | "try_upgradable_read_arc_until" => Some(Acquisition::TryExclusive),
            _ => None,
        };
    }
    // Acquiring an async mutex or its owned guard synchronizes; creating the returned future defers acquisition until polling.
    if matches!(
        receiver,
        "tokio::sync::mutex::Mutex"
            | "shuttle_tokio_impl_inner::sync::mutex::Mutex"
            | "shuttle::future::mutex::Mutex"
    ) && matches!(
        defining_crate,
        "tokio" | "shuttle_tokio_impl_inner" | "shuttle"
    ) {
        return match operation {
            "lock" | "lock_owned" | "blocking_lock" | "blocking_lock_owned" => {
                Some(Acquisition::Exclusive)
            }
            "try_lock" | "try_lock_owned" => Some(Acquisition::TryExclusive),
            _ => None,
        };
    }
    // Async read/write guards acquire the corresponding side, including owned and try APIs.
    if matches!(
        receiver,
        "tokio::sync::rwlock::RwLock"
            | "shuttle_tokio_impl_inner::sync::rwlock::RwLock"
            | "shuttle::future::rwlock::RwLock"
    ) && matches!(
        defining_crate,
        "tokio" | "shuttle_tokio_impl_inner" | "shuttle"
    ) {
        return match operation {
            "read" | "read_owned" | "blocking_read" => Some(Acquisition::Shared),
            "write" | "write_owned" | "blocking_write" => Some(Acquisition::Exclusive),
            "try_read" | "try_read_owned" => Some(Acquisition::TryShared),
            "try_write" | "try_write_owned" => Some(Acquisition::TryExclusive),
            _ => None,
        };
    }
    // The concrete blocking mutex API acquires or tries to acquire its backend lock.
    if matches!(
        receiver,
        "std::sync::poison::mutex::Mutex"
            | "shuttle::sync::mutex::Mutex"
            | "loom::sync::mutex::Mutex"
    ) && matches!(defining_crate, "std" | "shuttle" | "loom")
    {
        return match operation {
            "lock" => Some(Acquisition::Exclusive),
            "try_lock" => Some(Acquisition::TryExclusive),
            _ => None,
        };
    }
    // The concrete blocking RwLock API acquires or tries to acquire its backend lock.
    if matches!(
        receiver,
        "std::sync::poison::rwlock::RwLock"
            | "shuttle::sync::rwlock::RwLock"
            | "loom::sync::rwlock::RwLock"
    ) && matches!(defining_crate, "std" | "shuttle" | "loom")
    {
        return match operation {
            "read" => Some(Acquisition::Shared),
            "write" => Some(Acquisition::Exclusive),
            "try_read" => Some(Acquisition::TryShared),
            "try_write" => Some(Acquisition::TryExclusive),
            _ => None,
        };
    }
    // DashMap borrowed reads and iteration take shard read guards; mutation takes shard write guards. Iteration acquires lazily while advancing; try calls are acquisition attempts. Held guards and consuming iteration do not add an acquisition.
    if matches!(
        receiver,
        "dashmap::DashMap" | "shuttle_dashmap_impl::DashMap"
    ) && matches!(defining_crate, "dashmap" | "shuttle_dashmap_impl")
    {
        return match operation {
            "get" | "contains_key" | "iter" | "len" | "is_empty" | "capacity" | "view"
            | "clone" | "into_iter" => Some(Acquisition::Shared),
            "get_mut" | "entry" | "insert" | "remove" | "remove_if" | "remove_if_mut"
            | "iter_mut" | "retain" | "clear" | "alter" | "alter_all" | "shrink_to_fit"
            | "extend" | "try_reserve" => Some(Acquisition::Exclusive),
            "try_get" => Some(Acquisition::TryShared),
            "try_get_mut" | "try_entry" => Some(Acquisition::TryExclusive),
            _ => None,
        };
    }
    // The primitive Shuttle adapter forwards borrowed iteration to the modeled DashMap. Its authored caller creates a lazy shard-read iterator; consuming iteration is unavailable in this mode.
    if matches!(
        receiver,
        "nervix_primitives::collections::scheduled::DashMap"
    ) && matches!(defining_crate, "nervix_primitives")
    {
        return match operation {
            "into_iter" => Some(Acquisition::Shared),
            _ => None,
        };
    }
    None
}

pub fn may_acquire(operation: &str) -> bool {
    matches!(
        operation,
        "alter"
            | "alter_all"
            | "blocking_lock"
            | "blocking_lock_owned"
            | "blocking_read"
            | "blocking_write"
            | "capacity"
            | "clear"
            | "clone"
            | "contains_key"
            | "entry"
            | "extend"
            | "get"
            | "get_mut"
            | "insert"
            | "into_iter"
            | "is_empty"
            | "iter"
            | "iter_mut"
            | "len"
            | "lock"
            | "lock_arc"
            | "lock_owned"
            | "read"
            | "read_arc"
            | "read_owned"
            | "remove"
            | "remove_if"
            | "remove_if_mut"
            | "retain"
            | "shrink_to_fit"
            | "try_entry"
            | "try_get"
            | "try_get_mut"
            | "try_lock"
            | "try_lock_arc"
            | "try_lock_arc_for"
            | "try_lock_arc_until"
            | "try_lock_for"
            | "try_lock_owned"
            | "try_lock_until"
            | "try_read"
            | "try_read_arc"
            | "try_read_for"
            | "try_read_owned"
            | "try_read_until"
            | "try_reserve"
            | "try_upgradable_read"
            | "try_upgradable_read_arc"
            | "try_upgradable_read_arc_for"
            | "try_upgradable_read_arc_until"
            | "try_upgradable_read_for"
            | "try_upgradable_read_until"
            | "try_write"
            | "try_write_arc"
            | "try_write_for"
            | "try_write_owned"
            | "try_write_until"
            | "upgradable_read"
            | "upgradable_read_arc"
            | "view"
            | "write"
            | "write_arc"
            | "write_owned"
    )
}
