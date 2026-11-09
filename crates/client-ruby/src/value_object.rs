//! Ruby equality, hashing and inspection of immutable native metadata.
//!
//! Layer: edges.
//! - **Owns.** Field-based value semantics for the Ruby metadata types that opt in.
//! - **Depends on.** Magnus typed data and the values' own equality, hash and debug contracts.
//! - **Must not know.** Sessions, endpoints, transport, identity or recovery decisions.

use std::{
    fmt::Debug,
    hash::{DefaultHasher, Hash, Hasher},
};

use magnus::{TryConvert, TypedData, Value, typed_data::Obj};

pub(crate) trait ValueObject: TypedData + Eq + Hash + Debug {
    fn same_value(&self, other: Value) -> bool {
        Obj::<Self>::try_convert(other).is_ok_and(|other| self == &*other)
    }

    fn value_hash(&self) -> i64 {
        // This is Ruby Hash bucket selection for public metadata, never a digest or identity.
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish().cast_signed()
    }

    fn inspect(&self) -> String {
        format!("{self:?}")
    }
}
