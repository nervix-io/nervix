//! The allocations Java's Cleaner and short-lived native-call leases own.
//!
//! Layer: edges.
//! - **Owns.** Typed native handles, their shared leases and their exact release.
//! - **Depends on.** Host objects and the primitive boundary's shared ownership.
//! - **Must not know.** Session recovery or Java scheduling beyond the State monitor contract.
//!
//! A Java long is an external pointer encoding only here. State serializes borrowing and
//! release of each allocation, defers reentrant close until the active borrow ends, and refuses
//! nested native access to the same allocation. A blocking call borrows a distinct allocation holding a shared
//! host object, so release of the original cannot invalidate the call. Mutable drafts never
//! leave their State monitor; a field list passed to a blocking open is a complete snapshot.

use jni::sys::jlong;
use meticulous::ResultExt as _;
use nervix_client_host as host;
use nervix_primitives::sync::Arc;

use crate::error::Error;

macro_rules! objects {
    ($($variant:ident => $accessor:ident: $ty:ty),* $(,)?) => {
        pub(crate) enum Value {
            $($variant(Arc<$ty>),)*
            Session(host::Session),
            Schema(host::Schema),
            Fields(host::Fields),
            Builder(host::BatchBuilder),
            Options(nervix_client_core::ConnectOptions),
        }

        impl Value {
            $(pub(crate) fn $accessor(&self) -> Result<&$ty, Error> {
                match self {
                    Self::$variant(value) => Ok(value),
                    _ => Err(Self::wrong_kind()),
                }
            })*

            pub(crate) fn duplicate(&self) -> Result<Self, Error> {
                match self {
                    $(Self::$variant(value) => Ok(Self::$variant(value.clone())),)*
                    Self::Session(value) => Ok(Self::Session(value.clone())),
                    Self::Schema(value) => Ok(Self::Schema(value.clone())),
                    Self::Fields(fields) => Ok(Self::Fields(Self::snapshot(fields)?)),
                    Self::Builder(_) => Err(Self::wrong_kind()),
                    Self::Options(value) => Ok(Self::Options(value.clone())),
                }
            }
        }
    };
}

objects! {
    Cancel => cancel: host::Cancel,
    Execution => execution: host::Execution,
    Outcome => outcome: host::Outcome,
    Inspection => inspection: host::Inspection,
    EntityLabels => entity_labels: host::EntityLabelsInspection,
    InspectionFrame => inspection_frame: host::InspectionFrame,
    Event => event: host::Event,
    ClockEvent => clock_event: host::ClockEvent,
    Clock => clock: host::DomainClock,
    Producer => producer: host::Producer,
    Consumer => consumer: host::Consumer,
    Delivery => delivery: host::Delivery,
    Batch => batch: host::Batch,
    Submission => submission: host::SubmissionOutcome,
    Suggestions => suggestions: host::Suggestions,
}

impl Value {
    fn wrong_kind() -> Error {
        host::Failure::new(
            host::FailureKind::Type,
            "the native handle has another kind",
        )
        .into()
    }

    pub(crate) fn session(&self) -> Result<&host::Session, Error> {
        match self {
            Self::Session(value) => Ok(value),
            _ => Err(Self::wrong_kind()),
        }
    }

    pub(crate) fn schema(&self) -> Result<&host::Schema, Error> {
        match self {
            Self::Schema(value) => Ok(value),
            _ => Err(Self::wrong_kind()),
        }
    }

    pub(crate) fn fields(&self) -> Result<&host::Fields, Error> {
        match self {
            Self::Fields(value) => Ok(value),
            _ => Err(Self::wrong_kind()),
        }
    }

    pub(crate) fn fields_mut(&mut self) -> Result<&mut host::Fields, Error> {
        match self {
            Self::Fields(value) => Ok(value),
            _ => Err(Self::wrong_kind()),
        }
    }

    pub(crate) fn builder_mut(&mut self) -> Result<&mut host::BatchBuilder, Error> {
        match self {
            Self::Builder(value) => Ok(value),
            _ => Err(Self::wrong_kind()),
        }
    }

    pub(crate) fn options(&self) -> Result<&nervix_client_core::ConnectOptions, Error> {
        match self {
            Self::Options(value) => Ok(value),
            _ => Err(Self::wrong_kind()),
        }
    }

    pub(crate) fn options_mut(&mut self) -> Result<&mut nervix_client_core::ConnectOptions, Error> {
        match self {
            Self::Options(value) => Ok(value),
            _ => Err(Self::wrong_kind()),
        }
    }

    /// Copies a completed description for an open without holding the Java draft's monitor.
    fn snapshot(fields: &host::Fields) -> Result<host::Fields, Error> {
        let mut snapshot = host::Fields::default();
        for field in fields.schema_fields()? {
            let levels = host::TypeLevels::of(&field.ty);
            for index in 0..levels.count() {
                let ty = levels.level(index)?;
                let length = match ty {
                    nervix_models::ParseAsType::Array { len, .. } => len.get(),
                    _ => 0,
                };
                let kind = host::FieldType::from(ty);
                if index == 0 {
                    snapshot.add(
                        field.name.as_str(),
                        kind,
                        length,
                        field.optional,
                        field.sensitive,
                    )?;
                } else {
                    snapshot.element(kind, length)?;
                }
            }
        }
        Ok(snapshot)
    }

    pub(crate) fn into_handle(self) -> jlong {
        let address = Box::into_raw(Box::new(self)).expose_provenance();
        // JNI's long has 64 bits and preserves every pointer bit on the supported JVM targets.
        let bits = u64::try_from(address).assured("a supported JVM pointer has at most 64 bits");
        bits.cast_signed()
    }

    fn pointer(handle: jlong) -> Result<*mut Self, Error> {
        if handle == 0 {
            return Err(
                host::Failure::new(host::FailureKind::Closed, "the Java handle is closed").into(),
            );
        }
        let address = usize::try_from(handle.cast_unsigned())
            .map_err(|_| Error::argument("handle", "does not fit this JVM's pointer width"))?;
        Ok(std::ptr::with_exposed_provenance_mut(address))
    }

    /// # Safety
    /// The caller holds this allocation's Java State monitor or its independent native lease.
    pub(crate) unsafe fn borrow<'a>(handle: jlong) -> Result<&'a Self, Error> {
        // SAFETY: The caller keeps the allocation alive and excludes its mutable borrows.
        Ok(unsafe { &*Self::pointer(handle)? })
    }

    /// # Safety
    /// The caller holds the draft's Java State monitor, excluding every other access and release.
    pub(crate) unsafe fn borrow_mut<'a>(handle: jlong) -> Result<&'a mut Self, Error> {
        // SAFETY: The caller's monitor excludes every other access to this draft allocation.
        Ok(unsafe { &mut *Self::pointer(handle)? })
    }

    /// # Safety
    /// This is the sole release of the allocation, after its last borrow has ended.
    pub(crate) unsafe fn release(handle: jlong) -> Result<(), Error> {
        // SAFETY: The caller transferred the unique allocation from its State or lease.
        drop(unsafe { Box::from_raw(Self::pointer(handle)?) });
        Ok(())
    }
}
