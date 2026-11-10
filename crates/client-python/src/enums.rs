//! The enumerations a Python host reads and passes, with the stable value of every variant.
//!
//! Layer: edges.
//! - **Owns.** One Python class per enumeration of the host session runtime, whose members are
//!   spelled as constants and compare equal to the runtime's stable values, and the conversions
//!   between each class and the runtime's own enumeration.
//! - **Depends on.** The host session runtime's enumerations and the Rust client's TLS
//!   requirement.
//! - **Must not know.** What any value means; the runtime and the Rust client decide that.

use nervix_client_core::TlsRequirement;
use nervix_client_host as host;
use pyo3::prelude::*;

/// Declares a Python enumeration over one of the host session runtime's, with the same stable
/// values, and the conversions between the two.
macro_rules! host_enum {
    (
        $(#[$meta:meta])*
        $name:ident from $host:ident {
            $($variant:ident = $value:literal as $python:literal,)+
        }
    ) => {
        $(#[$meta])*
        #[pyclass(eq, eq_int, frozen, hash, from_py_object, module = "nervix_client")]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub(crate) enum $name {
            $(#[pyo3(name = $python)] $variant = $value,)+
        }

        impl From<host::$host> for $name {
            fn from(value: host::$host) -> Self {
                match value {
                    $(host::$host::$variant => Self::$variant,)+
                }
            }
        }

        impl From<$name> for host::$host {
            fn from(value: $name) -> Self {
                match value {
                    $($name::$variant => Self::$variant,)+
                }
            }
        }
    };
}

host_enum! { EntityLabelViewKind from EntityLabelViewKind {
    Committed = 1 as "COMMITTED",
    Staged = 2 as "STAGED",
} }

host_enum! { InspectionRejection from InspectionRejection {
    UnknownEntity = 1 as "UNKNOWN_ENTITY",
    NotMaterialized = 2 as "NOT_MATERIALIZED",
    InvalidSelection = 3 as "INVALID_SELECTION",
    InvalidLimit = 4 as "INVALID_LIMIT",
    StaleContinuation = 5 as "STALE_CONTINUATION",
    OwnerUnavailable = 6 as "OWNER_UNAVAILABLE",
    NotReady = 7 as "NOT_READY",
    Deadline = 8 as "DEADLINE",
    ReplyTooLarge = 9 as "REPLY_TOO_LARGE",
} }

host_enum! { InspectionStage from InspectionStage {
    Live = 1 as "LIVE",
    Stored = 2 as "STORED",
} }

host_enum! { InspectionUnitKind from InspectionUnitKind {
    Single = 1 as "SINGLE",
    Partition = 2 as "PARTITION",
    Instance = 3 as "INSTANCE",
} }

host_enum! {
    /// The kind of a failure, which names the exception class it raises.
    FailureKind from FailureKind {
        InvalidArgument = 1 as "INVALID_ARGUMENT",
        Connect = 2 as "CONNECT",
        Transport = 3 as "TRANSPORT",
        Uncertain = 4 as "UNCERTAIN",
        Rejected = 5 as "REJECTED",
        Deadline = 6 as "DEADLINE",
        Cancelled = 7 as "CANCELLED",
        Overflow = 8 as "OVERFLOW",
        Protocol = 9 as "PROTOCOL",
        Type = 10 as "TYPE",
        Closed = 11 as "CLOSED",
        Interrupted = 12 as "INTERRUPTED",
        ReopenRequired = 13 as "REOPEN_REQUIRED",
    }
}

host_enum! {
    /// What became of a command.
    Disposition from Disposition {
        Completed = 1 as "COMPLETED",
        Failed = 2 as "FAILED",
        NotLeader = 3 as "NOT_LEADER",
        TransactionDetached = 4 as "TRANSACTION_DETACHED",
        TransactionTakenOver = 5 as "TRANSACTION_TAKEN_OVER",
        OutcomeUnknown = 6 as "OUTCOME_UNKNOWN",
        ExecutionReferenceConflict = 7 as "EXECUTION_REFERENCE_CONFLICT",
        ExecutionReferenceExpired = 8 as "EXECUTION_REFERENCE_EXPIRED",
        PreviewStale = 9 as "PREVIEW_STALE",
    }
}

host_enum! {
    /// Why a resource upload failed.
    UploadFailure from UploadFailure {
        InvalidStream = 1 as "INVALID_STREAM",
        ResourceNotDeclared = 2 as "RESOURCE_NOT_DECLARED",
        SizeMismatch = 3 as "SIZE_MISMATCH",
        QuotaExceeded = 4 as "QUOTA_EXCEEDED",
        InstallationFailed = 5 as "INSTALLATION_FAILED",
        IdentityConflict = 6 as "IDENTITY_CONFLICT",
    }
}

host_enum! {
    /// What a subscription event reports.
    EventKind from EventKind {
        Rows = 1 as "ROWS",
        DeliveryLost = 2 as "DELIVERY_LOST",
        RowsSkipped = 3 as "ROWS_SKIPPED",
        Ended = 4 as "ENDED",
        Interrupted = 5 as "INTERRUPTED",
        ConsumerOverflow = 6 as "CONSUMER_OVERFLOW",
        RestorationFailed = 7 as "RESTORATION_FAILED",
    }
}

host_enum! {
    /// Why a subscription's generation ended: the server ended it, or a new session refused for
    /// good to open it again.
    EndReason from EndReason {
        RelayRemoved = 1 as "RELAY_REMOVED",
        RelayChanged = 2 as "RELAY_CHANGED",
    }
}

host_enum! {
    /// Why a session refused for now to open an interrupted subscription again, or to attach an
    /// interrupted clock again. A clock's restoration reports only `IN_TRANSACTION`, `REJECTED`
    /// or `FAILED`.
    RestorationCause from RestorationCause {
        InTransaction = 1 as "IN_TRANSACTION",
        InvalidStatement = 2 as "INVALID_STATEMENT",
        NameInUse = 3 as "NAME_IN_USE",
        InvalidFilter = 4 as "INVALID_FILTER",
        Unavailable = 5 as "UNAVAILABLE",
        Rejected = 6 as "REJECTED",
        Failed = 7 as "FAILED",
    }
}

host_enum! {
    /// What a cell holds: a value, a null, or a value the server withheld.
    CellState from CellState {
        Value = 1 as "VALUE",
        Null = 2 as "NULL",
        Redacted = 3 as "REDACTED",
    }
}

host_enum! {
    /// One level of a field's type: a scalar, or one of the two kinds of list.
    FieldType from FieldType {
        U8 = 1 as "U8",
        I8 = 2 as "I8",
        U16 = 3 as "U16",
        I16 = 4 as "I16",
        U32 = 5 as "U32",
        I32 = 6 as "I32",
        U64 = 7 as "U64",
        I64 = 8 as "I64",
        F32 = 9 as "F32",
        F64 = 10 as "F64",
        Bool = 11 as "BOOL",
        String = 12 as "STRING",
        Bytes = 13 as "BYTES",
        Datetime = 14 as "DATETIME",
        FixedList = 15 as "FIXED_LIST",
        List = 16 as "LIST",
    }
}

host_enum! {
    /// Which cells of an event a column is read from: its rows, or its branch key.
    Part from Part {
        Rows = 1 as "ROWS",
        BranchKey = 2 as "BRANCH_KEY",
    }
}

host_enum! {
    /// What a domain clock event reports.
    ClockEventKind from ClockEventKind {
        State = 1 as "STATE",
        Tick = 2 as "TICK",
        Ended = 3 as "ENDED",
        Interrupted = 4 as "INTERRUPTED",
        RestorationFailed = 5 as "RESTORATION_FAILED",
    }
}

host_enum! {
    /// The installation state of one clock generation.
    ClockState from ClockState {
        Stopped = 1 as "STOPPED",
        Uninstalled = 2 as "UNINSTALLED",
        Unpaced = 3 as "UNPACED",
        Paced = 4 as "PACED",
    }
}

host_enum! {
    /// Why the server ended an attachment to a domain clock.
    ClockEndReason from ClockEndReason {
        DomainRemoved = 1 as "DOMAIN_REMOVED",
    }
}

host_enum! {
    /// Whether a producer or a consumer is attached.
    EndpointState from EndpointState {
        Active = 1 as "ACTIVE",
        Interrupted = 2 as "INTERRUPTED",
        Restoring = 3 as "RESTORING",
        ReopenRequired = 4 as "REOPEN_REQUIRED",
        Closed = 5 as "CLOSED",
    }
}

host_enum! {
    /// Why the server refused to open a producer or a consumer.
    OpenRefusal from OpenRefusal {
        DomainNotFound = 1 as "DOMAIN_NOT_FOUND",
        DomainStopped = 2 as "DOMAIN_STOPPED",
        EndpointNotFound = 3 as "ENDPOINT_NOT_FOUND",
        NotClientEndpoint = 4 as "NOT_CLIENT_ENDPOINT",
        EndpointUnavailable = 5 as "ENDPOINT_UNAVAILABLE",
        SchemaMismatch = 6 as "SCHEMA_MISMATCH",
        TooManyEndpoints = 7 as "TOO_MANY_ENDPOINTS",
        SessionCapacityExhausted = 8 as "SESSION_CAPACITY_EXHAUSTED",
        NodeCapacityExhausted = 9 as "NODE_CAPACITY_EXHAUSTED",
        InvalidLimits = 10 as "INVALID_LIMITS",
        InTransaction = 11 as "IN_TRANSACTION",
    }
}

host_enum! {
    /// Why a producer or a consumer has to be opened again.
    ReopenReason from ReopenReason {
        DomainStopped = 1 as "DOMAIN_STOPPED",
        EndpointRemoved = 2 as "ENDPOINT_REMOVED",
        SchemaChanged = 3 as "SCHEMA_CHANGED",
        ContractChanged = 4 as "CONTRACT_CHANGED",
        GenerationChanged = 5 as "GENERATION_CHANGED",
        ProtocolViolated = 6 as "PROTOCOL_VIOLATED",
        Refused = 7 as "REFUSED",
    }
}

host_enum! {
    /// How an endpoint's acknowledgements may be outstanding.
    WindowKind from WindowKind {
        Sequential = 1 as "SEQUENTIAL",
        Parallel = 2 as "PARALLEL",
    }
}

host_enum! {
    /// Whether a producer's batches are admitted.
    Admission from Admission {
        Open = 1 as "OPEN",
        Suspended = 2 as "SUSPENDED",
    }
}

host_enum! {
    /// What the server did with a settlement of a delivery.
    Settlement from Settlement {
        Confirmed = 1 as "CONFIRMED",
        StaleReference = 2 as "STALE_REFERENCE",
        WrongConsumer = 3 as "WRONG_CONSUMER",
        InvalidReason = 4 as "INVALID_REASON",
        ConsumerEnded = 5 as "CONSUMER_ENDED",
    }
}

host_enum! {
    /// What became of a submitted batch.
    SubmissionResult from SubmissionResult {
        NotAdmitted = 1 as "NOT_ADMITTED",
        Completed = 2 as "COMPLETED",
        ProcessingFailed = 3 as "PROCESSING_FAILED",
        OutcomeUnknown = 4 as "OUTCOME_UNKNOWN",
    }
}

host_enum! {
    /// Why a batch was not admitted.
    SubmissionRefusal from SubmissionRefusal {
        InvalidBatch = 1 as "INVALID_BATCH",
        Suspended = 2 as "SUSPENDED",
        Busy = 3 as "BUSY",
        Draining = 4 as "DRAINING",
        ProducerEnded = 5 as "PRODUCER_ENDED",
        CreditExceeded = 6 as "CREDIT_EXCEEDED",
    }
}

host_enum! {
    /// What made a refused batch invalid.
    BatchDefect from BatchDefect {
        Malformed = 1 as "MALFORMED",
        UnexpectedMessage = 2 as "UNEXPECTED_MESSAGE",
        Compressed = 3 as "COMPRESSED",
        SchemaMismatch = 4 as "SCHEMA_MISMATCH",
        NotOneBatch = 5 as "NOT_ONE_BATCH",
        TooManyRows = 6 as "TOO_MANY_ROWS",
        TooLarge = 7 as "TOO_LARGE",
        InvalidData = 8 as "INVALID_DATA",
    }
}

host_enum! {
    /// Why an admitted batch's acknowledgement failed.
    ProcessingFailure from ProcessingFailure {
        AckTimedOut = 1 as "ACK_TIMED_OUT",
        Rejected = 2 as "REJECTED",
    }
}

host_enum! {
    /// Why no terminal result could be established for a batch.
    Uncertainty from Uncertainty {
        Interrupted = 1 as "INTERRUPTED",
        OwnerLost = 2 as "OWNER_LOST",
        SessionLost = 3 as "SESSION_LOST",
    }
}

host_enum! {
    /// Whether a completion page could be computed.
    CompletionStatus from CompletionStatus {
        Ready = 1 as "READY",
        MissingContext = 2 as "MISSING_CONTEXT",
        StaleContext = 3 as "STALE_CONTEXT",
        LookupFailed = 4 as "LOOKUP_FAILED",
    }
}

host_enum! {
    /// What a completion candidate inserts.
    CompletionKind from CompletionKind {
        Text = 1 as "TEXT",
        LocalDirectoryLookup = 2 as "LOCAL_DIRECTORY_LOOKUP",
    }
}

/// Whether a session may reach its server over plaintext.
#[pyclass(eq, eq_int, frozen, hash, from_py_object, module = "nervix_client")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Tls {
    /// TLS is used when the server's address asks for it.
    #[pyo3(name = "PREFERRED")]
    Preferred = 1,
    /// A server address that is not TLS is refused before anything is sent.
    #[pyo3(name = "REQUIRED")]
    Required = 2,
}

impl From<Tls> for TlsRequirement {
    fn from(tls: Tls) -> Self {
        match tls {
            Tls::Preferred => Self::Preferred,
            Tls::Required => Self::Required,
        }
    }
}

/// Adds every enumeration to the module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<EntityLabelViewKind>()?;
    module.add_class::<InspectionRejection>()?;
    module.add_class::<InspectionStage>()?;
    module.add_class::<InspectionUnitKind>()?;
    module.add_class::<FailureKind>()?;
    module.add_class::<Disposition>()?;
    module.add_class::<UploadFailure>()?;
    module.add_class::<EventKind>()?;
    module.add_class::<EndReason>()?;
    module.add_class::<RestorationCause>()?;
    module.add_class::<CellState>()?;
    module.add_class::<FieldType>()?;
    module.add_class::<Part>()?;
    module.add_class::<ClockEventKind>()?;
    module.add_class::<ClockState>()?;
    module.add_class::<ClockEndReason>()?;
    module.add_class::<EndpointState>()?;
    module.add_class::<OpenRefusal>()?;
    module.add_class::<ReopenReason>()?;
    module.add_class::<WindowKind>()?;
    module.add_class::<Admission>()?;
    module.add_class::<Settlement>()?;
    module.add_class::<SubmissionResult>()?;
    module.add_class::<SubmissionRefusal>()?;
    module.add_class::<BatchDefect>()?;
    module.add_class::<ProcessingFailure>()?;
    module.add_class::<Uncertainty>()?;
    module.add_class::<CompletionStatus>()?;
    module.add_class::<CompletionKind>()?;
    module.add_class::<Tls>()?;
    Ok(())
}
