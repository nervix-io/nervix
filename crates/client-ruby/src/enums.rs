//! Ruby constants and checked conversions of the shared host enumerations.
//!
//! Layer: edges.
//! - **Owns.** Ruby enum modules and their stable host values, with exhaustive native mappings.
//! - **Depends on.** The shared host vocabulary and magnus module registration.
//! - **Must not know.** What a state means or how its transitions are decided.

use magnus::{Error, RModule, prelude::*};
use nervix_client_host as host;

macro_rules! host_enum {
    ($name:ident from $host:ident { $($variant:ident = $value:literal as $ruby:literal,)+ }) => {
        pub(crate) struct $name;
        impl $name {
            pub(crate) const fn code(value: host::$host) -> i32 {
                match value { $(host::$host::$variant => $value,)+ }
            }
            pub(crate) fn register(module: RModule) -> Result<(), Error> {
                let values = module.define_module(stringify!($name))?;
                $(values.const_set($ruby, Self::code(host::$host::$variant))?;)+
                Ok(())
            }
        }
    };
}

host_enum! { FailureKind from FailureKind {
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
} }

host_enum! { Disposition from Disposition {
Completed = 1 as "COMPLETED",
        Failed = 2 as "FAILED",
        NotLeader = 3 as "NOT_LEADER",
        TransactionDetached = 4 as "TRANSACTION_DETACHED",
        TransactionTakenOver = 5 as "TRANSACTION_TAKEN_OVER",
        OutcomeUnknown = 6 as "OUTCOME_UNKNOWN",
        ExecutionReferenceConflict = 7 as "EXECUTION_REFERENCE_CONFLICT",
        ExecutionReferenceExpired = 8 as "EXECUTION_REFERENCE_EXPIRED",
        PreviewStale = 9 as "PREVIEW_STALE",
} }

host_enum! { UploadFailure from UploadFailure {
InvalidStream = 1 as "INVALID_STREAM",
        ResourceNotDeclared = 2 as "RESOURCE_NOT_DECLARED",
        SizeMismatch = 3 as "SIZE_MISMATCH",
        QuotaExceeded = 4 as "QUOTA_EXCEEDED",
        InstallationFailed = 5 as "INSTALLATION_FAILED",
        IdentityConflict = 6 as "IDENTITY_CONFLICT",
} }

host_enum! { EventKind from EventKind {
Rows = 1 as "ROWS",
        DeliveryLost = 2 as "DELIVERY_LOST",
        RowsSkipped = 3 as "ROWS_SKIPPED",
        Ended = 4 as "ENDED",
        Interrupted = 5 as "INTERRUPTED",
        ConsumerOverflow = 6 as "CONSUMER_OVERFLOW",
        RestorationFailed = 7 as "RESTORATION_FAILED",
} }

host_enum! { EndReason from EndReason {
RelayRemoved = 1 as "RELAY_REMOVED",
        RelayChanged = 2 as "RELAY_CHANGED",
} }

host_enum! { RestorationCause from RestorationCause {
InTransaction = 1 as "IN_TRANSACTION",
        InvalidStatement = 2 as "INVALID_STATEMENT",
        NameInUse = 3 as "NAME_IN_USE",
        InvalidFilter = 4 as "INVALID_FILTER",
        Unavailable = 5 as "UNAVAILABLE",
        Rejected = 6 as "REJECTED",
        Failed = 7 as "FAILED",
} }

host_enum! { CellState from CellState {
Value = 1 as "VALUE",
        Null = 2 as "NULL",
        Redacted = 3 as "REDACTED",
} }

host_enum! { FieldType from FieldType {
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
} }

host_enum! { Part from Part {
Rows = 1 as "ROWS",
        BranchKey = 2 as "BRANCH_KEY",
} }

host_enum! { ClockEventKind from ClockEventKind {
State = 1 as "STATE",
        Tick = 2 as "TICK",
        Ended = 3 as "ENDED",
        Interrupted = 4 as "INTERRUPTED",
        RestorationFailed = 5 as "RESTORATION_FAILED",
} }

host_enum! { ClockState from ClockState {
Stopped = 1 as "STOPPED",
        Uninstalled = 2 as "UNINSTALLED",
        Unpaced = 3 as "UNPACED",
        Paced = 4 as "PACED",
} }

host_enum! { ClockEndReason from ClockEndReason {
DomainRemoved = 1 as "DOMAIN_REMOVED",
} }

host_enum! { EndpointState from EndpointState {
Active = 1 as "ACTIVE",
        Interrupted = 2 as "INTERRUPTED",
        Restoring = 3 as "RESTORING",
        ReopenRequired = 4 as "REOPEN_REQUIRED",
        Closed = 5 as "CLOSED",
} }

host_enum! { OpenRefusal from OpenRefusal {
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
} }

host_enum! { ReopenReason from ReopenReason {
DomainStopped = 1 as "DOMAIN_STOPPED",
        EndpointRemoved = 2 as "ENDPOINT_REMOVED",
        SchemaChanged = 3 as "SCHEMA_CHANGED",
        ContractChanged = 4 as "CONTRACT_CHANGED",
        GenerationChanged = 5 as "GENERATION_CHANGED",
        ProtocolViolated = 6 as "PROTOCOL_VIOLATED",
        Refused = 7 as "REFUSED",
} }

host_enum! { WindowKind from WindowKind {
Sequential = 1 as "SEQUENTIAL",
        Parallel = 2 as "PARALLEL",
} }

host_enum! { Admission from Admission {
Open = 1 as "OPEN",
        Suspended = 2 as "SUSPENDED",
} }

host_enum! { Settlement from Settlement {
Confirmed = 1 as "CONFIRMED",
        StaleReference = 2 as "STALE_REFERENCE",
        WrongConsumer = 3 as "WRONG_CONSUMER",
        InvalidReason = 4 as "INVALID_REASON",
        ConsumerEnded = 5 as "CONSUMER_ENDED",
} }

host_enum! { SubmissionResult from SubmissionResult {
NotAdmitted = 1 as "NOT_ADMITTED",
        Completed = 2 as "COMPLETED",
        ProcessingFailed = 3 as "PROCESSING_FAILED",
        OutcomeUnknown = 4 as "OUTCOME_UNKNOWN",
} }

host_enum! { SubmissionRefusal from SubmissionRefusal {
InvalidBatch = 1 as "INVALID_BATCH",
        Suspended = 2 as "SUSPENDED",
        Busy = 3 as "BUSY",
        Draining = 4 as "DRAINING",
        ProducerEnded = 5 as "PRODUCER_ENDED",
        CreditExceeded = 6 as "CREDIT_EXCEEDED",
} }

host_enum! { BatchDefect from BatchDefect {
Malformed = 1 as "MALFORMED",
        UnexpectedMessage = 2 as "UNEXPECTED_MESSAGE",
        Compressed = 3 as "COMPRESSED",
        SchemaMismatch = 4 as "SCHEMA_MISMATCH",
        NotOneBatch = 5 as "NOT_ONE_BATCH",
        TooManyRows = 6 as "TOO_MANY_ROWS",
        TooLarge = 7 as "TOO_LARGE",
        InvalidData = 8 as "INVALID_DATA",
} }

host_enum! { ProcessingFailure from ProcessingFailure {
AckTimedOut = 1 as "ACK_TIMED_OUT",
        Rejected = 2 as "REJECTED",
} }

host_enum! { Uncertainty from Uncertainty {
Interrupted = 1 as "INTERRUPTED",
        OwnerLost = 2 as "OWNER_LOST",
        SessionLost = 3 as "SESSION_LOST",
} }

host_enum! { CompletionStatus from CompletionStatus {
Ready = 1 as "READY",
        MissingContext = 2 as "MISSING_CONTEXT",
        StaleContext = 3 as "STALE_CONTEXT",
        LookupFailed = 4 as "LOOKUP_FAILED",
} }

host_enum! { CompletionKind from CompletionKind {
Text = 1 as "TEXT",
        LocalDirectoryLookup = 2 as "LOCAL_DIRECTORY_LOOKUP",
} }

pub(crate) fn register(module: RModule) -> Result<(), Error> {
    FailureKind::register(module)?;
    Disposition::register(module)?;
    UploadFailure::register(module)?;
    EventKind::register(module)?;
    EndReason::register(module)?;
    RestorationCause::register(module)?;
    CellState::register(module)?;
    FieldType::register(module)?;
    Part::register(module)?;
    ClockEventKind::register(module)?;
    ClockState::register(module)?;
    ClockEndReason::register(module)?;
    EndpointState::register(module)?;
    OpenRefusal::register(module)?;
    ReopenReason::register(module)?;
    WindowKind::register(module)?;
    Admission::register(module)?;
    Settlement::register(module)?;
    SubmissionResult::register(module)?;
    SubmissionRefusal::register(module)?;
    BatchDefect::register(module)?;
    ProcessingFailure::register(module)?;
    Uncertainty::register(module)?;
    CompletionStatus::register(module)?;
    CompletionKind::register(module)?;
    let tls = module.define_module("Tls")?;
    tls.const_set("PREFERRED", 1)?;
    tls.const_set("REQUIRED", 2)?;
    Ok(())
}
