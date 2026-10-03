/*
 * The shared Rust binding of the Nervix client session.
 *
 * Every host that loads this library drives the same session state machine the Rust client uses:
 * request correlation, leader redirects, reconnection, execution identity across retries,
 * desired-subscription restoration and domain clock re-attachment all happen inside the library.
 * A host never retries, reroutes or reinterprets an outcome on its own.
 *
 * Text
 *   Every string crosses the boundary as a pointer and a byte length. Input strings are UTF-8 and
 *   need no terminator; output strings are UTF-8, are never terminated, and may contain NUL.
 *
 * Errors and ownership
 *   A function that can fail returns an `nx_error *`: NULL on success, otherwise an error the
 *   caller owns and releases with `nx_error_free`. Out-parameters are written only on success.
 *   Every object a function hands out through an out-parameter is owned by the caller and has
 *   exactly one release function. A borrowed pointer an accessor writes stays valid until the
 *   object it was read from is released, and never longer. A producer, a consumer and a delivery
 *   keep the session they came from running: `nx_session_free` releases the host's session, and
 *   the session ends once every producer, consumer and delivery of it is released as well, in any
 *   order.
 *
 * Threads
 *   The library calls no host code: it has no callbacks, so no host function ever runs on a thread
 *   the library owns. Every blocking call runs on the calling thread until it completes, fails,
 *   or is cancelled. A session, a producer, a consumer, a delivery and a batch may be used from
 *   several threads at once; a field list and a batch builder are used from one thread at a time.
 *   Releasing an object while another thread still uses it is the caller's error; releasing an
 *   event, a clock event, a domain clock, a delivery or a batch while another thread holds a
 *   retained reference to it is not.
 *
 * Cancellation and deadlines
 *   Every blocking call accepts an optional `nx_cancel`. Triggering it, from any thread, makes the
 *   call return NX_ERROR_CANCELLED; a token created with a deadline makes the call return
 *   NX_ERROR_DEADLINE once the deadline passes. Cancelling a wait never rolls back work the server
 *   already admitted: a cancelled command keeps its execution reference, and executing the same
 *   `nx_execution` again recovers its outcome; a cancelled submission wait leaves the batch with
 *   its producer; a cancelled read leaves the read with its consumer; a cancelled settlement may
 *   still have settled its delivery.
 *
 * Bulk data
 *   Row events keep the verified frame they were decoded from. `nx_event_frame` borrows those
 *   bytes without copying them. Column accessors copy one whole column into caller-provided
 *   memory in a single call, so reading a batch costs one call per column rather than one per
 *   value. `nx_event_cell_varlen` borrows one string or bytes value without copying it.
 *
 *   Producers and consumers exchange typed Arrow batches, each one canonical Arrow IPC stream: its
 *   schema message, one record batch and the end-of-stream marker, uncompressed, without
 *   dictionaries, with exactly the endpoint's fields and no metadata. A host builds a batch column
 *   by column with `nx_batch_builder`, which copies every buffer it is given before the call
 *   returns, and reads a delivered one column by column from `nx_batch`, one call per level of a
 *   column's type. A host with Arrow tooling of its own may instead submit a stream it wrote with
 *   `nx_producer_submit_ipc`, which copies it, and borrow the stream a delivery carried with
 *   `nx_delivery_ipc`, without a copy.
 */

#ifndef NERVIX_CLIENT_H
#define NERVIX_CLIENT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct nx_session nx_session;
typedef struct nx_execution nx_execution;
typedef struct nx_outcome nx_outcome;
typedef struct nx_schema nx_schema;
typedef struct nx_event nx_event;
typedef struct nx_clock_event nx_clock_event;
typedef struct nx_domain_clock nx_domain_clock;
typedef struct nx_cancel nx_cancel;
typedef struct nx_error nx_error;
typedef struct nx_suggestions nx_suggestions;
typedef struct nx_fields nx_fields;
typedef struct nx_producer nx_producer;
typedef struct nx_submission_outcome nx_submission_outcome;
typedef struct nx_consumer nx_consumer;
typedef struct nx_delivery nx_delivery;
typedef struct nx_batch nx_batch;
typedef struct nx_batch_builder nx_batch_builder;

/* What kind of failure an `nx_error` reports. */
typedef enum nx_error_kind {
    /* An argument was missing, not UTF-8, out of range, or too small for the result. */
    NX_ERROR_INVALID_ARGUMENT = 1,
    /* No session could be opened with the servers and credentials given. */
    NX_ERROR_CONNECT = 2,
    /* The session failed after it was opened. */
    NX_ERROR_TRANSPORT = 3,
    /* Admitted work may have taken effect, and its outcome is not known. For a command the error
       carries the execution reference, and executing the same `nx_execution` again recovers the
       outcome. For a settlement the session was lost before the server's answer: the delivery
       may be settled, or delivered again with a new reference. */
    NX_ERROR_UNCERTAIN = 4,
    /* The server refused to serve the request. Nothing was admitted. */
    NX_ERROR_REJECTED = 5,
    /* The call's deadline passed. */
    NX_ERROR_DEADLINE = 6,
    /* The call was cancelled, by its token or by the server. */
    NX_ERROR_CANCELLED = 7,
    /* The session could not retain more subscription events for the caller. */
    NX_ERROR_OVERFLOW = 8,
    /* The server answered with something the protocol does not allow here. */
    NX_ERROR_PROTOCOL = 9,
    /* A column or field was read as a type it does not hold, or a domain clock was read for what
       its state does not carry. */
    NX_ERROR_TYPE = 10,
    /* The session ended and cannot be recovered, or the producer or consumer was closed. */
    NX_ERROR_CLOSED = 11,
    /* The consumer's attachment ended with its session, or when its endpoint moved. The next read
       continues on a restored attachment; no delivery read before the gap can be settled, and the
       server delivers what they carried again. */
    NX_ERROR_INTERRUPTED = 12,
    /* The endpoint no longer has the START generation, schema or contract the handle was opened
       with, or it stopped or was removed: the handle cannot continue, and the host opens a new
       one. nx_producer_reopen_reason and nx_consumer_reopen_reason say why. */
    NX_ERROR_REOPEN_REQUIRED = 13
} nx_error_kind;

/* What became of a command. */
typedef enum nx_disposition {
    NX_DISPOSITION_COMPLETED = 1,
    NX_DISPOSITION_FAILED = 2,
    NX_DISPOSITION_NOT_LEADER = 3,
    NX_DISPOSITION_TRANSACTION_DETACHED = 4,
    NX_DISPOSITION_TRANSACTION_TAKEN_OVER = 5,
    NX_DISPOSITION_OUTCOME_UNKNOWN = 6,
    NX_DISPOSITION_EXECUTION_REFERENCE_CONFLICT = 7,
    NX_DISPOSITION_EXECUTION_REFERENCE_EXPIRED = 8,
    NX_DISPOSITION_PREVIEW_STALE = 9
} nx_disposition;

typedef enum nx_completion_status {
    NX_COMPLETION_READY = 1,
    NX_COMPLETION_MISSING_CONTEXT = 2,
    NX_COMPLETION_STALE_CONTEXT = 3,
    NX_COMPLETION_LOOKUP_FAILED = 4
} nx_completion_status;

typedef enum nx_completion_kind {
    NX_COMPLETION_TEXT = 1,
    NX_COMPLETION_LOCAL_DIRECTORY_LOOKUP = 2
} nx_completion_kind;

/* The type of a schema field, or of one level of it. A LIST or FIXED_LIST level holds lists whose
   elements are the next level; nx_schema_field_level reads every level of a field. A subscription's
   list values are read from its frame with a FlatBuffers reader, and an endpoint batch's one level
   at a time. */
typedef enum nx_type {
    NX_TYPE_U8 = 1,
    NX_TYPE_I8 = 2,
    NX_TYPE_U16 = 3,
    NX_TYPE_I16 = 4,
    NX_TYPE_U32 = 5,
    NX_TYPE_I32 = 6,
    NX_TYPE_U64 = 7,
    NX_TYPE_I64 = 8,
    NX_TYPE_F32 = 9,
    NX_TYPE_F64 = 10,
    NX_TYPE_BOOL = 11,
    NX_TYPE_STRING = 12,
    NX_TYPE_BYTES = 13,
    /* Signed nanoseconds since the Unix epoch in UTC, read as int64_t. */
    NX_TYPE_DATETIME = 14,
    NX_TYPE_FIXED_LIST = 15,
    NX_TYPE_LIST = 16
} nx_type;

/* Which cells of a schema or a batch an accessor reads. */
typedef enum nx_part {
    /* The rows' fields. */
    NX_PART_ROWS = 1,
    /* The branch key's fields. A batch of a branched relay has exactly one branch key. */
    NX_PART_BRANCH_KEY = 2
} nx_part;

/* What a cell holds. */
typedef enum nx_cell_state {
    NX_CELL_VALUE = 1,
    NX_CELL_NULL = 2,
    /* The field is sensitive and its value is withheld. */
    NX_CELL_REDACTED = 3
} nx_cell_state;

/* What a subscription event reports. */
typedef enum nx_event_kind {
    NX_EVENT_ROWS = 1,
    /* A dropping subscription discarded rows; `nx_event_row_count` is how many. */
    NX_EVENT_DELIVERY_LOST = 2,
    /* Rows were skipped and the subscription stays open; `nx_event_row_count` is how many. */
    NX_EVENT_ROWS_SKIPPED = 3,
    /* The server ended the subscription's generation, because its relay was redefined or removed.
       It is the generation's last event, and the session never opens the generation again. */
    NX_EVENT_ENDED = 4,
    /* The session was lost, leaving a gap before the subscription is restored. */
    NX_EVENT_INTERRUPTED = 5,
    /* The session could not retain more events for this subscription. */
    NX_EVENT_CONSUMER_OVERFLOW = 6,
    /* The session refused to open the interrupted subscription again, or did not answer; the
       subscription stays interrupted and the session tries again later. */
    NX_EVENT_RESTORATION_FAILED = 7
} nx_event_kind;

/* What a domain clock event reports. */
typedef enum nx_clock_event_kind {
    /* The serving node's installation of the clock changed, or an attachment the library restored
       on a new session reported the clock again. */
    NX_CLOCK_EVENT_STATE = 1,
    /* The serving node accepted newer progress of its installed paced generation. A tick never
       precedes the state of its generation, which the attach reported, as nx_session_domain_clock
       reads it, or a STATE event reported first. */
    NX_CLOCK_EVENT_TICK = 2,
    /* The server ended the attachment. Nothing more follows about the domain's clock unless the
       session attaches to it again. */
    NX_CLOCK_EVENT_ENDED = 3,
    /* The session holding the attachment ended. The library attaches to the clock again on its
       next session, and the clock that attachment reports follows as a STATE event; changes in
       between are not reported. */
    NX_CLOCK_EVENT_INTERRUPTED = 4,
    /* The session refused to attach the interrupted clock again, or did not answer; the
       attachment stays interrupted and the session tries again later. */
    NX_CLOCK_EVENT_RESTORATION_FAILED = 5
} nx_clock_event_kind;

/* The installation state of one domain clock generation on the serving node. */
typedef enum nx_clock_state {
    /* The generation is stopped and runs no domain work. */
    NX_CLOCK_STOPPED = 1,
    /* The generation is paced and running, and the serving node lacks its committed mapping or an
       assigned clock authority, so its clock cannot be read. */
    NX_CLOCK_UNINSTALLED = 2,
    /* The generation reads actual UTC. */
    NX_CLOCK_UNPACED = 3,
    /* The generation projects UTC through its committed mapping, which nx_clock_event_paced and
       nx_domain_clock_paced read. */
    NX_CLOCK_PACED = 4
} nx_clock_state;

/* Why the server ended an attachment to a domain clock. */
typedef enum nx_clock_end_reason {
    /* The domain no longer exists on the serving node. */
    NX_CLOCK_END_DOMAIN_REMOVED = 1
} nx_clock_end_reason;

/* Why the server refused to open a producer or a consumer. Nothing is left attached. */
typedef enum nx_open_refusal {
    NX_OPEN_DOMAIN_NOT_FOUND = 1,
    /* The domain exists and is not running. */
    NX_OPEN_DOMAIN_STOPPED = 2,
    /* The domain has no ingestor or emitter of that name. */
    NX_OPEN_ENDPOINT_NOT_FOUND = 3,
    /* The ingestor or emitter is not a CLIENT endpoint. */
    NX_OPEN_NOT_CLIENT_ENDPOINT = 4,
    /* The endpoint is not running on the node that owns it right now; opening again later may
       succeed. */
    NX_OPEN_ENDPOINT_UNAVAILABLE = 5,
    /* The expected fields differ from the endpoint's in a field, its position, its exact type,
       its nullability or its sensitivity. */
    NX_OPEN_SCHEMA_MISMATCH = 6,
    /* The session already holds as many producers, or consumers, as it may. */
    NX_OPEN_TOO_MANY_ENDPOINTS = 7,
    /* The session's byte budget cannot hold the credit asked for. */
    NX_OPEN_SESSION_CAPACITY_EXHAUSTED = 8,
    /* The byte budget of the serving or owning node cannot hold the credit asked for. */
    NX_OPEN_NODE_CAPACITY_EXHAUSTED = 9,
    /* The credit asked for is larger than one producer or consumer may ask for. */
    NX_OPEN_INVALID_LIMITS = 10,
    /* The session holds a transaction. */
    NX_OPEN_IN_TRANSACTION = 11
} nx_open_refusal;

/* Whether a producer or a consumer is attached. */
typedef enum nx_endpoint_state {
    NX_ENDPOINT_ACTIVE = 1,
    /* The session holding the attachment ended. The next call that needs the attachment opens a
       session and restores it. */
    NX_ENDPOINT_INTERRUPTED = 2,
    /* A new session is opening the endpoint again. */
    NX_ENDPOINT_RESTORING = 3,
    /* The handle cannot continue; the host opens a new one. */
    NX_ENDPOINT_REOPEN_REQUIRED = 4,
    /* The host closed the handle. */
    NX_ENDPOINT_CLOSED = 5
} nx_endpoint_state;

/* Why a handle has to be opened again. */
typedef enum nx_reopen_reason {
    /* The domain stopped, or its START generation ended. */
    NX_REOPEN_DOMAIN_STOPPED = 1,
    NX_REOPEN_ENDPOINT_REMOVED = 2,
    NX_REOPEN_SCHEMA_CHANGED = 3,
    /* The endpoint's contract, acknowledgement policy or granted credit changed. */
    NX_REOPEN_CONTRACT_CHANGED = 4,
    /* The domain was started again since the handle was opened. */
    NX_REOPEN_GENERATION_CHANGED = 5,
    /* The server broke the protocol, or the producer exceeded its credit. */
    NX_REOPEN_PROTOCOL_VIOLATED = 6,
    /* The server refused to restore the endpoint for good; the refusal says why. */
    NX_REOPEN_REFUSED = 7
} nx_reopen_reason;

/* How many acknowledgements of an endpoint may be outstanding at once. */
typedef enum nx_ack_window {
    /* One at a time: ACK SEQUENTIAL. */
    NX_ACK_SEQUENTIAL = 1,
    /* Up to a maximum: ACK PARALLEL MAX. */
    NX_ACK_PARALLEL = 2
} nx_ack_window;

/* Whether a producer's batches are admitted. */
typedef enum nx_admission {
    NX_ADMISSION_OPEN = 1,
    /* The ingestor is quiesced or shedding load: the producer keeps its batches and sends them
       once admission opens. */
    NX_ADMISSION_SUSPENDED = 2
} nx_admission;

/* What became of a submitted batch. */
typedef enum nx_submission_result {
    /* No row of the batch entered the graph. */
    NX_SUBMISSION_NOT_ADMITTED = 1,
    /* The batch's source acknowledgement resolved successfully under the graph's policies. */
    NX_SUBMISSION_COMPLETED = 2,
    /* The batch was admitted and its acknowledgement failed; some of its effects may have
       happened. */
    NX_SUBMISSION_PROCESSING_FAILED = 3,
    /* The batch may have been admitted and processed; replaying it may duplicate its effects. */
    NX_SUBMISSION_OUTCOME_UNKNOWN = 4
} nx_submission_result;

/* Why a batch was not admitted. A suspended or busy refusal is sent again by the library, after
   the ingestor's backoff, before any of them is reported. */
typedef enum nx_submission_refusal {
    /* The batch is not a canonical Arrow IPC batch of the producer's schema within its limits;
       nx_submission_outcome_defect says how. */
    NX_REFUSAL_INVALID_BATCH = 1,
    NX_REFUSAL_SUSPENDED = 2,
    NX_REFUSAL_BUSY = 3,
    /* The ingestor is stopping or moving; the producer's attachment ends. */
    NX_REFUSAL_DRAINING = 4,
    /* The producer was closed, or its attachment ended, before the batch was admitted. */
    NX_REFUSAL_PRODUCER_ENDED = 5,
    /* The batch exceeded the producer's credit. */
    NX_REFUSAL_CREDIT_EXCEEDED = 6
} nx_submission_refusal;

/* What made a refused batch invalid. */
typedef enum nx_batch_defect {
    NX_DEFECT_MALFORMED = 1,
    NX_DEFECT_UNEXPECTED_MESSAGE = 2,
    NX_DEFECT_COMPRESSED = 3,
    NX_DEFECT_SCHEMA_MISMATCH = 4,
    NX_DEFECT_NOT_ONE_BATCH = 5,
    NX_DEFECT_TOO_MANY_ROWS = 6,
    NX_DEFECT_TOO_LARGE = 7,
    NX_DEFECT_INVALID_DATA = 8
} nx_batch_defect;

/* Why an admitted batch's acknowledgement failed. */
typedef enum nx_processing_failure {
    /* No progress within the ingestor's ACK TIMEOUT; the admitted work may still complete. */
    NX_FAILURE_ACK_TIMED_OUT = 1,
    /* A route, a policy, a downstream node or a consumer's rejection failed the batch. */
    NX_FAILURE_REJECTED = 2
} nx_processing_failure;

/* Why no outcome could be established for a batch. */
typedef enum nx_submission_uncertainty {
    /* The ingestor's execution stopped, or the producer's attachment ended, while the batch was
       unresolved. */
    NX_UNCERTAINTY_INTERRUPTED = 1,
    /* The node executing the ingestor, or the connection to it, was lost. */
    NX_UNCERTAINTY_OWNER_LOST = 2,
    /* The session that carried the batch ended before its outcome arrived. */
    NX_UNCERTAINTY_SESSION_LOST = 3
} nx_submission_uncertainty;

/* What the server did with a settlement. */
typedef enum nx_settlement {
    NX_SETTLEMENT_CONFIRMED = 1,
    /* The reference is not the delivery's current attempt: it was retried, timed out or revoked. */
    NX_SETTLEMENT_STALE_REFERENCE = 2,
    NX_SETTLEMENT_WRONG_CONSUMER = 3,
    NX_SETTLEMENT_INVALID_REASON = 4,
    /* The consumer that received the delivery has ended. */
    NX_SETTLEMENT_CONSUMER_ENDED = 5
} nx_settlement;

/* ---- Errors ------------------------------------------------------------------------------- */

nx_error_kind nx_error_kind_of(const nx_error *error);
/* The error and every cause behind it, as one message. */
void nx_error_message(const nx_error *error, const uint8_t **message, size_t *message_len);
/* Whether the error names the execution reference of the command it concerns, and that
   reference when it does: an uncertain command, or a backup whose archive failed to download,
   which running the same execution again downloads while the server retains it. */
bool nx_error_execution_reference(const nx_error *error, const uint8_t **reference,
                                  size_t *reference_len);
/* Whether the error is the server's refusal to open a producer or a consumer, which is an
   NX_ERROR_REJECTED, and the refusal when it is. */
bool nx_error_open_refusal(const nx_error *error, nx_open_refusal *refusal);
void nx_error_free(nx_error *error);

/* ---- Cancellation ------------------------------------------------------------------------- */

nx_cancel *nx_cancel_new(void);
/* A token that also expires `deadline_millis` milliseconds from now. */
nx_error *nx_cancel_with_deadline(uint64_t deadline_millis, nx_cancel **out);
/* Cancels every call waiting on the token, now and later. Safe from any thread. */
void nx_cancel_trigger(const nx_cancel *cancel);
void nx_cancel_free(nx_cancel *cancel);

/* ---- Sessions ----------------------------------------------------------------------------- */

/* Opens a session on `server`, an http or https URI of a node's session endpoint. `domain` may
   be NULL for no selected domain. `username` and `password` are both NULL or both present. */
nx_error *nx_session_connect(const uint8_t *server, size_t server_len, const uint8_t *domain,
                             size_t domain_len, const uint8_t *username, size_t username_len,
                             const uint8_t *password, size_t password_len,
                             const nx_cancel *cancel, nx_session **out);
/* Releases the host's session, which ends once every producer, consumer and delivery of it is
   released too. Events, outcomes and batches read from it stay valid. */
void nx_session_free(nx_session *session);

/* Captures one command and its durable execution identity before anything is sent. */
nx_error *nx_session_prepare(nx_session *session, const uint8_t *query, size_t query_len,
                             const nx_cancel *cancel, nx_execution **out);
void nx_execution_reference(const nx_execution *execution, const uint8_t **reference,
                            size_t *reference_len);
void nx_execution_free(nx_execution *execution);

/* Runs a prepared command. Running the same execution again after an uncertain, cancelled or
   expired call recovers the command's outcome instead of running it twice. */
nx_error *nx_session_execute(nx_session *session, const nx_execution *execution,
                             const nx_cancel *cancel, nx_outcome **out);

/* Waits for the next event of any subscription the session holds. The wait continues across a
   lost session: the events of subscriptions restored or opened on the next session follow, and
   NX_ERROR_CLOSED is returned only when no server is known to open another session on. */
nx_error *nx_session_next_event(nx_session *session, const nx_cancel *cancel, nx_event **out);

/* Reads one bounded completion page for the full input at a UTF-8 byte cursor. `page_size` is
   1..100. Pass a returned continuation to read the next page; NULL starts a new search. */
nx_error *nx_session_suggest(const nx_session *session, const uint8_t *input, size_t input_len,
                             size_t cursor, uint16_t page_size, const uint8_t *continuation,
                             size_t continuation_len, const nx_cancel *cancel,
                             nx_suggestions **out);

/* Each candidate's edit range addresses the input passed to nx_session_suggest. Borrowed text
   pointers remain valid until nx_suggestions_free. A missing continuation returns false. */
nx_completion_status nx_suggestions_status(const nx_suggestions *suggestions);
size_t nx_suggestions_count(const nx_suggestions *suggestions);
nx_error *nx_suggestions_at(const nx_suggestions *suggestions, size_t index,
                             nx_completion_kind *kind, const uint8_t **value,
                             size_t *value_len, uint32_t *start, uint32_t *end,
                             const uint8_t **replacement, size_t *replacement_len);
bool nx_suggestions_continuation(const nx_suggestions *suggestions,
                                  const uint8_t **continuation, size_t *continuation_len);
void nx_suggestions_free(nx_suggestions *suggestions);

/* ---- Outcomes ----------------------------------------------------------------------------- */

nx_disposition nx_outcome_disposition(const nx_outcome *outcome);
void nx_outcome_message(const nx_outcome *outcome, const uint8_t **message, size_t *message_len);
bool nx_outcome_execution_reference(const nx_outcome *outcome, const uint8_t **reference,
                                    size_t *reference_len);
size_t nx_outcome_diagnostic_count(const nx_outcome *outcome);
/* One diagnostic. `has_span` says whether `start` and `end`, byte offsets into the query, were
   written. */
nx_error *nx_outcome_diagnostic(const nx_outcome *outcome, size_t index, const uint8_t **message,
                                size_t *message_len, bool *has_span, uint32_t *start,
                                uint32_t *end);
/* Whether the command opened a subscription, and its name and generation when it did. */
bool nx_outcome_subscription(const nx_outcome *outcome, const uint8_t **name, size_t *name_len,
                             uint64_t *generation);
/* Whether the command was a completed BACKUP, and the size and BLAKE3 digest of the archive the
   client verified and wrote to the file the statement names. `digest` points at 32 bytes. */
bool nx_outcome_backup(const nx_outcome *outcome, uint64_t *total_bytes, const uint8_t **digest,
                       size_t *digest_len);
/* Whether the command was a RESTORE that verified its archive, and what its report says: whether
   it was a DRY RUN, how many domains, completed resource versions and models it restores, and
   whether one of its steps failed, which leaves the steps before it applied. */
bool nx_outcome_restore(const nx_outcome *outcome, bool *dry_run, uint64_t *domains,
                        uint64_t *resource_versions, uint64_t *models, bool *failed);
/* The schema of the subscription the command opened. An outcome that opened none fails with
   NX_ERROR_INVALID_ARGUMENT. */
nx_error *nx_outcome_schema(const nx_outcome *outcome, nx_schema **out);
void nx_outcome_free(nx_outcome *outcome);

/* ---- Schemas ------------------------------------------------------------------------------ */

/* The fields of one part. An unbranched relay's branch key has none. */
nx_error *nx_schema_field_count(const nx_schema *schema, int32_t part, size_t *count);
nx_error *nx_schema_field(const nx_schema *schema, int32_t part, size_t index,
                          const uint8_t **name, size_t *name_len, nx_type *type, bool *nullable,
                          bool *sensitive);
/* How many levels the type of a field has: one for a scalar, and one more for every LIST or
   FIXED_LIST around its innermost element. */
nx_error *nx_schema_field_levels(const nx_schema *schema, int32_t part, size_t index,
                                 size_t *levels);
/* The type at one level of a field: level 0 is the type nx_schema_field reads, and every deeper
   level is the element of the list above it. `length` receives a FIXED_LIST level's element
   count and zero for every other level. A level past the field's innermost element fails with
   NX_ERROR_INVALID_ARGUMENT. */
nx_error *nx_schema_field_level(const nx_schema *schema, int32_t part, size_t index, size_t level,
                                nx_type *type, uint32_t *length);
/* Whether the relay is branched, and the branch's name when it is. An endpoint's schema is never
   branched. */
bool nx_schema_branch(const nx_schema *schema, const uint8_t **name, size_t *name_len);
void nx_schema_free(nx_schema *schema);

/* ---- Events ------------------------------------------------------------------------------- */

nx_event_kind nx_event_kind_of(const nx_event *event);
void nx_event_subscription(const nx_event *event, const uint8_t **name, size_t *name_len,
                           uint64_t *generation);
/* The rows a ROWS event carries, the rows a DELIVERY_LOST or ROWS_SKIPPED event counts, and zero
   for every other event. */
uint64_t nx_event_row_count(const nx_event *event);
/* Adds a reference. Every reference, including the one `nx_session_next_event` returned, is
   released once with `nx_event_release`; the event and its frame are freed with the last one. */
nx_event *nx_event_retain(nx_event *event);
void nx_event_release(nx_event *event);

/* The schema the rows of a ROWS event follow. This and every accessor below fail with NX_ERROR_TYPE
   for an event that carries no rows, and with NX_ERROR_INVALID_ARGUMENT for a part that is not an
   nx_part or a column the part does not have. */
nx_error *nx_event_schema(const nx_event *event, nx_schema **out);
/* Borrows the verified ServerMessage frame a ROWS event was decoded from. */
nx_error *nx_event_frame(const nx_event *event, const uint8_t **frame, size_t *frame_len);
/* Writes one nx_cell_state byte per cell of a column. `states_len` is the number of cells: the
   batch's rows, or 1 for the branch key. */
nx_error *nx_event_column_states(const nx_event *event, int32_t part, size_t column,
                                 uint8_t *states, size_t states_len);
/* Copies a fixed-width column in native byte order: 1 byte for U8, I8 and BOOL, 2 for U16 and
   I16, 4 for U32, I32 and F32, 8 for U64, I64, F64 and DATETIME. `values_len` is the cell count
   times that width. A null or redacted cell is written as zero bytes; its state tells it apart. */
nx_error *nx_event_column_fixed(const nx_event *event, int32_t part, size_t column, void *values,
                                size_t values_len);
/* Copies a STRING or BYTES column: `offsets` receives cell count + 1 offsets into `data`, and
   `data_len` the bytes the column needs. With `data` NULL only `data_len` is written, so a caller
   can size its buffer first; a `data_capacity` below `data_len` fails with
   NX_ERROR_INVALID_ARGUMENT after writing `data_len`. */
nx_error *nx_event_column_varlen(const nx_event *event, int32_t part, size_t column,
                                 uint64_t *offsets, size_t offsets_len, uint8_t *data,
                                 size_t data_capacity, size_t *data_len);
/* Borrows one STRING or BYTES value. A null or redacted cell fails with NX_ERROR_TYPE. */
nx_error *nx_event_cell_varlen(const nx_event *event, int32_t part, size_t row, size_t column,
                               const uint8_t **value, size_t *value_len);

/* ---- Domain clocks ------------------------------------------------------------------------ */

/* `ATTACH DOMAIN CLOCK;` and `DETACH DOMAIN CLOCK;` run through nx_session_prepare and
   nx_session_execute like any statement, for the session's selected domain. An attached session
   follows the domain's clock across every reconnection, until it detaches or the server ends the
   attachment.

   An attach that completes leaves the clock its reply reported for nx_session_domain_clock to
   read, before any event about the attachment. An attach the server refuses completes with
   NX_DISPOSITION_FAILED and the server's reason: the session already follows the clock, the domain
   does not exist on the serving node, or the request was refused. An attach whose session was lost
   is sent again on the next session the library opens, and the call fails when none opens. An
   attach that returns NX_ERROR_CANCELLED or NX_ERROR_DEADLINE may still have attached the session:
   executing the same nx_execution again is answered after the earlier attempt, completing or
   refused as already attached, and nx_session_domain_clock then tells whether the session follows
   the clock. */

/* Waits for the next event about the domain clocks the session follows. Events are coalesced per
   domain, so a caller that reads late receives the newest state and the newest tick of each domain
   rather than every change in between, and tick ids may skip. After an attach to a running paced
   clock, the first event is the tick its serving node accepted last, when it has accepted one, and
   nx_session_domain_clock reads the clock of its generation. A session that follows no clock waits
   until it attaches to one. */
nx_error *nx_session_next_clock_event(nx_session *session, const nx_cancel *cancel,
                                      nx_clock_event **out);

nx_clock_event_kind nx_clock_event_kind_of(const nx_clock_event *event);
/* The domain whose clock the event concerns. */
void nx_clock_event_domain(const nx_clock_event *event, const uint8_t **domain,
                           size_t *domain_len);
/* The START generation of a STATE or TICK event: the number of STARTs the domain has committed,
   zero before its first. This and every accessor below fail with NX_ERROR_TYPE for an event whose
   kind does not carry what they read. */
nx_error *nx_clock_event_generation(const nx_clock_event *event, uint64_t *generation);
/* The installation state a STATE event reports. */
nx_error *nx_clock_event_state(const nx_clock_event *event, nx_clock_state *state);
/* The committed clock of a STATE event in the PACED state: the tick period and the admission skew
   in nanoseconds, the mapping's logical origin and UTC anchor in signed nanoseconds since the Unix
   epoch, and its time rate. Any out-parameter may be NULL. */
nx_error *nx_clock_event_paced(const nx_clock_event *event, uint64_t *period_nanos,
                               uint64_t *skew_nanos, int64_t *logical_origin,
                               int64_t *utc_anchor, double *time_rate);
/* The progress a TICK event reports: the tick's id, which increases within a generation and may
   skip ids the clock authority coalesced; its logical boundary, the logical origin plus one period
   for every id before it; the clock authority's UTC observation of it; and the serving node's own
   logical reading taken as the frame was built, which anchors a host whose UTC differs from the
   cluster's. The instants are signed nanoseconds since the Unix epoch. Any out-parameter may be
   NULL. */
nx_error *nx_clock_event_tick(const nx_clock_event *event, uint64_t *tick_id,
                              int64_t *logical_boundary, int64_t *authority_utc,
                              int64_t *serving_logical);
/* Why the server ended the attachment an ENDED event reports. */
nx_error *nx_clock_event_end_reason(const nx_clock_event *event, nx_clock_end_reason *reason);
/* Adds a reference. Every reference, including the one nx_session_next_clock_event returned, is
   released once with nx_clock_event_release; the event is freed with the last one. */
nx_clock_event *nx_clock_event_retain(nx_clock_event *event);
void nx_clock_event_release(nx_clock_event *event);

/* Reads the clock of `domain` as the session last received it. It never blocks. After
   `ATTACH DOMAIN CLOCK;` completes it is the clock the attach reply carried: the domain's START
   generation and its installation on the serving node, so a host that attaches to a running clock
   knows its committed mapping before it uses a tick. It reflects every clock and tick the session
   has received since, including those of every event nx_session_next_clock_event has already
   returned about the domain, so it is never older than an event a host consumed, and it comes with
   the newest tick the session accepted for its generation. Once the session holding the
   attachment ends, which an NX_CLOCK_EVENT_INTERRUPTED event reports, it is the clock that session
   reported last, without a tick, until the restored attachment reports the clock again. `out` is
   set to NULL when the session follows no clock of the domain: before it attaches, when the server
   refused every attach, once a detach completes, and once the server ends the attachment, which an
   NX_CLOCK_EVENT_ENDED event reports. A domain name that is not valid fails with
   NX_ERROR_INVALID_ARGUMENT. */
nx_error *nx_session_domain_clock(const nx_session *session, const uint8_t *domain,
                                  size_t domain_len, nx_domain_clock **out);

/* A domain clock is one read of the session: it never changes, and a host reads the session again
   for a newer one. It stays valid after the session is freed. */

/* The domain whose clock was read. */
void nx_domain_clock_domain(const nx_domain_clock *clock, const uint8_t **domain,
                            size_t *domain_len);
/* The START generation: the number of STARTs the domain has committed, zero before its first. */
uint64_t nx_domain_clock_generation(const nx_domain_clock *clock);
/* The installation state of the generation on the serving node. */
nx_clock_state nx_domain_clock_state(const nx_domain_clock *clock);
/* The committed clock of a PACED clock, with nx_clock_event_paced's fields and units. Any other
   state fails with NX_ERROR_TYPE. Any out-parameter may be NULL. */
nx_error *nx_domain_clock_paced(const nx_domain_clock *clock, uint64_t *period_nanos,
                                uint64_t *skew_nanos, int64_t *logical_origin,
                                int64_t *utc_anchor, double *time_rate);
/* Whether the session holds an accepted tick of the clock's generation, and that tick, with
   nx_clock_event_tick's fields and units, when it does. Any out-parameter may be NULL. */
bool nx_domain_clock_tick(const nx_domain_clock *clock, uint64_t *tick_id,
                          int64_t *logical_boundary, int64_t *authority_utc,
                          int64_t *serving_logical);

/* The projections below are the ones the Rust client's attached clock answers with the arithmetic
   the ingestor admits by, so a host computes no mapping itself. They hold for the host's UTC: a host
   whose UTC is offset from the cluster's reads answers shifted by that offset times the rate.
   Instants are signed nanoseconds since the Unix epoch. A STOPPED or UNINSTALLED clock has no
   logical time, and every projection of it fails with NX_ERROR_TYPE; arithmetic that leaves the
   range of 64-bit nanoseconds fails with NX_ERROR_INVALID_ARGUMENT. */

/* The domain's logical time at the UTC instant `utc`: the committed mapping's projection of a
   PACED clock, rounded down to the nanosecond and never before the logical origin, and `utc`
   itself for an UNPACED one. */
nx_error *nx_domain_clock_logical_time_at(const nx_domain_clock *clock, int64_t utc,
                                          int64_t *logical);
/* How many nanoseconds after the UTC instant `utc` the domain's logical time reaches `target`:
   rounded up, so a host that waits it never arrives early, and zero once it has. */
nx_error *nx_domain_clock_wall_duration_until(const nx_domain_clock *clock, int64_t utc,
                                              int64_t target, uint64_t *wait_nanos);
/* The tick centers a `TIMESTAMP AT` ingestor of the domain admits events around at the UTC
   instant `utc`, reconstructed as the ingestor reconstructs them: one period apart, from the
   oldest it retains to the newest it has reached. An event is admitted within the skew of one of
   them. `has_window` is false for an UNPACED clock, whose ingestors admit every timestamp, and the
   centers are written only when it is true. Either center may be NULL. */
nx_error *nx_domain_clock_admission_window(const nx_domain_clock *clock, int64_t utc,
                                           bool *has_window, int64_t *earliest_center,
                                           int64_t *latest_center);
/* Whether a `TIMESTAMP AT` ingestor of the domain admits an event at `event` at the UTC instant
   `utc`. An UNPACED clock admits every event. */
nx_error *nx_domain_clock_admits(const nx_domain_clock *clock, int64_t utc, int64_t event,
                                 bool *admitted);
/* Adds a reference. Every reference, including the one nx_session_domain_clock returned, is
   released once with nx_domain_clock_release; the clock is freed with the last one. */
nx_domain_clock *nx_domain_clock_retain(nx_domain_clock *clock);
void nx_domain_clock_release(nx_domain_clock *clock);

/* ---- Expected fields ---------------------------------------------------------------------- */

/* The fields a host expects an endpoint to have, exactly: every name, its position, its type at
   every level, its nullability and its sensitivity. An open whose fields differ is refused with
   NX_OPEN_SCHEMA_MISMATCH. A field list is used from one thread at a time. */
nx_fields *nx_fields_new(void);
/* Adds a field whose type starts with `type`, an nx_type. A scalar type completes the field; a
   LIST or FIXED_LIST needs its element next, from nx_fields_element. `length` is a FIXED_LIST's
   element count, from 1 to INT32_MAX, and zero for every other type. Adding a field while the
   last one lacks its element fails with NX_ERROR_INVALID_ARGUMENT. */
nx_error *nx_fields_add(nx_fields *fields, const uint8_t *name, size_t name_len, int32_t type,
                        uint32_t length, bool nullable, bool sensitive);
/* Adds the element of the innermost list of the last field, with nx_fields_add's `type` and
   `length`. A field nests at most 32 lists. */
nx_error *nx_fields_element(nx_fields *fields, int32_t type, uint32_t length);
void nx_fields_free(nx_fields *fields);

/* ---- Producers ---------------------------------------------------------------------------- */

/* Opens a producer on the client ingestor `ingestor` of `domain`, which is named explicitly: the
   session's selected domain is not used, and a later USE does not move the producer. `batches`
   and `bytes` are the credit it asks for: how many batches, and bytes of their streams, may be
   outstanding at once, from a submission until the host takes its outcome. The library copies
   `fields`. A refusal fails with NX_ERROR_REJECTED, and nx_error_open_refusal reads why; nothing
   is left attached. A cancelled open that the server answers anyway releases what it opened.

   A producer follows its endpoint across sessions: after a session ends, the next call that needs
   the attachment opens a session and attaches again, but only while the endpoint keeps the START
   generation, schema, contract, policy and credit of this open. Otherwise the producer needs a
   new open, and its calls fail with NX_ERROR_REOPEN_REQUIRED. */
nx_error *nx_session_open_ingestor(const nx_session *session, const uint8_t *domain,
                                   size_t domain_len, const uint8_t *ingestor,
                                   size_t ingestor_len, const nx_fields *fields, uint32_t batches,
                                   uint64_t bytes, const nx_cancel *cancel, nx_producer **out);

/* What the open established, which the producer keeps across sessions: the schema of its
   batches; the domain's START generation; the 32-byte digest of the endpoint contract; the
   granted credit and the rows and bytes one batch may carry; and the ingestor's acknowledgement
   window, how many acknowledgements it lets be outstanding, its ACK TIMEOUT and the backoff the
   library applies before sending a temporarily refused batch again, in nanoseconds. Any
   out-parameter may be NULL. */
nx_error *nx_producer_schema(const nx_producer *producer, nx_schema **out);
uint64_t nx_producer_generation(const nx_producer *producer);
void nx_producer_contract(const nx_producer *producer, const uint8_t **digest,
                          size_t *digest_len);
void nx_producer_grant(const nx_producer *producer, uint32_t *batches, uint64_t *bytes,
                       uint32_t *max_batch_rows, uint64_t *max_batch_bytes);
void nx_producer_policy(const nx_producer *producer, nx_ack_window *window,
                        uint64_t *outstanding, uint64_t *ack_timeout_nanos,
                        uint64_t *retry_backoff_nanos, uint64_t *retry_max_backoff_nanos);
/* Whether the producer's batches are admitted right now. A producer without an attachment reads
   NX_ADMISSION_SUSPENDED. */
nx_admission nx_producer_admission(const nx_producer *producer);
nx_endpoint_state nx_producer_state(const nx_producer *producer);
/* Whether the producer needs a new open, and why when it does. `refusal` is written only for
   NX_REOPEN_REFUSED. */
bool nx_producer_reopen_reason(const nx_producer *producer, nx_reopen_reason *reason,
                               nx_open_refusal *refusal);

/* Waits for credit and submits `batch`, which must have exactly the producer's schema and at most
   its rows per batch, and writes the submission's identity once the producer holds the batch.
   The batch is written as its canonical stream, which the producer keeps, immutable, until the
   submission's outcome is taken or it is released; the host may release `batch` as soon as the
   call returns, and may submit it again. A batch of another schema or with too many rows fails
   with NX_ERROR_INVALID_ARGUMENT and sends nothing. A wait that is cancelled or expires before the
   producer holds the batch submits nothing; one that returns an identity has submitted it. While
   the producer is interrupted the call waits for its restoration. */
nx_error *nx_producer_submit(const nx_producer *producer, const nx_batch *batch,
                             const nx_cancel *cancel, uint64_t *submission);
/* The same for one canonical Arrow IPC stream the host wrote with its own Arrow tooling. The
   library copies `ipc` before it waits, so the host may reuse or free it as soon as the call
   returns; the server, not the library, checks it, and refuses a stream that is not the
   producer's canonical batch as NX_REFUSAL_INVALID_BATCH. */
nx_error *nx_producer_submit_ipc(const nx_producer *producer, const uint8_t *ipc, size_t ipc_len,
                                 const nx_cancel *cancel, uint64_t *submission);
/* Waits for a submission's terminal outcome and takes it, which returns its credit. A submission
   keeps its credit until its outcome is taken, so a host that stops taking outcomes stops being
   able to submit. A cancelled or expired wait leaves the submission and its outcome with the
   producer, and a later call takes it. An identity the producer does not hold, or holds no more,
   fails with NX_ERROR_INVALID_ARGUMENT. */
nx_error *nx_producer_rejoin(const nx_producer *producer, uint64_t submission,
                             const nx_cancel *cancel, nx_submission_outcome **out);
/* The submissions the producer holds, in submission order: writes `count`, the number it holds,
   and the identity and whether it has its outcome of the first `capacity` of them. Either buffer
   may be NULL. */
nx_error *nx_producer_pending(const nx_producer *producer, uint64_t *submissions, bool *resolved,
                              size_t capacity, size_t *count);
/* Lets go of a submission. One with its outcome returns it in `out` and its credit now; one
   without writes NULL, and returns its credit once its outcome arrives, which nobody reads. */
nx_error *nx_producer_release(const nx_producer *producer, uint64_t submission,
                              nx_submission_outcome **out);
/* Stops the producer's admission and waits until the server released it; every batch it sent
   has its outcome by then. A producer without an attachment, or closed already, returns at once.
   A cancelled close keeps releasing the attachment. */
nx_error *nx_producer_close(const nx_producer *producer, const nx_cancel *cancel);
/* Releases the producer, closing it without waiting when it is still open. Outcomes it had not
   handed out are not read by anyone. */
void nx_producer_free(nx_producer *producer);

/* ---- Submission outcomes ------------------------------------------------------------------ */

/* The library sends a batch again only after a suspended or busy refusal. It never sends again a
   batch whose outcome is a failure or unknown: replaying it is the host's decision, and it may
   duplicate the batch's effects. A batch that was sent when its session ended is
   NX_UNCERTAINTY_SESSION_LOST, and one that was waiting to be sent is NX_REFUSAL_PRODUCER_ENDED. */
nx_submission_result nx_submission_outcome_result(const nx_submission_outcome *outcome);
/* The server's bounded, non-sensitive description of the outcome; empty for a completed batch. */
void nx_submission_outcome_message(const nx_submission_outcome *outcome, const uint8_t **message,
                                   size_t *message_len);
/* The typed cause of an outcome. Each fails with NX_ERROR_TYPE for an outcome that does not carry
   it: a refusal for NOT_ADMITTED, a defect for NX_REFUSAL_INVALID_BATCH, a failure for
   PROCESSING_FAILED and an uncertainty for OUTCOME_UNKNOWN. */
nx_error *nx_submission_outcome_refusal(const nx_submission_outcome *outcome,
                                        nx_submission_refusal *refusal);
nx_error *nx_submission_outcome_defect(const nx_submission_outcome *outcome,
                                       nx_batch_defect *defect);
nx_error *nx_submission_outcome_failure(const nx_submission_outcome *outcome,
                                        nx_processing_failure *failure);
nx_error *nx_submission_outcome_uncertainty(const nx_submission_outcome *outcome,
                                            nx_submission_uncertainty *uncertainty);
void nx_submission_outcome_free(nx_submission_outcome *outcome);

/* ---- Consumers ---------------------------------------------------------------------------- */

/* Opens a competing consumer of the client emitter `emitter` of `domain`, named explicitly as for
   a producer. `batches` and `bytes` are the credit it asks for, and `bytes` must hold one batch
   of the emitter's maximum size. Refusals, cancellation and following the endpoint across
   sessions are a producer's. */
nx_error *nx_session_subscribe_emitter(const nx_session *session, const uint8_t *domain,
                                       size_t domain_len, const uint8_t *emitter,
                                       size_t emitter_len, const nx_fields *fields,
                                       uint32_t batches, uint64_t bytes, const nx_cancel *cancel,
                                       nx_consumer **out);

/* What the open established, with the producer accessors' fields and units. `max_batch_rows` and
   `max_batch_bytes` are the emitter's BATCH MAX MESSAGES and MAX SIZE. */
nx_error *nx_consumer_schema(const nx_consumer *consumer, nx_schema **out);
uint64_t nx_consumer_generation(const nx_consumer *consumer);
void nx_consumer_contract(const nx_consumer *consumer, const uint8_t **digest,
                          size_t *digest_len);
void nx_consumer_grant(const nx_consumer *consumer, uint32_t *batches, uint64_t *bytes,
                       uint32_t *max_batch_rows, uint64_t *max_batch_bytes);
void nx_consumer_policy(const nx_consumer *consumer, nx_ack_window *window, uint64_t *outstanding,
                        uint64_t *ack_timeout_nanos, uint64_t *retry_backoff_nanos,
                        uint64_t *retry_max_backoff_nanos);
nx_endpoint_state nx_consumer_state(const nx_consumer *consumer);
/* As nx_producer_reopen_reason. */
bool nx_consumer_reopen_reason(const nx_consumer *consumer, nx_reopen_reason *reason,
                               nx_open_refusal *refusal);

/* Waits for the next delivery. After the consumer's attachment ends, the next call fails once
   with NX_ERROR_INTERRUPTED, and the one after it restores the attachment and reads on. A
   cancelled or expired wait leaves its read with the consumer, and a later call receives what it
   reads, so no delivery waits for its ACK TIMEOUT because a wait was abandoned. A closed consumer
   fails with NX_ERROR_CLOSED, and one that needs a new open with NX_ERROR_REOPEN_REQUIRED. */
nx_error *nx_consumer_next(const nx_consumer *consumer, const nx_cancel *cancel,
                           nx_delivery **out);
/* Closes the consumer and waits until the server released its attachment, which revokes every
   delivery it has not settled. Its other behavior is nx_producer_close's. */
nx_error *nx_consumer_close(const nx_consumer *consumer, const nx_cancel *cancel);
/* Releases the consumer, closing it without waiting when it is still open. */
void nx_consumer_free(nx_consumer *consumer);

/* ---- Deliveries --------------------------------------------------------------------------- */

/* One delivered attempt of a batch. Releasing a delivery settles nothing: an attempt nobody
   settles stays with its consumer until the emitter's ACK TIMEOUT or the end of the consumer's
   attachment, and is then delivered again. A retried, timed-out or revoked attempt comes again
   with the same identity and a new reference, possibly to another consumer, so a host keys its
   effects by the identity. */

/* The identity every attempt of the batch shares, and this attempt's reference: 16 bytes each. */
void nx_delivery_identity(const nx_delivery *delivery, const uint8_t **identity,
                          size_t *identity_len);
void nx_delivery_reference(const nx_delivery *delivery, const uint8_t **reference,
                           size_t *reference_len);
/* The relay the batch's rows came from. */
void nx_delivery_source_relay(const nx_delivery *delivery, const uint8_t **name,
                              size_t *name_len);
/* Whether the rows came from a concrete branch, and its opaque 32-byte fingerprint when they did.
   It carries no branch key value. */
bool nx_delivery_branch_fingerprint(const nx_delivery *delivery, const uint8_t **fingerprint,
                                    size_t *fingerprint_len);
/* How many rows the batch carries. */
uint32_t nx_delivery_members(const nx_delivery *delivery);
/* The domain's time when the batch was prepared, in signed nanoseconds since the Unix epoch. */
int64_t nx_delivery_execution_now(const nx_delivery *delivery);
/* Borrows the canonical Arrow IPC stream the attempt carried, without a copy. */
void nx_delivery_ipc(const nx_delivery *delivery, const uint8_t **ipc, size_t *ipc_len);
/* The batch the stream holds, decoded the first time it is asked for and held to the consumer's
   schema and to the rows nx_delivery_members counts; a stream that differs fails with
   NX_ERROR_PROTOCOL. Every call returns a new reference to the same batch. */
nx_error *nx_delivery_batch(const nx_delivery *delivery, nx_batch **out);

/* Acknowledges the attempt: its batch is settled, and an attached producer's submission completes
   once every output of it is. Only this attempt's current reference settles it; the settlement
   reads what the server did. A delivery whose attachment ended fails with NX_ERROR_REJECTED and
   sends nothing; one whose answer the session lost fails with NX_ERROR_UNCERTAIN; a cancelled or
   expired wait may still have settled the attempt, and settling it again reads the result the
   server retains, or NX_SETTLEMENT_STALE_REFERENCE. */
nx_error *nx_delivery_ack(const nx_delivery *delivery, const nx_cancel *cancel,
                          nx_settlement *settlement);
/* Asks the server to deliver the batch again, unchanged, after the emitter's retry backoff. */
nx_error *nx_delivery_retry(const nx_delivery *delivery, const nx_cancel *cancel,
                            nx_settlement *settlement);
/* Rejects the batch for good: every row follows the emitter's ON MESSAGE ERROR policy. `reason` is
   UTF-8, 1 to 1024 bytes, and must not hold sensitive values; another fails with
   NX_ERROR_INVALID_ARGUMENT and sends nothing. */
nx_error *nx_delivery_reject(const nx_delivery *delivery, const uint8_t *reason,
                             size_t reason_len, const nx_cancel *cancel,
                             nx_settlement *settlement);
/* Adds a reference. Every reference, including the one nx_consumer_next returned, is released once
   with nx_delivery_release, on any thread; the delivery, its stream and its batch's share of it
   are freed with the last one. */
nx_delivery *nx_delivery_retain(nx_delivery *delivery);
void nx_delivery_release(nx_delivery *delivery);

/* ---- Batches ------------------------------------------------------------------------------ */

/* A batch is immutable. A column is read one level of its type at a time: level 0 holds one cell
   per row; a LIST or FIXED_LIST level holds lists whose elements are the cells of the next level;
   and the innermost level holds the values. Only a row can be null: list elements never are. The
   bytes a null row holds at any level mean nothing, and its state tells it apart. Every copy
   writes one whole level in one call, and a level's offsets start at zero. Accessors fail with
   NX_ERROR_INVALID_ARGUMENT for a column or level past the batch's, or a buffer of another size,
   and with NX_ERROR_TYPE for a level whose type does not hold what they read. */

size_t nx_batch_row_count(const nx_batch *batch);
/* The schema of the batch's endpoint, with every field's nullability and sensitivity. */
nx_error *nx_batch_schema(const nx_batch *batch, nx_schema **out);
/* Borrows the batch's canonical Arrow IPC stream: the one its delivery carried, or, for a batch a
   host built, the one the library writes the first time it is read. */
nx_error *nx_batch_ipc(const nx_batch *batch, const uint8_t **ipc, size_t *ipc_len);
/* How many cells one level of a column holds. */
nx_error *nx_batch_cells(const nx_batch *batch, size_t column, size_t level, size_t *cells);
/* Writes one nx_cell_state byte per row of a column: NX_CELL_VALUE or NX_CELL_NULL. */
nx_error *nx_batch_states(const nx_batch *batch, size_t column, uint8_t *states,
                          size_t states_len);
/* Copies the offsets of the lists of a LIST level into the next level: one per list and a final
   end. */
nx_error *nx_batch_offsets(const nx_batch *batch, size_t column, size_t level, uint64_t *offsets,
                           size_t offsets_len);
/* Copies the fixed-width values of a column's innermost level, with nx_event_column_fixed's widths
   and native byte order. */
nx_error *nx_batch_fixed(const nx_batch *batch, size_t column, size_t level, void *values,
                         size_t values_len);
/* Copies the STRING or BYTES values of a column's innermost level, as nx_event_column_varlen
   copies a column: with `data` NULL only `data_len` is written. */
nx_error *nx_batch_varlen(const nx_batch *batch, size_t column, size_t level, uint64_t *offsets,
                          size_t offsets_len, uint8_t *data, size_t data_capacity,
                          size_t *data_len);
/* Adds a reference. Every reference, including the one nx_delivery_batch or
   nx_batch_builder_finish returned, is released once with nx_batch_release, on any thread. */
nx_batch *nx_batch_retain(nx_batch *batch);
void nx_batch_release(nx_batch *batch);

/* ---- Batch builders ----------------------------------------------------------------------- */

/* Builds a batch of `rows` rows of a schema's row fields, such as a producer's. A builder copies
   every buffer before the call that received it returns, so a host may reuse or free its
   buffers at once, and it converts nothing: a value takes exactly its type's width, a BOOL is 0
   or 1, and a STRING is UTF-8. A builder is used from one thread at a time. */
nx_error *nx_batch_builder_new(const nx_schema *schema, size_t rows, nx_batch_builder **out);
/* Sets whether each row of a column holds a value: one NX_CELL_VALUE or NX_CELL_NULL byte per row.
   Unset, every row holds one. A null row of a field that is not nullable fails with
   NX_ERROR_INVALID_ARGUMENT. */
nx_error *nx_batch_builder_states(nx_batch_builder *builder, size_t column, const uint8_t *states,
                                  size_t states_len);
/* Sets the offsets of the lists of a LIST level into the next level: one per list and a final
   end, starting at zero and never decreasing. A FIXED_LIST level needs none. */
nx_error *nx_batch_builder_offsets(nx_batch_builder *builder, size_t column, size_t level,
                                   const uint64_t *offsets, size_t offsets_len);
/* Sets the fixed-width values of a column's innermost level, in native byte order. */
nx_error *nx_batch_builder_fixed(nx_batch_builder *builder, size_t column, size_t level,
                                 const void *values, size_t values_len);
/* Sets the STRING or BYTES values of a column's innermost level: one offset per value and a final
   end, starting at zero, into `data`, which the last offset ends. */
nx_error *nx_batch_builder_varlen(nx_batch_builder *builder, size_t column, size_t level,
                                  const uint64_t *offsets, size_t offsets_len,
                                  const uint8_t *data, size_t data_len);
/* Finishes the batch from every column the host set. A missing level, values that do not fill
   their level, or text that is not UTF-8 fail with NX_ERROR_INVALID_ARGUMENT. The builder is then
   empty, and may be filled again for another batch of the same rows. */
nx_error *nx_batch_builder_finish(nx_batch_builder *builder, nx_batch **out);
void nx_batch_builder_free(nx_batch_builder *builder);

#ifdef __cplusplus
}
#endif

#endif /* NERVIX_CLIENT_H */
