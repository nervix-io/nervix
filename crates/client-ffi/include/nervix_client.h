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
 *   object it was read from is released, and never longer.
 *
 * Threads
 *   The library calls no host code: it has no callbacks, so no host function ever runs on a thread
 *   the library owns. Every blocking call runs on the calling thread until it completes, fails,
 *   or is cancelled. A session may be used from several threads at once. Releasing an object while
 *   another thread still uses it is the caller's error; releasing an event, a clock event or a
 *   domain clock while another thread holds a retained reference to it is not.
 *
 * Cancellation and deadlines
 *   Every blocking call accepts an optional `nx_cancel`. Triggering it, from any thread, makes the
 *   call return NX_ERROR_CANCELLED; a token created with a deadline makes the call return
 *   NX_ERROR_DEADLINE once the deadline passes. Cancelling a wait never rolls back work the server
 *   already admitted: a cancelled command keeps its execution reference, and executing the same
 *   `nx_execution` again recovers its outcome.
 *
 * Bulk data
 *   Row events keep the verified frame they were decoded from. `nx_event_frame` borrows those
 *   bytes without copying them. Column accessors copy one whole column into caller-provided
 *   memory in a single call, so reading a batch costs one call per column rather than one per
 *   value. `nx_event_cell_varlen` borrows one string or bytes value without copying it.
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

/* What kind of failure an `nx_error` reports. */
typedef enum nx_error_kind {
    /* An argument was missing, not UTF-8, out of range, or too small for the result. */
    NX_ERROR_INVALID_ARGUMENT = 1,
    /* No session could be opened with the servers and credentials given. */
    NX_ERROR_CONNECT = 2,
    /* The session failed after it was opened. */
    NX_ERROR_TRANSPORT = 3,
    /* Admitted work may have taken effect, and its outcome is not known. The error carries the
       execution reference; executing the same `nx_execution` again recovers the outcome. */
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
    /* The session ended and cannot be recovered. */
    NX_ERROR_CLOSED = 11
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

/* The type of a schema field. List fields report their shape; their values are read from the
   frame with a FlatBuffers reader. */
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

/* ---- Errors ------------------------------------------------------------------------------- */

nx_error_kind nx_error_kind_of(const nx_error *error);
/* The error and every cause behind it, as one message. */
void nx_error_message(const nx_error *error, const uint8_t **message, size_t *message_len);
/* Whether the error names the execution reference of the command it concerns, and that
   reference when it does: an uncertain command, or a backup whose archive failed to download,
   which running the same execution again downloads while the server retains it. */
bool nx_error_execution_reference(const nx_error *error, const uint8_t **reference,
                                  size_t *reference_len);
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
/* Ends the session. Events and outcomes read from it stay valid. */
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
/* Whether the relay is branched, and the branch's name when it is. */
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

#ifdef __cplusplus
}
#endif

#endif /* NERVIX_CLIENT_H */
