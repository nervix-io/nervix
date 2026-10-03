/*
 * The C probe of the shared Rust binding.
 *
 * It reads its target from the NERVIX_PROBE_* environment, runs an operation, a failing command,
 * a subscription and its closure through nervix_client.h, and prints the conformance report the
 * scenario compares. Every column is copied in one call; every string and bytes value is also
 * borrowed and compared with its copy. Run as `c-probe clock`, it attaches to the domain's running
 * clock instead, reads the clock the attach reported before its first tick, follows the generation
 * a STOP and START begin and the attachment restored after its session ends, and detaches. Run as
 * `c-probe io`, it publishes typed batches through a client ingestor, built column by column from
 * buffers it overwrites as soon as each call returns, and reads, retries, rejects and acknowledges
 * their output through a client emitter, across the session the scenario cuts.
 */

#define _POSIX_C_SOURCE 200809L

#include <inttypes.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "nervix_client.h"

#define ROWS_DEADLINE_MILLIS 120000
#define CLOCK_DEADLINE_MILLIS 120000

static void fail(const char *what) {
    fprintf(stderr, "probe failed: %s\n", what);
    exit(1);
}

/* Reports a failed call and exits; returns for a successful one. */
static void check(nx_error *error, const char *what) {
    if (error == NULL) {
        return;
    }
    const uint8_t *message = NULL;
    size_t message_len = 0;
    nx_error_message(error, &message, &message_len);
    fprintf(stderr, "probe failed: %s: kind %d: %.*s\n", what, (int)nx_error_kind_of(error),
            (int)message_len, (const char *)message);
    nx_error_free(error);
    exit(1);
}

static const char *env(const char *name) {
    const char *value = getenv(name);
    if (value == NULL) {
        fprintf(stderr, "probe failed: %s is not set\n", name);
        exit(1);
    }
    return value;
}

static const uint8_t *text(const char *value) { return (const uint8_t *)value; }

static const char *disposition_name(nx_disposition disposition) {
    switch (disposition) {
    case NX_DISPOSITION_COMPLETED: return "completed";
    case NX_DISPOSITION_FAILED: return "failed";
    case NX_DISPOSITION_NOT_LEADER: return "not_leader";
    case NX_DISPOSITION_TRANSACTION_DETACHED: return "transaction_detached";
    case NX_DISPOSITION_TRANSACTION_TAKEN_OVER: return "transaction_taken_over";
    case NX_DISPOSITION_OUTCOME_UNKNOWN: return "outcome_unknown";
    case NX_DISPOSITION_EXECUTION_REFERENCE_CONFLICT: return "execution_reference_conflict";
    case NX_DISPOSITION_EXECUTION_REFERENCE_EXPIRED: return "execution_reference_expired";
    case NX_DISPOSITION_PREVIEW_STALE: return "preview_stale";
    }
    fail("unknown disposition");
    return NULL;
}

static const char *type_name(nx_type type) {
    switch (type) {
    case NX_TYPE_U8: return "U8";
    case NX_TYPE_I8: return "I8";
    case NX_TYPE_U16: return "U16";
    case NX_TYPE_I16: return "I16";
    case NX_TYPE_U32: return "U32";
    case NX_TYPE_I32: return "I32";
    case NX_TYPE_U64: return "U64";
    case NX_TYPE_I64: return "I64";
    case NX_TYPE_F32: return "F32";
    case NX_TYPE_F64: return "F64";
    case NX_TYPE_BOOL: return "BOOL";
    case NX_TYPE_STRING: return "STRING";
    case NX_TYPE_BYTES: return "BYTES";
    case NX_TYPE_DATETIME: return "DATETIME";
    case NX_TYPE_FIXED_LIST: return "FIXED_LIST";
    case NX_TYPE_LIST: return "LIST";
    }
    fail("unknown type");
    return NULL;
}

static size_t fixed_width(nx_type type) {
    switch (type) {
    case NX_TYPE_U8: case NX_TYPE_I8: case NX_TYPE_BOOL: return 1;
    case NX_TYPE_U16: case NX_TYPE_I16: return 2;
    case NX_TYPE_U32: case NX_TYPE_I32: case NX_TYPE_F32: return 4;
    case NX_TYPE_U64: case NX_TYPE_I64: case NX_TYPE_F64: case NX_TYPE_DATETIME: return 8;
    default: return 0;
    }
}

/* Prepares and runs one command. */
static nx_outcome *execute(nx_session *session, const char *query) {
    nx_execution *execution = NULL;
    check(nx_session_prepare(session, text(query), strlen(query), NULL, &execution), "prepare");
    nx_outcome *outcome = NULL;
    check(nx_session_execute(session, execution, NULL, &outcome), "execute");
    nx_execution_free(execution);
    return outcome;
}

typedef struct field {
    const uint8_t *name;
    size_t name_len;
    nx_type type;
    bool nullable;
    bool sensitive;
} field;

typedef struct fields {
    field *items;
    size_t count;
} fields;

static fields read_fields(const nx_schema *schema, nx_part part) {
    fields result = {NULL, 0};
    check(nx_schema_field_count(schema, part, &result.count), "field count");
    result.items = calloc(result.count == 0 ? 1 : result.count, sizeof(field));
    for (size_t index = 0; index < result.count; index++) {
        field *item = &result.items[index];
        check(nx_schema_field(schema, part, index, &item->name, &item->name_len, &item->type,
                              &item->nullable, &item->sensitive),
              "field");
    }
    return result;
}

static void print_field(const char *prefix, const field *item) {
    printf("%s %.*s %s %s %s\n", prefix, (int)item->name_len, (const char *)item->name,
           type_name(item->type), item->nullable ? "nullable" : "required",
           item->sensitive ? "sensitive" : "public");
}

/* Appends the rendering of every cell of `part` to one growing line per row. */
typedef struct line {
    char *text;
    size_t len;
} line;

__attribute__((format(printf, 2, 3))) static void append(line *target, const char *format, ...) {
    va_list arguments;
    va_start(arguments, format);
    char buffer[512];
    int written = vsnprintf(buffer, sizeof buffer, format, arguments);
    va_end(arguments);
    if (written < 0 || (size_t)written >= sizeof buffer) {
        fail("a rendered cell does not fit its buffer");
    }
    target->text = realloc(target->text, target->len + (size_t)written + 1);
    memcpy(target->text + target->len, buffer, (size_t)written + 1);
    target->len += (size_t)written;
}

static void append_hex(line *target, const uint8_t *bytes, size_t len) {
    for (size_t index = 0; index < len; index++) {
        append(target, "%02x", bytes[index]);
    }
}

static void render(const nx_event *event, nx_part part, const fields *columns, size_t cells,
                   line *lines) {
    for (size_t column = 0; column < columns->count; column++) {
        const field *item = &columns->items[column];
        uint8_t *states = malloc(cells);
        check(nx_event_column_states(event, part, column, states, cells), "states");
        size_t width = fixed_width(item->type);
        uint8_t *values = NULL;
        uint64_t *offsets = NULL;
        uint8_t *data = NULL;
        if (width > 0) {
            values = malloc(cells * width);
            check(nx_event_column_fixed(event, part, column, values, cells * width), "fixed");
        } else if (item->type == NX_TYPE_STRING || item->type == NX_TYPE_BYTES) {
            size_t data_len = 0;
            check(nx_event_column_varlen(event, part, column, NULL, 0, NULL, 0, &data_len),
                  "varlen size");
            offsets = malloc((cells + 1) * sizeof(uint64_t));
            data = malloc(data_len == 0 ? 1 : data_len);
            check(nx_event_column_varlen(event, part, column, offsets, cells + 1, data,
                                         data_len == 0 ? 1 : data_len, &data_len),
                  "varlen");
        } else {
            fail("list columns are read from the frame");
        }
        for (size_t row = 0; row < cells; row++) {
            line *target = &lines[row];
            append(target, "%s%.*s=", target->len == 0 ? "" : " ", (int)item->name_len,
                   (const char *)item->name);
            if (states[row] == NX_CELL_NULL) {
                append(target, "null");
                continue;
            }
            if (states[row] == NX_CELL_REDACTED) {
                append(target, "redacted");
                continue;
            }
            if (states[row] != NX_CELL_VALUE) {
                fail("unknown cell state");
            }
            const uint8_t *cell = values == NULL ? NULL : values + row * width;
            switch (item->type) {
            case NX_TYPE_U8: append(target, "u8:%" PRIu8, *(const uint8_t *)cell); break;
            case NX_TYPE_I8: append(target, "i8:%" PRId8, *(const int8_t *)cell); break;
            case NX_TYPE_U16: { uint16_t v; memcpy(&v, cell, 2); append(target, "u16:%" PRIu16, v); break; }
            case NX_TYPE_I16: { int16_t v; memcpy(&v, cell, 2); append(target, "i16:%" PRId16, v); break; }
            case NX_TYPE_U32: { uint32_t v; memcpy(&v, cell, 4); append(target, "u32:%" PRIu32, v); break; }
            case NX_TYPE_I32: { int32_t v; memcpy(&v, cell, 4); append(target, "i32:%" PRId32, v); break; }
            case NX_TYPE_U64: { uint64_t v; memcpy(&v, cell, 8); append(target, "u64:%" PRIu64, v); break; }
            case NX_TYPE_I64: { int64_t v; memcpy(&v, cell, 8); append(target, "i64:%" PRId64, v); break; }
            case NX_TYPE_F32: { uint32_t v; memcpy(&v, cell, 4); append(target, "f32:%08" PRIx32, v); break; }
            case NX_TYPE_F64: { uint64_t v; memcpy(&v, cell, 8); append(target, "f64:%016" PRIx64, v); break; }
            case NX_TYPE_BOOL: append(target, "bool:%s", *cell != 0 ? "true" : "false"); break;
            case NX_TYPE_DATETIME: { int64_t v; memcpy(&v, cell, 8); append(target, "datetime:%" PRId64, v); break; }
            case NX_TYPE_STRING:
            case NX_TYPE_BYTES: {
                const uint8_t *copied = data + offsets[row];
                size_t copied_len = (size_t)(offsets[row + 1] - offsets[row]);
                const uint8_t *borrowed = NULL;
                size_t borrowed_len = 0;
                check(nx_event_cell_varlen(event, part, row, column, &borrowed, &borrowed_len),
                      "borrowed cell");
                if (borrowed_len != copied_len ||
                    (copied_len > 0 && memcmp(borrowed, copied, copied_len) != 0)) {
                    fail("a copied value differs from the same value borrowed from the frame");
                }
                append(target, "%s:", item->type == NX_TYPE_STRING ? "str" : "bytes");
                append_hex(target, copied, copied_len);
                break;
            }
            default: fail("unexpected column type");
            }
        }
        free(states);
        free(values);
        free(offsets);
        free(data);
    }
}

/* The report lines of a rows event, concatenated with newlines. */
static char *row_lines(const nx_event *event, const fields *row_fields, const fields *key_fields) {
    size_t count = (size_t)nx_event_row_count(event);
    line key = {NULL, 0};
    append(&key, "%s", "");
    if (key_fields->count > 0) {
        render(event, NX_PART_BRANCH_KEY, key_fields, 1, &key);
    }
    line *rows = calloc(count, sizeof(line));
    for (size_t row = 0; row < count; row++) {
        append(&rows[row], "%s", "");
    }
    render(event, NX_PART_ROWS, row_fields, count, rows);
    line result = {NULL, 0};
    append(&result, "%s", "");
    for (size_t row = 0; row < count; row++) {
        append(&result, "ROW [%s] %s\n", key.text, rows[row].text);
        free(rows[row].text);
    }
    free(rows);
    free(key.text);
    return result.text;
}

typedef struct waiter {
    nx_session *session;
    nx_cancel *cancel;
    nx_error *error;
} waiter;

static void *wait_for_event(void *argument) {
    waiter *state = argument;
    nx_event *event = NULL;
    state->error = nx_session_next_event(state->session, state->cancel, &event);
    if (event != NULL) {
        nx_event_release(event);
    }
    return NULL;
}

static void expect_kind(nx_error *error, nx_error_kind kind, const char *what) {
    if (error == NULL || nx_error_kind_of(error) != kind) {
        fail(what);
    }
    nx_error_free(error);
}

static void check_cancellation(nx_session *session) {
    waiter state = {session, nx_cancel_new(), NULL};
    pthread_t thread;
    if (pthread_create(&thread, NULL, wait_for_event, &state) != 0) {
        fail("starting the waiter thread");
    }
    struct timespec pause = {0, 100 * 1000 * 1000};
    nanosleep(&pause, NULL);
    nx_cancel_trigger(state.cancel);
    pthread_join(thread, NULL);
    expect_kind(state.error, NX_ERROR_CANCELLED, "a cancelled wait did not report CANCELLED");
    nx_cancel_free(state.cancel);

    nx_cancel *deadline = NULL;
    check(nx_cancel_with_deadline(50, &deadline), "deadline token");
    nx_event *event = NULL;
    expect_kind(nx_session_next_event(session, deadline, &event), NX_ERROR_DEADLINE,
                "an expired wait did not report DEADLINE");
    nx_cancel_free(deadline);

    nx_cancel *cancelled = nx_cancel_new();
    nx_cancel_trigger(cancelled);
    const char *query = "SHOW DOMAINS;";
    nx_execution *execution = NULL;
    check(nx_session_prepare(session, text(query), strlen(query), NULL, &execution), "prepare");
    nx_outcome *outcome = NULL;
    nx_error *error = nx_session_execute(session, execution, cancelled, &outcome);
    const uint8_t *reference = NULL;
    size_t reference_len = 0;
    if (error == NULL || nx_error_kind_of(error) != NX_ERROR_CANCELLED ||
        !nx_error_execution_reference(error, &reference, &reference_len) || reference_len == 0) {
        fail("a cancelled command did not report CANCELLED with its execution reference");
    }
    nx_error_free(error);
    nx_execution_free(execution);
    nx_cancel_free(cancelled);
}

static const char *clock_kind_name(nx_clock_event_kind kind) {
    switch (kind) {
    case NX_CLOCK_EVENT_STATE: return "STATE";
    case NX_CLOCK_EVENT_TICK: return "TICK";
    case NX_CLOCK_EVENT_ENDED: return "ENDED";
    case NX_CLOCK_EVENT_INTERRUPTED: return "INTERRUPTED";
    case NX_CLOCK_EVENT_RESTORATION_FAILED: return "RESTORATION_FAILED";
    }
    fail("unknown clock event kind");
    return NULL;
}

/* Fails unless a clock event concerns `domain`. */
static void expect_domain(const nx_clock_event *event, const char *domain) {
    const uint8_t *name = NULL;
    size_t name_len = 0;
    nx_clock_event_domain(event, &name, &name_len);
    if (name_len != strlen(domain) || memcmp(name, domain, name_len) != 0) {
        fail("a clock event arrived for another domain");
    }
}

/* The committed clock of a paced generation. */
typedef struct paced_clock {
    uint64_t generation;
    uint64_t period;
    uint64_t skew;
    int64_t origin;
    int64_t anchor;
    double rate;
} paced_clock;

/* The progress a tick event reports. */
typedef struct tick {
    uint64_t generation;
    uint64_t id;
    int64_t boundary;
    int64_t serving_logical;
} tick;

static paced_clock event_paced(const nx_clock_event *event) {
    paced_clock clock = {0, 0, 0, 0, 0, 0.0};
    check(nx_clock_event_generation(event, &clock.generation), "clock generation");
    check(nx_clock_event_paced(event, &clock.period, &clock.skew, &clock.origin, &clock.anchor,
                               &clock.rate),
          "paced clock");
    return clock;
}

static paced_clock read_paced(const nx_domain_clock *read) {
    paced_clock clock = {nx_domain_clock_generation(read), 0, 0, 0, 0, 0.0};
    check(nx_domain_clock_paced(read, &clock.period, &clock.skew, &clock.origin, &clock.anchor,
                                &clock.rate),
          "paced domain clock");
    return clock;
}

static bool same_clock(const paced_clock *left, const paced_clock *right) {
    return left->generation == right->generation && left->period == right->period &&
           left->skew == right->skew && left->origin == right->origin &&
           left->anchor == right->anchor && left->rate == right->rate;
}

/* The report line of a paced clock, which the caller frees, with `prefix` naming where it was read.
   The UTC anchor depends on when the scenario's START committed, so it is read but not reported. */
static char *paced_line(const char *prefix, const paced_clock *clock, const char *domain) {
    uint64_t rate_bits = 0;
    memcpy(&rate_bits, &clock->rate, sizeof rate_bits);
    line result = {NULL, 0};
    append(&result,
           "%s domain=%s generation=%" PRIu64 " state=paced period=%" PRIu64 " skew=%" PRIu64
           " origin=%" PRId64 " rate=f64:%016" PRIx64,
           prefix, domain, clock->generation, clock->period, clock->skew, clock->origin, rate_bits);
    return result.text;
}

static tick event_tick(const nx_clock_event *event) {
    tick progress = {0, 0, 0, 0};
    check(nx_clock_event_generation(event, &progress.generation), "tick generation");
    /* The authority's UTC observation depends on when the tick was accepted, so it is not read. */
    check(nx_clock_event_tick(event, &progress.id, &progress.boundary, NULL,
                              &progress.serving_logical),
          "tick");
    return progress;
}

/* Holds a tick to a clock: the same generation, a boundary of the logical origin plus one period
   for every id before it, and a serving node's reading that never precedes the origin. */
static void check_tick(const paced_clock *clock, const tick *progress) {
    if (progress->generation != clock->generation) {
        fail("a tick of one generation followed the state of another");
    }
    uint64_t offset = 0;
    int64_t expected = 0;
    if (progress->id == 0 || __builtin_mul_overflow(progress->id - 1, clock->period, &offset) ||
        offset > (uint64_t)INT64_MAX ||
        __builtin_add_overflow(clock->origin, (int64_t)offset, &expected) ||
        progress->boundary != expected) {
        fail("a tick's boundary is not the origin plus one period for every id before it");
    }
    if (progress->serving_logical < clock->origin) {
        fail("the serving node's logical reading precedes the logical origin");
    }
}

/* The report line of a tick a clock holds, which the caller frees. */
static char *tick_line(const paced_clock *clock, const tick *progress, const char *domain) {
    check_tick(clock, progress);
    line result = {NULL, 0};
    append(&result, "TICK domain=%s generation=%" PRIu64 " boundary=origin+(id-1)*period", domain,
           progress->generation);
    return result.text;
}

/* An instant as the report names it: `origin` for the logical origin, the instant otherwise. */
static void append_relative(line *target, const paced_clock *clock, int64_t instant) {
    if (instant == clock->origin) {
        append(target, "origin");
    } else {
        append(target, "%" PRId64, instant);
    }
}

/* The report line of the projections of `read`, which holds `clock`, at its own UTC anchor, which
   the caller frees: the logical time there, the wait for the next tick center, the admission
   window, and whether an event at the skew's edge and one nanosecond past it are admitted. */
static char *projection_line(const paced_clock *clock, const nx_domain_clock *read,
                             const char *domain) {
    int64_t next_center = 0;
    int64_t edge = 0;
    int64_t beyond = 0;
    if (clock->period > (uint64_t)INT64_MAX || clock->skew > (uint64_t)INT64_MAX ||
        __builtin_add_overflow(clock->origin, (int64_t)clock->period, &next_center) ||
        __builtin_add_overflow(clock->origin, (int64_t)clock->skew, &edge) ||
        __builtin_add_overflow(edge, 1, &beyond)) {
        fail("the clock's fields leave the logical time range");
    }
    int64_t at_anchor = 0;
    check(nx_domain_clock_logical_time_at(read, clock->anchor, &at_anchor), "logical time");
    uint64_t wait = 0;
    check(nx_domain_clock_wall_duration_until(read, clock->anchor, next_center, &wait), "wait");
    bool has_window = false;
    int64_t earliest = 0;
    int64_t latest = 0;
    check(nx_domain_clock_admission_window(read, clock->anchor, &has_window, &earliest, &latest),
          "admission window");
    if (!has_window) {
        fail("a paced clock reports no admission window");
    }
    bool at_edge = false;
    bool past_edge = false;
    check(nx_domain_clock_admits(read, clock->anchor, edge, &at_edge), "admission at the edge");
    check(nx_domain_clock_admits(read, clock->anchor, beyond, &past_edge), "admission past it");
    line result = {NULL, 0};
    append(&result, "PROJECTION domain=%s generation=%" PRIu64 " anchor=", domain,
           clock->generation);
    append_relative(&result, clock, at_anchor);
    append(&result, " wait=%" PRIu64 " window=", wait);
    append_relative(&result, clock, earliest);
    append(&result, "..");
    append_relative(&result, clock, latest);
    append(&result, " skew=%s beyond=%s", at_edge ? "admitted" : "refused",
           past_edge ? "admitted" : "refused");
    return result.text;
}

typedef struct clock_waiter {
    nx_session *session;
    nx_cancel *cancel;
    nx_error *error;
} clock_waiter;

static void *wait_for_clock_event(void *argument) {
    clock_waiter *state = argument;
    nx_clock_event *event = NULL;
    state->error = nx_session_next_clock_event(state->session, state->cancel, &event);
    if (event != NULL) {
        nx_clock_event_release(event);
    }
    return NULL;
}

/* Cancels a clock wait from another thread, then lets a deadline end another. The session follows
   no clock yet, so nothing but its token ends either wait. */
static void check_clock_cancellation(nx_session *session) {
    clock_waiter state = {session, nx_cancel_new(), NULL};
    pthread_t thread;
    if (pthread_create(&thread, NULL, wait_for_clock_event, &state) != 0) {
        fail("starting the clock waiter thread");
    }
    struct timespec pause = {0, 100 * 1000 * 1000};
    nanosleep(&pause, NULL);
    nx_cancel_trigger(state.cancel);
    pthread_join(thread, NULL);
    expect_kind(state.error, NX_ERROR_CANCELLED, "a cancelled clock wait did not report CANCELLED");
    nx_cancel_free(state.cancel);

    nx_cancel *deadline = NULL;
    check(nx_cancel_with_deadline(50, &deadline), "deadline token");
    nx_clock_event *event = NULL;
    expect_kind(nx_session_next_clock_event(session, deadline, &event), NX_ERROR_DEADLINE,
                "an expired clock wait did not report DEADLINE");
    nx_cancel_free(deadline);
}

/* What the probe has read about the domain's clock: the generation of the newest state, its
   mapping while it is paced, and whether the session holding the attachment ended since. Every
   event is held to what was read before it, and a read of the clock taken right after it is held
   to be no older. */
typedef struct followed_clock {
    nx_session *session;
    const char *domain;
    uint64_t generation;
    bool paced;
    paced_clock clock;
    bool interrupted;
} followed_clock;

/* The clock the session holds for the domain, which it must follow; the caller releases it. */
static nx_domain_clock *read_clock(const followed_clock *followed) {
    nx_domain_clock *read = NULL;
    check(nx_session_domain_clock(followed->session, text(followed->domain),
                                  strlen(followed->domain), &read),
          "domain clock");
    if (read == NULL) {
        fail("the session follows no clock of the domain after an event about it");
    }
    return read;
}

static void observe(followed_clock *followed, const nx_clock_event *event) {
    uint64_t generation = 0;
    check(nx_clock_event_generation(event, &generation), "state generation");
    if (generation < followed->generation) {
        fail("a state went back to an earlier generation");
    }
    nx_clock_state state = NX_CLOCK_STOPPED;
    check(nx_clock_event_state(event, &state), "clock state");
    bool paced = state == NX_CLOCK_PACED;
    paced_clock clock = {generation, 0, 0, 0, 0, 0.0};
    if (paced) {
        clock = event_paced(event);
    }
    nx_domain_clock *read = read_clock(followed);
    if (nx_domain_clock_generation(read) < generation) {
        fail("a read of the clock is older than the state the probe took");
    }
    if (nx_domain_clock_generation(read) == generation) {
        if (nx_domain_clock_state(read) != state) {
            fail("a read of the clock differs from the state of its generation");
        }
        if (paced) {
            paced_clock held = read_paced(read);
            if (!same_clock(&held, &clock)) {
                fail("a read of the clock differs from the mapping of its generation");
            }
        }
    }
    nx_domain_clock_release(read);
    followed->generation = generation;
    followed->paced = paced;
    followed->clock = clock;
    followed->interrupted = false;
}

static void check_event_tick(const followed_clock *followed, const nx_clock_event *event) {
    if (followed->interrupted) {
        fail("a tick arrived before the restored attachment reported its clock");
    }
    if (!followed->paced) {
        fail("a tick arrived while the clock was not paced");
    }
    tick progress = event_tick(event);
    check_tick(&followed->clock, &progress);
    nx_domain_clock *read = read_clock(followed);
    if (nx_domain_clock_generation(read) < progress.generation) {
        fail("a read of the clock is older than the tick the probe took");
    }
    uint64_t held = 0;
    if (nx_domain_clock_generation(read) == progress.generation &&
        nx_domain_clock_tick(read, &held, NULL, NULL, NULL) && held < progress.id) {
        fail("a read of the clock holds an older tick than the probe took");
    }
    nx_domain_clock_release(read);
}

/* The next event about the domain, held to what the probe read before it; the caller releases
   it. */
static nx_clock_event *follow_next(followed_clock *followed, const nx_cancel *deadline) {
    nx_clock_event *event = NULL;
    check(nx_session_next_clock_event(followed->session, deadline, &event), "next clock event");
    expect_domain(event, followed->domain);
    switch (nx_clock_event_kind_of(event)) {
    case NX_CLOCK_EVENT_STATE: observe(followed, event); break;
    case NX_CLOCK_EVENT_TICK: check_event_tick(followed, event); break;
    case NX_CLOCK_EVENT_INTERRUPTED: followed->interrupted = true; break;
    case NX_CLOCK_EVENT_RESTORATION_FAILED: break;
    case NX_CLOCK_EVENT_ENDED: fail("the server ended the attachment");
    }
    return event;
}

static void unexpected(nx_clock_event *event, const char *before) {
    fprintf(stderr, "probe failed: the clock reported %s before %s\n",
            clock_kind_name(nx_clock_event_kind_of(event)), before);
    exit(1);
}

/* The first tick of the followed generation, which the caller releases. A state reporting that
   generation again is taken on the way; one of another generation fails the probe. */
static nx_clock_event *first_tick(followed_clock *followed, const nx_cancel *deadline) {
    uint64_t generation = followed->generation;
    for (;;) {
        nx_clock_event *event = follow_next(followed, deadline);
        nx_clock_event_kind kind = nx_clock_event_kind_of(event);
        if (kind == NX_CLOCK_EVENT_TICK) {
            return event;
        }
        if (kind != NX_CLOCK_EVENT_STATE || followed->generation != generation) {
            unexpected(event, "the first tick of the followed generation");
        }
        nx_clock_event_release(event);
    }
}

/* The paced state of a generation after the followed one, which the caller releases. The followed
   generation's ticks and the states before the new paced one are taken on the way. */
static nx_clock_event *next_generation(followed_clock *followed, const nx_cancel *deadline) {
    uint64_t previous = followed->generation;
    for (;;) {
        nx_clock_event *event = follow_next(followed, deadline);
        nx_clock_event_kind kind = nx_clock_event_kind_of(event);
        if (kind == NX_CLOCK_EVENT_STATE && followed->generation > previous && followed->paced) {
            return event;
        }
        if (kind != NX_CLOCK_EVENT_TICK && kind != NX_CLOCK_EVENT_STATE) {
            unexpected(event, "a generation after the followed one");
        }
        nx_clock_event_release(event);
    }
}

/* Waits for the interruption of the attachment. The followed generation's ticks and states are
   taken on the way. */
static void interruption(followed_clock *followed, const nx_cancel *deadline) {
    uint64_t generation = followed->generation;
    for (;;) {
        nx_clock_event *event = follow_next(followed, deadline);
        nx_clock_event_kind kind = nx_clock_event_kind_of(event);
        if (kind == NX_CLOCK_EVENT_INTERRUPTED) {
            nx_clock_event_release(event);
            return;
        }
        if (kind != NX_CLOCK_EVENT_TICK &&
            (kind != NX_CLOCK_EVENT_STATE || followed->generation != generation)) {
            unexpected(event, "the interruption");
        }
        nx_clock_event_release(event);
    }
}

/* The paced state the restored attachment reports, which the caller releases. A refused
   restoration, which the session repeats, and a clock reported uninstalled are taken on the way. */
static nx_clock_event *restored(followed_clock *followed, const nx_cancel *deadline) {
    for (;;) {
        nx_clock_event *event = follow_next(followed, deadline);
        if (nx_clock_event_kind_of(event) == NX_CLOCK_EVENT_STATE && followed->paced) {
            return event;
        }
        nx_clock_event_release(event);
    }
}

static void *release_clock_references(void *argument) {
    void **references = argument;
    nx_domain_clock_release(references[0]);
    nx_clock_event_release(references[1]);
    return NULL;
}

static nx_cancel *clock_deadline(void) {
    nx_cancel *deadline = NULL;
    check(nx_cancel_with_deadline(CLOCK_DEADLINE_MILLIS, &deadline), "clock deadline");
    return deadline;
}

/* Prints the paced state a STATE event reports and the first tick after it, releasing both. */
static void report_state_and_first_tick(followed_clock *followed, nx_clock_event *state,
                                        const nx_cancel *deadline) {
    paced_clock clock = event_paced(state);
    char *reported = paced_line("STATE", &clock, followed->domain);
    printf("%s\n", reported);
    free(reported);
    nx_clock_event_release(state);
    nx_clock_event *event = first_tick(followed, deadline);
    tick progress = event_tick(event);
    char *reported_tick = tick_line(&followed->clock, &progress, followed->domain);
    printf("%s\n", reported_tick);
    free(reported_tick);
    nx_clock_event_release(event);
}

/* Attaches to the domain's running clock and reads the clock the attach reported before its first
   tick, then follows the generation the scenario's STOP and START begin and the attachment
   restored after the scenario ends the session, and detaches. */
static int run_clock(nx_session *session, const char *domain) {
    check_clock_cancellation(session);
    nx_outcome *attached = execute(session, "ATTACH DOMAIN CLOCK;");
    printf("ATTACHED %s\n", disposition_name(nx_outcome_disposition(attached)));
    nx_outcome_free(attached);

    /* The clock the attach reported, read before any event about the attachment. */
    followed_clock followed = {session, domain, 0, false, {0, 0, 0, 0, 0, 0.0}, false};
    nx_domain_clock *read = read_clock(&followed);
    if (nx_domain_clock_state(read) != NX_CLOCK_PACED) {
        fail("the attach reported a clock other than the running paced one");
    }
    paced_clock clock = read_paced(read);
    char *reported_clock = paced_line("CLOCK", &clock, domain);
    printf("%s\n", reported_clock);
    char *projected = projection_line(&clock, read, domain);
    printf("%s\n", projected);
    free(projected);
    followed.generation = clock.generation;
    followed.paced = true;
    followed.clock = clock;

    nx_cancel *deadline = clock_deadline();
    nx_clock_event *tick_event = first_tick(&followed, deadline);
    tick progress = event_tick(tick_event);
    char *reported_tick = tick_line(&clock, &progress, domain);
    printf("%s\n", reported_tick);
    nx_cancel_free(deadline);

    /* Keep a second reference to the read and the tick and release the first ones on another
       thread, so both must read the same on the second alone. */
    nx_domain_clock *retained_read = nx_domain_clock_retain(read);
    nx_clock_event *retained_tick = nx_clock_event_retain(tick_event);
    void *firsts[2] = {read, tick_event};
    pthread_t releaser;
    if (pthread_create(&releaser, NULL, release_clock_references, firsts) != 0) {
        fail("starting the releasing thread");
    }
    pthread_join(releaser, NULL);
    paced_clock clock_again = read_paced(retained_read);
    tick progress_again = event_tick(retained_tick);
    char *clock_line_again = paced_line("CLOCK", &clock_again, domain);
    char *tick_line_again = tick_line(&clock_again, &progress_again, domain);
    if (strcmp(clock_line_again, reported_clock) != 0 || strcmp(tick_line_again, reported_tick) != 0) {
        fail("a retained clock or tick reads differently than it did");
    }
    free(clock_line_again);
    free(tick_line_again);
    free(reported_clock);
    free(reported_tick);
    nx_domain_clock_release(retained_read);
    nx_clock_event_release(retained_tick);

    /* The scenario stops the domain and starts it again at another origin and rate. */
    deadline = clock_deadline();
    report_state_and_first_tick(&followed, next_generation(&followed, deadline), deadline);
    nx_cancel_free(deadline);

    /* The scenario ends the session, and the binding attaches the clock again on the next one. */
    deadline = clock_deadline();
    interruption(&followed, deadline);
    printf("INTERRUPTED domain=%s\n", domain);
    report_state_and_first_tick(&followed, restored(&followed, deadline), deadline);
    nx_cancel_free(deadline);

    nx_outcome *detached = execute(session, "DETACH DOMAIN CLOCK;");
    printf("DETACHED %s\n", disposition_name(nx_outcome_disposition(detached)));
    nx_outcome_free(detached);
    printf("CHECKS ok\n");
    nx_session_free(session);
    printf("PASS\n");
    return 0;
}

/* ---- Producers and consumers ------------------------------------------------------------- */

#define WAIT_MILLIS 120000
#define EXPIRING_MILLIS 200
#define PRODUCER_BATCHES 2
#define CONSUMER_BATCHES 4
#define ENDPOINT_BYTES 1048576
#define SCRIBBLE 0xaa

/* One level of a field's type, as a host names it. */
typedef struct level_spec {
    nx_type type;
    uint32_t length;
} level_spec;

/* One field the probe expects an endpoint to have. */
typedef struct field_spec {
    const char *name;
    const level_spec *levels;
    size_t level_count;
    bool nullable;
    bool sensitive;
} field_spec;

static const level_spec LEVELS_U32[] = {{NX_TYPE_U32, 0}};
static const level_spec LEVELS_STRING[] = {{NX_TYPE_STRING, 0}};
static const level_spec LEVELS_U8[] = {{NX_TYPE_U8, 0}};
static const level_spec LEVELS_I8[] = {{NX_TYPE_I8, 0}};
static const level_spec LEVELS_U16[] = {{NX_TYPE_U16, 0}};
static const level_spec LEVELS_I16[] = {{NX_TYPE_I16, 0}};
static const level_spec LEVELS_I32[] = {{NX_TYPE_I32, 0}};
static const level_spec LEVELS_U64[] = {{NX_TYPE_U64, 0}};
static const level_spec LEVELS_I64[] = {{NX_TYPE_I64, 0}};
static const level_spec LEVELS_F32[] = {{NX_TYPE_F32, 0}};
static const level_spec LEVELS_F64[] = {{NX_TYPE_F64, 0}};
static const level_spec LEVELS_BOOL[] = {{NX_TYPE_BOOL, 0}};
static const level_spec LEVELS_BYTES[] = {{NX_TYPE_BYTES, 0}};
static const level_spec LEVELS_DATETIME[] = {{NX_TYPE_DATETIME, 0}};
static const level_spec LEVELS_TAGS[] = {{NX_TYPE_LIST, 0}, {NX_TYPE_STRING, 0}};
static const level_spec LEVELS_GRID[] = {{NX_TYPE_FIXED_LIST, 2}, {NX_TYPE_FIXED_LIST, 2},
                                         {NX_TYPE_I16, 0}};
static const level_spec LEVELS_SPANS[] = {{NX_TYPE_LIST, 0}, {NX_TYPE_FIXED_LIST, 2},
                                          {NX_TYPE_DATETIME, 0}};

#define FIELD(name, levels, nullable, sensitive)                                                  \
    { name, levels, sizeof levels / sizeof levels[0], nullable, sensitive }

/* The input schema of the probe's ingestor, in declared order. */
static const field_spec INPUT_FIELDS[] = {
    FIELD("id", LEVELS_U32, false, false),
    FIELD("tenant", LEVELS_STRING, false, false),
    FIELD("u8v", LEVELS_U8, false, false),
    FIELD("i8v", LEVELS_I8, false, false),
    FIELD("u16v", LEVELS_U16, false, false),
    FIELD("i16v", LEVELS_I16, false, false),
    FIELD("u32v", LEVELS_U32, false, false),
    FIELD("i32v", LEVELS_I32, false, false),
    FIELD("u64v", LEVELS_U64, false, false),
    FIELD("i64v", LEVELS_I64, false, false),
    FIELD("f32v", LEVELS_F32, false, false),
    FIELD("f64v", LEVELS_F64, false, false),
    FIELD("flag", LEVELS_BOOL, false, false),
    FIELD("text", LEVELS_STRING, true, false),
    FIELD("raw", LEVELS_BYTES, true, false),
    FIELD("at", LEVELS_DATETIME, false, false),
    FIELD("maybe", LEVELS_I64, true, false),
    FIELD("tags", LEVELS_TAGS, false, false),
    FIELD("grid", LEVELS_GRID, false, false),
    FIELD("spans", LEVELS_SPANS, true, false),
    FIELD("secret", LEVELS_STRING, false, true),
};
#define INPUT_FIELD_COUNT (sizeof INPUT_FIELDS / sizeof INPUT_FIELDS[0])

/* The field the emitter adds to every output row: the row's `id` again. */
static const field_spec ECHO_FIELD = FIELD("echo", LEVELS_U32, false, false);

/* The ingestor's secret as a host that forgot its sensitivity expects it. */
static const field_spec PUBLIC_SECRET = FIELD("secret", LEVELS_STRING, false, false);

/* A byte string that may be absent. */
typedef struct blob {
    const char *data;
    size_t len;
    bool present;
} blob;

#define PRESENT(literal) {literal, sizeof literal - 1, true}
#define ABSENT {NULL, 0, false}

/* One row of an input batch, as the probe's application holds it. */
typedef struct row {
    uint32_t id;
    const char *tenant;
    uint8_t u8v;
    int8_t i8v;
    uint16_t u16v;
    int16_t i16v;
    uint32_t u32v;
    int32_t i32v;
    uint64_t u64v;
    int64_t i64v;
    uint32_t f32v;
    uint64_t f64v;
    bool flag;
    blob text;
    blob raw;
    int64_t at;
    bool has_maybe;
    int64_t maybe;
    const char *const *tags;
    size_t tag_count;
    int16_t grid[2][2];
    bool has_spans;
    const int64_t (*spans)[2];
    size_t span_count;
    const char *secret;
} row;

static const char *const FIRST_TAGS[] = {"a", "", "h\xc3\xa9llo"};
static const char *const THIRD_TAGS[] = {"x"};
static const int64_t FIRST_SPANS[][2] = {{INT64_MIN, INT64_MAX}};

/* The typed rows of the first batch. */
static row typed_rows[3] = {
    {1, "acme", UINT8_MAX, INT8_MAX, UINT16_MAX, INT16_MAX, UINT32_MAX, INT32_MAX, UINT64_MAX,
     INT64_MAX, 0x7f7fffffu, 0x7fefffffffffffffull, true,
     PRESENT("h\xc3\xa9llo \xe4\xb8\x96\xe7\x95\x8c \xf0\x9f\x9a\x80"),
     PRESENT("\x00\xff\xfe\x80\x00"), INT64_MAX, false, 0, FIRST_TAGS, 3,
     {{1, -2}, {INT16_MAX, INT16_MIN}}, true, FIRST_SPANS, 1, "s1"},
    {2, "acme", 0, INT8_MIN, 0, INT16_MIN, 0, INT32_MIN, 0, INT64_MIN, 0x80000000u,
     0x0000000000000001ull, false, PRESENT("a\x00" "b"), PRESENT(""), INT64_MIN, true, 0, NULL, 0,
     {{0, 0}, {0, 0}}, false, NULL, 0, "s2"},
    {3, "acme", 1, -1, 1, -1, 1, -1, 9007199254740993ull, -9007199254740993ll, 0x3fc00000u,
     0x3fb999999999999aull, true, ABSENT, ABSENT, 1, true, 9007199254740992ll, THIRD_TAGS, 1,
     {{-1, 1}, {2, -2}}, true, NULL, 0, "s3"},
};

/* A row of `tenant` whose every other value is the zero of its type. */
static row plain_row(uint32_t id, const char *tenant) {
    row plain;
    memset(&plain, 0, sizeof plain);
    plain.id = id;
    plain.tenant = tenant;
    plain.secret = "s";
    return plain;
}

/* A growing buffer of bytes or offsets. */
typedef struct bytes {
    uint8_t *data;
    size_t len;
} bytes;

static void push(bytes *target, const void *data, size_t len) {
    target->data = realloc(target->data, target->len + len + 1);
    if (len > 0) {
        memcpy(target->data + target->len, data, len);
    }
    target->len += len;
}

typedef struct offsets {
    uint64_t *items;
    size_t len;
} offsets;

static void push_offset(offsets *target, uint64_t offset) {
    target->items = realloc(target->items, (target->len + 1) * sizeof(uint64_t));
    target->items[target->len++] = offset;
}

/* What a host passes the binding for one column. */
typedef struct host_column {
    bool has_states;
    bytes states;
    /* The offsets of the only variable-length list level any probe field has: level 0. */
    bool has_list_offsets;
    offsets list_offsets;
    size_t leaf;
    bool varlen;
    bytes values;
    offsets value_offsets;
} host_column;

static void add_varlen(host_column *column, const void *data, size_t len) {
    if (column->value_offsets.len == 0) {
        push_offset(&column->value_offsets, 0);
    }
    push(&column->values, data, len);
    push_offset(&column->value_offsets, column->values.len);
}

static void add_state(host_column *column, bool present) {
    uint8_t state = present ? NX_CELL_VALUE : NX_CELL_NULL;
    push(&column->states, &state, 1);
}

/* The column `name` of `rows`, as a host lays it out. */
static host_column column_of(const char *name, const row *rows, size_t count) {
    host_column column;
    memset(&column, 0, sizeof column);
    for (size_t index = 0; index < count; index++) {
        const row *item = &rows[index];
        if (strcmp(name, "id") == 0 || strcmp(name, "echo") == 0) {
            push(&column.values, &item->id, 4);
        } else if (strcmp(name, "tenant") == 0) {
            column.varlen = true;
            add_varlen(&column, item->tenant, strlen(item->tenant));
        } else if (strcmp(name, "u8v") == 0) {
            push(&column.values, &item->u8v, 1);
        } else if (strcmp(name, "i8v") == 0) {
            push(&column.values, &item->i8v, 1);
        } else if (strcmp(name, "u16v") == 0) {
            push(&column.values, &item->u16v, 2);
        } else if (strcmp(name, "i16v") == 0) {
            push(&column.values, &item->i16v, 2);
        } else if (strcmp(name, "u32v") == 0) {
            push(&column.values, &item->u32v, 4);
        } else if (strcmp(name, "i32v") == 0) {
            push(&column.values, &item->i32v, 4);
        } else if (strcmp(name, "u64v") == 0) {
            push(&column.values, &item->u64v, 8);
        } else if (strcmp(name, "i64v") == 0) {
            push(&column.values, &item->i64v, 8);
        } else if (strcmp(name, "f32v") == 0) {
            push(&column.values, &item->f32v, 4);
        } else if (strcmp(name, "f64v") == 0) {
            push(&column.values, &item->f64v, 8);
        } else if (strcmp(name, "flag") == 0) {
            uint8_t flag = item->flag ? 1 : 0;
            push(&column.values, &flag, 1);
        } else if (strcmp(name, "at") == 0) {
            push(&column.values, &item->at, 8);
        } else if (strcmp(name, "text") == 0 || strcmp(name, "raw") == 0) {
            const blob *value = strcmp(name, "text") == 0 ? &item->text : &item->raw;
            column.varlen = true;
            column.has_states = true;
            add_state(&column, value->present);
            add_varlen(&column, value->data, value->len);
        } else if (strcmp(name, "maybe") == 0) {
            column.has_states = true;
            add_state(&column, item->has_maybe);
            push(&column.values, &item->maybe, 8);
        } else if (strcmp(name, "tags") == 0) {
            column.varlen = true;
            column.has_list_offsets = true;
            column.leaf = 1;
            if (column.list_offsets.len == 0) {
                push_offset(&column.list_offsets, 0);
                push_offset(&column.value_offsets, 0);
            }
            for (size_t tag = 0; tag < item->tag_count; tag++) {
                push(&column.values, item->tags[tag], strlen(item->tags[tag]));
                push_offset(&column.value_offsets, column.values.len);
            }
            push_offset(&column.list_offsets, column.value_offsets.len - 1);
        } else if (strcmp(name, "grid") == 0) {
            column.leaf = 2;
            push(&column.values, item->grid, sizeof item->grid);
        } else if (strcmp(name, "spans") == 0) {
            column.has_states = true;
            column.has_list_offsets = true;
            column.leaf = 2;
            add_state(&column, item->has_spans);
            if (column.list_offsets.len == 0) {
                push_offset(&column.list_offsets, 0);
            }
            push(&column.values, item->spans, item->span_count * sizeof(int64_t[2]));
            push_offset(&column.list_offsets, column.values.len / sizeof(int64_t[2]));
        } else if (strcmp(name, "secret") == 0) {
            column.varlen = true;
            add_varlen(&column, item->secret, strlen(item->secret));
        } else {
            fail("the probe holds no column of that name");
        }
    }
    if (column.varlen && column.value_offsets.len == 0) {
        push_offset(&column.value_offsets, 0);
    }
    return column;
}

static void free_column(host_column *column) {
    free(column->states.data);
    free(column->list_offsets.items);
    free(column->values.data);
    free(column->value_offsets.items);
}

static nx_fields *fields_of(const field_spec *const *specs, size_t count) {
    nx_fields *fields = nx_fields_new();
    for (size_t index = 0; index < count; index++) {
        const field_spec *spec = specs[index];
        check(nx_fields_add(fields, text(spec->name), strlen(spec->name), spec->levels[0].type,
                            spec->levels[0].length, spec->nullable, spec->sensitive),
              "add field");
        for (size_t level = 1; level < spec->level_count; level++) {
            check(nx_fields_element(fields, spec->levels[level].type, spec->levels[level].length),
                  "add element");
        }
    }
    return fields;
}

/* One field of a schema the binding reported, with every level of its type. */
typedef struct reported_field {
    const uint8_t *name;
    size_t name_len;
    level_spec levels[4];
    size_t level_count;
    bool nullable;
    bool sensitive;
} reported_field;

typedef struct reported_fields {
    reported_field items[32];
    size_t count;
} reported_fields;

static reported_fields reported_fields_of(const nx_schema *schema) {
    reported_fields result;
    memset(&result, 0, sizeof result);
    check(nx_schema_field_count(schema, NX_PART_ROWS, &result.count), "field count");
    if (result.count > 32) {
        fail("a schema has more fields than the probe reads");
    }
    for (size_t index = 0; index < result.count; index++) {
        reported_field *item = &result.items[index];
        nx_type type = NX_TYPE_U8;
        check(nx_schema_field(schema, NX_PART_ROWS, index, &item->name, &item->name_len, &type,
                              &item->nullable, &item->sensitive),
              "field");
        check(nx_schema_field_levels(schema, NX_PART_ROWS, index, &item->level_count), "levels");
        if (item->level_count > 4) {
            fail("a field nests deeper than the probe reads");
        }
        for (size_t level = 0; level < item->level_count; level++) {
            check(nx_schema_field_level(schema, NX_PART_ROWS, index, level,
                                        &item->levels[level].type, &item->levels[level].length),
                  "level");
        }
        if (item->levels[0].type != type) {
            fail("a field's first level differs from its type");
        }
    }
    return result;
}

/* A field's type in NSPL's spelling, with nested fixed-size lists as dimensions. */
static void append_type(line *target, const level_spec *levels, size_t count) {
    if (levels[0].type == NX_TYPE_LIST) {
        append(target, "VEC<");
        append_type(target, levels + 1, count - 1);
        append(target, ">");
        return;
    }
    if (levels[0].type == NX_TYPE_FIXED_LIST) {
        size_t dimensions = 0;
        while (dimensions < count && levels[dimensions].type == NX_TYPE_FIXED_LIST) {
            dimensions++;
        }
        append(target, "ARRAY<");
        append_type(target, levels + dimensions, count - dimensions);
        for (size_t index = 0; index < dimensions; index++) {
            append(target, ", %" PRIu32, levels[index].length);
        }
        append(target, ">");
        return;
    }
    append(target, "%s", type_name(levels[0].type));
}

static void print_reported(const char *prefix, const reported_field *item) {
    line type = {NULL, 0};
    append(&type, "%s", "");
    append_type(&type, item->levels, item->level_count);
    printf("%s %.*s %s %s %s\n", prefix, (int)item->name_len, (const char *)item->name, type.text,
           item->nullable ? "nullable" : "required", item->sensitive ? "sensitive" : "public");
    free(type.text);
}

static bool named(const reported_field *item, const char *name) {
    return item->name_len == strlen(name) && memcmp(item->name, name, item->name_len) == 0;
}

/* Builds a batch of `rows` for `schema`, writing over every buffer as soon as the binding has
   copied it. */
static nx_batch *build_batch(const nx_schema *schema, const row *rows, size_t count) {
    reported_fields fields = reported_fields_of(schema);
    nx_batch_builder *builder = NULL;
    check(nx_batch_builder_new(schema, count, &builder), "builder");
    for (size_t index = 0; index < fields.count; index++) {
        char name[64];
        snprintf(name, sizeof name, "%.*s", (int)fields.items[index].name_len,
                 (const char *)fields.items[index].name);
        host_column column = column_of(name, rows, count);
        if (column.has_states) {
            check(nx_batch_builder_states(builder, index, column.states.data, column.states.len),
                  "states");
            memset(column.states.data, SCRIBBLE, column.states.len);
        }
        if (column.has_list_offsets) {
            check(nx_batch_builder_offsets(builder, index, 0, column.list_offsets.items,
                                           column.list_offsets.len),
                  "list offsets");
            memset(column.list_offsets.items, 0xff, column.list_offsets.len * sizeof(uint64_t));
        }
        if (column.varlen) {
            check(nx_batch_builder_varlen(builder, index, column.leaf, column.value_offsets.items,
                                          column.value_offsets.len, column.values.data,
                                          column.values.len),
                  "varlen values");
            memset(column.value_offsets.items, 0xff, column.value_offsets.len * sizeof(uint64_t));
        } else {
            check(nx_batch_builder_fixed(builder, index, column.leaf, column.values.data,
                                         column.values.len),
                  "fixed values");
        }
        if (column.values.data != NULL) {
            memset(column.values.data, SCRIBBLE, column.values.len);
        }
        free_column(&column);
    }
    nx_batch *batch = NULL;
    check(nx_batch_builder_finish(builder, &batch), "finish");
    nx_batch_builder_free(builder);
    return batch;
}

/* One column as the binding copied it out. */
typedef struct column_view {
    uint8_t *states;
    /* One entry per list level: the offsets of a LIST level, or NULL for a FIXED_LIST level. */
    uint64_t *list_offsets[3];
    uint32_t list_lengths[3];
    size_t list_count;
    nx_type leaf_type;
    uint8_t *values;
    uint64_t *value_offsets;
} column_view;

static size_t batch_cells(const nx_batch *batch, size_t column, size_t level) {
    size_t cells = 0;
    check(nx_batch_cells(batch, column, level, &cells), "cells");
    return cells;
}

/* Reads one column in one call per level of its type. */
static column_view read_column(const nx_batch *batch, size_t index, const reported_field *field) {
    column_view view;
    memset(&view, 0, sizeof view);
    size_t rows = batch_cells(batch, index, 0);
    view.states = malloc(rows + 1);
    check(nx_batch_states(batch, index, view.states, rows), "states");
    size_t innermost = field->level_count - 1;
    view.list_count = innermost;
    for (size_t level = 0; level < innermost; level++) {
        if (field->levels[level].type == NX_TYPE_LIST) {
            size_t cells = batch_cells(batch, index, level);
            view.list_offsets[level] = malloc((cells + 1) * sizeof(uint64_t));
            check(nx_batch_offsets(batch, index, level, view.list_offsets[level], cells + 1),
                  "offsets");
        } else {
            view.list_lengths[level] = field->levels[level].length;
        }
    }
    view.leaf_type = field->levels[innermost].type;
    size_t cells = batch_cells(batch, index, innermost);
    if (view.leaf_type == NX_TYPE_STRING || view.leaf_type == NX_TYPE_BYTES) {
        view.value_offsets = malloc((cells + 1) * sizeof(uint64_t));
        size_t data_len = 0;
        check(nx_batch_varlen(batch, index, innermost, view.value_offsets, cells + 1, NULL, 0,
                              &data_len),
              "varlen length");
        view.values = malloc(data_len + 1);
        check(nx_batch_varlen(batch, index, innermost, view.value_offsets, cells + 1, view.values,
                              data_len, &data_len),
              "varlen");
    } else {
        size_t len = cells * fixed_width(view.leaf_type);
        view.values = malloc(len + 1);
        check(nx_batch_fixed(batch, index, innermost, view.values, len), "fixed");
    }
    return view;
}

static void free_view(column_view *view) {
    free(view->states);
    for (size_t level = 0; level < view->list_count; level++) {
        free(view->list_offsets[level]);
    }
    free(view->values);
    free(view->value_offsets);
}

static void render_leaf(line *target, const column_view *view, size_t index) {
    if (view->leaf_type == NX_TYPE_STRING || view->leaf_type == NX_TYPE_BYTES) {
        uint64_t start = view->value_offsets[index];
        uint64_t end = view->value_offsets[index + 1];
        append(target, "%s:", view->leaf_type == NX_TYPE_STRING ? "str" : "bytes");
        append_hex(target, view->values + start, (size_t)(end - start));
        return;
    }
    const uint8_t *cell = view->values + index * fixed_width(view->leaf_type);
    switch (view->leaf_type) {
    case NX_TYPE_U8: append(target, "u8:%" PRIu8, *cell); break;
    case NX_TYPE_I8: { int8_t value; memcpy(&value, cell, 1); append(target, "i8:%" PRId8, value); break; }
    case NX_TYPE_BOOL: append(target, "bool:%s", *cell == 1 ? "true" : "false"); break;
    case NX_TYPE_U16: { uint16_t value; memcpy(&value, cell, 2); append(target, "u16:%" PRIu16, value); break; }
    case NX_TYPE_I16: { int16_t value; memcpy(&value, cell, 2); append(target, "i16:%" PRId16, value); break; }
    case NX_TYPE_U32: { uint32_t value; memcpy(&value, cell, 4); append(target, "u32:%" PRIu32, value); break; }
    case NX_TYPE_I32: { int32_t value; memcpy(&value, cell, 4); append(target, "i32:%" PRId32, value); break; }
    case NX_TYPE_F32: { uint32_t value; memcpy(&value, cell, 4); append(target, "f32:%08" PRIx32, value); break; }
    case NX_TYPE_U64: { uint64_t value; memcpy(&value, cell, 8); append(target, "u64:%" PRIu64, value); break; }
    case NX_TYPE_I64: { int64_t value; memcpy(&value, cell, 8); append(target, "i64:%" PRId64, value); break; }
    case NX_TYPE_F64: { uint64_t value; memcpy(&value, cell, 8); append(target, "f64:%016" PRIx64, value); break; }
    case NX_TYPE_DATETIME: { int64_t value; memcpy(&value, cell, 8); append(target, "datetime:%" PRId64, value); break; }
    default: fail("a leaf is not a scalar");
    }
}

static void render_cell(line *target, const column_view *view, size_t level, size_t index) {
    if (level == view->list_count) {
        render_leaf(target, view, index);
        return;
    }
    size_t start = 0;
    size_t end = 0;
    if (view->list_offsets[level] != NULL) {
        start = (size_t)view->list_offsets[level][index];
        end = (size_t)view->list_offsets[level][index + 1];
    } else {
        start = index * view->list_lengths[level];
        end = start + view->list_lengths[level];
    }
    append(target, "[");
    for (size_t element = start; element < end; element++) {
        if (element > start) {
            append(target, ",");
        }
        render_cell(target, view, level + 1, element);
    }
    append(target, "]");
}

/* Every row of the batch rendered as a report line. The caller frees the lines. */
static char **batch_rows(const nx_batch *batch, const reported_fields *fields, size_t *count) {
    *count = nx_batch_row_count(batch);
    column_view *views = calloc(fields->count, sizeof(column_view));
    for (size_t index = 0; index < fields->count; index++) {
        views[index] = read_column(batch, index, &fields->items[index]);
    }
    char **rows = calloc(*count + 1, sizeof(char *));
    for (size_t index = 0; index < *count; index++) {
        line target = {NULL, 0};
        append(&target, "ROW");
        for (size_t column = 0; column < fields->count; column++) {
            append(&target, " %.*s=", (int)fields->items[column].name_len,
                   (const char *)fields->items[column].name);
            if (views[column].states[index] == NX_CELL_NULL) {
                append(&target, "null");
            } else {
                render_cell(&target, &views[column], 0, index);
            }
        }
        rows[index] = target.text;
    }
    for (size_t index = 0; index < fields->count; index++) {
        free_view(&views[index]);
    }
    free(views);
    return rows;
}

static void free_rows(char **rows, size_t count) {
    for (size_t index = 0; index < count; index++) {
        free(rows[index]);
    }
    free(rows);
}

/* Whether the batch's `id` column holds exactly `expected`. */
static bool has_ids(const nx_batch *batch, const reported_fields *fields, const uint32_t *expected,
                    size_t count) {
    for (size_t index = 0; index < fields->count; index++) {
        if (!named(&fields->items[index], "id")) {
            continue;
        }
        if (nx_batch_row_count(batch) != count) {
            return false;
        }
        column_view view = read_column(batch, index, &fields->items[index]);
        bool same = memcmp(view.values, expected, count * sizeof(uint32_t)) == 0;
        free_view(&view);
        return same;
    }
    return false;
}

static nx_cancel *deadline_after(uint64_t millis) {
    nx_cancel *deadline = NULL;
    check(nx_cancel_with_deadline(millis, &deadline), "deadline");
    return deadline;
}

static const char *submission_refusal_name(nx_submission_refusal refusal) {
    switch (refusal) {
    case NX_REFUSAL_INVALID_BATCH: return "invalid_batch";
    case NX_REFUSAL_SUSPENDED: return "suspended";
    case NX_REFUSAL_BUSY: return "busy";
    case NX_REFUSAL_DRAINING: return "draining";
    case NX_REFUSAL_PRODUCER_ENDED: return "producer_ended";
    case NX_REFUSAL_CREDIT_EXCEEDED: return "credit_exceeded";
    }
    fail("unknown refusal");
    return NULL;
}

static const char *defect_name(nx_batch_defect defect) {
    switch (defect) {
    case NX_DEFECT_MALFORMED: return "malformed";
    case NX_DEFECT_UNEXPECTED_MESSAGE: return "unexpected_message";
    case NX_DEFECT_COMPRESSED: return "compressed";
    case NX_DEFECT_SCHEMA_MISMATCH: return "schema_mismatch";
    case NX_DEFECT_NOT_ONE_BATCH: return "not_one_batch";
    case NX_DEFECT_TOO_MANY_ROWS: return "too_many_rows";
    case NX_DEFECT_TOO_LARGE: return "too_large";
    case NX_DEFECT_INVALID_DATA: return "invalid_data";
    }
    fail("unknown defect");
    return NULL;
}

/* Waits for a submission's outcome and prints it. */
static void print_outcome(const nx_producer *producer, uint64_t submission) {
    nx_cancel *deadline = deadline_after(WAIT_MILLIS);
    nx_submission_outcome *outcome = NULL;
    check(nx_producer_rejoin(producer, submission, deadline, &outcome), "rejoin");
    nx_cancel_free(deadline);
    switch (nx_submission_outcome_result(outcome)) {
    case NX_SUBMISSION_COMPLETED:
        printf("OUTCOME completed\n");
        break;
    case NX_SUBMISSION_NOT_ADMITTED: {
        nx_submission_refusal refusal = NX_REFUSAL_BUSY;
        check(nx_submission_outcome_refusal(outcome, &refusal), "refusal");
        printf("OUTCOME not_admitted %s", submission_refusal_name(refusal));
        if (refusal == NX_REFUSAL_INVALID_BATCH) {
            nx_batch_defect defect = NX_DEFECT_MALFORMED;
            check(nx_submission_outcome_defect(outcome, &defect), "defect");
            printf(" %s", defect_name(defect));
        }
        printf("\n");
        break;
    }
    case NX_SUBMISSION_PROCESSING_FAILED: {
        nx_processing_failure failure = NX_FAILURE_ACK_TIMED_OUT;
        check(nx_submission_outcome_failure(outcome, &failure), "failure");
        printf("OUTCOME processing_failed %s\n",
               failure == NX_FAILURE_REJECTED ? "rejected" : "ack_timed_out");
        break;
    }
    case NX_SUBMISSION_OUTCOME_UNKNOWN: {
        nx_submission_uncertainty cause = NX_UNCERTAINTY_INTERRUPTED;
        check(nx_submission_outcome_uncertainty(outcome, &cause), "uncertainty");
        printf("OUTCOME outcome_unknown %s\n",
               cause == NX_UNCERTAINTY_SESSION_LOST ? "session_lost"
               : cause == NX_UNCERTAINTY_OWNER_LOST ? "owner_lost"
                                                    : "interrupted");
        break;
    }
    }
    nx_submission_outcome_free(outcome);
}

static uint64_t submit(const nx_producer *producer, const nx_batch *batch) {
    uint64_t submission = 0;
    check(nx_producer_submit(producer, batch, NULL, &submission), "submit");
    return submission;
}

static nx_delivery *next_delivery(const nx_consumer *consumer) {
    nx_cancel *deadline = deadline_after(WAIT_MILLIS);
    nx_delivery *delivery = NULL;
    check(nx_consumer_next(consumer, deadline, &delivery), "next delivery");
    nx_cancel_free(deadline);
    return delivery;
}

static const char *settlement_name(nx_settlement settlement) {
    switch (settlement) {
    case NX_SETTLEMENT_CONFIRMED: return "confirmed";
    case NX_SETTLEMENT_STALE_REFERENCE: return "stale_reference";
    case NX_SETTLEMENT_WRONG_CONSUMER: return "wrong_consumer";
    case NX_SETTLEMENT_INVALID_REASON: return "invalid_reason";
    case NX_SETTLEMENT_CONSUMER_ENDED: return "consumer_ended";
    }
    fail("unknown settlement");
    return NULL;
}

static const char *ack(const nx_delivery *delivery) {
    nx_cancel *deadline = deadline_after(WAIT_MILLIS);
    nx_settlement settlement = NX_SETTLEMENT_CONSUMER_ENDED;
    check(nx_delivery_ack(delivery, deadline, &settlement), "ack");
    nx_cancel_free(deadline);
    return settlement_name(settlement);
}

static const char *state_name(nx_endpoint_state state) {
    switch (state) {
    case NX_ENDPOINT_ACTIVE: return "active";
    case NX_ENDPOINT_INTERRUPTED: return "interrupted";
    case NX_ENDPOINT_RESTORING: return "restoring";
    case NX_ENDPOINT_REOPEN_REQUIRED: return "reopen_required";
    case NX_ENDPOINT_CLOSED: return "closed";
    }
    fail("unknown endpoint state");
    return NULL;
}

/* A borrowed byte string, copied. */
typedef struct copy {
    uint8_t *data;
    size_t len;
} copy;

static copy copied_bytes(const uint8_t *data, size_t len) {
    copy result = {malloc(len + 1), len};
    if (len > 0) {
        memcpy(result.data, data, len);
    }
    return result;
}

static bool same_bytes(copy left, copy right) {
    return left.len == right.len && (left.len == 0 || memcmp(left.data, right.data, left.len) == 0);
}

static copy delivery_identity(const nx_delivery *delivery) {
    const uint8_t *data = NULL;
    size_t len = 0;
    nx_delivery_identity(delivery, &data, &len);
    return copied_bytes(data, len);
}

static copy delivery_reference(const nx_delivery *delivery) {
    const uint8_t *data = NULL;
    size_t len = 0;
    nx_delivery_reference(delivery, &data, &len);
    return copied_bytes(data, len);
}

static copy delivery_stream(const nx_delivery *delivery) {
    const uint8_t *data = NULL;
    size_t len = 0;
    nx_delivery_ipc(delivery, &data, &len);
    return copied_bytes(data, len);
}

static nx_batch *delivery_batch(const nx_delivery *delivery) {
    nx_batch *batch = NULL;
    check(nx_delivery_batch(delivery, &batch), "delivery batch");
    return batch;
}

/* Opening with `specs` has to be refused; prints the refusal. */
static void expect_refused_producer(nx_session *session, const char *domain, const char *ingestor,
                                    nx_fields *fields) {
    nx_producer *producer = NULL;
    nx_error *error = nx_session_open_ingestor(session, text(domain), strlen(domain), text(ingestor),
                                               strlen(ingestor), fields, PRODUCER_BATCHES,
                                               ENDPOINT_BYTES, NULL, &producer);
    nx_open_refusal refusal = NX_OPEN_DOMAIN_NOT_FOUND;
    if (error == NULL || nx_error_kind_of(error) != NX_ERROR_REJECTED ||
        !nx_error_open_refusal(error, &refusal) || refusal != NX_OPEN_SCHEMA_MISMATCH) {
        fail("an open with another schema was not refused as a schema mismatch");
    }
    nx_error_free(error);
    printf("REFUSED producer schema mismatch\n");
}

static void expect_refused_consumer(nx_session *session, const char *domain, const char *emitter,
                                    nx_fields *fields) {
    nx_consumer *consumer = NULL;
    nx_error *error = nx_session_subscribe_emitter(session, text(domain), strlen(domain),
                                                   text(emitter), strlen(emitter), fields,
                                                   CONSUMER_BATCHES, ENDPOINT_BYTES, NULL, &consumer);
    nx_open_refusal refusal = NX_OPEN_DOMAIN_NOT_FOUND;
    if (error == NULL || nx_error_kind_of(error) != NX_ERROR_REJECTED ||
        !nx_error_open_refusal(error, &refusal) || refusal != NX_OPEN_SCHEMA_MISMATCH) {
        fail("an open with another schema was not refused as a schema mismatch");
    }
    nx_error_free(error);
    printf("REFUSED consumer schema mismatch\n");
}

typedef struct next_waiter {
    const nx_consumer *consumer;
    nx_cancel *cancel;
    nx_error *error;
} next_waiter;

static void *wait_for_delivery(void *argument) {
    next_waiter *state = argument;
    nx_delivery *delivery = NULL;
    state->error = nx_consumer_next(state->consumer, state->cancel, &delivery);
    if (delivery != NULL) {
        nx_delivery_release(delivery);
    }
    return NULL;
}

static void *release_delivery(void *argument) {
    nx_delivery_release(argument);
    return NULL;
}

static int run_endpoints(nx_session *session, const char *domain) {
    const char *ingestor = env("NERVIX_PROBE_INGESTOR");
    const char *emitter = env("NERVIX_PROBE_EMITTER");
    bool checks = true;

    const field_spec *input_specs[INPUT_FIELD_COUNT];
    const field_spec *output_specs[INPUT_FIELD_COUNT + 1];
    const field_spec *mismatched_specs[INPUT_FIELD_COUNT];
    for (size_t index = 0; index < INPUT_FIELD_COUNT; index++) {
        input_specs[index] = &INPUT_FIELDS[index];
        output_specs[index] = &INPUT_FIELDS[index];
        mismatched_specs[index] =
            strcmp(INPUT_FIELDS[index].name, "secret") == 0 ? &PUBLIC_SECRET : &INPUT_FIELDS[index];
    }
    output_specs[INPUT_FIELD_COUNT] = &ECHO_FIELD;
    nx_fields *input_fields = fields_of(input_specs, INPUT_FIELD_COUNT);
    nx_fields *output_fields = fields_of(output_specs, INPUT_FIELD_COUNT + 1);
    nx_fields *mismatched = fields_of(mismatched_specs, INPUT_FIELD_COUNT);

    /* An open whose expected fields differ from the endpoint's is refused exactly. */
    expect_refused_producer(session, domain, ingestor, mismatched);
    expect_refused_consumer(session, domain, emitter, input_fields);
    nx_fields_free(mismatched);

    nx_consumer *consumer = NULL;
    check(nx_session_subscribe_emitter(session, text(domain), strlen(domain), text(emitter),
                                       strlen(emitter), output_fields, CONSUMER_BATCHES,
                                       ENDPOINT_BYTES, NULL, &consumer),
          "open consumer");
    nx_producer *producer = NULL;
    check(nx_session_open_ingestor(session, text(domain), strlen(domain), text(ingestor),
                                   strlen(ingestor), input_fields, PRODUCER_BATCHES,
                                   ENDPOINT_BYTES, NULL, &producer),
          "open producer");
    nx_schema *output_schema = NULL;
    check(nx_consumer_schema(consumer, &output_schema), "consumer schema");
    nx_schema *input_schema = NULL;
    check(nx_producer_schema(producer, &input_schema), "producer schema");
    reported_fields output = reported_fields_of(output_schema);
    reported_fields input = reported_fields_of(input_schema);

    nx_ack_window window = NX_ACK_PARALLEL;
    uint64_t outstanding = 0;
    uint64_t ack_timeout = 0;
    uint64_t backoff = 0;
    uint64_t max_backoff = 0;
    uint32_t batches = 0;
    uint64_t bytes_granted = 0;
    uint32_t max_rows = 0;
    uint64_t max_bytes = 0;
    nx_consumer_policy(consumer, &window, &outstanding, &ack_timeout, &backoff, &max_backoff);
    nx_consumer_grant(consumer, &batches, &bytes_granted, &max_rows, &max_bytes);
    printf("CONSUMER generation=%" PRIu64 " state=%s window=%s/%" PRIu64 " ack_timeout=%" PRIu64
           " credit=%" PRIu32 "/%" PRIu64 " max=%" PRIu32 "/%" PRIu64 "\n",
           nx_consumer_generation(consumer), state_name(nx_consumer_state(consumer)),
           window == NX_ACK_SEQUENTIAL ? "sequential" : "parallel", outstanding, ack_timeout,
           batches, bytes_granted, max_rows, max_bytes);
    for (size_t index = 0; index < output.count; index++) {
        print_reported("OUTPUT FIELD", &output.items[index]);
    }
    nx_producer_policy(producer, &window, &outstanding, &ack_timeout, &backoff, &max_backoff);
    nx_producer_grant(producer, &batches, &bytes_granted, &max_rows, &max_bytes);
    checks = checks && max_rows > 0 && max_bytes > 0 && max_bytes <= bytes_granted;
    printf("PRODUCER generation=%" PRIu64 " state=%s admission=%s window=%s/%" PRIu64
           " ack_timeout=%" PRIu64 " credit=%" PRIu32 "/%" PRIu64 "\n",
           nx_producer_generation(producer), state_name(nx_producer_state(producer)),
           nx_producer_admission(producer) == NX_ADMISSION_OPEN ? "open" : "suspended",
           window == NX_ACK_SEQUENTIAL ? "sequential" : "parallel", outstanding, ack_timeout,
           batches, bytes_granted);
    for (size_t index = 0; index < input.count; index++) {
        print_reported("INPUT FIELD", &input.items[index]);
    }
    printf("OPENED\n");

    /* A wait for output that nothing produces ends by its deadline, and one cancelled from another
       thread by its token; the reads they leave behind are the consumer's. */
    nx_cancel *expiring = deadline_after(EXPIRING_MILLIS);
    nx_delivery *nothing = NULL;
    expect_kind(nx_consumer_next(consumer, expiring, &nothing), NX_ERROR_DEADLINE,
                "an expired read did not report DEADLINE");
    nx_cancel_free(expiring);
    printf("NEXT deadline\n");
    next_waiter waiter = {consumer, nx_cancel_new(), NULL};
    pthread_t waiting;
    if (pthread_create(&waiting, NULL, wait_for_delivery, &waiter) != 0) {
        fail("starting the reading thread");
    }
    struct timespec pause = {0, 100 * 1000 * 1000};
    nanosleep(&pause, NULL);
    nx_cancel_trigger(waiter.cancel);
    pthread_join(waiting, NULL);
    expect_kind(waiter.error, NX_ERROR_CANCELLED, "a cancelled read did not report CANCELLED");
    nx_cancel_free(waiter.cancel);
    printf("NEXT cancelled\n");

    /* A batch built for another schema is refused before anything is sent, and the same batch
       written as a stream by other tooling is refused by the server. */
    size_t row_count = sizeof typed_rows / sizeof typed_rows[0];
    nx_batch *other = build_batch(output_schema, typed_rows, row_count);
    uint64_t submission = 0;
    expect_kind(nx_producer_submit(producer, other, NULL, &submission), NX_ERROR_INVALID_ARGUMENT,
                "a batch of another schema was not refused as the host's argument");
    printf("SUBMIT invalid argument\n");
    const uint8_t *foreign = NULL;
    size_t foreign_len = 0;
    check(nx_batch_ipc(other, &foreign, &foreign_len), "stream");
    check(nx_producer_submit_ipc(producer, foreign, foreign_len, NULL, &submission), "submit stream");
    nx_batch_release(other);
    print_outcome(producer, submission);

    /* The typed batch: its outcome waits for the application's acknowledgement. */
    nx_batch *first_batch = build_batch(input_schema, typed_rows, row_count);
    uint64_t first = submit(producer, first_batch);
    nx_batch_release(first_batch);
    printf("SUBMITTED first\n");
    nx_delivery *delivery = next_delivery(consumer);
    const uint8_t *relay = NULL;
    size_t relay_len = 0;
    const uint8_t *fingerprint = NULL;
    size_t fingerprint_len = 0;
    nx_delivery_source_relay(delivery, &relay, &relay_len);
    bool branched = nx_delivery_branch_fingerprint(delivery, &fingerprint, &fingerprint_len);
    copy identity = delivery_identity(delivery);
    copy reference = delivery_reference(delivery);
    printf("DELIVERY relay=%.*s members=%" PRIu32 " branch=", (int)relay_len, (const char *)relay,
           nx_delivery_members(delivery));
    if (branched) {
        printf("%zu", fingerprint_len);
    } else {
        printf("none");
    }
    printf(" identity=%zu reference=%zu\n", identity.len, reference.len);
    uint64_t pending_ids[4] = {0};
    bool pending_resolved[4] = {false};
    size_t pending_count = 0;
    check(nx_producer_pending(producer, pending_ids, pending_resolved, 4, &pending_count), "pending");
    checks = checks && pending_count == 1 && pending_ids[0] == first && !pending_resolved[0];
    expiring = deadline_after(EXPIRING_MILLIS);
    nx_submission_outcome *early = NULL;
    expect_kind(nx_producer_rejoin(producer, first, expiring, &early), NX_ERROR_DEADLINE,
                "a submission had an outcome before its output was acknowledged");
    nx_cancel_free(expiring);
    printf("PENDING first unresolved\n");
    nx_batch *delivered = delivery_batch(delivery);
    size_t printed_count = 0;
    char **printed = batch_rows(delivered, &output, &printed_count);
    for (size_t index = 0; index < printed_count; index++) {
        printf("%s\n", printed[index]);
    }
    const uint8_t *batch_stream = NULL;
    size_t batch_stream_len = 0;
    check(nx_batch_ipc(delivered, &batch_stream, &batch_stream_len), "batch stream");
    copy carried = delivery_stream(delivery);
    checks = checks && same_bytes(copied_bytes(batch_stream, batch_stream_len), carried);
    nx_batch_release(delivered);

    /* A retried attempt comes back with the same identity and a new reference; the first
       reference is stale from then on. */
    nx_cancel *deadline = deadline_after(WAIT_MILLIS);
    nx_settlement settlement = NX_SETTLEMENT_CONSUMER_ENDED;
    check(nx_delivery_retry(delivery, deadline, &settlement), "retry");
    nx_cancel_free(deadline);
    printf("RETRY %s\n", settlement_name(settlement));
    nx_delivery *again = next_delivery(consumer);
    checks = checks && same_bytes(delivery_identity(again), identity) &&
             !same_bytes(delivery_reference(again), reference);
    printf("REDELIVERED same identity new reference\n");
    printf("ACK %s\n", ack(delivery));
    nx_delivery_release(delivery);

    /* A reference retained here outlives the first, released on another thread, and reads and
       settles the same attempt. */
    nx_delivery *retained = nx_delivery_retain(again);
    nx_batch *retained_batch = delivery_batch(again);
    copy stream_before = delivery_stream(retained);
    pthread_t releaser;
    if (pthread_create(&releaser, NULL, release_delivery, again) != 0) {
        fail("starting the releasing thread");
    }
    pthread_join(releaser, NULL);
    checks = checks && same_bytes(delivery_stream(retained), stream_before);
    size_t again_count = 0;
    char **again_rows = batch_rows(retained_batch, &output, &again_count);
    checks = checks && again_count == printed_count;
    for (size_t index = 0; index < again_count && index < printed_count; index++) {
        checks = checks && strcmp(again_rows[index], printed[index]) == 0;
    }
    free_rows(again_rows, again_count);
    free_rows(printed, printed_count);
    printf("ACK %s\n", ack(retained));
    nx_delivery_release(retained);
    nx_batch_release(retained_batch);
    print_outcome(producer, first);

    /* An application rejection finishes the batch through the emitter's message error policy. */
    row second_row = plain_row(10, "beta");
    nx_batch *second_batch = build_batch(input_schema, &second_row, 1);
    uint64_t second = submit(producer, second_batch);
    nx_batch_release(second_batch);
    printf("SUBMITTED second\n");
    nx_delivery *rejected = next_delivery(consumer);
    const char *reason = "application refused";
    deadline = deadline_after(WAIT_MILLIS);
    check(nx_delivery_reject(rejected, text(reason), strlen(reason), deadline, &settlement),
          "reject");
    nx_cancel_free(deadline);
    printf("REJECT %s\n", settlement_name(settlement));
    nx_delivery_release(rejected);
    print_outcome(producer, second);

    /* Two outstanding batches use up the producer's credit, so a third waits for an outcome. */
    row third_rows[2] = {plain_row(20, "acme"), plain_row(21, "acme")};
    row fourth_rows[2] = {plain_row(22, "acme"), plain_row(23, "acme")};
    row fifth_rows[2] = {plain_row(24, "acme"), plain_row(25, "acme")};
    nx_batch *third_batch = build_batch(input_schema, third_rows, 2);
    nx_batch *fourth_batch = build_batch(input_schema, fourth_rows, 2);
    nx_batch *fifth_batch = build_batch(input_schema, fifth_rows, 2);
    uint64_t third = submit(producer, third_batch);
    printf("SUBMITTED third\n");
    uint64_t fourth = submit(producer, fourth_batch);
    printf("SUBMITTED fourth\n");
    expiring = deadline_after(EXPIRING_MILLIS);
    expect_kind(nx_producer_submit(producer, fifth_batch, expiring, &submission), NX_ERROR_DEADLINE,
                "a batch beyond the credit was submitted");
    nx_cancel_free(expiring);
    printf("SUBMIT deadline\n");
    /* A batch refused as busy is sent again after the ingestor's backoff, behind the batch
       submitted after it, so the two outputs may arrive in either order; each keeps its own
       rows. */
    const uint32_t third_ids[2] = {20, 21};
    const uint32_t fourth_ids[2] = {22, 23};
    bool third_seen = false;
    bool fourth_seen = false;
    for (size_t index = 0; index < 2; index++) {
        nx_delivery *output_delivery = next_delivery(consumer);
        nx_batch *output_batch = delivery_batch(output_delivery);
        if (!third_seen && has_ids(output_batch, &output, third_ids, 2)) {
            third_seen = true;
        } else if (!fourth_seen && has_ids(output_batch, &output, fourth_ids, 2)) {
            fourth_seen = true;
        }
        nx_batch_release(output_batch);
        printf("ACK %s\n", ack(output_delivery));
        nx_delivery_release(output_delivery);
    }
    checks = checks && third_seen && fourth_seen;
    print_outcome(producer, third);
    print_outcome(producer, fourth);
    uint64_t fifth = submit(producer, fifth_batch);
    printf("SUBMITTED fifth\n");
    nx_delivery *fifth_output = next_delivery(consumer);
    nx_batch *fifth_output_batch = delivery_batch(fifth_output);
    const uint32_t fifth_ids[2] = {24, 25};
    checks = checks && has_ids(fifth_output_batch, &output, fifth_ids, 2);
    nx_batch_release(fifth_output_batch);
    printf("ACK %s\n", ack(fifth_output));
    nx_delivery_release(fifth_output);
    print_outcome(producer, fifth);
    nx_batch_release(third_batch);
    nx_batch_release(fourth_batch);
    nx_batch_release(fifth_batch);

    /* The scenario cuts the session while one delivery is held unacknowledged. */
    nx_producer *extra = NULL;
    check(nx_session_open_ingestor(session, text(domain), strlen(domain), text(ingestor),
                                   strlen(ingestor), input_fields, 1, 65536, NULL, &extra),
          "open extra producer");
    printf("PRODUCER extra opened\n");
    row held_row = plain_row(30, "gamma");
    nx_batch *held_batch = build_batch(input_schema, &held_row, 1);
    uint64_t held = submit(producer, held_batch);
    nx_batch_release(held_batch);
    printf("SUBMITTED held\n");
    nx_delivery *held_delivery = next_delivery(consumer);
    copy held_identity = delivery_identity(held_delivery);
    printf("HOLDING\n");
    deadline = deadline_after(WAIT_MILLIS);
    expect_kind(nx_consumer_next(consumer, deadline, &nothing), NX_ERROR_INTERRUPTED,
                "a lost session did not interrupt the consumer");
    nx_cancel_free(deadline);
    printf("NEXT interrupted\n");
    deadline = deadline_after(WAIT_MILLIS);
    expect_kind(nx_delivery_ack(held_delivery, deadline, &settlement), NX_ERROR_REJECTED,
                "a delivery of a lost session was settled");
    nx_cancel_free(deadline);
    printf("ACK expired\n");
    nx_delivery_release(held_delivery);
    print_outcome(producer, held);
    for (int attempt = 0; nx_producer_state(extra) == NX_ENDPOINT_ACTIVE; attempt++) {
        if (attempt > WAIT_MILLIS / 10) {
            fail("the extra producer stayed active after its session ended");
        }
        struct timespec short_pause = {0, 10 * 1000 * 1000};
        nanosleep(&short_pause, NULL);
    }
    deadline = deadline_after(WAIT_MILLIS);
    check(nx_producer_close(extra, deadline), "close extra producer");
    nx_cancel_free(deadline);
    checks = checks && nx_producer_state(extra) == NX_ENDPOINT_CLOSED;
    printf("CLOSED extra producer\n");
    printf("WAITING restore\n");

    /* The restored consumer receives the held batch again, and the restored producer
       publishes. */
    nx_delivery *redelivered = next_delivery(consumer);
    checks = checks && same_bytes(delivery_identity(redelivered), held_identity);
    printf("REDELIVERED held same identity\n");
    printf("ACK %s\n", ack(redelivered));
    nx_delivery_release(redelivered);
    row sixth_row = plain_row(40, "gamma");
    nx_batch *sixth_batch = build_batch(input_schema, &sixth_row, 1);
    uint64_t sixth = submit(producer, sixth_batch);
    nx_batch_release(sixth_batch);
    printf("SUBMITTED sixth\n");
    nx_delivery *sixth_output = next_delivery(consumer);
    printf("ACK %s\n", ack(sixth_output));
    nx_delivery_release(sixth_output);
    print_outcome(producer, sixth);
    printf("STATE producer=%s extra=%s consumer=%s\n", state_name(nx_producer_state(producer)),
           state_name(nx_producer_state(extra)), state_name(nx_consumer_state(consumer)));
    deadline = deadline_after(WAIT_MILLIS);
    check(nx_consumer_close(consumer, deadline), "close consumer");
    check(nx_producer_close(producer, deadline), "close producer");
    nx_cancel_free(deadline);
    checks = checks && nx_consumer_state(consumer) == NX_ENDPOINT_CLOSED &&
             nx_producer_state(producer) == NX_ENDPOINT_CLOSED;
    printf("CLOSED completed\n");
    if (!checks) {
        fail("the probe's checks failed");
    }
    printf("CHECKS ok\n");

    nx_producer_free(extra);
    nx_producer_free(producer);
    nx_consumer_free(consumer);
    nx_schema_free(input_schema);
    nx_schema_free(output_schema);
    nx_fields_free(input_fields);
    nx_fields_free(output_fields);
    nx_session_free(session);
    printf("PASS\n");
    return 0;
}

int main(int argc, char **argv) {
    const char *server = env("NERVIX_PROBE_GRPC_URI");
    const char *username = env("NERVIX_PROBE_USERNAME");
    const char *password = env("NERVIX_PROBE_PASSWORD");
    const char *domain = env("NERVIX_PROBE_DOMAIN");
    setvbuf(stdout, NULL, _IOLBF, 0);

    nx_session *session = NULL;
    check(nx_session_connect(text(server), strlen(server), text(domain), strlen(domain),
                             text(username), strlen(username), text(password), strlen(password),
                             NULL, &session),
          "connect");
    if (argc > 1 && strcmp(argv[1], "clock") == 0) {
        return run_clock(session, domain);
    }
    if (argc > 1 && strcmp(argv[1], "io") == 0) {
        return run_endpoints(session, domain);
    }

    const char *relay = env("NERVIX_PROBE_RELAY");
    const char *subscription = env("NERVIX_PROBE_SUBSCRIPTION");
    size_t expected_rows = (size_t)strtoull(env("NERVIX_PROBE_ROWS"), NULL, 10);
    char query[512];
    snprintf(query, sizeof query, "SHOW CREATE RELAY %s;", relay);
    nx_outcome *operation = execute(session, query);
    printf("OPERATION %s\n", disposition_name(nx_outcome_disposition(operation)));
    nx_outcome_free(operation);

    nx_outcome *failed = execute(session, "CREATE RELAY;");
    size_t diagnostics = nx_outcome_diagnostic_count(failed);
    printf("ERROR %s diagnostics=%zu span=", disposition_name(nx_outcome_disposition(failed)),
           diagnostics);
    if (diagnostics > 0) {
        const uint8_t *message = NULL;
        size_t message_len = 0;
        bool has_span = false;
        uint32_t start = 0;
        uint32_t end = 0;
        check(nx_outcome_diagnostic(failed, 0, &message, &message_len, &has_span, &start, &end),
              "diagnostic");
        if (has_span) {
            printf("%" PRIu32 "..%" PRIu32 "\n", start, end);
        } else {
            printf("none\n");
        }
    } else {
        printf("none\n");
    }
    nx_outcome_free(failed);

    snprintf(query, sizeof query, "CREATE SUBSCRIPTION %s TO %s;", subscription, relay);
    nx_outcome *opened = execute(session, query);
    const uint8_t *opened_name = NULL;
    size_t opened_name_len = 0;
    uint64_t generation = 0;
    if (!nx_outcome_subscription(opened, &opened_name, &opened_name_len, &generation) ||
        generation == 0) {
        fail("the subscribe command opened no subscription");
    }
    nx_schema *schema = NULL;
    check(nx_outcome_schema(opened, &schema), "schema");
    fields row_fields = read_fields(schema, NX_PART_ROWS);
    fields key_fields = read_fields(schema, NX_PART_BRANCH_KEY);
    for (size_t index = 0; index < row_fields.count; index++) {
        print_field("FIELD", &row_fields.items[index]);
    }
    const uint8_t *branch = NULL;
    size_t branch_len = 0;
    if (nx_schema_branch(schema, &branch, &branch_len)) {
        printf("BRANCH %.*s\n", (int)branch_len, (const char *)branch);
    }
    for (size_t index = 0; index < key_fields.count; index++) {
        print_field("KEY", &key_fields.items[index]);
    }
    printf("SUBSCRIBED\n");

    nx_cancel *rows_deadline = NULL;
    check(nx_cancel_with_deadline(ROWS_DEADLINE_MILLIS, &rows_deadline), "rows deadline");
    size_t seen = 0;
    size_t retained_count = 0;
    nx_event **retained = calloc(expected_rows, sizeof(nx_event *));
    char **reported = calloc(expected_rows, sizeof(char *));
    while (seen < expected_rows) {
        nx_event *event = NULL;
        check(nx_session_next_event(session, rows_deadline, &event), "next event");
        if (nx_event_kind_of(event) != NX_EVENT_ROWS) {
            fail("the subscription reported something other than rows before its rows arrived");
        }
        const uint8_t *name = NULL;
        size_t name_len = 0;
        uint64_t event_generation = 0;
        nx_event_subscription(event, &name, &name_len, &event_generation);
        if (name_len != strlen(subscription) || memcmp(name, subscription, name_len) != 0 ||
            event_generation != generation) {
            fail("rows arrived for another subscription");
        }
        char *lines = row_lines(event, &row_fields, &key_fields);
        fputs(lines, stdout);
        seen += (size_t)nx_event_row_count(event);
        /* Keep a second reference and drop the first, so the rows must survive on it alone. */
        retained[retained_count] = nx_event_retain(event);
        reported[retained_count] = lines;
        retained_count++;
        nx_event_release(event);
    }
    nx_cancel_free(rows_deadline);

    for (size_t index = 0; index < retained_count; index++) {
        const uint8_t *frame = NULL;
        size_t frame_len = 0;
        check(nx_event_frame(retained[index], &frame, &frame_len), "frame");
        if (frame_len < 8 || memcmp(frame + 4, "NXSM", 4) != 0) {
            fail("a retained frame is not a ServerMessage frame");
        }
        char *again = row_lines(retained[index], &row_fields, &key_fields);
        if (strcmp(again, reported[index]) != 0) {
            fail("a retained event reads differently than it did");
        }
        free(again);
        free(reported[index]);
        nx_event_release(retained[index]);
    }
    free(retained);
    free(reported);
    check_cancellation(session);
    printf("CHECKS ok\n");

    snprintf(query, sizeof query, "DELETE SUBSCRIPTION %s;", subscription);
    nx_outcome *closed = execute(session, query);
    printf("CLOSED %s\n", disposition_name(nx_outcome_disposition(closed)));
    nx_outcome_free(closed);

    free(row_fields.items);
    free(key_fields.items);
    nx_schema_free(schema);
    nx_outcome_free(opened);
    nx_session_free(session);
    printf("PASS\n");
    return 0;
}
