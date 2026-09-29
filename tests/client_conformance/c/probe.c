/*
 * The C probe of the shared Rust binding.
 *
 * It reads its target from the NERVIX_PROBE_* environment, runs an operation, a failing command,
 * a subscription and its closure through nervix_client.h, and prints the conformance report the
 * scenario compares. Every column is copied in one call; every string and bytes value is also
 * borrowed and compared with its copy. Run as `c-probe clock`, it attaches to the domain's running
 * clock instead, reads the clock the attach reported before its first tick, follows the generation
 * a STOP and START begin and the attachment restored after its session ends, and detaches.
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
