/*
 * The C probe of the shared Rust binding.
 *
 * It reads its target from the NERVIX_PROBE_* environment, runs an operation, a failing command,
 * a subscription and its closure through nervix_client.h, and prints the conformance report the
 * scenario compares. Every column is copied in one call; every string and bytes value is also
 * borrowed and compared with its copy. Run as `c-probe clock`, it attaches to the domain's clock
 * instead, reads the state and the first tick of the generation the scenario starts, and detaches.
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

/* The committed clock of the generation the probe follows. */
typedef struct paced_clock {
    uint64_t generation;
    uint64_t period;
    uint64_t skew;
    int64_t origin;
    double rate;
} paced_clock;

static paced_clock read_paced(const nx_clock_event *event) {
    paced_clock clock = {0, 0, 0, 0, 0.0};
    check(nx_clock_event_generation(event, &clock.generation), "clock generation");
    /* The UTC anchor depends on when the scenario's START committed, so it is not reported. */
    check(nx_clock_event_paced(event, &clock.period, &clock.skew, &clock.origin, NULL, &clock.rate),
          "paced clock");
    return clock;
}

/* The report line of a paced state event, which the caller frees. */
static char *state_line(const nx_clock_event *event, const char *domain) {
    paced_clock clock = read_paced(event);
    uint64_t rate_bits = 0;
    memcpy(&rate_bits, &clock.rate, sizeof rate_bits);
    line result = {NULL, 0};
    append(&result,
           "STATE domain=%s generation=%" PRIu64 " state=paced period=%" PRIu64 " skew=%" PRIu64
           " origin=%" PRId64 " rate=f64:%016" PRIx64,
           domain, clock.generation, clock.period, clock.skew, clock.origin, rate_bits);
    return result.text;
}

/* The report line of a tick, which the caller frees, after holding the tick to the committed clock
   of its generation: its boundary is the logical origin plus one period for every id before it,
   and the serving node's reading never precedes the origin. */
static char *tick_line(const nx_clock_event *event, const char *domain, const paced_clock *clock) {
    uint64_t generation = 0;
    check(nx_clock_event_generation(event, &generation), "tick generation");
    if (generation != clock->generation) {
        fail("a tick belongs to another generation than the state before it");
    }
    uint64_t id = 0;
    int64_t boundary = 0;
    int64_t authority_utc = 0;
    int64_t serving_logical = 0;
    check(nx_clock_event_tick(event, &id, &boundary, &authority_utc, &serving_logical), "tick");
    uint64_t offset = 0;
    int64_t expected = 0;
    if (id == 0 || __builtin_mul_overflow(id - 1, clock->period, &offset) ||
        offset > (uint64_t)INT64_MAX ||
        __builtin_add_overflow(clock->origin, (int64_t)offset, &expected) || boundary != expected) {
        fail("a tick's boundary is not the origin plus one period for every id before it");
    }
    if (serving_logical < clock->origin) {
        fail("the serving node's logical reading precedes the logical origin");
    }
    line result = {NULL, 0};
    append(&result, "TICK domain=%s generation=%" PRIu64 " boundary=origin+(id-1)*period", domain,
           generation);
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

static void *release_clock_event(void *argument) {
    nx_clock_event_release(argument);
    return NULL;
}

/* Attaches to the domain's clock, reads the state and the first tick of the generation the
   scenario starts, and detaches. */
static int run_clock(nx_session *session, const char *domain) {
    check_clock_cancellation(session);
    nx_outcome *attached = execute(session, "ATTACH DOMAIN CLOCK;");
    printf("ATTACHED %s\n", disposition_name(nx_outcome_disposition(attached)));
    nx_outcome_free(attached);

    nx_cancel *deadline = NULL;
    check(nx_cancel_with_deadline(CLOCK_DEADLINE_MILLIS, &deadline), "clock deadline");
    nx_clock_event *state = NULL;
    while (state == NULL) {
        nx_clock_event *event = NULL;
        check(nx_session_next_clock_event(session, deadline, &event), "next clock event");
        expect_domain(event, domain);
        if (nx_clock_event_kind_of(event) != NX_CLOCK_EVENT_STATE) {
            fprintf(stderr, "probe failed: the clock reported %s before the started state\n",
                    clock_kind_name(nx_clock_event_kind_of(event)));
            exit(1);
        }
        /* The serving node may report the started generation uninstalled until it holds the
           committed mapping and an assigned clock authority. */
        nx_clock_state installed = NX_CLOCK_STOPPED;
        check(nx_clock_event_state(event, &installed), "clock state");
        if (installed == NX_CLOCK_PACED) {
            state = event;
        } else {
            nx_clock_event_release(event);
        }
    }
    char *reported_state = state_line(state, domain);
    printf("%s\n", reported_state);
    paced_clock clock = read_paced(state);

    nx_clock_event *tick = NULL;
    while (tick == NULL) {
        nx_clock_event *event = NULL;
        check(nx_session_next_clock_event(session, deadline, &event), "next clock event");
        expect_domain(event, domain);
        nx_clock_event_kind kind = nx_clock_event_kind_of(event);
        if (kind == NX_CLOCK_EVENT_TICK) {
            tick = event;
        } else if (kind == NX_CLOCK_EVENT_STATE) {
            /* The serving node reported the installation again; it is still the same generation. */
            uint64_t generation = 0;
            check(nx_clock_event_generation(event, &generation), "clock generation");
            if (generation != clock.generation) {
                fail("the clock moved to another generation before its first tick");
            }
            nx_clock_event_release(event);
        } else {
            fprintf(stderr, "probe failed: the clock reported %s before its first tick\n",
                    clock_kind_name(kind));
            exit(1);
        }
    }
    nx_cancel_free(deadline);
    char *reported_tick = tick_line(tick, domain, &clock);
    printf("%s\n", reported_tick);

    /* Keep a second reference to each event and release the first on another thread, so the
       events must read the same on the second alone. */
    nx_clock_event *retained_state = nx_clock_event_retain(state);
    nx_clock_event *retained_tick = nx_clock_event_retain(tick);
    pthread_t releasers[2];
    if (pthread_create(&releasers[0], NULL, release_clock_event, state) != 0 ||
        pthread_create(&releasers[1], NULL, release_clock_event, tick) != 0) {
        fail("starting the releasing threads");
    }
    pthread_join(releasers[0], NULL);
    pthread_join(releasers[1], NULL);
    char *state_again = state_line(retained_state, domain);
    char *tick_again = tick_line(retained_tick, domain, &clock);
    if (strcmp(state_again, reported_state) != 0 || strcmp(tick_again, reported_tick) != 0) {
        fail("a retained clock event reads differently than it did");
    }
    free(state_again);
    free(tick_again);
    free(reported_state);
    free(reported_tick);
    nx_clock_event_release(retained_state);
    nx_clock_event_release(retained_tick);

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
