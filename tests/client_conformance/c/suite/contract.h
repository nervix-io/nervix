/* Application-level cases of the shared C ABI, compiled independently as C11 and C++17.
 * The host program owns allocations and runs the named table; these cases own their assertions.
 * No probe or Rust fixture is included, and no test compiles or installs anything. */
#ifndef NERVIX_SUITE_CONTRACT_H
#define NERVIX_SUITE_CONTRACT_H

#include "nervix_client.h"
#include <arpa/inet.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define REQUIRE(condition) suite_require((condition), #condition)

struct suite_test { const char *tier; const char *name; void (*run)(void); };

#define SUITE_RELEASE(type, operation) \
    static void suite_release_##type(void *pointer) { operation((type *)pointer); }
SUITE_RELEASE(nx_error, nx_error_free)
SUITE_RELEASE(nx_cancel, nx_cancel_free)
SUITE_RELEASE(nx_connect_options, nx_connect_options_free)
SUITE_RELEASE(nx_session, nx_session_free)
SUITE_RELEASE(nx_execution, nx_execution_free)
SUITE_RELEASE(nx_outcome, nx_outcome_free)
SUITE_RELEASE(nx_schema, nx_schema_free)
SUITE_RELEASE(nx_suggestions, nx_suggestions_free)
SUITE_RELEASE(nx_inspection, nx_inspection_free)
SUITE_RELEASE(nx_event, nx_event_release)
SUITE_RELEASE(nx_clock_event, nx_clock_event_release)
SUITE_RELEASE(nx_domain_clock, nx_domain_clock_release)
SUITE_RELEASE(nx_fields, nx_fields_free)
SUITE_RELEASE(nx_producer, nx_producer_free)
SUITE_RELEASE(nx_consumer, nx_consumer_free)
SUITE_RELEASE(nx_delivery, nx_delivery_release)
SUITE_RELEASE(nx_batch, nx_batch_release)
SUITE_RELEASE(nx_batch_builder, nx_batch_builder_free)
SUITE_RELEASE(nx_submission_outcome, nx_submission_outcome_free)
#undef SUITE_RELEASE

#define HOLD(type, pointer) suite_hold((pointer), suite_release_##type)

static const uint8_t *suite_bytes(const char *text) { return (const uint8_t *)text; }

static void *suite_allocate(size_t count) {
    void *pointer = calloc(count ? count : 1, 1);
    suite_hold(pointer, free);
    return pointer;
}

static bool suite_equal(const uint8_t *actual, size_t length, const char *expected) {
    return length == strlen(expected) && memcmp(actual, expected, length) == 0;
}

static void suite_check(nx_error *error) {
    if (error == NULL) return;
    HOLD(nx_error, error);
    const uint8_t *message = NULL;
    size_t length = 0;
    nx_error_message(error, &message, &length);
    fprintf(stderr, "binding error %d: %.*s\n", (int)nx_error_kind_of(error),
            (int)length, (const char *)message);
    REQUIRE(error == NULL);
}

static nx_error *suite_kind(nx_error *error, nx_error_kind kind) {
    REQUIRE(error != NULL);
    HOLD(nx_error, error);
    REQUIRE(nx_error_kind_of(error) == kind);
    const uint8_t *message = NULL;
    size_t length = 0;
    nx_error_message(error, &message, &length);
    REQUIRE(message != NULL && length > 0);
    return error;
}

static nx_cancel *suite_deadline(uint64_t millis) {
    nx_cancel *cancel = NULL;
    suite_check(nx_cancel_with_deadline(millis, &cancel));
    HOLD(nx_cancel, cancel);
    return cancel;
}

static const char *suite_env(const char *key) {
    const char *value = getenv(key);
    REQUIRE(value != NULL && value[0] != '\0');
    return value;
}

// ANCHOR: connect_options
static nx_connect_options *suite_options(void) {
    nx_connect_options *options = nx_connect_options_new();
    HOLD(nx_connect_options, options);
    const char *username = suite_env("NERVIX_PROBE_USERNAME");
    const char *password = suite_env("NERVIX_PROBE_PASSWORD");
    suite_check(nx_connect_options_basic_auth(options, suite_bytes(username), strlen(username),
                                              suite_bytes(password), strlen(password)));
    FILE *file = fopen(suite_env("NERVIX_SUITE_CA_CERTIFICATE"), "rb");
    REQUIRE(file != NULL);
    uint8_t certificate[16384];
    size_t length = fread(certificate, 1, sizeof certificate, file);
    int ended = feof(file);
    int closed = fclose(file);
    REQUIRE(length > 0 && ended && closed == 0);
    suite_check(nx_connect_options_ca_certificate(options, certificate, length));
    suite_check(nx_connect_options_timeout(options, NX_TIMEOUT_CONNECT, 30000));
    suite_check(nx_connect_options_timeout(options, NX_TIMEOUT_REQUEST, 120000));
    suite_check(nx_connect_options_timeout(options, NX_TIMEOUT_RETRY, 120000));
    suite_check(nx_connect_options_timeout(options, NX_TIMEOUT_BACKUP_WAIT, 300000));
    memset(certificate, 0, length); /* B-4: the setter copied the caller's bytes. */
    return options;
}
// ANCHOR_END: connect_options

// ANCHOR: connect
static nx_session *suite_connect(const char *server, const char *domain) {
    if (server == NULL) server = suite_env("NERVIX_PROBE_GRPC_URI");
    if (domain == NULL) domain = suite_env("NERVIX_PROBE_DOMAIN");
    nx_connect_options *options = suite_options();
    nx_session *session = NULL;
    suite_check(nx_session_connect(suite_bytes(server), strlen(server), suite_bytes(domain),
                                    strlen(domain), options, suite_deadline(120000), &session));
    HOLD(nx_session, session);
    suite_drop(options);
    return session;
}
// ANCHOR_END: connect

static nx_execution *suite_prepare(nx_session *session, const char *query) {
    nx_execution *execution = NULL;
    suite_check(nx_session_prepare(session, suite_bytes(query), strlen(query),
                                    suite_deadline(120000), &execution));
    HOLD(nx_execution, execution);
    return execution;
}

static nx_outcome *suite_execute(nx_session *session, const char *query) {
    nx_execution *execution = suite_prepare(session, query);
    nx_outcome *outcome = NULL;
    suite_check(nx_session_execute(session, execution, suite_deadline(120000), &outcome));
    HOLD(nx_outcome, outcome);
    suite_drop(execution);
    return outcome;
}

static nx_outcome *suite_completed(nx_session *session, const char *query) {
    nx_outcome *outcome = suite_execute(session, query);
    if (nx_outcome_disposition(outcome) != NX_DISPOSITION_COMPLETED) {
        const uint8_t *message = NULL;
        size_t length = 0;
        nx_outcome_message(outcome, &message, &length);
        fprintf(stderr, "%s: %.*s\n", query, (int)length, (const char *)message);
    }
    REQUIRE(nx_outcome_disposition(outcome) == NX_DISPOSITION_COMPLETED);
    return outcome;
}

static nx_fields *suite_fields(bool output) {
    const char *names[] = {"id", "tenant", "amount", "ratio", "flag", "note", "raw", "at",
                           "tags", "grid", "secret", "echo"};
    nx_type types[] = {NX_TYPE_U32, NX_TYPE_STRING, NX_TYPE_I64, NX_TYPE_F64, NX_TYPE_BOOL,
                       NX_TYPE_STRING, NX_TYPE_BYTES, NX_TYPE_DATETIME, NX_TYPE_LIST,
                       NX_TYPE_FIXED_LIST, NX_TYPE_STRING, NX_TYPE_U32};
    nx_fields *fields = nx_fields_new();
    HOLD(nx_fields, fields);
    for (size_t i = 0; i < (output ? 12u : 11u); ++i) {
        suite_check(nx_fields_add(fields, suite_bytes(names[i]), strlen(names[i]), types[i],
                                   i == 9 ? 2 : 0, i == 5 || i == 6, i == 10));
        if (i == 8) suite_check(nx_fields_element(fields, NX_TYPE_STRING, 0));
        if (i == 9) suite_check(nx_fields_element(fields, NX_TYPE_I16, 0));
    }
    return fields;
}

static nx_producer *suite_producer(nx_session *session, const char *name) {
    const char *domain = suite_env("NERVIX_PROBE_DOMAIN");
    nx_fields *fields = suite_fields(false);
    nx_producer *producer = NULL;
    suite_check(nx_session_open_ingestor(session, suite_bytes(domain), strlen(domain),
                  suite_bytes(name), strlen(name), fields, 4, 1u << 20,
                  suite_deadline(120000), &producer));
    HOLD(nx_producer, producer);
    suite_drop(fields);
    return producer;
}

static nx_consumer *suite_consumer(nx_session *session) {
    const char *domain = suite_env("NERVIX_PROBE_DOMAIN");
    nx_fields *fields = suite_fields(true);
    nx_consumer *consumer = NULL;
    suite_check(nx_session_subscribe_emitter(session, suite_bytes(domain), strlen(domain),
                  suite_bytes("typed_out"), 9, fields, 4, 1u << 20,
                  suite_deadline(120000), &consumer));
    HOLD(nx_consumer, consumer);
    suite_drop(fields);
    return consumer;
}

static void suite_text_column(nx_batch_builder *builder, size_t column, size_t level,
                              const uint8_t *bytes, size_t length) {
    uint64_t offsets[] = {0, (uint64_t)length, (uint64_t)length};
    suite_check(nx_batch_builder_varlen(builder, column, level, offsets, 3, bytes, length));
}

static nx_batch *suite_build_schema(nx_schema *schema, uint32_t first, bool output) {
    nx_batch_builder *builder = NULL;
    suite_check(nx_batch_builder_new(schema, 2, &builder));
    HOLD(nx_batch_builder, builder);
    uint32_t ids[] = {first, first + 1};
    int64_t amounts[] = {INT64_MIN, INT64_MAX};
    uint64_t ratio_bits[] = {UINT64_C(0x8000000000000000), UINT64_C(1)};
    uint8_t flags[] = {1, 0};
    int64_t at[] = {INT64_MIN, INT64_MAX};
    int16_t grid[] = {INT16_MIN, INT16_MAX, -1, 0};
    uint8_t nulls[] = {NX_CELL_VALUE, NX_CELL_NULL};
    uint8_t note[] = {'A', 0, 'B', 0xe2, 0x98, 0x83};
    uint8_t raw[] = {0, 0xff};
    uint64_t tenant_offsets[] = {0, 4, 8};
    suite_check(nx_batch_builder_fixed(builder, 0, 0, ids, sizeof ids));
    suite_check(nx_batch_builder_varlen(builder, 1, 0, tenant_offsets, 3,
                                        suite_bytes("acmeacme"), 8));
    suite_check(nx_batch_builder_fixed(builder, 2, 0, amounts, sizeof amounts));
    suite_check(nx_batch_builder_fixed(builder, 3, 0, ratio_bits, sizeof ratio_bits));
    suite_check(nx_batch_builder_fixed(builder, 4, 0, flags, sizeof flags));
    suite_check(nx_batch_builder_states(builder, 5, nulls, 2));
    suite_text_column(builder, 5, 0, note, sizeof note);
    suite_check(nx_batch_builder_states(builder, 6, nulls, 2));
    suite_text_column(builder, 6, 0, raw, sizeof raw);
    suite_check(nx_batch_builder_fixed(builder, 7, 0, at, sizeof at));
    uint64_t lists[] = {0, 1, 2};
    uint64_t tag_offsets[] = {0, 3, 3};
    suite_check(nx_batch_builder_offsets(builder, 8, 0, lists, 3));
    suite_check(nx_batch_builder_varlen(builder, 8, 1, tag_offsets, 3,
                                        suite_bytes("tag"), 3));
    suite_check(nx_batch_builder_fixed(builder, 9, 1, grid, sizeof grid));
    suite_text_column(builder, 10, 0, suite_bytes("secret"), 6);
    if (output) suite_check(nx_batch_builder_fixed(builder, 11, 0, ids, sizeof ids));
    memset(ids, 0, sizeof ids); /* B-13: changing a caller buffer cannot change the builder. */
    nx_batch *batch = NULL;
    suite_check(nx_batch_builder_finish(builder, &batch));
    HOLD(nx_batch, batch);
    suite_drop(builder);
    return batch;
}

static nx_batch *suite_build(nx_producer *producer, uint32_t first) {
    nx_schema *schema = NULL;
    suite_check(nx_producer_schema(producer, &schema));
    HOLD(nx_schema, schema);
    nx_batch *batch = suite_build_schema(schema, first, false);
    suite_drop(schema);
    return batch;
}

static uint64_t suite_submit(nx_producer *producer, nx_batch *batch) {
    uint64_t submission = 0;
    suite_check(nx_producer_submit(producer, batch, suite_deadline(120000), &submission));
    return submission;
}

static nx_submission_outcome *suite_rejoin(nx_producer *producer, uint64_t submission) {
    nx_submission_outcome *outcome = NULL;
    suite_check(nx_producer_rejoin(producer, submission, suite_deadline(120000), &outcome));
    HOLD(nx_submission_outcome, outcome);
    return outcome;
}

static nx_delivery *suite_next(nx_consumer *consumer) {
    nx_delivery *delivery = NULL;
    suite_check(nx_consumer_next(consumer, suite_deadline(120000), &delivery));
    HOLD(nx_delivery, delivery);
    return delivery;
}

static void suite_ack(nx_delivery *delivery) {
    nx_settlement settlement = NX_SETTLEMENT_CONSUMER_ENDED;
    suite_check(nx_delivery_ack(delivery, suite_deadline(120000), &settlement));
    REQUIRE(settlement == NX_SETTLEMENT_CONFIRMED);
}

static nx_event *suite_rows(nx_session *session) {
    nx_event *event = NULL;
    suite_check(nx_session_next_event(session, suite_deadline(120000), &event));
    HOLD(nx_event, event);
    REQUIRE(nx_event_kind_of(event) == NX_EVENT_ROWS);
    return event;
}

static nx_outcome *suite_subscribe(nx_session *session, const char *name) {
    char query[256];
    int length = snprintf(query, sizeof query, "CREATE SUBSCRIPTION %s TO watched_rows;", name);
    REQUIRE(length > 0 && (size_t)length < sizeof query);
    return suite_completed(session, query);
}

static void suite_publish(nx_session *session, uint32_t first) {
    nx_producer *producer = suite_producer(session, "watched_in");
    uint64_t submission = suite_submit(producer, suite_build(producer, first));
    nx_submission_outcome *outcome = suite_rejoin(producer, submission);
    REQUIRE(nx_submission_outcome_result(outcome) == NX_SUBMISSION_COMPLETED);
    suite_check(nx_producer_close(producer, suite_deadline(120000)));
    suite_drop(producer);
}

static void suite_assert_batch(nx_batch *batch, uint32_t first, bool output) {
    REQUIRE(nx_batch_row_count(batch) == 2);
    nx_schema *schema = NULL;
    suite_check(nx_batch_schema(batch, &schema));
    HOLD(nx_schema, schema);
    size_t field_count = 0;
    suite_check(nx_schema_field_count(schema, NX_PART_ROWS, &field_count));
    REQUIRE(field_count == (output ? 12u : 11u));
    suite_drop(schema);
    size_t cells = 0;
    suite_check(nx_batch_cells(batch, 9, 1, &cells));
    REQUIRE(cells == 4);
    uint32_t ids[2];
    suite_check(nx_batch_fixed(batch, 0, 0, ids, sizeof ids));
    REQUIRE(ids[0] == first && ids[1] == first + 1);
    int64_t amounts[2];
    suite_check(nx_batch_fixed(batch, 2, 0, amounts, sizeof amounts));
    REQUIRE(amounts[0] == INT64_MIN && amounts[1] == INT64_MAX);
    uint64_t bits[2];
    suite_check(nx_batch_fixed(batch, 3, 0, bits, sizeof bits));
    REQUIRE(bits[0] == UINT64_C(0x8000000000000000) && bits[1] == 1);
    int64_t at[2];
    suite_check(nx_batch_fixed(batch, 7, 0, at, sizeof at));
    REQUIRE(at[0] == INT64_MIN && at[1] == INT64_MAX);
    uint8_t states[2];
    suite_check(nx_batch_states(batch, 5, states, sizeof states));
    REQUIRE(states[0] == NX_CELL_VALUE && states[1] == NX_CELL_NULL);
    uint64_t offsets[3];
    suite_check(nx_batch_offsets(batch, 8, 0, offsets, 3));
    REQUIRE(offsets[0] == 0 && offsets[1] == 1 && offsets[2] == 2);
    int16_t grid[4];
    suite_check(nx_batch_fixed(batch, 9, 1, grid, sizeof grid));
    REQUIRE(grid[0] == INT16_MIN && grid[1] == INT16_MAX && grid[2] == -1 && grid[3] == 0);
    size_t bytes = 0;
    suite_check(nx_batch_varlen(batch, 5, 0, NULL, 0, NULL, 0, &bytes));
    REQUIRE(bytes == 6);
    uint8_t text[6];
    suite_check(nx_batch_varlen(batch, 5, 0, offsets, 3, text, sizeof text, &bytes));
    const uint8_t expected[] = {'A', 0, 'B', 0xe2, 0x98, 0x83};
    REQUIRE(memcmp(text, expected, sizeof expected) == 0);
    REQUIRE(offsets[0] == 0 && offsets[1] == 6 && offsets[2] == 6);
    suite_kind(nx_batch_fixed(batch, 5, 0, ids, sizeof ids), NX_ERROR_TYPE);
    suite_kind(nx_batch_fixed(batch, 0, 0, ids, 1), NX_ERROR_INVALID_ARGUMENT);
    suite_kind(nx_batch_cells(batch, 0, 1, &cells), NX_ERROR_INVALID_ARGUMENT);
    if (output) {
        uint32_t echoes[2];
        suite_check(nx_batch_fixed(batch, 11, 0, echoes, sizeof echoes));
        REQUIRE(memcmp(ids, echoes, sizeof ids) == 0);
    }
}

/* Opaque loopback TCP fixtures. They inspect no HTTP, TLS or session bytes. The worker owns
 * both connected descriptors, and the controller only shuts them down while holding its lock;
 * descriptors are closed before they can be reused. One 64 KiB buffer bounds each relay. */
struct suite_peer {
    int listener;
    int client;
    int upstream;
    unsigned short target;
    bool stop;
    bool cut;
    bool started;
    bool accepted;
    pthread_mutex_t mutex;
    pthread_cond_t changed;
    pthread_t worker;
    unsigned short port;
};

static void *suite_peer_worker(void *argument) {
    struct suite_peer *peer = (struct suite_peer *)argument;
    for (;;) {
        pthread_mutex_lock(&peer->mutex);
        bool stop = peer->stop;
        pthread_mutex_unlock(&peer->mutex);
        if (stop) break;
        struct pollfd listening = {peer->listener, POLLIN, 0};
        if (poll(&listening, 1, 100) <= 0) continue;
        int client = accept(peer->listener, NULL, NULL);
        if (client < 0) continue;
#ifdef SO_NOSIGPIPE
        int no_sigpipe = 1;
        if (setsockopt(client, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe, sizeof no_sigpipe) != 0) {
            close(client);
            continue;
        }
#endif
        pthread_mutex_lock(&peer->mutex);
        peer->client = client;
        peer->accepted = true;
        pthread_cond_broadcast(&peer->changed);
        if (peer->cut || peer->stop) {
            close(client);
            peer->client = -1;
            pthread_mutex_unlock(&peer->mutex);
            continue;
        }
        if (peer->target == 0) {
            while (!peer->stop && !peer->cut) pthread_cond_wait(&peer->changed, &peer->mutex);
            close(client);
            peer->client = -1;
            pthread_mutex_unlock(&peer->mutex);
            continue;
        }
        int upstream = socket(AF_INET, SOCK_STREAM, 0);
#ifdef SO_NOSIGPIPE
        if (upstream >= 0 &&
            setsockopt(upstream, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe, sizeof no_sigpipe) != 0) {
            close(upstream);
            upstream = -1;
        }
#endif
        peer->upstream = upstream;
        pthread_mutex_unlock(&peer->mutex);
        struct sockaddr_in address;
        memset(&address, 0, sizeof address);
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(peer->target);
        if (upstream >= 0 && connect(upstream, (struct sockaddr *)&address, sizeof address) == 0) {
            uint8_t buffer[65536];
            bool ended = false;
            while (!ended) {
                struct pollfd pair[] = {{client, POLLIN, 0}, {upstream, POLLIN, 0}};
                if (poll(pair, 2, 100) < 0) break;
                pthread_mutex_lock(&peer->mutex);
                stop = peer->stop || peer->cut;
                pthread_mutex_unlock(&peer->mutex);
                if (stop) break;
                for (size_t i = 0; i < 2; ++i) {
                    if (!pair[i].revents) continue;
                    ssize_t length = recv(pair[i].fd, buffer, sizeof buffer, 0);
                    if (length <= 0) { ended = true; break; }
                    size_t sent = 0;
                    while (sent < (size_t)length) {
                        int flags = 0;
#ifdef MSG_NOSIGNAL
                        flags = MSG_NOSIGNAL;
#endif
                        ssize_t count = send(pair[1 - i].fd, buffer + sent,
                                             (size_t)length - sent, flags);
                        if (count <= 0) { ended = true; break; }
                        sent += (size_t)count;
                    }
                }
            }
        }
        pthread_mutex_lock(&peer->mutex);
        close(client);
        if (upstream >= 0) close(upstream);
        peer->client = -1;
        peer->upstream = -1;
        pthread_mutex_unlock(&peer->mutex);
    }
    return NULL;
}

static void suite_peer_free(void *argument) {
    struct suite_peer *peer = (struct suite_peer *)argument;
    pthread_mutex_lock(&peer->mutex);
    peer->stop = true;
    pthread_cond_broadcast(&peer->changed);
    if (peer->client >= 0) shutdown(peer->client, SHUT_RDWR);
    if (peer->upstream >= 0) shutdown(peer->upstream, SHUT_RDWR);
    pthread_mutex_unlock(&peer->mutex);
    if (peer->started) pthread_join(peer->worker, NULL);
    if (peer->listener >= 0) close(peer->listener);
    pthread_cond_destroy(&peer->changed);
    pthread_mutex_destroy(&peer->mutex);
    free(peer);
}

static struct suite_peer *suite_peer_new(unsigned short target) {
    struct suite_peer *peer = (struct suite_peer *)calloc(1, sizeof *peer);
    REQUIRE(peer != NULL);
    peer->listener = -1;
    peer->client = -1;
    peer->upstream = -1;
    peer->target = target;
    REQUIRE(pthread_mutex_init(&peer->mutex, NULL) == 0);
    REQUIRE(pthread_cond_init(&peer->changed, NULL) == 0);
    suite_hold(peer, suite_peer_free);
    peer->listener = socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(peer->listener >= 0);
    struct sockaddr_in address;
    memset(&address, 0, sizeof address);
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    REQUIRE(bind(peer->listener, (struct sockaddr *)&address, sizeof address) == 0);
    socklen_t length = sizeof address;
    REQUIRE(getsockname(peer->listener, (struct sockaddr *)&address, &length) == 0);
    peer->port = ntohs(address.sin_port);
    REQUIRE(listen(peer->listener, 16) == 0);
    REQUIRE(pthread_create(&peer->worker, NULL, suite_peer_worker, peer) == 0);
    peer->started = true;
    return peer;
}

static void suite_peer_cut(struct suite_peer *peer, bool cut) {
    pthread_mutex_lock(&peer->mutex);
    peer->cut = cut;
    if (cut && peer->client >= 0) shutdown(peer->client, SHUT_RDWR);
    if (cut && peer->upstream >= 0) shutdown(peer->upstream, SHUT_RDWR);
    pthread_cond_broadcast(&peer->changed);
    pthread_mutex_unlock(&peer->mutex);
}

static nx_session *suite_through(struct suite_peer **peer_out, const char *domain) {
    char *end = NULL;
    unsigned long port = strtoul(suite_env("NERVIX_SUITE_LEADER_PORT"), &end, 10);
    REQUIRE(end != NULL && *end == '\0' && port > 0 && port <= UINT16_MAX);
    struct suite_peer *peer = suite_peer_new((unsigned short)port);
    const char *scheme = suite_env("NERVIX_SUITE_SCHEME");
    char uri[80];
    int length = snprintf(uri, sizeof uri, "%s://127.0.0.1:%u", scheme, (unsigned)peer->port);
    REQUIRE(length > 0 && (size_t)length < sizeof uri);
    *peer_out = peer;
    return suite_connect(uri, domain);
}

/* One outstanding action on the scenario's ordered pipe, bounded independently of the suite.
 * Cleanup uses the boolean result instead of throwing from a C++ deleter. */
static uint64_t suite_action_sequence;
static bool suite_refusal_armed;

static bool suite_action_confirmed(const char *action) {
    if (suite_action_sequence == UINT64_MAX) return false;
    ++suite_action_sequence;
    char expected[192], received[256];
    int length = snprintf(expected, sizeof expected, "NERVIX-SUITE-CONFIRMED %" PRIu64 " %s\n",
                          suite_action_sequence, action);
    if (length <= 0 || (size_t)length >= sizeof expected) return false;
    if (printf("NERVIX-SUITE-ACTION %" PRIu64 " %s\n", suite_action_sequence, action) < 0 ||
        fflush(stdout) != 0) return false;
    struct timespec began, now;
    if (clock_gettime(CLOCK_MONOTONIC, &began) != 0) return false;
    size_t count = 0;
    while (count < sizeof received) {
        if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) return false;
        int64_t remaining = 120000 - (int64_t)(now.tv_sec - began.tv_sec) * 1000 -
                            (now.tv_nsec - began.tv_nsec) / 1000000;
        if (remaining <= 0) return false;
        struct pollfd input = {STDIN_FILENO, POLLIN, 0};
        int ready = poll(&input, 1, (int)remaining);
        if (ready < 0 && errno == EINTR) continue;
        if (ready <= 0) return false;
        ssize_t taken = read(STDIN_FILENO, received + count, 1);
        if (taken < 0 && errno == EINTR) continue;
        if (taken != 1) return false;
        if (received[count++] == '\n') {
            return count == (size_t)length && memcmp(received, expected, count) == 0;
        }
    }
    return false;
}

static bool suite_clear_refusals(void) {
    if (!suite_refusal_armed) return true;
    suite_refusal_armed = false;
    return suite_action_confirmed("clear-subscription-opening-refusals");
}

struct suite_wait {
    nx_session *session;
    nx_session *connected;
    nx_connect_options *options;
    nx_cancel *cancel;
    nx_error *error;
    nx_event *event;
    char uri[80];
    pthread_t worker;
    bool started;
    bool joined;
};

static void *suite_connect_wait(void *argument) {
    struct suite_wait *wait = (struct suite_wait *)argument;
    wait->error = nx_session_connect(suite_bytes(wait->uri), strlen(wait->uri), NULL, 0,
                                      wait->options, wait->cancel, &wait->connected);
    return NULL;
}

static void *suite_event_wait(void *argument) {
    struct suite_wait *wait = (struct suite_wait *)argument;
    wait->error = nx_session_next_event(wait->session, wait->cancel, &wait->event);
    return NULL;
}

static void suite_wait_free(void *argument) {
    struct suite_wait *wait = (struct suite_wait *)argument;
    if (wait->started && !wait->joined) {
        nx_cancel_trigger(wait->cancel);
        pthread_join(wait->worker, NULL);
    }
    if (wait->error != NULL) nx_error_free(wait->error);
    if (wait->event != NULL) nx_event_release(wait->event);
    if (wait->connected != NULL) nx_session_free(wait->connected);
    free(wait);
}

static struct suite_wait *suite_wait_new(nx_cancel *cancel) {
    struct suite_wait *wait = (struct suite_wait *)calloc(1, sizeof *wait);
    REQUIRE(wait != NULL);
    wait->cancel = cancel;
    suite_hold(wait, suite_wait_free);
    return wait;
}

struct suite_release_job { void *pointer; void (*release)(void *); };

static void *suite_release_on_thread(void *argument) {
    struct suite_release_job *job = (struct suite_release_job *)argument;
    job->release(job->pointer);
    return NULL;
}

static void suite_cross_thread(void *pointer, void (*release)(void *)) {
    struct suite_release_job job = {suite_forget(pointer), release};
    pthread_t worker;
    int started = pthread_create(&worker, NULL, suite_release_on_thread, &job);
    if (started != 0) release(job.pointer);
    REQUIRE(started == 0);
    REQUIRE(pthread_join(worker, NULL) == 0);
}

static void test_boundary_fields_and_arguments(void) {
    nx_fields *fields = nx_fields_new();
    HOLD(nx_fields, fields);
    const nx_type scalars[] = {NX_TYPE_U8, NX_TYPE_I8, NX_TYPE_U16, NX_TYPE_I16,
        NX_TYPE_U32, NX_TYPE_I32, NX_TYPE_U64, NX_TYPE_I64, NX_TYPE_F32, NX_TYPE_F64,
        NX_TYPE_BOOL, NX_TYPE_STRING, NX_TYPE_BYTES, NX_TYPE_DATETIME};
    for (size_t i = 0; i < sizeof scalars / sizeof scalars[0]; ++i) {
        char name[32];
        snprintf(name, sizeof name, "field_%zu", i);
        suite_check(nx_fields_add(fields, suite_bytes(name), strlen(name), scalars[i], 0,
                                    false, false));
    }
    suite_check(nx_fields_add(fields, suite_bytes("nested"), 6, NX_TYPE_LIST, 0, true, true));
    suite_kind(nx_fields_add(fields, suite_bytes("too_early"), 9, NX_TYPE_I8, 0, false, false),
                NX_ERROR_INVALID_ARGUMENT);
    suite_check(nx_fields_element(fields, NX_TYPE_FIXED_LIST, 2));
    suite_check(nx_fields_element(fields, NX_TYPE_U64, 0));
    suite_kind(nx_fields_element(fields, NX_TYPE_U8, 0), NX_ERROR_INVALID_ARGUMENT);
    suite_kind(nx_fields_add(fields, suite_bytes("bad"), 3, 999, 0, false, false),
                NX_ERROR_INVALID_ARGUMENT);
    const uint8_t invalid_utf8[] = {0xff};
    suite_kind(nx_fields_add(fields, invalid_utf8, 1, NX_TYPE_U8, 0, false, false),
                NX_ERROR_INVALID_ARGUMENT);
    nx_connect_options *options = nx_connect_options_new();
    HOLD(nx_connect_options, options);
    suite_kind(nx_connect_options_basic_auth(options, invalid_utf8, 1, NULL, 0),
                NX_ERROR_INVALID_ARGUMENT);
    suite_kind(nx_connect_options_tls_requirement(options, 999), NX_ERROR_INVALID_ARGUMENT);
    suite_check(nx_connect_options_timeout(options, NX_TIMEOUT_CONNECT, 0));
    nx_session *session = NULL;
    suite_kind(nx_session_connect(suite_bytes("http://127.0.0.1:1"), 18, NULL, 0, options,
                                  NULL, &session), NX_ERROR_INVALID_ARGUMENT);
    REQUIRE(session == NULL);
    suite_kind(nx_cancel_with_deadline(UINT64_MAX, NULL), NX_ERROR_INVALID_ARGUMENT);
}

/* ABI enums are signed discriminants: every named state is positive and distinguishable from
 * the others in its domain. The live cases below hold returned states to their actual meaning;
 * this boundary case also holds the full header vocabulary to those representation guarantees. */
#define DOMAIN(type, ...) do { \
    const type states[] = {__VA_ARGS__}; \
    for (size_t i = 0; i < sizeof states / sizeof states[0]; ++i) { \
        REQUIRE((int32_t)states[i] > 0); \
        for (size_t j = 0; j < i; ++j) REQUIRE(states[i] != states[j]); \
    } \
} while (0)

static void test_boundary_public_enumeration_domains(void) {
    DOMAIN(nx_tls_requirement, NX_TLS_PREFERRED, NX_TLS_REQUIRED);
    DOMAIN(nx_timeout_kind, NX_TIMEOUT_CONNECT, NX_TIMEOUT_REQUEST, NX_TIMEOUT_RETRY, NX_TIMEOUT_BACKUP_WAIT);
    DOMAIN(nx_error_kind, NX_ERROR_INVALID_ARGUMENT, NX_ERROR_CONNECT, NX_ERROR_TRANSPORT,
        NX_ERROR_UNCERTAIN, NX_ERROR_REJECTED, NX_ERROR_DEADLINE, NX_ERROR_CANCELLED,
        NX_ERROR_OVERFLOW, NX_ERROR_PROTOCOL, NX_ERROR_TYPE, NX_ERROR_CLOSED,
        NX_ERROR_INTERRUPTED, NX_ERROR_REOPEN_REQUIRED);
    DOMAIN(nx_disposition, NX_DISPOSITION_COMPLETED, NX_DISPOSITION_FAILED, NX_DISPOSITION_NOT_LEADER,
        NX_DISPOSITION_TRANSACTION_DETACHED, NX_DISPOSITION_TRANSACTION_TAKEN_OVER,
        NX_DISPOSITION_OUTCOME_UNKNOWN, NX_DISPOSITION_EXECUTION_REFERENCE_CONFLICT,
        NX_DISPOSITION_EXECUTION_REFERENCE_EXPIRED, NX_DISPOSITION_PREVIEW_STALE);
    DOMAIN(nx_upload_failure, NX_UPLOAD_INVALID_STREAM, NX_UPLOAD_RESOURCE_NOT_DECLARED,
        NX_UPLOAD_SIZE_MISMATCH, NX_UPLOAD_QUOTA_EXCEEDED, NX_UPLOAD_INSTALLATION_FAILED,
        NX_UPLOAD_IDENTITY_CONFLICT);
    DOMAIN(nx_completion_status, NX_COMPLETION_READY, NX_COMPLETION_MISSING_CONTEXT,
        NX_COMPLETION_STALE_CONTEXT, NX_COMPLETION_LOOKUP_FAILED);
    DOMAIN(nx_completion_kind, NX_COMPLETION_TEXT, NX_COMPLETION_LOCAL_DIRECTORY_LOOKUP);
    DOMAIN(nx_part, NX_PART_ROWS, NX_PART_BRANCH_KEY);
    DOMAIN(nx_cell_state, NX_CELL_VALUE, NX_CELL_NULL, NX_CELL_REDACTED);
    DOMAIN(nx_event_kind, NX_EVENT_ROWS, NX_EVENT_DELIVERY_LOST, NX_EVENT_ROWS_SKIPPED,
        NX_EVENT_ENDED, NX_EVENT_INTERRUPTED, NX_EVENT_CONSUMER_OVERFLOW, NX_EVENT_RESTORATION_FAILED);
    DOMAIN(nx_end_reason, NX_END_RELAY_REMOVED, NX_END_RELAY_CHANGED);
    DOMAIN(nx_restoration_cause, NX_RESTORATION_IN_TRANSACTION, NX_RESTORATION_INVALID_STATEMENT,
        NX_RESTORATION_NAME_IN_USE, NX_RESTORATION_INVALID_FILTER, NX_RESTORATION_UNAVAILABLE,
        NX_RESTORATION_REJECTED, NX_RESTORATION_FAILED);
    DOMAIN(nx_clock_event_kind, NX_CLOCK_EVENT_STATE, NX_CLOCK_EVENT_TICK, NX_CLOCK_EVENT_ENDED,
        NX_CLOCK_EVENT_INTERRUPTED, NX_CLOCK_EVENT_RESTORATION_FAILED);
    DOMAIN(nx_clock_state, NX_CLOCK_STOPPED, NX_CLOCK_UNINSTALLED, NX_CLOCK_UNPACED, NX_CLOCK_PACED);
    DOMAIN(nx_clock_end_reason, NX_CLOCK_END_DOMAIN_REMOVED);
    DOMAIN(nx_open_refusal, NX_OPEN_DOMAIN_NOT_FOUND, NX_OPEN_DOMAIN_STOPPED, NX_OPEN_ENDPOINT_NOT_FOUND,
        NX_OPEN_NOT_CLIENT_ENDPOINT, NX_OPEN_ENDPOINT_UNAVAILABLE, NX_OPEN_SCHEMA_MISMATCH,
        NX_OPEN_TOO_MANY_ENDPOINTS, NX_OPEN_SESSION_CAPACITY_EXHAUSTED,
        NX_OPEN_NODE_CAPACITY_EXHAUSTED, NX_OPEN_INVALID_LIMITS, NX_OPEN_IN_TRANSACTION);
    DOMAIN(nx_endpoint_state, NX_ENDPOINT_ACTIVE, NX_ENDPOINT_INTERRUPTED, NX_ENDPOINT_RESTORING,
        NX_ENDPOINT_REOPEN_REQUIRED, NX_ENDPOINT_CLOSED);
    DOMAIN(nx_reopen_reason, NX_REOPEN_DOMAIN_STOPPED, NX_REOPEN_ENDPOINT_REMOVED,
        NX_REOPEN_SCHEMA_CHANGED, NX_REOPEN_CONTRACT_CHANGED, NX_REOPEN_GENERATION_CHANGED,
        NX_REOPEN_PROTOCOL_VIOLATED, NX_REOPEN_REFUSED);
    DOMAIN(nx_ack_window, NX_ACK_SEQUENTIAL, NX_ACK_PARALLEL);
    DOMAIN(nx_admission, NX_ADMISSION_OPEN, NX_ADMISSION_SUSPENDED);
    DOMAIN(nx_submission_result, NX_SUBMISSION_NOT_ADMITTED, NX_SUBMISSION_COMPLETED,
        NX_SUBMISSION_PROCESSING_FAILED, NX_SUBMISSION_OUTCOME_UNKNOWN);
    DOMAIN(nx_submission_refusal, NX_REFUSAL_INVALID_BATCH, NX_REFUSAL_SUSPENDED, NX_REFUSAL_BUSY,
        NX_REFUSAL_DRAINING, NX_REFUSAL_PRODUCER_ENDED, NX_REFUSAL_CREDIT_EXCEEDED);
    DOMAIN(nx_batch_defect, NX_DEFECT_MALFORMED, NX_DEFECT_UNEXPECTED_MESSAGE,
        NX_DEFECT_COMPRESSED, NX_DEFECT_SCHEMA_MISMATCH, NX_DEFECT_NOT_ONE_BATCH,
        NX_DEFECT_TOO_MANY_ROWS, NX_DEFECT_TOO_LARGE, NX_DEFECT_INVALID_DATA);
    DOMAIN(nx_processing_failure, NX_FAILURE_ACK_TIMED_OUT, NX_FAILURE_REJECTED);
    DOMAIN(nx_submission_uncertainty, NX_UNCERTAINTY_INTERRUPTED, NX_UNCERTAINTY_OWNER_LOST,
        NX_UNCERTAINTY_SESSION_LOST);
    DOMAIN(nx_settlement, NX_SETTLEMENT_CONFIRMED, NX_SETTLEMENT_STALE_REFERENCE,
        NX_SETTLEMENT_WRONG_CONSUMER, NX_SETTLEMENT_INVALID_REASON, NX_SETTLEMENT_CONSUMER_ENDED);
}
#undef DOMAIN

static void test_boundary_silent_peer_deadline(void) {
    struct suite_peer *peer = suite_peer_new(0);
    nx_connect_options *options = nx_connect_options_new();
    HOLD(nx_connect_options, options);
    char uri[80];
    snprintf(uri, sizeof uri, "http://127.0.0.1:%u", (unsigned)peer->port);
    nx_session *session = NULL;
    // ANCHOR: deadline
    nx_cancel *deadline = suite_deadline(200);
    suite_kind(nx_session_connect(suite_bytes(uri), strlen(uri), NULL, 0, options, deadline,
                                    &session), NX_ERROR_DEADLINE);
    // ANCHOR_END: deadline
    REQUIRE(session == NULL);
    pthread_mutex_lock(&peer->mutex);
    bool accepted = peer->accepted;
    pthread_mutex_unlock(&peer->mutex);
    REQUIRE(accepted);
}

static void test_boundary_connect_cancelled_from_another_thread(void) {
    struct suite_peer *peer = suite_peer_new(0);
    nx_connect_options *options = nx_connect_options_new();
    HOLD(nx_connect_options, options);
    struct suite_wait *wait = suite_wait_new(suite_deadline(120000));
    wait->options = options;
    snprintf(wait->uri, sizeof wait->uri, "http://127.0.0.1:%u", (unsigned)peer->port);
    REQUIRE(pthread_create(&wait->worker, NULL, suite_connect_wait, wait) == 0);
    wait->started = true;
    struct timespec until;
    REQUIRE(timespec_get(&until, TIME_UTC) == TIME_UTC);
    until.tv_sec += 120;
    pthread_mutex_lock(&peer->mutex);
    int observed = 0;
    while (!peer->accepted && observed == 0)
        observed = pthread_cond_timedwait(&peer->changed, &peer->mutex, &until);
    bool accepted = peer->accepted;
    pthread_mutex_unlock(&peer->mutex);
    // ANCHOR: cancel_connect
    nx_cancel_trigger(wait->cancel);
    REQUIRE(pthread_join(wait->worker, NULL) == 0);
    wait->joined = true;
    nx_error *error = wait->error;
    wait->error = NULL;
    suite_kind(error, NX_ERROR_CANCELLED);
    // ANCHOR_END: cancel_connect
    REQUIRE(accepted && wait->connected == NULL);
}

static void test_b1_prepare_once_and_recover_cancelled_execution(void) {
    nx_session *session = suite_connect(NULL, NULL);
    // ANCHOR: execute
    nx_execution *execution = suite_prepare(session, "CREATE SCHEMA c_recovered ( value I64 );");
    const uint8_t *reference = NULL;
    size_t reference_len = 0;
    nx_execution_reference(execution, &reference, &reference_len);
    REQUIRE(reference_len > 0);
    nx_cancel *cancel = nx_cancel_new();
    HOLD(nx_cancel, cancel);
    nx_cancel_trigger(cancel);
    nx_outcome *outcome = NULL;
    nx_error *ended = suite_kind(nx_session_execute(session, execution, cancel, &outcome),
                                  NX_ERROR_CANCELLED);
    const uint8_t *failed_reference = NULL;
    size_t failed_len = 0;
    REQUIRE(nx_error_execution_reference(ended, &failed_reference, &failed_len));
    REQUIRE(failed_len == reference_len && memcmp(reference, failed_reference, failed_len) == 0);
    suite_check(nx_session_execute(session, execution, suite_deadline(120000), &outcome));
    HOLD(nx_outcome, outcome);
    REQUIRE(nx_outcome_disposition(outcome) == NX_DISPOSITION_COMPLETED);
    // ANCHOR_END: execute
    const uint8_t *completed_reference = NULL;
    size_t completed_len = 0;
    REQUIRE(nx_outcome_execution_reference(outcome, &completed_reference, &completed_len));
    REQUIRE(completed_len == reference_len && memcmp(reference, completed_reference, completed_len) == 0);
    nx_outcome *again = NULL;
    suite_check(nx_session_execute(session, execution, suite_deadline(120000), &again));
    HOLD(nx_outcome, again);
    REQUIRE(nx_outcome_disposition(again) == NX_DISPOSITION_COMPLETED);
    nx_outcome *diagnostic = suite_execute(session, "CREATE RELAY;");
    REQUIRE(nx_outcome_disposition(diagnostic) == NX_DISPOSITION_FAILED);
    REQUIRE(nx_outcome_diagnostic_count(diagnostic) == 1);
    const uint8_t *message = NULL;
    size_t message_len = 0;
    bool has_span = false;
    uint32_t start = 0, end = 0;
    suite_check(nx_outcome_diagnostic(diagnostic, 0, &message, &message_len, &has_span, &start, &end));
    REQUIRE(message_len > 0 && has_span && start == 12 && end == 12);
    nx_upload_failure upload = NX_UPLOAD_INVALID_STREAM;
    REQUIRE(!nx_outcome_upload_failure(outcome, &upload));
}

static void test_b2_objects_outlive_their_session_and_release_once(void) {
    nx_session *session = suite_connect(NULL, NULL);
    nx_outcome *opened = suite_subscribe(session, "c_owned");
    nx_schema *schema = NULL;
    suite_check(nx_outcome_schema(opened, &schema));
    HOLD(nx_schema, schema);
    suite_drop(session);
    // ANCHOR: lifetimes
    const uint8_t *name = NULL;
    size_t length = 0;
    uint64_t generation = 0;
    REQUIRE(nx_outcome_subscription(opened, &name, &length, &generation));
    REQUIRE(suite_equal(name, length, "c_owned") && generation > 0);
    suite_drop(opened);
    size_t fields = 0;
    suite_check(nx_schema_field_count(schema, NX_PART_ROWS, &fields));
    REQUIRE(fields == 11);
    suite_drop(schema);
    // ANCHOR_END: lifetimes
}

static void test_b3_event_references_keep_borrowed_frames_alive(void) {
    nx_session *session = suite_connect(NULL, NULL);
    suite_subscribe(session, "c_retained");
    suite_publish(session, 301);
    nx_event *event = suite_rows(session);
    // ANCHOR: retain_event
    nx_event *held = nx_event_retain(event);
    HOLD(nx_event, held);
    const uint8_t *frame = NULL;
    size_t length = 0;
    suite_check(nx_event_frame(held, &frame, &length));
    REQUIRE(length > 8 && memcmp(frame + 4, "NXSM", 4) == 0);
    suite_drop(event);
    REQUIRE(memcmp(frame + 4, "NXSM", 4) == 0);
    suite_cross_thread(held, suite_release_nx_event);
    // ANCHOR_END: retain_event
    suite_completed(session, "DELETE SUBSCRIPTION c_retained;");
}

static void test_b4_strings_options_tls_authentication_and_seeds(void) {
    const char *server = suite_env("NERVIX_PROBE_GRPC_URI");
    const char *domain = suite_env("NERVIX_PROBE_DOMAIN");
    const nx_tls_requirement requirements[] = {NX_TLS_PREFERRED, NX_TLS_REQUIRED};
    bool tls = strcmp(suite_env("NERVIX_SUITE_SCHEME"), "https") == 0;
    for (size_t i = 0; i < 2; ++i) {
        nx_connect_options *options = suite_options();
        suite_check(nx_connect_options_tls_requirement(options, requirements[i]));
        nx_session *session = NULL;
        nx_error *error = nx_session_connect(suite_bytes(server), strlen(server), suite_bytes(domain),
                            strlen(domain), options, suite_deadline(120000), &session);
        if (!tls && requirements[i] == NX_TLS_REQUIRED) {
            suite_kind(error, NX_ERROR_CONNECT);
            REQUIRE(session == NULL);
        } else {
            suite_check(error);
            HOLD(nx_session, session);
            suite_completed(session, "LIST DOMAINS;");
            suite_drop(session);
        }
        suite_drop(options);
    }
    nx_connect_options *options = suite_options();
    suite_check(nx_connect_options_basic_auth(options, suite_bytes("default"), 7,
                                              suite_bytes("wrong-password"), 14));
    nx_session *session = NULL;
    nx_error *refused = suite_kind(nx_session_connect(suite_bytes(server), strlen(server),
        suite_bytes(domain), strlen(domain), options, suite_deadline(120000), &session), NX_ERROR_CONNECT);
    nx_open_refusal reason = NX_OPEN_DOMAIN_NOT_FOUND;
    REQUIRE(!nx_error_open_refusal(refused, &reason));
    suite_drop(options);
    options = suite_options();
    suite_check(nx_connect_options_add_seed(options, suite_bytes(server), strlen(server)));
    const char *unreachable = tls ? "https://127.0.0.1:1" : "http://127.0.0.1:1";
    suite_check(nx_session_connect(suite_bytes(unreachable), strlen(unreachable), suite_bytes(domain),
                  strlen(domain), options, suite_deadline(120000), &session));
    HOLD(nx_session, session);
    suite_completed(session, "LIST DOMAINS;");
    if (tls) {
        nx_connect_options *untrusted = nx_connect_options_new();
        HOLD(nx_connect_options, untrusted);
        nx_session *none = NULL;
        suite_kind(nx_session_connect(suite_bytes(server), strlen(server), NULL, 0, untrusted,
                                      suite_deadline(120000), &none), NX_ERROR_CONNECT);
        REQUIRE(none == NULL);
    }
}

struct suite_command_job {
    nx_session *session;
    nx_cancel *cancel;
    nx_error *error;
    bool completed;
    bool started;
    bool joined;
    pthread_t worker;
};
static void *suite_command_thread(void *argument) {
    struct suite_command_job *job = (struct suite_command_job *)argument;
    nx_execution *execution = NULL;
    nx_outcome *outcome = NULL;
    const char *query = "LIST DOMAINS;";
    job->error = nx_session_prepare(job->session, suite_bytes(query), strlen(query), job->cancel,
                                    &execution);
    if (job->error == NULL) job->error = nx_session_execute(job->session, execution, job->cancel, &outcome);
    if (job->error == NULL) job->completed = nx_outcome_disposition(outcome) == NX_DISPOSITION_COMPLETED;
    if (outcome != NULL) nx_outcome_free(outcome);
    if (execution != NULL) nx_execution_free(execution);
    return NULL;
}

struct suite_command_group { struct suite_command_job jobs[4]; };
static void suite_commands_free(void *argument) {
    struct suite_command_group *group = (struct suite_command_group *)argument;
    for (size_t i = 0; i < 4; ++i) {
        struct suite_command_job *job = &group->jobs[i];
        if (job->cancel != NULL) nx_cancel_trigger(job->cancel);
    }
    for (size_t i = 0; i < 4; ++i) {
        struct suite_command_job *job = &group->jobs[i];
        if (job->started && !job->joined) pthread_join(job->worker, NULL);
        if (job->error != NULL) nx_error_free(job->error);
        if (job->cancel != NULL) nx_cancel_free(job->cancel);
    }
    free(group);
}

static void test_b5_calls_run_on_native_host_threads(void) {
    nx_session *session = suite_connect(NULL, NULL);
    struct suite_command_group *group = (struct suite_command_group *)calloc(1, sizeof *group);
    REQUIRE(group != NULL);
    suite_hold(group, suite_commands_free);
    for (size_t i = 0; i < 4; ++i) {
        struct suite_command_job *job = &group->jobs[i];
        job->session = session;
        suite_check(nx_cancel_with_deadline(120000, &job->cancel));
        REQUIRE(pthread_create(&job->worker, NULL, suite_command_thread, job) == 0);
        job->started = true;
    }
    for (size_t i = 0; i < 4; ++i) {
        struct suite_command_job *job = &group->jobs[i];
        REQUIRE(pthread_join(job->worker, NULL) == 0);
        job->joined = true;
        nx_error *error = job->error;
        job->error = NULL;
        suite_check(error);
        REQUIRE(job->completed);
    }
    struct suite_wait *wait = suite_wait_new(suite_deadline(120000));
    wait->session = session;
    // ANCHOR: threads
    REQUIRE(pthread_create(&wait->worker, NULL, suite_event_wait, wait) == 0);
    wait->started = true;
    suite_completed(session, "LIST DOMAINS;");
    nx_cancel_trigger(wait->cancel);
    REQUIRE(pthread_join(wait->worker, NULL) == 0);
    wait->joined = true;
    nx_error *error = wait->error;
    wait->error = NULL;
    suite_kind(error, NX_ERROR_CANCELLED);
    // ANCHOR_END: threads
    REQUIRE(wait->event == NULL);
}

static void test_b6_columns_use_size_queries_and_pointer_lengths(void) {
    nx_session *session = suite_connect(NULL, NULL);
    nx_outcome *opened = suite_subscribe(session, "c_columns");
    nx_schema *schema = NULL;
    suite_check(nx_outcome_schema(opened, &schema));
    HOLD(nx_schema, schema);
    suite_publish(session, 601);
    nx_event *event = suite_rows(session);
    REQUIRE(nx_event_row_count(event) == 2);
    nx_schema *from_event = NULL;
    suite_check(nx_event_schema(event, &from_event));
    HOLD(nx_schema, from_event);
    size_t levels = 0;
    suite_check(nx_schema_field_levels(schema, NX_PART_ROWS, 8, &levels));
    REQUIRE(levels == 2);
    nx_type type = NX_TYPE_U8;
    uint32_t width = 1;
    suite_check(nx_schema_field_level(schema, NX_PART_ROWS, 8, 1, &type, &width));
    REQUIRE(type == NX_TYPE_STRING && width == 0);
    const uint8_t *branch = NULL;
    size_t branch_len = 0;
    REQUIRE(nx_schema_branch(schema, &branch, &branch_len));
    REQUIRE(suite_equal(branch, branch_len, "by_tenant"));
    bool nullable = false, sensitive = false;
    const uint8_t *name = NULL;
    size_t name_len = 0;
    suite_check(nx_schema_field(schema, NX_PART_ROWS, 10, &name, &name_len, &type, &nullable, &sensitive));
    REQUIRE(type == NX_TYPE_STRING && !nullable && sensitive && suite_equal(name, name_len, "secret"));
    // ANCHOR: columns
    uint32_t ids[2];
    suite_check(nx_event_column_fixed(event, NX_PART_ROWS, 0, ids, sizeof ids));
    REQUIRE(ids[0] == 601 && ids[1] == 602);
    uint8_t states[2];
    suite_check(nx_event_column_states(event, NX_PART_ROWS, 5, states, 2));
    REQUIRE(states[0] == NX_CELL_VALUE && states[1] == NX_CELL_NULL);
    size_t bytes = 0;
    suite_check(nx_event_column_varlen(event, NX_PART_ROWS, 5, NULL, 0, NULL, 0, &bytes));
    uint8_t *text = (uint8_t *)suite_allocate(bytes);
    uint64_t offsets[3];
    suite_check(nx_event_column_varlen(event, NX_PART_ROWS, 5, offsets, 3, text, bytes, &bytes));
    REQUIRE(bytes == 6 && text[0] == 'A' && text[1] == 0 && text[2] == 'B');
    // ANCHOR_END: columns
    const uint8_t *cell = NULL;
    size_t cell_len = 0;
    suite_check(nx_event_cell_varlen(event, NX_PART_ROWS, 0, 5, &cell, &cell_len));
    REQUIRE(cell_len == bytes && memcmp(cell, text, bytes) == 0);
    suite_kind(nx_event_cell_varlen(event, NX_PART_ROWS, 1, 5, &cell, &cell_len), NX_ERROR_TYPE);
    suite_check(nx_event_column_states(event, NX_PART_ROWS, 10, states, 2));
    REQUIRE(states[0] == NX_CELL_REDACTED && states[1] == NX_CELL_REDACTED);
    suite_kind(nx_event_cell_varlen(event, NX_PART_ROWS, 0, 10, &cell, &cell_len), NX_ERROR_TYPE);
    suite_check(nx_event_cell_varlen(event, NX_PART_BRANCH_KEY, 0, 0, &cell, &cell_len));
    REQUIRE(suite_equal(cell, cell_len, "acme"));
    suite_completed(session, "DELETE SUBSCRIPTION c_columns;");
}

static void test_completion_pages_have_utf8_byte_ranges(void) {
    nx_session *session = suite_connect(NULL, NULL);
    // ANCHOR: completion
    nx_suggestions *suggestions = NULL;
    suite_check(nx_session_suggest(session, suite_bytes("CRE"), 3, 3, 1, NULL, 0,
                                    suite_deadline(120000), &suggestions));
    HOLD(nx_suggestions, suggestions);
    REQUIRE(nx_suggestions_status(suggestions) == NX_COMPLETION_READY);
    REQUIRE(nx_suggestions_count(suggestions) == 1);
    nx_completion_kind kind = NX_COMPLETION_LOCAL_DIRECTORY_LOOKUP;
    const uint8_t *value = NULL, *replacement = NULL;
    size_t length = 0, replacement_len = 0;
    uint32_t start = 0, end = 0;
    suite_check(nx_suggestions_at(suggestions, 0, &kind, &value, &length, &start, &end,
                                   &replacement, &replacement_len));
    REQUIRE(kind == NX_COMPLETION_TEXT && length > 0 && replacement_len > 0);
    REQUIRE(start <= 3 && end <= 3 && start <= end);
    // ANCHOR_END: completion
    suite_kind(nx_suggestions_at(suggestions, nx_suggestions_count(suggestions), &kind,
                  &value, &length, &start, &end, &replacement, &replacement_len), NX_ERROR_INVALID_ARGUMENT);
    // ANCHOR: completion_pages
    nx_suggestions *page = NULL;
    const uint8_t *continuation = NULL;
    size_t continuation_len = 0, candidates = 0;
    bool finished = false;
    for (size_t number = 0; number < 64; ++number) {
        nx_suggestions *next = NULL;
        suite_check(nx_session_suggest(session, suite_bytes("CREATE "), 7, 7, 2,
                        continuation, continuation_len, suite_deadline(120000), &next));
        HOLD(nx_suggestions, next);
        /* Keep the preceding page alive until the call has copied its continuation. */
        if (page != NULL) suite_drop(page);
        page = next;
        REQUIRE(nx_suggestions_status(page) == NX_COMPLETION_READY);
        size_t count = nx_suggestions_count(page);
        REQUIRE(count > 0 && count <= 2);
        candidates += count;
        if (!nx_suggestions_continuation(page, &continuation, &continuation_len)) {
            finished = true;
            break;
        }
        REQUIRE(continuation_len > 0);
    }
    REQUIRE(finished && candidates > 2);
    // ANCHOR_END: completion_pages
}

static bool suite_contains(const uint8_t *data, size_t length, const char *text) {
    size_t wanted = strlen(text);
    for (size_t at = 0; wanted <= length && at <= length - wanted; ++at) {
        if (memcmp(data + at, text, wanted) == 0) return true;
    }
    return false;
}

/* Rows reach a materialized relay's owner after their submission completes, so a read polls,
 * bounded at 30 seconds, until the owner reports the expected count. */
static void suite_await_state_rows(nx_session *session, const char *show, uint64_t expected) {
    for (int attempt = 0; attempt < 120; ++attempt) {
        size_t mark = suite_mark();
        nx_outcome *outcome = suite_completed(session, show);
        nx_inspection *inspection = NULL;
        suite_check(nx_outcome_inspection(outcome, &inspection));
        HOLD(nx_inspection, inspection);
        uint64_t branches = 0, rows = 0, bytes = 0;
        bool page = nx_inspection_counts(inspection, &branches, &rows, &bytes);
        suite_cleanup_to(mark);
        REQUIRE(page);
        if (rows == expected) return;
        struct timespec pause = {0, 250000000};
        nanosleep(&pause, NULL);
    }
    suite_require(false, "the materialized state reached its owner within 30 seconds");
}

static void test_inspection_retains_typed_state_after_its_outcome_is_released(void) {
    nx_session *session = suite_connect(NULL, NULL);
    nx_outcome *created = suite_completed(session,
        "CREATE RELAY c_state SCHEMA suite_row UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;");
    suite_completed(session,
        "CREATE CLIENT PRODUCER INGESTOR c_state_in FROM SCHEMA suite_row "
        "MODE ACK SEQUENTIAL ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND "
        "TIMESTAMP NOW TO c_state INHERIT ALL UNBRANCHED "
        "FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;");
    nx_producer *producer = suite_producer(session, "c_state_in");
    uint64_t submission = suite_submit(producer, suite_build(producer, 1901));
    REQUIRE(nx_submission_outcome_result(suite_rejoin(producer, submission)) == NX_SUBMISSION_COMPLETED);
    suite_check(nx_producer_close(producer, suite_deadline(120000)));
    const char *show = "SHOW RELAY c_state MATERIALIZED STATE FOR UNBRANCHED LIMIT 10 FORMAT JSON;";
    suite_await_state_rows(session, show, 1);
    // ANCHOR: inspection
    {
        nx_outcome *license_outcome = suite_completed(session, "DESCRIBE LICENSE;");
        nx_inspection *license_inspection = NULL;
        suite_check(nx_outcome_inspection(license_outcome, &license_inspection));
        HOLD(nx_inspection, license_inspection);
        suite_drop(license_outcome);
        uint64_t license_branches = 0, license_rows = 0, license_bytes = 0;
        REQUIRE(nx_inspection_counts(license_inspection, &license_branches, &license_rows, &license_bytes));
        REQUIRE(license_branches == 0 && license_rows == 3 && license_bytes > 0);
        const uint8_t *license_json = NULL;
        size_t license_json_len = 0;
        nx_inspection_json(license_inspection, &license_json, &license_json_len);
        REQUIRE(suite_contains(license_json, license_json_len, "\"cluster_license\""));
        suite_drop(license_inspection);
    }
    nx_outcome *outcome = suite_completed(session, show);
    nx_inspection *inspection = NULL;
    suite_check(nx_outcome_inspection(outcome, &inspection));
    HOLD(nx_inspection, inspection);
    suite_drop(outcome); /* The inspection keeps what it read after its outcome is released. */
    uint64_t branches = 0, rows = 0, bytes = 0;
    REQUIRE(nx_inspection_counts(inspection, &branches, &rows, &bytes)); /* false when refused */
    REQUIRE(branches == 0 && rows == 1 && bytes > 0); /* one row: the last of an unbranched relay */
    const uint8_t *json = NULL;
    size_t json_len = 0;
    nx_inspection_json(inspection, &json, &json_len);
    REQUIRE(json_len > 0 && json[0] == '{' && suite_contains(json, json_len, "\"c_state\""));
    nx_schema *schema = NULL;
    suite_check(nx_inspection_schema(inspection, 0, 0, &schema));
    HOLD(nx_schema, schema);
    const uint8_t *frame = NULL;
    size_t frame_len = 0;
    suite_check(nx_inspection_row_frame(inspection, 0, 0, 0, &frame, &frame_len));
    REQUIRE(frame_len > 8 && memcmp(frame + 4, "NXRB", 4) == 0); /* a verified Row batch */
    // ANCHOR_END: inspection
    suite_kind(nx_inspection_row_frame(inspection, 0, 0, 1, &frame, &frame_len),
               NX_ERROR_INVALID_ARGUMENT);
    suite_drop(inspection);
    size_t fields = 0;
    suite_check(nx_schema_field_count(schema, NX_PART_ROWS, &fields)); /* owned past the inspection */
    REQUIRE(fields == 13); /* suite_row's eleven fields, then the low and high ingestion watermarks */

    nx_outcome *refused = suite_execute(session, "SHOW RELAY c_absent MATERIALIZED STATE FORMAT JSON;");
    REQUIRE(nx_outcome_disposition(refused) == NX_DISPOSITION_FAILED);
    nx_inspection *rejection = NULL;
    suite_check(nx_outcome_inspection(refused, &rejection));
    HOLD(nx_inspection, rejection);
    REQUIRE(!nx_inspection_counts(rejection, &branches, &rows, &bytes));
    nx_schema *absent = NULL;
    suite_kind(nx_inspection_schema(rejection, 0, 0, &absent), NX_ERROR_INVALID_ARGUMENT);
    REQUIRE(absent == NULL);
    nx_inspection *none = NULL;
    suite_kind(nx_outcome_inspection(created, &none), NX_ERROR_INVALID_ARGUMENT);
    REQUIRE(none == NULL);
}

static void test_b7_subscription_gap_end_and_variant_accessors(void) {
    nx_session *publishing = suite_connect(NULL, NULL);
    struct suite_peer *peer = NULL;
    nx_session *watching = suite_through(&peer, NULL);
    suite_subscribe(watching, "c_gap");
    suite_publish(publishing, 701);
    nx_event *before = suite_rows(watching);
    const uint8_t *name = NULL;
    size_t name_len = 0;
    uint64_t generation = 0;
    nx_event_subscription(before, &name, &name_len, &generation);
    REQUIRE(suite_equal(name, name_len, "c_gap") && generation > 0);
    nx_restoration_cause cause = NX_RESTORATION_FAILED;
    uint64_t retry_after = 0;
    suite_kind(nx_event_restoration_cause(before, &cause), NX_ERROR_TYPE);
    suite_kind(nx_event_restoration_message(before, &name, &name_len), NX_ERROR_TYPE);
    suite_kind(nx_event_restoration_retry_after(before, &retry_after), NX_ERROR_TYPE);
    nx_end_reason ended = NX_END_RELAY_CHANGED;
    suite_kind(nx_event_end_reason(before, &ended), NX_ERROR_TYPE);
    // ANCHOR: subscription_gap
    suite_peer_cut(peer, true);
    nx_event *gap = NULL;
    suite_check(nx_session_next_event(watching, suite_deadline(120000), &gap));
    HOLD(nx_event, gap);
    REQUIRE(nx_event_kind_of(gap) == NX_EVENT_INTERRUPTED);
    nx_event_subscription(gap, &name, &name_len, &generation);
    REQUIRE(suite_equal(name, name_len, "c_gap"));
    suite_peer_cut(peer, false);
    // ANCHOR_END: subscription_gap
    /* A restored subscription is witnessed by its rows, not by a leader-routed command. */
    bool restored = false;
    for (size_t attempt = 0; attempt < 24 && !restored; ++attempt) {
        size_t mark = suite_mark();
        suite_publish(publishing, 703);
        nx_event *event = NULL;
        nx_error *error = nx_session_next_event(watching, suite_deadline(5000), &event);
        if (error == NULL) {
            HOLD(nx_event, event);
            REQUIRE(nx_event_kind_of(event) == NX_EVENT_ROWS);
            uint32_t ids[2];
            suite_check(nx_event_column_fixed(event, NX_PART_ROWS, 0, ids, sizeof ids));
            REQUIRE(ids[0] == 703 && ids[1] == 704);
            restored = true;
        } else {
            suite_kind(error, NX_ERROR_DEADLINE);
        }
        suite_cleanup_to(mark);
    }
    REQUIRE(restored);
    suite_completed(watching, "DELETE SUBSCRIPTION c_gap;");
    suite_completed(watching, "CREATE SCHEMA c_ending_schema ( value I64 );");
    suite_completed(watching, "CREATE RELAY c_ending SCHEMA c_ending_schema UNBRANCHED;");
    suite_completed(watching, "CREATE SUBSCRIPTION c_ended TO c_ending;");
    suite_completed(watching, "DROP RELAY c_ending;");
    nx_event *last = NULL;
    suite_check(nx_session_next_event(watching, suite_deadline(120000), &last));
    HOLD(nx_event, last);
    REQUIRE(nx_event_kind_of(last) == NX_EVENT_ENDED);
    suite_check(nx_event_end_reason(last, &ended));
    REQUIRE(ended == NX_END_RELAY_REMOVED);
}

static void test_b7_refused_subscription_restoration_reports_its_cause_and_recovers(void) {
    nx_session *publishing = suite_connect(NULL, NULL);
    struct suite_peer *peer = NULL;
    nx_session *watching = suite_through(&peer, NULL);
    suite_subscribe(watching, "c_refused");
    suite_peer_cut(peer, true);
    suite_refusal_armed = true; /* Main clears even when an assertion aborts this case. */
    REQUIRE(suite_action_confirmed("refuse-next-subscription-opening"));
    suite_peer_cut(peer, false);
    nx_event *event = NULL;
    suite_check(nx_session_next_event(watching, suite_deadline(120000), &event));
    HOLD(nx_event, event);
    REQUIRE(nx_event_kind_of(event) == NX_EVENT_INTERRUPTED);
    // ANCHOR: restoration_failed
    suite_check(nx_session_next_event(watching, suite_deadline(120000), &event));
    HOLD(nx_event, event);
    REQUIRE(nx_event_kind_of(event) == NX_EVENT_RESTORATION_FAILED);
    nx_restoration_cause cause = NX_RESTORATION_FAILED;
    const uint8_t *message = NULL, *name = NULL;
    size_t message_len = 0, name_len = 0;
    uint64_t retry_after = 0, generation = 0;
    suite_check(nx_event_restoration_cause(event, &cause));
    suite_check(nx_event_restoration_message(event, &message, &message_len));
    suite_check(nx_event_restoration_retry_after(event, &retry_after));
    REQUIRE(cause == NX_RESTORATION_UNAVAILABLE && message != NULL && message_len > 0 && retry_after > 0);
    nx_event_subscription(event, &name, &name_len, &generation);
    REQUIRE(suite_equal(name, name_len, "c_refused") && generation > 0);
    // ANCHOR_END: restoration_failed
    bool restored = false;
    for (size_t attempt = 0; attempt < 24 && !restored; ++attempt) {
        size_t mark = suite_mark();
        suite_publish(publishing, 711);
        nx_event *next = NULL;
        nx_error *error = nx_session_next_event(watching, suite_deadline(5000), &next);
        if (error != NULL) {
            suite_kind(error, NX_ERROR_DEADLINE);
        } else {
            HOLD(nx_event, next);
            if (nx_event_kind_of(next) == NX_EVENT_RESTORATION_FAILED) {
                suite_check(nx_event_restoration_cause(next, &cause));
                REQUIRE(cause == NX_RESTORATION_UNAVAILABLE);
            } else {
                REQUIRE(nx_event_kind_of(next) == NX_EVENT_ROWS);
                uint32_t ids[2];
                suite_check(nx_event_column_fixed(next, NX_PART_ROWS, 0, ids, sizeof ids));
                REQUIRE(ids[0] == 711 && ids[1] == 712);
                uint64_t next_generation = 0;
                nx_event_subscription(next, &name, &name_len, &next_generation);
                REQUIRE(suite_equal(name, name_len, "c_refused") && next_generation == generation);
                restored = true;
            }
        }
        suite_cleanup_to(mark);
    }
    REQUIRE(restored);
    REQUIRE(suite_clear_refusals());
    suite_completed(watching, "DELETE SUBSCRIPTION c_refused;");
}

static void test_b8_backup_recovers_under_its_execution_reference(void) {
    nx_session *session = suite_connect(NULL, NULL);
    char query[2048], path[1024];
    int length = snprintf(path, sizeof path, "%s/c-backup.nvxb", suite_env("NERVIX_SUITE_BACKUP_DIRECTORY"));
    REQUIRE(length > 0 && (size_t)length < sizeof path && strchr(path, '\'') == NULL);
    length = snprintf(query, sizeof query, "BACKUP DOMAIN %s TO '%s';", suite_env("NERVIX_PROBE_DOMAIN"), path);
    REQUIRE(length > 0 && (size_t)length < sizeof query);
    nx_execution *execution = suite_prepare(session, query);
    nx_cancel *cancel = nx_cancel_new();
    HOLD(nx_cancel, cancel);
    nx_cancel_trigger(cancel);
    nx_outcome *outcome = NULL;
    nx_error *error = suite_kind(nx_session_execute(session, execution, cancel, &outcome), NX_ERROR_CANCELLED);
    const uint8_t *reference = NULL;
    size_t reference_len = 0;
    REQUIRE(nx_error_execution_reference(error, &reference, &reference_len));
    // ANCHOR: backup
    suite_check(nx_session_execute(session, execution, suite_deadline(120000), &outcome));
    HOLD(nx_outcome, outcome);
    REQUIRE(nx_outcome_disposition(outcome) == NX_DISPOSITION_COMPLETED);
    uint64_t bytes = 0;
    const uint8_t *digest = NULL;
    size_t digest_len = 0;
    REQUIRE(nx_outcome_backup(outcome, &bytes, &digest, &digest_len));
    struct stat archive;
    REQUIRE(stat(path, &archive) == 0 && archive.st_size > 0);
    REQUIRE(bytes == (uint64_t)archive.st_size && digest_len == 32 && digest != NULL);
    // ANCHOR_END: backup
}

static void test_b10_restore_streams_the_archive_and_reports_its_steps(void) {
    nx_session *session = suite_connect(NULL, NULL);
    char query[2048], path[1024];
    int length = snprintf(path, sizeof path, "%s/c-restore.nvxb", suite_env("NERVIX_SUITE_BACKUP_DIRECTORY"));
    REQUIRE(length > 0 && (size_t)length < sizeof path && strchr(path, '\'') == NULL);
    length = snprintf(query, sizeof query, "BACKUP DOMAIN %s TO '%s';", suite_env("NERVIX_PROBE_DOMAIN"), path);
    REQUIRE(length > 0 && (size_t)length < sizeof query);
    suite_completed(session, query);
    length = snprintf(query, sizeof query, "RESTORE DOMAIN %s AS c_restored FROM '%s' DRY RUN;",
                        suite_env("NERVIX_PROBE_DOMAIN"), path);
    REQUIRE(length > 0 && (size_t)length < sizeof query);
    nx_outcome *planned = suite_completed(session, query);
    bool dry_run = false, failed = true;
    uint64_t domains = 0, resources = 0, models = 0;
    REQUIRE(nx_outcome_restore(planned, &dry_run, &domains, &resources, &models, &failed));
    REQUIRE(dry_run && !failed && domains == 1 && resources == 0 && models > 0);
    length = snprintf(query, sizeof query, "RESTORE DOMAIN %s AS c_restored FROM '%s';",
                        suite_env("NERVIX_PROBE_DOMAIN"), path);
    REQUIRE(length > 0 && (size_t)length < sizeof query);
    // ANCHOR: restore
    nx_execution *execution = suite_prepare(session, query);
    nx_cancel *cancel = nx_cancel_new();
    HOLD(nx_cancel, cancel);
    nx_cancel_trigger(cancel);
    nx_outcome *restored = NULL;
    suite_kind(nx_session_execute(session, execution, cancel, &restored), NX_ERROR_CANCELLED);
    suite_check(nx_session_execute(session, execution, suite_deadline(120000), &restored));
    HOLD(nx_outcome, restored);
    REQUIRE(nx_outcome_disposition(restored) == NX_DISPOSITION_COMPLETED);
    REQUIRE(nx_outcome_restore(restored, &dry_run, &domains, &resources, &models, &failed));
    REQUIRE(!dry_run && !failed && domains == 1 && resources == 0 && models > 0);
    // ANCHOR_END: restore
}

static nx_session *suite_paced(nx_session *administration, const char *domain) {
    char query[256];
    int length = snprintf(query, sizeof query, "CREATE PACED DOMAIN %s WITH PERIOD 100ms SKEW 10ms;", domain);
    REQUIRE(length > 0 && (size_t)length < sizeof query);
    suite_completed(administration, query);
    nx_session *session = suite_connect(suite_env("NERVIX_SUITE_LEADER_GRPC_URI"), domain);
    suite_completed(session, "START AT '2030-01-01T00:00:00Z' TIME RATE 2.0;");
    return session;
}

static nx_clock_event *suite_tick(nx_session *session, uint64_t generation) {
    for (size_t attempt = 0; attempt < 64; ++attempt) {
        nx_clock_event *event = NULL;
        suite_check(nx_session_next_clock_event(session, suite_deadline(120000), &event));
        HOLD(nx_clock_event, event);
        nx_clock_event_kind kind = nx_clock_event_kind_of(event);
        REQUIRE(kind == NX_CLOCK_EVENT_STATE || kind == NX_CLOCK_EVENT_TICK);
        uint64_t actual = 0;
        suite_check(nx_clock_event_generation(event, &actual));
        if (kind == NX_CLOCK_EVENT_TICK && actual == generation) return event;
        suite_drop(event);
    }
    REQUIRE(false);
    return NULL;
}

static void test_b9_clock_events_retain_release_and_restore_after_a_gap(void) {
    nx_session *administration = suite_connect(NULL, NULL);
    suite_paced(administration, "c_clock_events");
    struct suite_peer *peer = NULL;
    nx_session *session = suite_through(&peer, "c_clock_events");
    suite_completed(session, "ATTACH DOMAIN CLOCK;");
    nx_clock_event *event = suite_tick(session, 1);
    // ANCHOR: clock_events
    uint64_t id = 0;
    int64_t boundary = 0, authority = 0, serving = 0;
    suite_check(nx_clock_event_tick(event, &id, &boundary, &authority, &serving));
    REQUIRE(id > 0 && boundary == INT64_C(1893456000000000000) + (int64_t)(id - 1) * 100000000);
    REQUIRE(authority > 0 && serving >= INT64_C(1893456000000000000));
    const uint8_t *domain = NULL;
    size_t length = 0;
    nx_clock_event_domain(event, &domain, &length);
    REQUIRE(suite_equal(domain, length, "c_clock_events"));
    nx_clock_event *held = nx_clock_event_retain(event);
    HOLD(nx_clock_event, held);
    suite_drop(event);
    suite_cross_thread(held, suite_release_nx_clock_event);
    // ANCHOR_END: clock_events
    event = suite_tick(session, 1);
    nx_clock_state state = NX_CLOCK_STOPPED;
    nx_clock_end_reason ending = NX_CLOCK_END_DOMAIN_REMOVED;
    nx_restoration_cause cause = NX_RESTORATION_FAILED;
    uint64_t wait = 0;
    suite_kind(nx_clock_event_state(event, &state), NX_ERROR_TYPE);
    suite_kind(nx_clock_event_paced(event, NULL, NULL, NULL, NULL, NULL), NX_ERROR_TYPE);
    suite_kind(nx_clock_event_end_reason(event, &ending), NX_ERROR_TYPE);
    suite_kind(nx_clock_event_restoration_cause(event, &cause), NX_ERROR_TYPE);
    suite_kind(nx_clock_event_restoration_message(event, &domain, &length), NX_ERROR_TYPE);
    suite_kind(nx_clock_event_restoration_retry_after(event, &wait), NX_ERROR_TYPE);
    suite_peer_cut(peer, true);
    bool interrupted = false;
    for (size_t attempt = 0; attempt < 64 && !interrupted; ++attempt) {
        nx_clock_event *next = NULL;
        suite_check(nx_session_next_clock_event(session, suite_deadline(120000), &next));
        HOLD(nx_clock_event, next);
        interrupted = nx_clock_event_kind_of(next) == NX_CLOCK_EVENT_INTERRUPTED;
        suite_drop(next);
    }
    REQUIRE(interrupted);
    suite_peer_cut(peer, false);
    bool restored = false;
    for (size_t attempt = 0; attempt < 64 && !restored; ++attempt) {
        nx_clock_event *next = NULL;
        suite_check(nx_session_next_clock_event(session, suite_deadline(120000), &next));
        HOLD(nx_clock_event, next);
        if (nx_clock_event_kind_of(next) == NX_CLOCK_EVENT_STATE) {
            suite_check(nx_clock_event_state(next, &state));
            restored = state == NX_CLOCK_PACED;
        }
        suite_drop(next);
    }
    REQUIRE(restored);
    suite_tick(session, 1);
    suite_completed(session, "DETACH DOMAIN CLOCK;");
}

static void test_b11_read_the_attached_clock_before_ticks_and_project_it(void) {
    nx_session *administration = suite_connect(NULL, NULL);
    nx_session *session = suite_paced(administration, "c_clock_reads");
    nx_domain_clock *clock = NULL;
    suite_check(nx_session_domain_clock(session, suite_bytes("c_clock_reads"), 13, &clock));
    REQUIRE(clock == NULL);
    // ANCHOR: clock_read
    suite_completed(session, "ATTACH DOMAIN CLOCK;");
    suite_check(nx_session_domain_clock(session, suite_bytes("c_clock_reads"), 13, &clock));
    HOLD(nx_domain_clock, clock);
    REQUIRE(nx_domain_clock_state(clock) == NX_CLOCK_PACED);
    REQUIRE(nx_domain_clock_generation(clock) == 1);
    uint64_t period = 0, skew = 0;
    int64_t origin = 0, anchor = 0;
    double rate = 0;
    suite_check(nx_domain_clock_paced(clock, &period, &skew, &origin, &anchor, &rate));
    REQUIRE(period == 100000000 && skew == 10000000 && rate == 2.0);
    REQUIRE(origin == INT64_C(1893456000000000000));
    // ANCHOR_END: clock_read
    const uint8_t *domain = NULL;
    size_t length = 0;
    nx_domain_clock_domain(clock, &domain, &length);
    REQUIRE(suite_equal(domain, length, "c_clock_reads"));
    // ANCHOR: clock_project
    int64_t logical = 0, earliest = 0, latest = 0;
    uint64_t wait = 0;
    bool has_window = false, admitted = false;
    suite_check(nx_domain_clock_logical_time_at(clock, anchor, &logical));
    REQUIRE(logical == origin);
    suite_check(nx_domain_clock_wall_duration_until(clock, anchor, origin + (int64_t)period, &wait));
    REQUIRE(wait == period / 2);
    suite_check(nx_domain_clock_admission_window(clock, anchor, &has_window, &earliest, &latest));
    REQUIRE(has_window && earliest == origin && latest == origin);
    suite_check(nx_domain_clock_admits(clock, anchor, origin + (int64_t)skew, &admitted));
    REQUIRE(admitted);
    suite_check(nx_domain_clock_admits(clock, anchor, origin + (int64_t)skew + 1, &admitted));
    REQUIRE(!admitted);
    // ANCHOR_END: clock_project
    uint64_t id = 0;
    int64_t boundary = 0, authority = 0, serving = 0;
    if (nx_domain_clock_tick(clock, &id, &boundary, &authority, &serving)) {
        REQUIRE(id > 0 && authority > 0 && serving >= origin);
    }
    suite_completed(session, "STOP;");
    suite_completed(session, "START AT '2031-01-01T00:00:00Z' TIME RATE 4.0;");
    REQUIRE(nx_domain_clock_generation(clock) == 1); /* This is an immutable read. */
    bool changed = false;
    for (size_t attempt = 0; attempt < 64 && !changed; ++attempt) {
        nx_clock_event *event = NULL;
        suite_check(nx_session_next_clock_event(session, suite_deadline(120000), &event));
        HOLD(nx_clock_event, event);
        if (nx_clock_event_kind_of(event) == NX_CLOCK_EVENT_STATE) {
            uint64_t generation = 0;
            nx_clock_state state = NX_CLOCK_STOPPED;
            suite_check(nx_clock_event_generation(event, &generation));
            suite_check(nx_clock_event_state(event, &state));
            if (generation == 2 && state == NX_CLOCK_PACED) {
                suite_check(nx_clock_event_paced(event, &period, &skew, &origin, &anchor, &rate));
                REQUIRE(origin == INT64_C(1924992000000000000) && rate == 4.0);
                changed = true;
            }
        }
        suite_drop(event);
    }
    REQUIRE(changed);
    suite_completed(session, "DETACH DOMAIN CLOCK;");
    nx_domain_clock *none = NULL;
    suite_check(nx_session_domain_clock(session, suite_bytes("c_clock_reads"), 13, &none));
    REQUIRE(none == NULL);
    nx_domain_clock *held = nx_domain_clock_retain(clock);
    HOLD(nx_domain_clock, held);
    suite_drop(clock);
    suite_drop(session);
    REQUIRE(nx_domain_clock_state(held) == NX_CLOCK_PACED);
    suite_cross_thread(held, suite_release_nx_domain_clock);
}

static void test_b12_exact_fields_and_typed_open_refusals(void) {
    nx_session *session = suite_connect(NULL, NULL);
    const char *domain = suite_env("NERVIX_PROBE_DOMAIN");
    nx_fields *wrong = nx_fields_new();
    HOLD(nx_fields, wrong);
    suite_check(nx_fields_add(wrong, suite_bytes("value"), 5, NX_TYPE_I64, 0, false, false));
    // ANCHOR: open_refusal
    nx_producer *none = NULL;
    nx_error *error = suite_kind(nx_session_open_ingestor(session, suite_bytes(domain), strlen(domain),
        suite_bytes("typed_in"), 8, wrong, 4, 1u << 20, suite_deadline(120000), &none), NX_ERROR_REJECTED);
    nx_open_refusal refusal = NX_OPEN_DOMAIN_NOT_FOUND;
    REQUIRE(nx_error_open_refusal(error, &refusal));
    REQUIRE(refusal == NX_OPEN_SCHEMA_MISMATCH && none == NULL);
    // ANCHOR_END: open_refusal
    nx_fields *fields = suite_fields(false);
    error = suite_kind(nx_session_open_ingestor(session, suite_bytes(domain), strlen(domain),
        suite_bytes("absent"), 6, fields, 4, 1u << 20, suite_deadline(120000), &none), NX_ERROR_REJECTED);
    REQUIRE(nx_error_open_refusal(error, &refusal) && refusal == NX_OPEN_ENDPOINT_NOT_FOUND);
    suite_completed(session, "BEGIN;");
    error = suite_kind(nx_session_open_ingestor(session, suite_bytes(domain), strlen(domain),
        suite_bytes("typed_in"), 8, fields, 4, 1u << 20, suite_deadline(120000), &none), NX_ERROR_REJECTED);
    REQUIRE(nx_error_open_refusal(error, &refusal) && refusal == NX_OPEN_IN_TRANSACTION);
    suite_completed(session, "REVERT;");
    nx_producer *producer = suite_producer(session, "typed_in");
    nx_consumer *consumer = suite_consumer(session);
    REQUIRE(nx_producer_state(producer) == NX_ENDPOINT_ACTIVE);
    REQUIRE(nx_consumer_state(consumer) == NX_ENDPOINT_ACTIVE);
    REQUIRE(nx_producer_admission(producer) == NX_ADMISSION_OPEN);
    REQUIRE(nx_producer_generation(producer) > 0 && nx_consumer_generation(consumer) > 0);
    const uint8_t *contract = NULL;
    size_t contract_len = 0;
    nx_producer_contract(producer, &contract, &contract_len);
    REQUIRE(contract != NULL && contract_len == 32);
    nx_consumer_contract(consumer, &contract, &contract_len);
    REQUIRE(contract != NULL && contract_len == 32);
    uint32_t batches = 0, rows = 0;
    uint64_t bytes = 0, max_bytes = 0;
    nx_producer_grant(producer, &batches, &bytes, &rows, &max_bytes);
    REQUIRE(batches == 4 && bytes == (1u << 20) && rows > 0 && max_bytes > 0);
    nx_consumer_grant(consumer, &batches, &bytes, &rows, &max_bytes);
    REQUIRE(batches == 4 && bytes == (1u << 20) && rows == 16 && max_bytes == (1u << 20));
    nx_ack_window window = NX_ACK_PARALLEL;
    uint64_t outstanding = 0, timeout = 0, backoff = 0, maximum = 0;
    nx_producer_policy(producer, &window, &outstanding, &timeout, &backoff, &maximum);
    REQUIRE(window == NX_ACK_SEQUENTIAL && outstanding == 1 && timeout == UINT64_C(60000000000));
    REQUIRE(backoff == 100000000 && maximum == 1000000000);
    nx_consumer_policy(consumer, &window, &outstanding, &timeout, &backoff, &maximum);
    REQUIRE(window == NX_ACK_SEQUENTIAL && outstanding == 1 && timeout == UINT64_C(60000000000));
    nx_reopen_reason reason = NX_REOPEN_REFUSED;
    REQUIRE(!nx_producer_reopen_reason(producer, &reason, &refusal));
    REQUIRE(!nx_consumer_reopen_reason(consumer, &reason, &refusal));
    suite_check(nx_producer_close(producer, suite_deadline(120000)));
    suite_check(nx_consumer_close(consumer, suite_deadline(120000)));
    REQUIRE(nx_producer_state(producer) == NX_ENDPOINT_CLOSED);
    REQUIRE(nx_consumer_state(consumer) == NX_ENDPOINT_CLOSED);
    nx_delivery *delivery = NULL;
    suite_kind(nx_consumer_next(consumer, suite_deadline(120000), &delivery), NX_ERROR_CLOSED);
}

static void test_b13_builders_copy_every_level_and_batches_keep_their_owners(void) {
    nx_session *session = suite_connect(NULL, NULL);
    nx_producer *producer = suite_producer(session, "typed_in");
    nx_consumer *consumer = suite_consumer(session);
    // ANCHOR: build_submit
    nx_batch *batch = suite_build(producer, 1301);
    suite_assert_batch(batch, 1301, false);
    uint64_t submission = suite_submit(producer, batch);
    suite_drop(batch);
    nx_delivery *delivery = suite_next(consumer);
    nx_batch *received = NULL;
    suite_check(nx_delivery_batch(delivery, &received));
    HOLD(nx_batch, received);
    suite_assert_batch(received, 1301, true);
    suite_ack(delivery);
    REQUIRE(nx_submission_outcome_result(suite_rejoin(producer, submission)) == NX_SUBMISSION_COMPLETED);
    // ANCHOR_END: build_submit
    const uint8_t *stream = NULL, *from_batch = NULL;
    size_t length = 0, batch_len = 0;
    nx_delivery_ipc(delivery, &stream, &length);
    suite_check(nx_batch_ipc(received, &from_batch, &batch_len));
    REQUIRE(length == batch_len && memcmp(stream, from_batch, length) == 0);
    nx_batch *retained = nx_batch_retain(received);
    HOLD(nx_batch, retained);
    suite_drop(received);
    suite_drop(delivery);
    suite_assert_batch(retained, 1301, true);
    suite_cross_thread(retained, suite_release_nx_batch);
    nx_schema *schema = NULL;
    suite_check(nx_producer_schema(producer, &schema));
    HOLD(nx_schema, schema);
    nx_batch_builder *incomplete = NULL;
    suite_check(nx_batch_builder_new(schema, 2, &incomplete));
    HOLD(nx_batch_builder, incomplete);
    nx_batch *none = NULL;
    suite_kind(nx_batch_builder_finish(incomplete, &none), NX_ERROR_INVALID_ARGUMENT);
    REQUIRE(none == NULL);
    nx_schema *output_schema = NULL;
    suite_check(nx_consumer_schema(consumer, &output_schema));
    HOLD(nx_schema, output_schema);
    nx_batch *other = suite_build_schema(output_schema, 1303, true);
    uint64_t ignored = 0;
    suite_kind(nx_producer_submit(producer, other, suite_deadline(120000), &ignored), NX_ERROR_INVALID_ARGUMENT);
    suite_check(nx_producer_close(producer, suite_deadline(120000)));
    suite_check(nx_consumer_close(consumer, suite_deadline(120000)));
}

static void test_b14_take_or_release_each_submission_and_read_its_outcome_class(void) {
    nx_session *session = suite_connect(NULL, NULL);
    nx_producer *producer = suite_producer(session, "typed_in");
    nx_consumer *consumer = suite_consumer(session);
    nx_batch *batch = suite_build(producer, 1401);
    uint64_t submission = suite_submit(producer, batch);
    // ANCHOR: pending_rejoin
    size_t count = 0;
    uint64_t pending[4];
    bool resolved[4];
    suite_check(nx_producer_pending(producer, pending, resolved, 4, &count));
    REQUIRE(count == 1 && pending[0] == submission && !resolved[0]);
    nx_cancel *cancel = nx_cancel_new();
    HOLD(nx_cancel, cancel);
    nx_cancel_trigger(cancel);
    nx_submission_outcome *outcome = NULL;
    suite_kind(nx_producer_rejoin(producer, submission, cancel, &outcome), NX_ERROR_CANCELLED);
    suite_ack(suite_next(consumer));
    outcome = suite_rejoin(producer, submission);
    REQUIRE(nx_submission_outcome_result(outcome) == NX_SUBMISSION_COMPLETED);
    // ANCHOR_END: pending_rejoin
    const uint8_t *message = NULL;
    size_t message_len = 0;
    nx_submission_outcome_message(outcome, &message, &message_len);
    REQUIRE(message_len == 0);
    nx_submission_refusal refusal = NX_REFUSAL_BUSY;
    nx_processing_failure failure = NX_FAILURE_ACK_TIMED_OUT;
    nx_submission_uncertainty uncertainty = NX_UNCERTAINTY_OWNER_LOST;
    suite_kind(nx_submission_outcome_refusal(outcome, &refusal), NX_ERROR_TYPE);
    suite_kind(nx_submission_outcome_failure(outcome, &failure), NX_ERROR_TYPE);
    suite_kind(nx_submission_outcome_uncertainty(outcome, &uncertainty), NX_ERROR_TYPE);
    suite_check(nx_producer_pending(producer, NULL, NULL, 0, &count));
    REQUIRE(count == 0);
    /* A canonical stream of the output schema is valid IPC but invalid for this ingestor. */
    nx_schema *output_schema = NULL;
    suite_check(nx_consumer_schema(consumer, &output_schema));
    HOLD(nx_schema, output_schema);
    nx_batch *foreign = suite_build_schema(output_schema, 1403, true);
    const uint8_t *ipc = NULL;
    size_t ipc_len = 0;
    suite_check(nx_batch_ipc(foreign, &ipc, &ipc_len));
    uint8_t *copy = (uint8_t *)suite_allocate(ipc_len);
    memcpy(copy, ipc, ipc_len);
    suite_check(nx_producer_submit_ipc(producer, copy, ipc_len, suite_deadline(120000), &submission));
    memset(copy, 0, ipc_len);
    outcome = suite_rejoin(producer, submission);
    // ANCHOR: submission_classes
    REQUIRE(nx_submission_outcome_result(outcome) == NX_SUBMISSION_NOT_ADMITTED);
    suite_check(nx_submission_outcome_refusal(outcome, &refusal));
    REQUIRE(refusal == NX_REFUSAL_INVALID_BATCH);
    nx_batch_defect defect = NX_DEFECT_MALFORMED;
    suite_check(nx_submission_outcome_defect(outcome, &defect));
    REQUIRE(defect == NX_DEFECT_SCHEMA_MISMATCH);
    submission = suite_submit(producer, suite_build(producer, 1405));
    nx_delivery *rejected = suite_next(consumer);
    nx_settlement settlement = NX_SETTLEMENT_CONSUMER_ENDED;
    suite_check(nx_delivery_reject(rejected, suite_bytes("application refusal"), 19,
                                    suite_deadline(120000), &settlement));
    REQUIRE(settlement == NX_SETTLEMENT_CONFIRMED);
    outcome = suite_rejoin(producer, submission);
    REQUIRE(nx_submission_outcome_result(outcome) == NX_SUBMISSION_PROCESSING_FAILED);
    suite_check(nx_submission_outcome_failure(outcome, &failure));
    REQUIRE(failure == NX_FAILURE_REJECTED);
    // ANCHOR_END: submission_classes
    submission = suite_submit(producer, suite_build(producer, 1407));
    outcome = NULL;
    suite_check(nx_producer_release(producer, submission, &outcome));
    REQUIRE(outcome == NULL);
    suite_ack(suite_next(consumer));
    suite_check(nx_producer_pending(producer, NULL, NULL, 0, &count));
    REQUIRE(count == 0);
    suite_kind(nx_producer_rejoin(producer, submission, suite_deadline(120000), &outcome), NX_ERROR_INVALID_ARGUMENT);
    struct suite_peer *peer = NULL;
    nx_session *publishing = suite_through(&peer, NULL);
    nx_producer *interrupted = suite_producer(publishing, "typed_in");
    submission = suite_submit(interrupted, suite_build(interrupted, 1409));
    nx_delivery *held = suite_next(consumer); /* Public evidence of admission before the cut. */
    suite_peer_cut(peer, true);
    // ANCHOR: unknown_submission
    outcome = suite_rejoin(interrupted, submission);
    REQUIRE(nx_submission_outcome_result(outcome) == NX_SUBMISSION_OUTCOME_UNKNOWN);
    suite_check(nx_submission_outcome_uncertainty(outcome, &uncertainty));
    REQUIRE(uncertainty == NX_UNCERTAINTY_SESSION_LOST);
    // ANCHOR_END: unknown_submission
    suite_ack(held);
    suite_peer_cut(peer, false);
    submission = suite_submit(interrupted, suite_build(interrupted, 1411));
    REQUIRE(nx_producer_state(interrupted) == NX_ENDPOINT_ACTIVE);
    suite_ack(suite_next(consumer));
    REQUIRE(nx_submission_outcome_result(suite_rejoin(interrupted, submission)) == NX_SUBMISSION_COMPLETED);
    suite_check(nx_producer_close(interrupted, suite_deadline(120000)));
    suite_check(nx_producer_close(producer, suite_deadline(120000)));
    suite_check(nx_consumer_close(consumer, suite_deadline(120000)));
}

static void test_b15_delivery_identity_settlements_and_retained_buffers(void) {
    nx_session *session = suite_connect(NULL, NULL);
    nx_producer *producer = suite_producer(session, "typed_in");
    nx_consumer *consumer = suite_consumer(session);
    uint64_t submission = suite_submit(producer, suite_build(producer, 1501));
    nx_delivery *first = suite_next(consumer);
    const uint8_t *identity = NULL, *reference = NULL, *relay = NULL, *fingerprint = NULL;
    size_t identity_len = 0, reference_len = 0, relay_len = 0, fingerprint_len = 0;
    // ANCHOR: delivery_metadata
    nx_delivery_identity(first, &identity, &identity_len);
    nx_delivery_reference(first, &reference, &reference_len);
    nx_delivery_source_relay(first, &relay, &relay_len);
    REQUIRE(identity_len == 16 && reference_len == 16 && suite_equal(relay, relay_len, "typed_rows"));
    REQUIRE(nx_delivery_branch_fingerprint(first, &fingerprint, &fingerprint_len));
    REQUIRE(fingerprint_len == 32 && nx_delivery_members(first) == 2);
    REQUIRE(nx_delivery_execution_now(first) > 0);
    // ANCHOR_END: delivery_metadata
    uint8_t saved_identity[16], saved_reference[16];
    memcpy(saved_identity, identity, 16);
    memcpy(saved_reference, reference, 16);
    nx_settlement settlement = NX_SETTLEMENT_CONSUMER_ENDED;
    // ANCHOR: retry_delivery
    suite_check(nx_delivery_retry(first, suite_deadline(120000), &settlement));
    REQUIRE(settlement == NX_SETTLEMENT_CONFIRMED);
    nx_delivery *again = suite_next(consumer);
    nx_delivery_identity(again, &identity, &identity_len);
    nx_delivery_reference(again, &reference, &reference_len);
    REQUIRE(identity_len == 16 && memcmp(identity, saved_identity, 16) == 0);
    REQUIRE(reference_len == 16 && memcmp(reference, saved_reference, 16) != 0);
    // ANCHOR_END: retry_delivery
    suite_check(nx_delivery_ack(first, suite_deadline(120000), &settlement));
    REQUIRE(settlement == NX_SETTLEMENT_STALE_REFERENCE);
    suite_kind(nx_delivery_reject(again, NULL, 0, suite_deadline(120000), &settlement), NX_ERROR_INVALID_ARGUMENT);
    nx_delivery *kept = nx_delivery_retain(again);
    HOLD(nx_delivery, kept);
    suite_drop(again);
    const uint8_t *ipc = NULL;
    size_t ipc_len = 0;
    nx_delivery_ipc(kept, &ipc, &ipc_len);
    REQUIRE(ipc != NULL && ipc_len > 0);
    suite_ack(kept);
    suite_cross_thread(kept, suite_release_nx_delivery);
    REQUIRE(nx_submission_outcome_result(suite_rejoin(producer, submission)) == NX_SUBMISSION_COMPLETED);
    suite_check(nx_producer_close(producer, suite_deadline(120000)));
    suite_check(nx_consumer_close(consumer, suite_deadline(120000)));
}

static void test_b16_consumer_gaps_and_contract_changes_require_checked_reopens(void) {
    nx_session *publishing = suite_connect(NULL, NULL);
    nx_producer *producer = suite_producer(publishing, "typed_in");
    struct suite_peer *peer = NULL;
    nx_session *receiving = suite_through(&peer, NULL);
    nx_consumer *consumer = suite_consumer(receiving);
    uint64_t submission = suite_submit(producer, suite_build(producer, 1601));
    nx_delivery *before = suite_next(consumer);
    const uint8_t *identity = NULL;
    size_t identity_len = 0;
    nx_delivery_identity(before, &identity, &identity_len);
    REQUIRE(identity_len == 16);
    uint8_t saved[16];
    memcpy(saved, identity, 16);
    suite_peer_cut(peer, true);
    nx_delivery *none = NULL;
    // ANCHOR: consumer_gap
    suite_kind(nx_consumer_next(consumer, suite_deadline(120000), &none), NX_ERROR_INTERRUPTED);
    nx_settlement settlement = NX_SETTLEMENT_CONSUMER_ENDED;
    suite_kind(nx_delivery_ack(before, suite_deadline(120000), &settlement), NX_ERROR_REJECTED);
    suite_peer_cut(peer, false);
    nx_delivery *after = suite_next(consumer);
    nx_delivery_identity(after, &identity, &identity_len);
    REQUIRE(identity_len == 16 && memcmp(identity, saved, 16) == 0);
    suite_ack(after);
    REQUIRE(nx_submission_outcome_result(suite_rejoin(producer, submission)) == NX_SUBMISSION_COMPLETED);
    // ANCHOR_END: consumer_gap
    suite_check(nx_consumer_close(consumer, suite_deadline(120000)));
    suite_check(nx_producer_close(producer, suite_deadline(120000)));

    suite_completed(publishing,
        "CREATE CLIENT PRODUCER INGESTOR c_changing FROM SCHEMA suite_row "
        "MODE ACK PARALLEL MAX 4 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND "
        "TIMESTAMP NOW TO watched_rows INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant "
        "FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;");
    nx_producer *changing = suite_producer(publishing, "c_changing");
    suite_completed(publishing,
        "ALTER CLIENT PRODUCER INGESTOR c_changing SET FROM SCHEMA suite_row "
        "MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND;");
    nx_cancel *deadline = suite_deadline(120000);
    nx_batch *batch = suite_build(changing, 1603);
    uint64_t ignored = 0;
    // ANCHOR: reopen
    suite_kind(nx_producer_submit(changing, batch, deadline, &ignored), NX_ERROR_REOPEN_REQUIRED);
    nx_reopen_reason reason = NX_REOPEN_REFUSED;
    nx_open_refusal refusal = NX_OPEN_ENDPOINT_NOT_FOUND;
    REQUIRE(nx_producer_reopen_reason(changing, &reason, &refusal));
    REQUIRE(reason == NX_REOPEN_CONTRACT_CHANGED);
    REQUIRE(nx_producer_state(changing) == NX_ENDPOINT_REOPEN_REQUIRED);
    nx_producer *replacement = suite_producer(publishing, "c_changing");
    uint64_t later = suite_submit(replacement, suite_build(replacement, 1605));
    REQUIRE(nx_submission_outcome_result(suite_rejoin(replacement, later)) == NX_SUBMISSION_COMPLETED);
    // ANCHOR_END: reopen
    suite_check(nx_producer_close(replacement, suite_deadline(120000)));
}

static void test_a_complete_application_keeps_endpoints_alive_after_session_release(void) {
    nx_session *session = suite_connect(NULL, NULL);
    // ANCHOR: application
    nx_producer *producer = suite_producer(session, "typed_in");
    nx_consumer *consumer = suite_consumer(session);
    suite_drop(session);
    uint64_t submission = suite_submit(producer, suite_build(producer, 1701));
    nx_delivery *delivery = suite_next(consumer);
    nx_batch *batch = NULL;
    suite_check(nx_delivery_batch(delivery, &batch));
    HOLD(nx_batch, batch);
    suite_assert_batch(batch, 1701, true);
    suite_ack(delivery);
    nx_submission_outcome *outcome = suite_rejoin(producer, submission);
    REQUIRE(nx_submission_outcome_result(outcome) == NX_SUBMISSION_COMPLETED);
    suite_check(nx_consumer_close(consumer, suite_deadline(120000)));
    suite_check(nx_producer_close(producer, suite_deadline(120000)));
    suite_drop(delivery);
    REQUIRE(nx_batch_row_count(batch) == 2);
    suite_drop(batch);
    // ANCHOR_END: application
}

struct suite_execution_job {
    nx_session *session;
    nx_execution *execution;
    nx_cancel *cancel;
    nx_error *error;
    nx_outcome *outcome;
    pthread_t worker;
    bool started;
    bool joined;
};

static void *suite_execution_thread(void *argument) {
    struct suite_execution_job *job = (struct suite_execution_job *)argument;
    job->error = nx_session_execute(job->session, job->execution, job->cancel, &job->outcome);
    return NULL;
}

static void suite_execution_free(void *argument) {
    struct suite_execution_job *job = (struct suite_execution_job *)argument;
    nx_cancel_trigger(job->cancel);
    if (job->started && !job->joined) pthread_join(job->worker, NULL);
    if (job->error != NULL) nx_error_free(job->error);
    if (job->outcome != NULL) nx_outcome_free(job->outcome);
    free(job);
}

static void test_b1_cancel_an_admitted_commit_and_recover_its_recorded_outcome(void) {
    nx_session *administration = suite_connect(suite_env("NERVIX_SUITE_LEADER_GRPC_URI"), NULL);
    nx_session *session = suite_connect(NULL, NULL);
    suite_completed(administration,
        "CREATE CLIENT PRODUCER INGESTOR c_commit_input FROM SCHEMA suite_row "
        "MODE ACK PARALLEL MAX 4 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND "
        "TIMESTAMP NOW TO typed_rows INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant "
        "FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;");
    nx_producer *producer = suite_producer(session, "c_commit_input");
    nx_consumer *consumer = suite_consumer(session);
    uint64_t submission = suite_submit(producer, suite_build(producer, 1801));
    nx_delivery *held = suite_next(consumer);
    suite_completed(administration, "BEGIN;");
    suite_completed(administration,
        "ALTER CLIENT PRODUCER INGESTOR c_commit_input REPLACE ROUTE TO typed_rows "
        "INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant "
        "FLUSH EACH 10ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;");
    nx_execution *execution = suite_prepare(administration, "COMMIT;");
    nx_cancel *cancel = suite_deadline(120000);
    struct suite_execution_job *job = (struct suite_execution_job *)calloc(1, sizeof *job);
    REQUIRE(job != NULL);
    job->session = administration;
    job->execution = execution;
    job->cancel = cancel;
    suite_hold(job, suite_execution_free);
    REQUIRE(pthread_create(&job->worker, NULL, suite_execution_thread, job) == 0);
    job->started = true;
    struct timespec began, now;
    REQUIRE(clock_gettime(CLOCK_MONOTONIC, &began) == 0);
    bool suspended = false;
    do {
        suspended = nx_producer_admission(producer) == NX_ADMISSION_SUSPENDED;
        if (suspended) break;
        REQUIRE(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
        if (now.tv_sec - began.tv_sec >= 120) break;
        struct timespec poll_wait = {0, 10000000};
        nanosleep(&poll_wait, NULL); /* Condition poll, bounded by the admission witness. */
    } while (!suspended);
    // ANCHOR: cancel_admitted
    nx_cancel_trigger(cancel);
    REQUIRE(pthread_join(job->worker, NULL) == 0);
    job->joined = true;
    REQUIRE(suspended);
    nx_error *error = job->error;
    job->error = NULL;
    suite_kind(error, NX_ERROR_CANCELLED);
    const uint8_t *expected = NULL, *actual = NULL;
    size_t expected_len = 0, actual_len = 0;
    nx_execution_reference(execution, &expected, &expected_len);
    REQUIRE(nx_error_execution_reference(error, &actual, &actual_len));
    REQUIRE(expected_len == actual_len && memcmp(expected, actual, actual_len) == 0);
    suite_ack(held);
    nx_outcome *recovered = NULL;
    suite_check(nx_session_execute(administration, execution, suite_deadline(120000), &recovered));
    HOLD(nx_outcome, recovered);
    nx_disposition disposition = nx_outcome_disposition(recovered);
    REQUIRE(disposition == NX_DISPOSITION_COMPLETED || disposition == NX_DISPOSITION_FAILED);
    nx_outcome *again = NULL;
    suite_check(nx_session_execute(administration, execution, suite_deadline(120000), &again));
    HOLD(nx_outcome, again);
    REQUIRE(nx_outcome_disposition(again) == disposition);
    // ANCHOR_END: cancel_admitted
    REQUIRE(nx_submission_outcome_result(suite_rejoin(producer, submission)) == NX_SUBMISSION_COMPLETED);
    nx_outcome *definition = suite_completed(administration, "SHOW CREATE INGESTOR c_commit_input;");
    const uint8_t *message = NULL;
    size_t message_len = 0;
    nx_outcome_message(definition, &message, &message_len);
    char *terminated = (char *)suite_allocate(message_len + 1);
    memcpy(terminated, message, message_len);
    const char *cadence = disposition == NX_DISPOSITION_COMPLETED ? "FLUSH EACH 10ms" : "FLUSH IMMEDIATE";
    REQUIRE(strstr(terminated, cadence) != NULL);
    suite_check(nx_producer_close(producer, suite_deadline(120000)));
    suite_check(nx_consumer_close(consumer, suite_deadline(120000)));
}

static void suite_write_file(const char *path, const char *text) {
    FILE *file = fopen(path, "wb");
    REQUIRE(file != NULL);
    size_t length = strlen(text);
    size_t written = fwrite(text, 1, length, file);
    int closed = fclose(file);
    REQUIRE(written == length && closed == 0);
}

/* Registered after the backup and restore cases: no statement removes a resource, and their
 * archives of this domain must hold no resource version. */
static void test_b1_an_upload_completes_again_under_its_identity_and_refuses_another_archive(void) {
    nx_session *session = suite_connect(NULL, NULL);
    char directory[1024], payload[1100], query[1200];
    int length = snprintf(directory, sizeof directory, "%s/c-bundle", suite_env("NERVIX_SUITE_BACKUP_DIRECTORY"));
    REQUIRE(length > 0 && (size_t)length < sizeof directory && strchr(directory, '\'') == NULL);
    REQUIRE(mkdir(directory, 0700) == 0);
    length = snprintf(payload, sizeof payload, "%s/payload.txt", directory);
    REQUIRE(length > 0 && (size_t)length < sizeof payload);
    suite_write_file(payload, "first archive\n");
    suite_completed(session, "CREATE RESOURCE c_bundle;");
    length = snprintf(query, sizeof query, "UPLOAD RESOURCE c_bundle VERSION '%s';", directory);
    REQUIRE(length > 0 && (size_t)length < sizeof query);
    // ANCHOR: upload
    nx_execution *execution = suite_prepare(session, query);
    nx_upload_failure failure = NX_UPLOAD_INVALID_STREAM;
    for (size_t call = 0; call < 2; ++call) {
        /* A second call of the same execution sends the same archive under the same upload
         * identity, so it completes again instead of installing a second version. */
        nx_outcome *uploaded = NULL;
        suite_check(nx_session_execute(session, execution, suite_deadline(120000), &uploaded));
        HOLD(nx_outcome, uploaded);
        REQUIRE(nx_outcome_disposition(uploaded) == NX_DISPOSITION_COMPLETED);
        REQUIRE(!nx_outcome_upload_failure(uploaded, &failure));
    }
    suite_write_file(payload, "different archive\n");
    nx_outcome *refused = NULL;
    suite_check(nx_session_execute(session, execution, suite_deadline(120000), &refused));
    HOLD(nx_outcome, refused);
    REQUIRE(nx_outcome_disposition(refused) == NX_DISPOSITION_FAILED);
    REQUIRE(nx_outcome_upload_failure(refused, &failure) && failure == NX_UPLOAD_IDENTITY_CONFLICT);
    // ANCHOR_END: upload
}

#define SUITE_CONTRACT_TESTS \
    {"boundary", "test_boundary_fields_and_arguments", test_boundary_fields_and_arguments}, \
    {"boundary", "test_boundary_public_enumeration_domains", test_boundary_public_enumeration_domains}, \
    {"boundary", "test_boundary_silent_peer_deadline", test_boundary_silent_peer_deadline}, \
    {"boundary", "test_boundary_connect_cancelled_from_another_thread", test_boundary_connect_cancelled_from_another_thread}, \
    {"cluster", "test_b1_prepare_once_and_recover_cancelled_execution", test_b1_prepare_once_and_recover_cancelled_execution}, \
    {"cluster", "test_b1_cancel_an_admitted_commit_and_recover_its_recorded_outcome", test_b1_cancel_an_admitted_commit_and_recover_its_recorded_outcome}, \
    {"cluster", "test_b2_objects_outlive_their_session_and_release_once", test_b2_objects_outlive_their_session_and_release_once}, \
    {"cluster", "test_b3_event_references_keep_borrowed_frames_alive", test_b3_event_references_keep_borrowed_frames_alive}, \
    {"cluster", "test_b4_strings_options_tls_authentication_and_seeds", test_b4_strings_options_tls_authentication_and_seeds}, \
    {"cluster", "test_b5_calls_run_on_native_host_threads", test_b5_calls_run_on_native_host_threads}, \
    {"cluster", "test_b6_columns_use_size_queries_and_pointer_lengths", test_b6_columns_use_size_queries_and_pointer_lengths}, \
    {"cluster", "test_completion_pages_have_utf8_byte_ranges", test_completion_pages_have_utf8_byte_ranges}, \
    {"cluster", "test_inspection_retains_typed_state_after_its_outcome_is_released", test_inspection_retains_typed_state_after_its_outcome_is_released}, \
    {"cluster", "test_b7_subscription_gap_end_and_variant_accessors", test_b7_subscription_gap_end_and_variant_accessors}, \
    {"cluster", "test_b7_refused_subscription_restoration_reports_its_cause_and_recovers", test_b7_refused_subscription_restoration_reports_its_cause_and_recovers}, \
    {"cluster", "test_b8_backup_recovers_under_its_execution_reference", test_b8_backup_recovers_under_its_execution_reference}, \
    {"cluster", "test_b9_clock_events_retain_release_and_restore_after_a_gap", test_b9_clock_events_retain_release_and_restore_after_a_gap}, \
    {"cluster", "test_b10_restore_streams_the_archive_and_reports_its_steps", test_b10_restore_streams_the_archive_and_reports_its_steps}, \
    {"cluster", "test_b1_an_upload_completes_again_under_its_identity_and_refuses_another_archive", test_b1_an_upload_completes_again_under_its_identity_and_refuses_another_archive}, \
    {"cluster", "test_b11_read_the_attached_clock_before_ticks_and_project_it", test_b11_read_the_attached_clock_before_ticks_and_project_it}, \
    {"cluster", "test_b12_exact_fields_and_typed_open_refusals", test_b12_exact_fields_and_typed_open_refusals}, \
    {"cluster", "test_b13_builders_copy_every_level_and_batches_keep_their_owners", test_b13_builders_copy_every_level_and_batches_keep_their_owners}, \
    {"cluster", "test_b14_take_or_release_each_submission_and_read_its_outcome_class", test_b14_take_or_release_each_submission_and_read_its_outcome_class}, \
    {"cluster", "test_b15_delivery_identity_settlements_and_retained_buffers", test_b15_delivery_identity_settlements_and_retained_buffers}, \
    {"cluster", "test_b16_consumer_gaps_and_contract_changes_require_checked_reopens", test_b16_consumer_gaps_and_contract_changes_require_checked_reopens}, \
    {"cluster", "test_a_complete_application_keeps_endpoints_alive_after_session_release", test_a_complete_application_keeps_endpoints_alive_after_session_release},

#endif
