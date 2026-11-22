// The same C ABI contract compiled as C++17, with RAII ownership and typed C++ examples.
#include <algorithm>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

struct SuiteDeleter {
    void (*release)(void *);
    void operator()(void *pointer) const { release(pointer); }
};
using SuiteOwned = std::unique_ptr<void, SuiteDeleter>;
static std::vector<SuiteOwned> suite_objects;

static void suite_require(bool passed, const char *expression) {
    if (!passed) throw std::runtime_error(expression);
}

static void suite_hold(void *pointer, void (*release)(void *)) {
    SuiteOwned owned(pointer, SuiteDeleter{release});
    suite_require(pointer != nullptr, "an owned allocation is present");
    suite_require(suite_objects.size() < 512, "a test fits the 512-allocation bound");
    suite_objects.push_back(std::move(owned));
}

static void *suite_forget(void *pointer) {
    for (auto i = suite_objects.end(); i != suite_objects.begin();) {
        --i;
        if (i->get() == pointer) {
            void *transferred = i->release();
            suite_objects.erase(i);
            return transferred;
        }
    }
    throw std::runtime_error("a transferred object has one owned reference");
}

static void suite_drop(void *pointer) {
    for (auto i = suite_objects.end(); i != suite_objects.begin();) {
        --i;
        if (i->get() == pointer) {
            suite_objects.erase(i);
            return;
        }
    }
    throw std::runtime_error("an explicitly released object is owned");
}

static std::size_t suite_mark() { return suite_objects.size(); }
static void suite_cleanup_to(std::size_t mark) {
    while (suite_objects.size() > mark) suite_objects.pop_back();
}

#include "../../c/suite/contract.h"

// ANCHOR: cpp_owners
template <class T, void (*Release)(T *)> struct ReleaseHandle {
    void operator()(T *pointer) const { Release(pointer); }
};
using Session = std::unique_ptr<nx_session, ReleaseHandle<nx_session, nx_session_free>>;
using Options = std::unique_ptr<nx_connect_options,
                                ReleaseHandle<nx_connect_options, nx_connect_options_free>>;
using Cancel = std::unique_ptr<nx_cancel, ReleaseHandle<nx_cancel, nx_cancel_free>>;
using Event = std::unique_ptr<nx_event, ReleaseHandle<nx_event, nx_event_release>>;
using Notice = std::unique_ptr<nx_notice, ReleaseHandle<nx_notice, nx_notice_release>>;
using Batch = std::unique_ptr<nx_batch, ReleaseHandle<nx_batch, nx_batch_release>>;
using Producer = std::unique_ptr<nx_producer, ReleaseHandle<nx_producer, nx_producer_free>>;
using Consumer = std::unique_ptr<nx_consumer, ReleaseHandle<nx_consumer, nx_consumer_free>>;
using Delivery = std::unique_ptr<nx_delivery, ReleaseHandle<nx_delivery, nx_delivery_release>>;
using SubmissionOutcome = std::unique_ptr<nx_submission_outcome,
    ReleaseHandle<nx_submission_outcome, nx_submission_outcome_free>>;
// ANCHOR_END: cpp_owners

template <class Owner, class T> static Owner suite_take(T *pointer) {
    return Owner(static_cast<T *>(suite_forget(pointer)));
}

static void test_cpp_unique_ptr_releases_on_an_exception() {
    // ANCHOR: cpp_exception
    try {
        Options options(nx_connect_options_new());
        Cancel cancel(nx_cancel_new());
        suite_check(nx_connect_options_tls_requirement(options.get(), NX_TLS_REQUIRED));
        nx_cancel_trigger(cancel.get());
        throw std::runtime_error("application cleanup");
    } catch (const std::runtime_error &failure) {
        REQUIRE(std::string(failure.what()) == "application cleanup");
    }
    // ANCHOR_END: cpp_exception
}

static void test_cpp_raii_keeps_a_retained_batch_across_a_thread() {
    // ANCHOR: cpp_connect
    nx_session *raw = suite_connect(NULL, NULL);
    Session session(static_cast<nx_session *>(suite_forget(raw)));
    // ANCHOR_END: cpp_connect
    nx_producer *producer = suite_producer(session.get(), "watched_in");
    nx_batch *built = suite_build(producer, 9101);
    // ANCHOR: cpp_batch
    Batch first(static_cast<nx_batch *>(suite_forget(built)));
    Batch held(nx_batch_retain(first.get()));
    bool read_correctly = false;
    std::thread reader([kept = std::move(held), &read_correctly] {
        read_correctly = nx_batch_row_count(kept.get()) == 2;
    });
    first.reset();
    reader.join();
    REQUIRE(read_correctly);
    // ANCHOR_END: cpp_batch
    suite_check(nx_producer_close(producer, suite_deadline(120000)));
}

static void test_cpp_application_owns_endpoints_and_delivery_with_typed_deleters() {
    // ANCHOR: cpp_application
    Session session = suite_take<Session>(suite_connect(NULL, NULL));
    Producer producer = suite_take<Producer>(suite_producer(session.get(), "typed_in"));
    Consumer consumer = suite_take<Consumer>(suite_consumer(session.get()));
    session.reset();
    uint64_t submission = suite_submit(producer.get(), suite_build(producer.get(), 9201));
    Delivery delivery = suite_take<Delivery>(suite_next(consumer.get()));
    nx_batch *raw = nullptr;
    suite_check(nx_delivery_batch(delivery.get(), &raw));
    Batch batch(raw);
    suite_assert_batch(batch.get(), 9201, true);
    suite_ack(delivery.get());
    SubmissionOutcome outcome = suite_take<SubmissionOutcome>(suite_rejoin(producer.get(), submission));
    REQUIRE(nx_submission_outcome_result(outcome.get()) == NX_SUBMISSION_COMPLETED);
    suite_check(nx_consumer_close(consumer.get(), suite_deadline(120000)));
    suite_check(nx_producer_close(producer.get(), suite_deadline(120000)));
    delivery.reset();
    REQUIRE(nx_batch_row_count(batch.get()) == 2);
    // ANCHOR_END: cpp_application
}

static void test_cpp_owned_notices_are_routed_by_kind_and_read_on_another_thread() {
    Session session = suite_take<Session>(suite_connect(NULL, NULL));
    suite_failing_junction(session.get(), "cpp_notice");
    Producer producer = suite_take<Producer>(suite_producer(session.get(), "cpp_notice_in"));
    suite_submit_failing(producer.get(), 2201);
    // ANCHOR: cpp_notices
    nx_cancel *deadline = suite_deadline(120000);
    Notice found;
    for (int attempt = 0; attempt < 64 && !found; ++attempt) {
        nx_notice *raw = nullptr;
        suite_check(nx_session_next_notice(session.get(), deadline, &raw));
        Notice notice(raw); // Released when this iteration ends, unless it is kept.
        const uint8_t *node = nullptr;
        std::size_t node_len = 0;
        if (nx_notice_kind_of(notice.get()) == NX_NOTICE_KIND_RUNTIME &&
            nx_notice_error(notice.get(), nullptr, nullptr, nullptr, nullptr, &node, &node_len) &&
            std::string(reinterpret_cast<const char *>(node), node_len) == "cpp_notice_scorer") {
            found = std::move(notice);
        }
    }
    REQUIRE(found != nullptr);
    Notice held(nx_notice_retain(found.get()));
    found.reset();
    std::string error_class, node_kind, message;
    std::thread reader([kept = std::move(held), &error_class, &node_kind, &message] {
        const uint8_t *class_bytes = nullptr, *kind_bytes = nullptr, *text = nullptr;
        std::size_t class_len = 0, kind_len = 0, text_len = 0;
        if (nx_notice_error(kept.get(), &class_bytes, &class_len, &kind_bytes, &kind_len,
                            nullptr, nullptr)) {
            error_class.assign(reinterpret_cast<const char *>(class_bytes), class_len);
            node_kind.assign(reinterpret_cast<const char *>(kind_bytes), kind_len);
        }
        nx_notice_message(kept.get(), &text, &text_len);
        message.assign(reinterpret_cast<const char *>(text), text_len);
    }); // The reader releases the last reference when it ends.
    reader.join();
    REQUIRE(error_class == "evaluation" && node_kind == "junction");
    // ANCHOR_END: cpp_notices
    std::string expected = std::string("junction 'cpp_notice_scorer' message error in domain '") +
                           suite_env("NERVIX_PROBE_DOMAIN") + "'";
    REQUIRE(message.find(expected) != std::string::npos);
}

static const struct suite_test suite_tests[] = {
    SUITE_CONTRACT_TESTS
    {"boundary", "test_cpp_unique_ptr_releases_on_an_exception", test_cpp_unique_ptr_releases_on_an_exception},
    {"cluster", "test_cpp_raii_keeps_a_retained_batch_across_a_thread", test_cpp_raii_keeps_a_retained_batch_across_a_thread},
    {"cluster", "test_cpp_application_owns_endpoints_and_delivery_with_typed_deleters", test_cpp_application_owns_endpoints_and_delivery_with_typed_deleters},
    {"cluster", "test_cpp_owned_notices_are_routed_by_kind_and_read_on_another_thread", test_cpp_owned_notices_are_routed_by_kind_and_read_on_another_thread},
};

int main(int argc, char **argv) {
    if (argc != 2 || (std::strcmp(argv[1], "boundary") && std::strcmp(argv[1], "cluster"))) {
        std::fprintf(stderr, "usage: cpp-suite boundary|cluster\n");
        return 2;
    }
    // An embedding application may keep SIGPIPE's default action, which ends the process. The
    // binding must never raise it, so every test runs with that action whatever the parent set.
    if (std::signal(SIGPIPE, SIG_DFL) == SIG_ERR) {
        std::perror("signal");
        return 2;
    }
    std::size_t ran = 0;
    bool failed = false;
    for (const auto &test : suite_tests) {
        if (std::strcmp(test.tier, argv[1])) continue;
        ++ran;
        bool passed = false;
        try {
            test.run();
            passed = true;
        } catch (const std::exception &failure) {
            std::fprintf(stderr, "%s: %s\n", test.name, failure.what());
        }
        // Reverse release order matches the C fixture and ends every owner before reporting.
        suite_cleanup_to(0);
        if (!suite_clear_refusals()) {
            std::fprintf(stderr, "scenario did not confirm refusal cleanup\n");
            passed = false;
        }
        std::printf("NERVIX-SUITE %s %s\n", passed ? "passed" : "failed", test.name);
        std::fflush(stdout);
        if (!passed) failed = true;
    }
    return ran ? (failed ? 1 : 0) : 2;
}
