// The C++ probe of the shared Rust binding.
//
// It owns every handle through std::unique_ptr with the binding's release function, so no path
// leaks or double-releases a handle, and turns every returned error into an exception. It prints
// the same conformance report as every other probe. Run as `cpp-probe clock`, it attaches to the
// domain's running clock instead, reads the clock the attach reported before its first tick,
// follows the generation a STOP and START begin and the attachment restored after its session
// ends, and detaches. Run as `cpp-probe io`, it publishes typed batches through a client ingestor
// and reads, retries, rejects and acknowledges their output through a client emitter, across the
// session the scenario cuts.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "nervix_client.h"

namespace {

struct Failure : std::runtime_error {
    nx_error_kind kind;
    std::string reference;
    Failure(nx_error_kind kind, const std::string &message, std::string reference)
        : std::runtime_error(message), kind(kind), reference(std::move(reference)) {}
};

std::string borrowed(const uint8_t *data, size_t len) {
    return std::string(reinterpret_cast<const char *>(data), len);
}

// Throws the error a call returned, releasing it.
void check(nx_error *error) {
    if (error == nullptr) {
        return;
    }
    const uint8_t *message = nullptr;
    size_t message_len = 0;
    nx_error_message(error, &message, &message_len);
    const uint8_t *reference = nullptr;
    size_t reference_len = 0;
    std::string named;
    if (nx_error_execution_reference(error, &reference, &reference_len)) {
        named = borrowed(reference, reference_len);
    }
    Failure failure(nx_error_kind_of(error), borrowed(message, message_len), named);
    nx_error_free(error);
    throw failure;
}

template <typename T, void (*Release)(T *)> struct Releaser {
    void operator()(T *handle) const { Release(handle); }
};

using Session = std::unique_ptr<nx_session, Releaser<nx_session, nx_session_free>>;
using Execution = std::unique_ptr<nx_execution, Releaser<nx_execution, nx_execution_free>>;
using Outcome = std::unique_ptr<nx_outcome, Releaser<nx_outcome, nx_outcome_free>>;
using Schema = std::unique_ptr<nx_schema, Releaser<nx_schema, nx_schema_free>>;
using Event = std::unique_ptr<nx_event, Releaser<nx_event, nx_event_release>>;
using ClockEvent = std::unique_ptr<nx_clock_event, Releaser<nx_clock_event, nx_clock_event_release>>;
using DomainClock =
    std::unique_ptr<nx_domain_clock, Releaser<nx_domain_clock, nx_domain_clock_release>>;
using Cancel = std::unique_ptr<nx_cancel, Releaser<nx_cancel, nx_cancel_free>>;

const uint8_t *bytes(std::string_view text) {
    return reinterpret_cast<const uint8_t *>(text.data());
}

std::string env(const char *name) {
    const char *value = std::getenv(name);
    if (value == nullptr) {
        throw std::runtime_error(std::string(name) + " is not set");
    }
    return value;
}

std::string disposition_name(nx_disposition disposition) {
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
    throw std::runtime_error("unknown disposition");
}

std::string type_name(nx_type type) {
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
    throw std::runtime_error("unknown type");
}

std::string hex(std::string_view value) {
    static const char digits[] = "0123456789abcdef";
    std::string text;
    for (unsigned char byte : value) {
        text.push_back(digits[byte >> 4]);
        text.push_back(digits[byte & 0x0f]);
    }
    return text;
}

struct Field {
    std::string name;
    nx_type type;
    bool nullable;
    bool sensitive;

    std::string line(const std::string &prefix) const {
        return prefix + " " + name + " " + type_name(type) + " " +
               (nullable ? "nullable" : "required") + " " + (sensitive ? "sensitive" : "public");
    }
};

std::vector<Field> fields_of(const nx_schema *schema, nx_part part) {
    size_t count = 0;
    check(nx_schema_field_count(schema, part, &count));
    std::vector<Field> fields;
    for (size_t index = 0; index < count; ++index) {
        const uint8_t *name = nullptr;
        size_t name_len = 0;
        nx_type type{};
        bool nullable = false;
        bool sensitive = false;
        check(nx_schema_field(schema, part, index, &name, &name_len, &type, &nullable,
                              &sensitive));
        fields.push_back(Field{borrowed(name, name_len), type, nullable, sensitive});
    }
    return fields;
}

// Reads one fixed-width value of type T out of a copied column.
template <typename T> T value_at(const std::vector<uint8_t> &values, size_t row) {
    T value;
    std::memcpy(&value, values.data() + row * sizeof(T), sizeof(T));
    return value;
}

size_t fixed_width(nx_type type) {
    switch (type) {
    case NX_TYPE_U8: case NX_TYPE_I8: case NX_TYPE_BOOL: return 1;
    case NX_TYPE_U16: case NX_TYPE_I16: return 2;
    case NX_TYPE_U32: case NX_TYPE_I32: case NX_TYPE_F32: return 4;
    case NX_TYPE_U64: case NX_TYPE_I64: case NX_TYPE_F64: case NX_TYPE_DATETIME: return 8;
    default: return 0;
    }
}

// One column copied out of a batch: fixed-width values, or offsets into concatenated data.
struct Column {
    std::vector<uint8_t> values;
    std::vector<uint64_t> offsets;
    std::vector<uint8_t> data;

    static Column copy(const Field &field, const nx_event *event, nx_part part, size_t column,
                       size_t cells) {
        Column copied;
        size_t width = fixed_width(field.type);
        if (width > 0) {
            copied.values.resize(cells * width);
            check(nx_event_column_fixed(event, part, column, copied.values.data(),
                                        copied.values.size()));
            return copied;
        }
        if (field.type != NX_TYPE_STRING && field.type != NX_TYPE_BYTES) {
            throw std::runtime_error("list columns are read from the frame");
        }
        size_t data_len = 0;
        check(nx_event_column_varlen(event, part, column, nullptr, 0, nullptr, 0, &data_len));
        copied.offsets.resize(cells + 1);
        copied.data.resize(data_len == 0 ? 1 : data_len);
        check(nx_event_column_varlen(event, part, column, copied.offsets.data(),
                                     copied.offsets.size(), copied.data.data(),
                                     copied.data.size(), &data_len));
        return copied;
    }

    std::string render(const Field &field, const nx_event *event, nx_part part, size_t column,
                       size_t row) const {
        std::ostringstream out;
        char buffer[32];
        switch (field.type) {
        case NX_TYPE_U8: out << "u8:" << unsigned(value_at<uint8_t>(values, row)); break;
        case NX_TYPE_I8: out << "i8:" << int(value_at<int8_t>(values, row)); break;
        case NX_TYPE_U16: out << "u16:" << value_at<uint16_t>(values, row); break;
        case NX_TYPE_I16: out << "i16:" << value_at<int16_t>(values, row); break;
        case NX_TYPE_U32: out << "u32:" << value_at<uint32_t>(values, row); break;
        case NX_TYPE_I32: out << "i32:" << value_at<int32_t>(values, row); break;
        case NX_TYPE_U64: out << "u64:" << value_at<uint64_t>(values, row); break;
        case NX_TYPE_I64: out << "i64:" << value_at<int64_t>(values, row); break;
        case NX_TYPE_F32:
            std::snprintf(buffer, sizeof buffer, "%08" PRIx32, value_at<uint32_t>(values, row));
            out << "f32:" << buffer;
            break;
        case NX_TYPE_F64:
            std::snprintf(buffer, sizeof buffer, "%016" PRIx64, value_at<uint64_t>(values, row));
            out << "f64:" << buffer;
            break;
        case NX_TYPE_BOOL: out << "bool:" << (value_at<uint8_t>(values, row) != 0 ? "true" : "false"); break;
        case NX_TYPE_DATETIME: out << "datetime:" << value_at<int64_t>(values, row); break;
        case NX_TYPE_STRING:
        case NX_TYPE_BYTES: {
            std::string copied(reinterpret_cast<const char *>(data.data()) + offsets[row],
                               offsets[row + 1] - offsets[row]);
            const uint8_t *value = nullptr;
            size_t value_len = 0;
            check(nx_event_cell_varlen(event, part, row, column, &value, &value_len));
            if (borrowed(value, value_len) != copied) {
                throw std::runtime_error(
                    "a copied value differs from the same value borrowed from the frame");
            }
            out << (field.type == NX_TYPE_STRING ? "str:" : "bytes:") << hex(copied);
            break;
        }
        default:
            throw std::runtime_error("unexpected column type");
        }
        return out.str();
    }
};

// The rendered cells of every row of `part`, copying each column once.
std::vector<std::string> render(const nx_event *event, nx_part part,
                                const std::vector<Field> &fields, size_t cells) {
    std::vector<std::string> rows(cells);
    for (size_t column = 0; column < fields.size(); ++column) {
        const Field &field = fields[column];
        std::vector<uint8_t> states(cells);
        check(nx_event_column_states(event, part, column, states.data(), states.size()));
        Column copied = Column::copy(field, event, part, column, cells);
        for (size_t row = 0; row < cells; ++row) {
            std::string rendered;
            if (states[row] == NX_CELL_NULL) {
                rendered = "null";
            } else if (states[row] == NX_CELL_REDACTED) {
                rendered = "redacted";
            } else {
                rendered = copied.render(field, event, part, column, row);
            }
            rows[row] += (rows[row].empty() ? "" : " ") + field.name + "=" + rendered;
        }
    }
    return rows;
}

std::vector<std::string> row_lines(const nx_event *event, const std::vector<Field> &fields,
                                   const std::vector<Field> &key_fields) {
    std::string key;
    if (!key_fields.empty()) {
        key = render(event, NX_PART_BRANCH_KEY, key_fields, 1).at(0);
    }
    std::vector<std::string> lines;
    for (const std::string &row :
         render(event, NX_PART_ROWS, fields, static_cast<size_t>(nx_event_row_count(event)))) {
        lines.push_back("ROW [" + key + "] " + row);
    }
    return lines;
}

Outcome execute(const Session &session, const std::string &query, const nx_cancel *cancel) {
    nx_execution *raw_execution = nullptr;
    check(nx_session_prepare(session.get(), bytes(query), query.size(), nullptr, &raw_execution));
    Execution execution(raw_execution);
    nx_outcome *outcome = nullptr;
    check(nx_session_execute(session.get(), execution.get(), cancel, &outcome));
    return Outcome(outcome);
}

Cancel with_deadline(uint64_t millis) {
    nx_cancel *cancel = nullptr;
    check(nx_cancel_with_deadline(millis, &cancel));
    return Cancel(cancel);
}

nx_error_kind failure_kind(const std::function<void()> &call) {
    try {
        call();
    } catch (const Failure &failure) {
        return failure.kind;
    }
    throw std::runtime_error("a call succeeded where it had to fail");
}

void check_cancellation(const Session &session) {
    Cancel cancel(nx_cancel_new());
    std::atomic<nx_error_kind> waited{};
    std::thread waiter([&] {
        waited = failure_kind([&] {
            nx_event *event = nullptr;
            check(nx_session_next_event(session.get(), cancel.get(), &event));
            Event owned(event);
        });
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    nx_cancel_trigger(cancel.get());
    waiter.join();
    if (waited != NX_ERROR_CANCELLED) {
        throw std::runtime_error("a cancelled wait did not report CANCELLED");
    }

    Cancel deadline = with_deadline(50);
    nx_error_kind expired = failure_kind([&] {
        nx_event *event = nullptr;
        check(nx_session_next_event(session.get(), deadline.get(), &event));
        Event owned(event);
    });
    if (expired != NX_ERROR_DEADLINE) {
        throw std::runtime_error("an expired wait did not report DEADLINE");
    }

    Cancel cancelled(nx_cancel_new());
    nx_cancel_trigger(cancelled.get());
    try {
        execute(session, "SHOW DOMAINS;", cancelled.get());
        throw std::runtime_error("a cancelled command completed");
    } catch (const Failure &failure) {
        if (failure.kind != NX_ERROR_CANCELLED || failure.reference.empty()) {
            throw std::runtime_error(
                "a cancelled command did not report CANCELLED with its execution reference");
        }
    }
}

std::string clock_kind_name(nx_clock_event_kind kind) {
    switch (kind) {
    case NX_CLOCK_EVENT_STATE: return "STATE";
    case NX_CLOCK_EVENT_TICK: return "TICK";
    case NX_CLOCK_EVENT_ENDED: return "ENDED";
    case NX_CLOCK_EVENT_INTERRUPTED: return "INTERRUPTED";
    case NX_CLOCK_EVENT_RESTORATION_FAILED: return "RESTORATION_FAILED";
    }
    throw std::runtime_error("unknown clock event kind");
}

ClockEvent next_clock_event(const Session &session, const nx_cancel *cancel,
                            const std::string &domain) {
    nx_clock_event *raw_event = nullptr;
    check(nx_session_next_clock_event(session.get(), cancel, &raw_event));
    ClockEvent event(raw_event);
    const uint8_t *name = nullptr;
    size_t name_len = 0;
    nx_clock_event_domain(event.get(), &name, &name_len);
    if (borrowed(name, name_len) != domain) {
        throw std::runtime_error("a clock event arrived for another domain");
    }
    return event;
}

// The clock the session holds for `domain`, or an empty pointer when it follows none.
DomainClock read_domain_clock(const Session &session, const std::string &domain) {
    nx_domain_clock *raw_clock = nullptr;
    check(nx_session_domain_clock(session.get(), bytes(domain), domain.size(), &raw_clock));
    return DomainClock(raw_clock);
}

uint64_t generation_of(const nx_clock_event *event) {
    uint64_t generation = 0;
    check(nx_clock_event_generation(event, &generation));
    return generation;
}

// The progress a tick event reports. The authority's UTC observation depends on when the tick was
// accepted, so it is not read.
struct Tick {
    uint64_t generation = 0;
    uint64_t id = 0;
    int64_t boundary = 0;
    int64_t serving_logical = 0;

    static Tick of(const nx_clock_event *event) {
        Tick tick;
        tick.generation = generation_of(event);
        check(nx_clock_event_tick(event, &tick.id, &tick.boundary, nullptr, &tick.serving_logical));
        return tick;
    }
};

// The committed clock of a paced generation.
struct PacedClock {
    uint64_t generation = 0;
    uint64_t period = 0;
    uint64_t skew = 0;
    int64_t origin = 0;
    int64_t anchor = 0;
    double rate = 0.0;

    static PacedClock of(const nx_clock_event *event) {
        PacedClock clock;
        clock.generation = generation_of(event);
        check(nx_clock_event_paced(event, &clock.period, &clock.skew, &clock.origin, &clock.anchor,
                                   &clock.rate));
        return clock;
    }

    static PacedClock of(const nx_domain_clock *read) {
        PacedClock clock;
        clock.generation = nx_domain_clock_generation(read);
        check(nx_domain_clock_paced(read, &clock.period, &clock.skew, &clock.origin, &clock.anchor,
                                    &clock.rate));
        return clock;
    }

    bool operator==(const PacedClock &other) const {
        return generation == other.generation && period == other.period && skew == other.skew &&
               origin == other.origin && anchor == other.anchor && rate == other.rate;
    }

    // The report line of the clock, with `prefix` naming where it was read. The UTC anchor depends
    // on when the scenario's START committed, so it is read but not reported.
    std::string line(const std::string &prefix, const std::string &domain) const {
        uint64_t rate_bits = 0;
        std::memcpy(&rate_bits, &rate, sizeof rate_bits);
        char bits[17];
        std::snprintf(bits, sizeof bits, "%016" PRIx64, rate_bits);
        return prefix + " domain=" + domain + " generation=" + std::to_string(generation) +
               " state=paced period=" + std::to_string(period) + " skew=" + std::to_string(skew) +
               " origin=" + std::to_string(origin) + " rate=f64:" + bits;
    }

    // An instant as the report names it: `origin` for the logical origin, the instant otherwise.
    std::string relative(int64_t instant) const {
        return instant == origin ? "origin" : std::to_string(instant);
    }

    // The report line of the projections of `read`, which holds this clock, at its own UTC anchor:
    // the logical time there, the wait for the next tick center, the admission window, and whether
    // an event at the skew's edge and one nanosecond past it are admitted.
    std::string projection_line(const nx_domain_clock *read, const std::string &domain) const {
        int64_t next_center = 0;
        int64_t edge = 0;
        int64_t beyond = 0;
        if (period > static_cast<uint64_t>(INT64_MAX) || skew > static_cast<uint64_t>(INT64_MAX) ||
            __builtin_add_overflow(origin, static_cast<int64_t>(period), &next_center) ||
            __builtin_add_overflow(origin, static_cast<int64_t>(skew), &edge) ||
            __builtin_add_overflow(edge, 1, &beyond)) {
            throw std::runtime_error("the clock's fields leave the logical time range");
        }
        int64_t at_anchor = 0;
        check(nx_domain_clock_logical_time_at(read, anchor, &at_anchor));
        uint64_t wait = 0;
        check(nx_domain_clock_wall_duration_until(read, anchor, next_center, &wait));
        bool has_window = false;
        int64_t earliest = 0;
        int64_t latest = 0;
        check(nx_domain_clock_admission_window(read, anchor, &has_window, &earliest, &latest));
        if (!has_window) {
            throw std::runtime_error("a paced clock reports no admission window");
        }
        bool at_edge = false;
        bool past_edge = false;
        check(nx_domain_clock_admits(read, anchor, edge, &at_edge));
        check(nx_domain_clock_admits(read, anchor, beyond, &past_edge));
        return "PROJECTION domain=" + domain + " generation=" + std::to_string(generation) +
               " anchor=" + relative(at_anchor) + " wait=" + std::to_string(wait) + " window=" +
               relative(earliest) + ".." + relative(latest) +
               " skew=" + (at_edge ? "admitted" : "refused") +
               " beyond=" + (past_edge ? "admitted" : "refused");
    }

    // Holds a tick to this clock: the same generation, a boundary of the logical origin plus one
    // period for every id before it, and a serving node's reading that never precedes the origin.
    void check_tick(const Tick &tick) const {
        if (tick.generation != generation) {
            throw std::runtime_error("a tick of one generation followed the state of another");
        }
        uint64_t offset = 0;
        int64_t expected = 0;
        if (tick.id == 0 || __builtin_mul_overflow(tick.id - 1, period, &offset) ||
            offset > static_cast<uint64_t>(INT64_MAX) ||
            __builtin_add_overflow(origin, static_cast<int64_t>(offset), &expected) ||
            tick.boundary != expected) {
            throw std::runtime_error(
                "a tick's boundary is not the origin plus one period for every id before it");
        }
        if (tick.serving_logical < origin) {
            throw std::runtime_error("the serving node's logical reading precedes the logical origin");
        }
    }

    // The report line of a tick this clock holds.
    std::string tick_line(const Tick &tick, const std::string &domain) const {
        check_tick(tick);
        return "TICK domain=" + domain + " generation=" + std::to_string(tick.generation) +
               " boundary=origin+(id-1)*period";
    }
};

// Cancels a clock wait from another thread, then lets a deadline end another. The session follows
// no clock yet, so nothing but its token ends either wait.
void check_clock_cancellation(const Session &session) {
    Cancel cancel(nx_cancel_new());
    std::atomic<nx_error_kind> waited{};
    std::thread waiter([&] {
        waited = failure_kind([&] {
            nx_clock_event *event = nullptr;
            check(nx_session_next_clock_event(session.get(), cancel.get(), &event));
            ClockEvent owned(event);
        });
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    nx_cancel_trigger(cancel.get());
    waiter.join();
    if (waited != NX_ERROR_CANCELLED) {
        throw std::runtime_error("a cancelled clock wait did not report CANCELLED");
    }

    Cancel deadline = with_deadline(50);
    nx_error_kind expired = failure_kind([&] {
        nx_clock_event *event = nullptr;
        check(nx_session_next_clock_event(session.get(), deadline.get(), &event));
        ClockEvent owned(event);
    });
    if (expired != NX_ERROR_DEADLINE) {
        throw std::runtime_error("an expired clock wait did not report DEADLINE");
    }
}

// What the probe has read about the domain's clock: the generation of the newest state, its
// mapping while it is paced, and whether the session holding the attachment ended since. Every
// event is held to what was read before it, and a read of the clock taken right after it is held
// to be no older.
class FollowedClock {
  public:
    FollowedClock(const Session &session, std::string domain, PacedClock clock)
        : session_(session), domain_(std::move(domain)), generation_(clock.generation),
          paced_(true), clock_(clock) {}

    const PacedClock &clock() const {
        if (!paced_) {
            throw std::runtime_error("the followed clock is not paced");
        }
        return clock_;
    }

    // The first tick of the followed generation. A state reporting that generation again is taken
    // on the way; one of another generation fails the probe.
    ClockEvent first_tick(const nx_cancel *deadline) {
        const uint64_t generation = generation_;
        for (;;) {
            ClockEvent event = next(deadline);
            nx_clock_event_kind kind = nx_clock_event_kind_of(event.get());
            if (kind == NX_CLOCK_EVENT_TICK) {
                return event;
            }
            if (kind != NX_CLOCK_EVENT_STATE || generation_ != generation) {
                throw std::runtime_error("the clock reported " + clock_kind_name(kind) +
                                         " before the first tick of the followed generation");
            }
        }
    }

    // The paced state of a generation after the followed one. The followed generation's ticks and
    // the states before the new paced one are taken on the way.
    ClockEvent next_generation(const nx_cancel *deadline) {
        const uint64_t previous = generation_;
        for (;;) {
            ClockEvent event = next(deadline);
            nx_clock_event_kind kind = nx_clock_event_kind_of(event.get());
            if (kind == NX_CLOCK_EVENT_STATE && generation_ > previous && paced_) {
                return event;
            }
            if (kind != NX_CLOCK_EVENT_TICK && kind != NX_CLOCK_EVENT_STATE) {
                throw std::runtime_error("the clock reported " + clock_kind_name(kind) +
                                         " before a generation after the followed one");
            }
        }
    }

    // Waits for the interruption of the attachment. The followed generation's ticks and states
    // are taken on the way.
    void interruption(const nx_cancel *deadline) {
        const uint64_t generation = generation_;
        for (;;) {
            ClockEvent event = next(deadline);
            nx_clock_event_kind kind = nx_clock_event_kind_of(event.get());
            if (kind == NX_CLOCK_EVENT_INTERRUPTED) {
                return;
            }
            if (kind != NX_CLOCK_EVENT_TICK &&
                (kind != NX_CLOCK_EVENT_STATE || generation_ != generation)) {
                throw std::runtime_error("the clock reported " + clock_kind_name(kind) +
                                         " before the interruption");
            }
        }
    }

    // The paced state the restored attachment reports. A refused restoration, which the session
    // repeats, and a clock reported uninstalled are taken on the way.
    ClockEvent restored(const nx_cancel *deadline) {
        for (;;) {
            ClockEvent event = next(deadline);
            if (nx_clock_event_kind_of(event.get()) == NX_CLOCK_EVENT_STATE && paced_) {
                return event;
            }
        }
    }

  private:
    // The next event about the domain, held to what the probe read before it.
    ClockEvent next(const nx_cancel *deadline) {
        ClockEvent event = next_clock_event(session_, deadline, domain_);
        switch (nx_clock_event_kind_of(event.get())) {
        case NX_CLOCK_EVENT_STATE: observe(event.get()); break;
        case NX_CLOCK_EVENT_TICK: check_tick(event.get()); break;
        case NX_CLOCK_EVENT_INTERRUPTED: interrupted_ = true; break;
        case NX_CLOCK_EVENT_RESTORATION_FAILED: break;
        case NX_CLOCK_EVENT_ENDED: throw std::runtime_error("the server ended the attachment");
        }
        return event;
    }

    DomainClock read() const {
        DomainClock read = read_domain_clock(session_, domain_);
        if (!read) {
            throw std::runtime_error(
                "the session follows no clock of the domain after an event about it");
        }
        return read;
    }

    void observe(const nx_clock_event *event) {
        const uint64_t generation = generation_of(event);
        if (generation < generation_) {
            throw std::runtime_error("a state went back to an earlier generation");
        }
        nx_clock_state state = NX_CLOCK_STOPPED;
        check(nx_clock_event_state(event, &state));
        const bool paced = state == NX_CLOCK_PACED;
        PacedClock clock;
        clock.generation = generation;
        if (paced) {
            clock = PacedClock::of(event);
        }
        DomainClock held = read();
        if (nx_domain_clock_generation(held.get()) < generation) {
            throw std::runtime_error("a read of the clock is older than the state the probe took");
        }
        if (nx_domain_clock_generation(held.get()) == generation) {
            if (nx_domain_clock_state(held.get()) != state) {
                throw std::runtime_error(
                    "a read of the clock differs from the state of its generation");
            }
            if (paced && !(PacedClock::of(held.get()) == clock)) {
                throw std::runtime_error(
                    "a read of the clock differs from the mapping of its generation");
            }
        }
        generation_ = generation;
        paced_ = paced;
        clock_ = clock;
        interrupted_ = false;
    }

    void check_tick(const nx_clock_event *event) const {
        if (interrupted_) {
            throw std::runtime_error(
                "a tick arrived before the restored attachment reported its clock");
        }
        const Tick tick = Tick::of(event);
        clock().check_tick(tick);
        DomainClock held = read();
        if (nx_domain_clock_generation(held.get()) < tick.generation) {
            throw std::runtime_error("a read of the clock is older than the tick the probe took");
        }
        uint64_t held_id = 0;
        if (nx_domain_clock_generation(held.get()) == tick.generation &&
            nx_domain_clock_tick(held.get(), &held_id, nullptr, nullptr, nullptr) &&
            held_id < tick.id) {
            throw std::runtime_error("a read of the clock holds an older tick than the probe took");
        }
    }

    const Session &session_;
    std::string domain_;
    uint64_t generation_ = 0;
    bool paced_ = false;
    PacedClock clock_;
    bool interrupted_ = false;
};

// Prints the paced state a STATE event reports and the first tick after it.
void report_state_and_first_tick(FollowedClock &followed, const ClockEvent &state,
                                 const nx_cancel *deadline, const std::string &domain) {
    std::cout << PacedClock::of(state.get()).line("STATE", domain) << std::endl;
    ClockEvent tick = followed.first_tick(deadline);
    std::cout << followed.clock().tick_line(Tick::of(tick.get()), domain) << std::endl;
}

// Attaches to the domain's running clock and reads the clock the attach reported before its first
// tick, then follows the generation the scenario's STOP and START begin and the attachment restored
// after the scenario ends the session, and detaches.
void run_clock(const Session &session, const std::string &domain) {
    check_clock_cancellation(session);
    Outcome attached = execute(session, "ATTACH DOMAIN CLOCK;", nullptr);
    std::cout << "ATTACHED " << disposition_name(nx_outcome_disposition(attached.get()))
              << std::endl;

    // The clock the attach reported, read before any event about the attachment.
    DomainClock read = read_domain_clock(session, domain);
    if (!read) {
        throw std::runtime_error("the session follows no clock after its attach completed");
    }
    if (nx_domain_clock_state(read.get()) != NX_CLOCK_PACED) {
        throw std::runtime_error("the attach reported a clock other than the running paced one");
    }
    const PacedClock clock = PacedClock::of(read.get());
    const std::string reported_clock = clock.line("CLOCK", domain);
    std::cout << reported_clock << std::endl;
    std::cout << clock.projection_line(read.get(), domain) << std::endl;

    FollowedClock followed(session, domain, clock);
    Cancel deadline = with_deadline(120000);
    ClockEvent tick = followed.first_tick(deadline.get());
    const std::string reported_tick = clock.tick_line(Tick::of(tick.get()), domain);
    std::cout << reported_tick << std::endl;

    // Keep a second reference to the read and the tick and release the first ones on another
    // thread, so both must read the same on the second alone.
    DomainClock retained_read(nx_domain_clock_retain(read.get()));
    ClockEvent retained_tick(nx_clock_event_retain(tick.get()));
    std::thread releaser([first_read = std::move(read), first_tick = std::move(tick)]() mutable {
        first_read.reset();
        first_tick.reset();
    });
    releaser.join();
    const PacedClock clock_again = PacedClock::of(retained_read.get());
    if (clock_again.line("CLOCK", domain) != reported_clock ||
        clock_again.tick_line(Tick::of(retained_tick.get()), domain) != reported_tick) {
        throw std::runtime_error("a retained clock or tick reads differently than it did");
    }

    // The scenario stops the domain and starts it again at another origin and rate.
    Cancel started_deadline = with_deadline(120000);
    ClockEvent started = followed.next_generation(started_deadline.get());
    report_state_and_first_tick(followed, started, started_deadline.get(), domain);

    // The scenario ends the session, and the binding attaches the clock again on the next one.
    Cancel restored_deadline = with_deadline(120000);
    followed.interruption(restored_deadline.get());
    std::cout << "INTERRUPTED domain=" << domain << std::endl;
    ClockEvent restored = followed.restored(restored_deadline.get());
    report_state_and_first_tick(followed, restored, restored_deadline.get(), domain);

    Outcome detached = execute(session, "DETACH DOMAIN CLOCK;", nullptr);
    std::cout << "DETACHED " << disposition_name(nx_outcome_disposition(detached.get()))
              << std::endl;
    std::cout << "CHECKS ok" << std::endl;
    std::cout << "PASS" << std::endl;
}

// ---- Producers and consumers --------------------------------------------------------------

using Fields = std::unique_ptr<nx_fields, Releaser<nx_fields, nx_fields_free>>;
using Producer = std::unique_ptr<nx_producer, Releaser<nx_producer, nx_producer_free>>;
using Consumer = std::unique_ptr<nx_consumer, Releaser<nx_consumer, nx_consumer_free>>;
using Delivery = std::unique_ptr<nx_delivery, Releaser<nx_delivery, nx_delivery_release>>;
using Batch = std::unique_ptr<nx_batch, Releaser<nx_batch, nx_batch_release>>;
using Builder = std::unique_ptr<nx_batch_builder, Releaser<nx_batch_builder, nx_batch_builder_free>>;
using Submission =
    std::unique_ptr<nx_submission_outcome, Releaser<nx_submission_outcome, nx_submission_outcome_free>>;

constexpr uint64_t wait_millis = 120000;
constexpr uint64_t expiring_millis = 200;
constexpr uint32_t producer_batches = 2;
constexpr uint32_t consumer_batches = 4;
constexpr uint64_t endpoint_bytes = 1048576;
constexpr uint8_t scribble = 0xaa;

// One level of a field's type, as a host names it.
struct Level {
    nx_type type;
    uint32_t length;
};

// One field the probe expects an endpoint to have.
struct FieldSpec {
    std::string name;
    std::vector<Level> levels;
    bool nullable;
    bool sensitive;
};

const std::vector<FieldSpec> &input_fields() {
    static const std::vector<FieldSpec> fields = {
        {"id", {{NX_TYPE_U32, 0}}, false, false},
        {"tenant", {{NX_TYPE_STRING, 0}}, false, false},
        {"u8v", {{NX_TYPE_U8, 0}}, false, false},
        {"i8v", {{NX_TYPE_I8, 0}}, false, false},
        {"u16v", {{NX_TYPE_U16, 0}}, false, false},
        {"i16v", {{NX_TYPE_I16, 0}}, false, false},
        {"u32v", {{NX_TYPE_U32, 0}}, false, false},
        {"i32v", {{NX_TYPE_I32, 0}}, false, false},
        {"u64v", {{NX_TYPE_U64, 0}}, false, false},
        {"i64v", {{NX_TYPE_I64, 0}}, false, false},
        {"f32v", {{NX_TYPE_F32, 0}}, false, false},
        {"f64v", {{NX_TYPE_F64, 0}}, false, false},
        {"flag", {{NX_TYPE_BOOL, 0}}, false, false},
        {"text", {{NX_TYPE_STRING, 0}}, true, false},
        {"raw", {{NX_TYPE_BYTES, 0}}, true, false},
        {"at", {{NX_TYPE_DATETIME, 0}}, false, false},
        {"maybe", {{NX_TYPE_I64, 0}}, true, false},
        {"tags", {{NX_TYPE_LIST, 0}, {NX_TYPE_STRING, 0}}, false, false},
        {"grid", {{NX_TYPE_FIXED_LIST, 2}, {NX_TYPE_FIXED_LIST, 2}, {NX_TYPE_I16, 0}}, false, false},
        {"spans", {{NX_TYPE_LIST, 0}, {NX_TYPE_FIXED_LIST, 2}, {NX_TYPE_DATETIME, 0}}, true, false},
        {"secret", {{NX_TYPE_STRING, 0}}, false, true},
    };
    return fields;
}

// One row of an input batch, as the probe's application holds it.
struct Row {
    uint32_t id = 0;
    std::string tenant;
    uint8_t u8v = 0;
    int8_t i8v = 0;
    uint16_t u16v = 0;
    int16_t i16v = 0;
    uint32_t u32v = 0;
    int32_t i32v = 0;
    uint64_t u64v = 0;
    int64_t i64v = 0;
    uint32_t f32v = 0;
    uint64_t f64v = 0;
    bool flag = false;
    bool has_text = false;
    std::string text;
    bool has_raw = false;
    std::string raw;
    int64_t at = 0;
    bool has_maybe = false;
    int64_t maybe = 0;
    std::vector<std::string> tags;
    int16_t grid[2][2] = {{0, 0}, {0, 0}};
    bool has_spans = false;
    std::vector<std::array<int64_t, 2>> spans;
    std::string secret = "s";
};

// A row of `tenant` whose every other value is the zero of its type.
Row plain_row(uint32_t id, const std::string &tenant) {
    Row row;
    row.id = id;
    row.tenant = tenant;
    return row;
}

// The typed rows of the first batch.
std::vector<Row> typed_rows() {
    Row first;
    first.id = 1;
    first.tenant = "acme";
    first.u8v = UINT8_MAX;
    first.i8v = INT8_MAX;
    first.u16v = UINT16_MAX;
    first.i16v = INT16_MAX;
    first.u32v = UINT32_MAX;
    first.i32v = INT32_MAX;
    first.u64v = UINT64_MAX;
    first.i64v = INT64_MAX;
    first.f32v = 0x7f7fffffu;
    first.f64v = 0x7fefffffffffffffull;
    first.flag = true;
    first.has_text = true;
    first.text = "h\xc3\xa9llo \xe4\xb8\x96\xe7\x95\x8c \xf0\x9f\x9a\x80";
    first.has_raw = true;
    first.raw = std::string("\x00\xff\xfe\x80\x00", 5);
    first.at = INT64_MAX;
    first.tags = {"a", "", "h\xc3\xa9llo"};
    first.grid[0][0] = 1;
    first.grid[0][1] = -2;
    first.grid[1][0] = INT16_MAX;
    first.grid[1][1] = INT16_MIN;
    first.has_spans = true;
    first.spans = {{INT64_MIN, INT64_MAX}};
    first.secret = "s1";

    Row second;
    second.id = 2;
    second.tenant = "acme";
    second.i8v = INT8_MIN;
    second.i16v = INT16_MIN;
    second.i32v = INT32_MIN;
    second.i64v = INT64_MIN;
    second.f32v = 0x80000000u;
    second.f64v = 0x0000000000000001ull;
    second.has_text = true;
    second.text = std::string("a\0b", 3);
    second.has_raw = true;
    second.at = INT64_MIN;
    second.has_maybe = true;
    second.maybe = 0;
    second.secret = "s2";

    Row third;
    third.id = 3;
    third.tenant = "acme";
    third.u8v = 1;
    third.i8v = -1;
    third.u16v = 1;
    third.i16v = -1;
    third.u32v = 1;
    third.i32v = -1;
    third.u64v = 9007199254740993ull;
    third.i64v = -9007199254740993ll;
    third.f32v = 0x3fc00000u;
    third.f64v = 0x3fb999999999999aull;
    third.flag = true;
    third.at = 1;
    third.has_maybe = true;
    third.maybe = 9007199254740992ll;
    third.tags = {"x"};
    third.grid[0][0] = -1;
    third.grid[0][1] = 1;
    third.grid[1][0] = 2;
    third.grid[1][1] = -2;
    third.has_spans = true;
    third.secret = "s3";
    return {first, second, third};
}

template <typename T> void push_native(std::vector<uint8_t> &target, T value) {
    uint8_t buffer[sizeof(T)];
    std::memcpy(buffer, &value, sizeof(T));
    target.insert(target.end(), buffer, buffer + sizeof(T));
}

// What a host passes the binding for one column.
struct HostColumn {
    bool has_states = false;
    std::vector<uint8_t> states;
    bool has_list_offsets = false;
    std::vector<uint64_t> list_offsets;
    size_t leaf = 0;
    bool varlen = false;
    std::vector<uint8_t> values;
    std::vector<uint64_t> value_offsets;

    void add_varlen(const std::string &value) {
        if (value_offsets.empty()) {
            value_offsets.push_back(0);
        }
        values.insert(values.end(), value.begin(), value.end());
        value_offsets.push_back(values.size());
    }
};

// The column `name` of `rows`, as a host lays it out.
HostColumn column_of(const std::string &name, const std::vector<Row> &rows) {
    HostColumn column;
    for (const Row &row : rows) {
        if (name == "id" || name == "echo") {
            push_native(column.values, row.id);
        } else if (name == "tenant") {
            column.varlen = true;
            column.add_varlen(row.tenant);
        } else if (name == "u8v") {
            push_native(column.values, row.u8v);
        } else if (name == "i8v") {
            push_native(column.values, row.i8v);
        } else if (name == "u16v") {
            push_native(column.values, row.u16v);
        } else if (name == "i16v") {
            push_native(column.values, row.i16v);
        } else if (name == "u32v") {
            push_native(column.values, row.u32v);
        } else if (name == "i32v") {
            push_native(column.values, row.i32v);
        } else if (name == "u64v") {
            push_native(column.values, row.u64v);
        } else if (name == "i64v") {
            push_native(column.values, row.i64v);
        } else if (name == "f32v") {
            push_native(column.values, row.f32v);
        } else if (name == "f64v") {
            push_native(column.values, row.f64v);
        } else if (name == "flag") {
            column.values.push_back(row.flag ? 1 : 0);
        } else if (name == "at") {
            push_native(column.values, row.at);
        } else if (name == "text" || name == "raw") {
            bool present = name == "text" ? row.has_text : row.has_raw;
            column.varlen = true;
            column.has_states = true;
            column.states.push_back(present ? NX_CELL_VALUE : NX_CELL_NULL);
            column.add_varlen(present ? (name == "text" ? row.text : row.raw) : std::string());
        } else if (name == "maybe") {
            column.has_states = true;
            column.states.push_back(row.has_maybe ? NX_CELL_VALUE : NX_CELL_NULL);
            push_native(column.values, row.maybe);
        } else if (name == "tags") {
            column.varlen = true;
            column.has_list_offsets = true;
            column.leaf = 1;
            if (column.list_offsets.empty()) {
                column.list_offsets.push_back(0);
                column.value_offsets.push_back(0);
            }
            for (const std::string &tag : row.tags) {
                column.add_varlen(tag);
            }
            column.list_offsets.push_back(column.value_offsets.size() - 1);
        } else if (name == "grid") {
            column.leaf = 2;
            for (const auto &pair : row.grid) {
                for (int16_t cell : pair) {
                    push_native(column.values, cell);
                }
            }
        } else if (name == "spans") {
            column.has_states = true;
            column.has_list_offsets = true;
            column.leaf = 2;
            column.states.push_back(row.has_spans ? NX_CELL_VALUE : NX_CELL_NULL);
            if (column.list_offsets.empty()) {
                column.list_offsets.push_back(0);
            }
            for (const auto &span : row.spans) {
                push_native(column.values, span[0]);
                push_native(column.values, span[1]);
            }
            column.list_offsets.push_back(column.values.size() / 16);
        } else if (name == "secret") {
            column.varlen = true;
            column.add_varlen(row.secret);
        } else {
            throw std::runtime_error("the probe holds no column named " + name);
        }
    }
    if (column.varlen && column.value_offsets.empty()) {
        column.value_offsets.push_back(0);
    }
    return column;
}

Fields fields_of(const std::vector<FieldSpec> &specs) {
    Fields fields(nx_fields_new());
    for (const FieldSpec &spec : specs) {
        check(nx_fields_add(fields.get(), bytes(spec.name), spec.name.size(), spec.levels[0].type,
                            spec.levels[0].length, spec.nullable, spec.sensitive));
        for (size_t level = 1; level < spec.levels.size(); level++) {
            check(nx_fields_element(fields.get(), spec.levels[level].type,
                                    spec.levels[level].length));
        }
    }
    return fields;
}

// One field of a schema the binding reported, with every level of its type.
struct ReportedField {
    std::string name;
    std::vector<Level> levels;
    bool nullable;
    bool sensitive;

    // The type in NSPL's spelling, with nested fixed-size lists as dimensions.
    static std::string type_text(const std::vector<Level> &levels, size_t from) {
        if (levels[from].type == NX_TYPE_LIST) {
            return "VEC<" + type_text(levels, from + 1) + ">";
        }
        if (levels[from].type == NX_TYPE_FIXED_LIST) {
            size_t end = from;
            std::string dimensions;
            while (end < levels.size() && levels[end].type == NX_TYPE_FIXED_LIST) {
                dimensions += ", " + std::to_string(levels[end].length);
                end++;
            }
            return "ARRAY<" + type_text(levels, end) + dimensions + ">";
        }
        return type_name(levels[from].type);
    }

    std::string line() const {
        return name + " " + type_text(levels, 0) + " " + (nullable ? "nullable" : "required") + " " +
               (sensitive ? "sensitive" : "public");
    }
};

std::vector<ReportedField> reported_fields(const nx_schema *schema) {
    size_t count = 0;
    check(nx_schema_field_count(schema, NX_PART_ROWS, &count));
    std::vector<ReportedField> fields;
    for (size_t index = 0; index < count; index++) {
        const uint8_t *name = nullptr;
        size_t name_len = 0;
        nx_type type = NX_TYPE_U8;
        bool nullable = false;
        bool sensitive = false;
        check(nx_schema_field(schema, NX_PART_ROWS, index, &name, &name_len, &type, &nullable,
                              &sensitive));
        size_t level_count = 0;
        check(nx_schema_field_levels(schema, NX_PART_ROWS, index, &level_count));
        std::vector<Level> levels;
        for (size_t level = 0; level < level_count; level++) {
            Level read{NX_TYPE_U8, 0};
            check(nx_schema_field_level(schema, NX_PART_ROWS, index, level, &read.type,
                                        &read.length));
            levels.push_back(read);
        }
        if (levels.empty() || levels[0].type != type) {
            throw std::runtime_error("a field's first level differs from its type");
        }
        fields.push_back({borrowed(name, name_len), levels, nullable, sensitive});
    }
    return fields;
}

// Builds a batch of `rows` for `schema`, writing over every buffer as soon as the binding has
// copied it.
Batch build_batch(const nx_schema *schema, const std::vector<Row> &rows) {
    nx_batch_builder *raw_builder = nullptr;
    check(nx_batch_builder_new(schema, rows.size(), &raw_builder));
    Builder builder(raw_builder);
    std::vector<ReportedField> fields = reported_fields(schema);
    for (size_t index = 0; index < fields.size(); index++) {
        HostColumn column = column_of(fields[index].name, rows);
        if (column.has_states) {
            check(nx_batch_builder_states(builder.get(), index, column.states.data(),
                                          column.states.size()));
            std::fill(column.states.begin(), column.states.end(), scribble);
        }
        if (column.has_list_offsets) {
            check(nx_batch_builder_offsets(builder.get(), index, 0, column.list_offsets.data(),
                                           column.list_offsets.size()));
            std::fill(column.list_offsets.begin(), column.list_offsets.end(), UINT64_MAX);
        }
        if (column.varlen) {
            check(nx_batch_builder_varlen(builder.get(), index, column.leaf,
                                          column.value_offsets.data(), column.value_offsets.size(),
                                          column.values.data(), column.values.size()));
            std::fill(column.value_offsets.begin(), column.value_offsets.end(), UINT64_MAX);
        } else {
            check(nx_batch_builder_fixed(builder.get(), index, column.leaf, column.values.data(),
                                         column.values.size()));
        }
        std::fill(column.values.begin(), column.values.end(), scribble);
    }
    nx_batch *batch = nullptr;
    check(nx_batch_builder_finish(builder.get(), &batch));
    return Batch(batch);
}

size_t batch_cells(const nx_batch *batch, size_t column, size_t level) {
    size_t cells = 0;
    check(nx_batch_cells(batch, column, level, &cells));
    return cells;
}

// One column as the binding copied it out.
struct ColumnView {
    std::vector<uint8_t> states;
    // The offsets of a LIST level, or empty for a FIXED_LIST level with its length.
    std::vector<std::vector<uint64_t>> list_offsets;
    std::vector<uint32_t> list_lengths;
    nx_type leaf_type = NX_TYPE_U8;
    std::vector<uint8_t> values;
    std::vector<uint64_t> value_offsets;

    // Reads one column in one call per level of its type.
    ColumnView(const nx_batch *batch, size_t index, const ReportedField &field) {
        size_t rows = batch_cells(batch, index, 0);
        states.resize(rows);
        check(nx_batch_states(batch, index, states.data(), states.size()));
        size_t innermost = field.levels.size() - 1;
        for (size_t level = 0; level < innermost; level++) {
            std::vector<uint64_t> offsets;
            if (field.levels[level].type == NX_TYPE_LIST) {
                offsets.resize(batch_cells(batch, index, level) + 1);
                check(nx_batch_offsets(batch, index, level, offsets.data(), offsets.size()));
            }
            list_offsets.push_back(offsets);
            list_lengths.push_back(field.levels[level].length);
        }
        leaf_type = field.levels[innermost].type;
        size_t cells = batch_cells(batch, index, innermost);
        if (leaf_type == NX_TYPE_STRING || leaf_type == NX_TYPE_BYTES) {
            value_offsets.resize(cells + 1);
            size_t data_len = 0;
            check(nx_batch_varlen(batch, index, innermost, value_offsets.data(),
                                  value_offsets.size(), nullptr, 0, &data_len));
            values.resize(data_len);
            check(nx_batch_varlen(batch, index, innermost, value_offsets.data(),
                                  value_offsets.size(), values.data(), values.size(), &data_len));
        } else {
            values.resize(cells * fixed_width(leaf_type));
            check(nx_batch_fixed(batch, index, innermost, values.data(), values.size()));
        }
    }

    std::string leaf(size_t index) const {
        char buffer[64];
        if (leaf_type == NX_TYPE_STRING || leaf_type == NX_TYPE_BYTES) {
            std::string_view value(reinterpret_cast<const char *>(values.data()) + value_offsets[index],
                                   value_offsets[index + 1] - value_offsets[index]);
            return std::string(leaf_type == NX_TYPE_STRING ? "str:" : "bytes:") + hex(value);
        }
        switch (leaf_type) {
        case NX_TYPE_U8: std::snprintf(buffer, sizeof buffer, "u8:%" PRIu8, value_at<uint8_t>(values, index)); break;
        case NX_TYPE_I8: std::snprintf(buffer, sizeof buffer, "i8:%" PRId8, value_at<int8_t>(values, index)); break;
        case NX_TYPE_BOOL: std::snprintf(buffer, sizeof buffer, "bool:%s", value_at<uint8_t>(values, index) == 1 ? "true" : "false"); break;
        case NX_TYPE_U16: std::snprintf(buffer, sizeof buffer, "u16:%" PRIu16, value_at<uint16_t>(values, index)); break;
        case NX_TYPE_I16: std::snprintf(buffer, sizeof buffer, "i16:%" PRId16, value_at<int16_t>(values, index)); break;
        case NX_TYPE_U32: std::snprintf(buffer, sizeof buffer, "u32:%" PRIu32, value_at<uint32_t>(values, index)); break;
        case NX_TYPE_I32: std::snprintf(buffer, sizeof buffer, "i32:%" PRId32, value_at<int32_t>(values, index)); break;
        case NX_TYPE_F32: std::snprintf(buffer, sizeof buffer, "f32:%08" PRIx32, value_at<uint32_t>(values, index)); break;
        case NX_TYPE_U64: std::snprintf(buffer, sizeof buffer, "u64:%" PRIu64, value_at<uint64_t>(values, index)); break;
        case NX_TYPE_I64: std::snprintf(buffer, sizeof buffer, "i64:%" PRId64, value_at<int64_t>(values, index)); break;
        case NX_TYPE_F64: std::snprintf(buffer, sizeof buffer, "f64:%016" PRIx64, value_at<uint64_t>(values, index)); break;
        case NX_TYPE_DATETIME: std::snprintf(buffer, sizeof buffer, "datetime:%" PRId64, value_at<int64_t>(values, index)); break;
        default: throw std::runtime_error("a leaf is not a scalar");
        }
        return buffer;
    }

    std::string cell(size_t level, size_t index) const {
        if (level == list_offsets.size()) {
            return leaf(index);
        }
        size_t start = index * list_lengths[level];
        size_t end = start + list_lengths[level];
        if (!list_offsets[level].empty()) {
            start = list_offsets[level][index];
            end = list_offsets[level][index + 1];
        }
        std::string text = "[";
        for (size_t element = start; element < end; element++) {
            if (element > start) {
                text += ",";
            }
            text += cell(level + 1, element);
        }
        return text + "]";
    }

    std::string row(size_t index) const {
        return states[index] == NX_CELL_NULL ? "null" : cell(0, index);
    }
};

// Every row of the batch rendered as a report line.
std::vector<std::string> batch_rows(const nx_batch *batch, const std::vector<ReportedField> &fields) {
    std::vector<ColumnView> columns;
    for (size_t index = 0; index < fields.size(); index++) {
        columns.emplace_back(batch, index, fields[index]);
    }
    std::vector<std::string> rows;
    for (size_t row = 0; row < nx_batch_row_count(batch); row++) {
        std::string line = "ROW";
        for (size_t column = 0; column < fields.size(); column++) {
            line += " " + fields[column].name + "=" + columns[column].row(row);
        }
        rows.push_back(line);
    }
    return rows;
}

std::vector<uint32_t> batch_ids(const nx_batch *batch, const std::vector<ReportedField> &fields) {
    for (size_t index = 0; index < fields.size(); index++) {
        if (fields[index].name == "id") {
            ColumnView view(batch, index, fields[index]);
            std::vector<uint32_t> ids;
            for (size_t row = 0; row < view.states.size(); row++) {
                ids.push_back(value_at<uint32_t>(view.values, row));
            }
            return ids;
        }
    }
    throw std::runtime_error("the batch has no id column");
}

std::string outcome_text(const nx_submission_outcome *outcome) {
    switch (nx_submission_outcome_result(outcome)) {
    case NX_SUBMISSION_COMPLETED:
        return "completed";
    case NX_SUBMISSION_NOT_ADMITTED: {
        nx_submission_refusal refusal = NX_REFUSAL_BUSY;
        check(nx_submission_outcome_refusal(outcome, &refusal));
        static const char *const refusals[] = {"", "invalid_batch", "suspended", "busy",
                                               "draining", "producer_ended", "credit_exceeded"};
        std::string text = std::string("not_admitted ") + refusals[refusal];
        if (refusal == NX_REFUSAL_INVALID_BATCH) {
            nx_batch_defect defect = NX_DEFECT_MALFORMED;
            check(nx_submission_outcome_defect(outcome, &defect));
            static const char *const defects[] = {"",
                                                  "malformed",
                                                  "unexpected_message",
                                                  "compressed",
                                                  "schema_mismatch",
                                                  "not_one_batch",
                                                  "too_many_rows",
                                                  "too_large",
                                                  "invalid_data"};
            text += std::string(" ") + defects[defect];
        }
        return text;
    }
    case NX_SUBMISSION_PROCESSING_FAILED: {
        nx_processing_failure failure = NX_FAILURE_ACK_TIMED_OUT;
        check(nx_submission_outcome_failure(outcome, &failure));
        return failure == NX_FAILURE_REJECTED ? "processing_failed rejected"
                                              : "processing_failed ack_timed_out";
    }
    case NX_SUBMISSION_OUTCOME_UNKNOWN: {
        nx_submission_uncertainty cause = NX_UNCERTAINTY_INTERRUPTED;
        check(nx_submission_outcome_uncertainty(outcome, &cause));
        static const char *const causes[] = {"", "interrupted", "owner_lost", "session_lost"};
        return std::string("outcome_unknown ") + causes[cause];
    }
    }
    throw std::runtime_error("unknown submission result");
}

std::string settlement_name(nx_settlement settlement) {
    static const char *const names[] = {"",
                                        "confirmed",
                                        "stale_reference",
                                        "wrong_consumer",
                                        "invalid_reason",
                                        "consumer_ended"};
    return names[settlement];
}

std::string state_name(nx_endpoint_state state) {
    static const char *const names[] = {"",           "active",          "interrupted",
                                        "restoring",  "reopen_required", "closed"};
    return names[state];
}

class Endpoints {
  public:
    Endpoints(const Session &session, std::string domain)
        : session_(session), domain_(std::move(domain)), ingestor_(env("NERVIX_PROBE_INGESTOR")),
          emitter_(env("NERVIX_PROBE_EMITTER")) {}

    void run();

  private:
    Producer open_producer(const nx_fields *fields, uint32_t batches, uint64_t bytes_asked) const {
        nx_producer *producer = nullptr;
        check(nx_session_open_ingestor(session_.get(), bytes(domain_), domain_.size(),
                                       bytes(ingestor_), ingestor_.size(), fields, batches,
                                       bytes_asked, nullptr, &producer));
        return Producer(producer);
    }

    Consumer open_consumer(const nx_fields *fields) const {
        nx_consumer *consumer = nullptr;
        check(nx_session_subscribe_emitter(session_.get(), bytes(domain_), domain_.size(),
                                           bytes(emitter_), emitter_.size(), fields,
                                           consumer_batches, endpoint_bytes, nullptr, &consumer));
        return Consumer(consumer);
    }

    static void expect_refused(nx_error *error, const std::string &what) {
        nx_open_refusal refusal = NX_OPEN_DOMAIN_NOT_FOUND;
        if (error == nullptr || nx_error_kind_of(error) != NX_ERROR_REJECTED ||
            !nx_error_open_refusal(error, &refusal) || refusal != NX_OPEN_SCHEMA_MISMATCH) {
            throw std::runtime_error("an open with another schema was not refused exactly");
        }
        nx_error_free(error);
        std::cout << "REFUSED " << what << " schema mismatch" << std::endl;
    }

    static uint64_t submit(const Producer &producer, const Batch &batch) {
        uint64_t submission = 0;
        check(nx_producer_submit(producer.get(), batch.get(), nullptr, &submission));
        return submission;
    }

    static void print_outcome(const Producer &producer, uint64_t submission) {
        Cancel deadline = with_deadline(wait_millis);
        nx_submission_outcome *raw = nullptr;
        check(nx_producer_rejoin(producer.get(), submission, deadline.get(), &raw));
        Submission outcome(raw);
        std::cout << "OUTCOME " << outcome_text(outcome.get()) << std::endl;
    }

    static Delivery next(const Consumer &consumer) {
        Cancel deadline = with_deadline(wait_millis);
        nx_delivery *delivery = nullptr;
        check(nx_consumer_next(consumer.get(), deadline.get(), &delivery));
        return Delivery(delivery);
    }

    static std::string ack(const nx_delivery *delivery) {
        Cancel deadline = with_deadline(wait_millis);
        nx_settlement settlement = NX_SETTLEMENT_CONSUMER_ENDED;
        check(nx_delivery_ack(delivery, deadline.get(), &settlement));
        return settlement_name(settlement);
    }

    static std::string identity(const nx_delivery *delivery) {
        const uint8_t *data = nullptr;
        size_t len = 0;
        nx_delivery_identity(delivery, &data, &len);
        return borrowed(data, len);
    }

    static std::string reference(const nx_delivery *delivery) {
        const uint8_t *data = nullptr;
        size_t len = 0;
        nx_delivery_reference(delivery, &data, &len);
        return borrowed(data, len);
    }

    static std::string stream(const nx_delivery *delivery) {
        const uint8_t *data = nullptr;
        size_t len = 0;
        nx_delivery_ipc(delivery, &data, &len);
        return borrowed(data, len);
    }

    static Batch batch_of(const nx_delivery *delivery) {
        nx_batch *batch = nullptr;
        check(nx_delivery_batch(delivery, &batch));
        return Batch(batch);
    }

    void expect(bool holds, const std::string &what) {
        if (!holds) {
            failures_ += what + "; ";
        }
    }

    const Session &session_;
    std::string domain_;
    std::string ingestor_;
    std::string emitter_;
    std::string failures_;
};

void Endpoints::run() {
    std::vector<FieldSpec> output_specs = input_fields();
    output_specs.push_back({"echo", {{NX_TYPE_U32, 0}}, false, false});
    std::vector<FieldSpec> mismatched_specs = input_fields();
    for (FieldSpec &spec : mismatched_specs) {
        if (spec.name == "secret") {
            spec.sensitive = false;
        }
    }
    Fields input = fields_of(input_fields());
    Fields output_fields = fields_of(output_specs);
    Fields mismatched = fields_of(mismatched_specs);

    // An open whose expected fields differ from the endpoint's is refused exactly.
    nx_producer *refused_producer = nullptr;
    expect_refused(nx_session_open_ingestor(session_.get(), bytes(domain_), domain_.size(),
                                            bytes(ingestor_), ingestor_.size(), mismatched.get(),
                                            producer_batches, endpoint_bytes, nullptr,
                                            &refused_producer),
                   "producer");
    nx_consumer *refused_consumer = nullptr;
    expect_refused(nx_session_subscribe_emitter(session_.get(), bytes(domain_), domain_.size(),
                                                bytes(emitter_), emitter_.size(), input.get(),
                                                consumer_batches, endpoint_bytes, nullptr,
                                                &refused_consumer),
                   "consumer");

    Consumer consumer = open_consumer(output_fields.get());
    Producer producer = open_producer(input.get(), producer_batches, endpoint_bytes);
    nx_schema *raw_schema = nullptr;
    check(nx_consumer_schema(consumer.get(), &raw_schema));
    Schema output_schema(raw_schema);
    check(nx_producer_schema(producer.get(), &raw_schema));
    Schema input_schema(raw_schema);
    std::vector<ReportedField> output = reported_fields(output_schema.get());
    std::vector<ReportedField> input_reported = reported_fields(input_schema.get());

    nx_ack_window window = NX_ACK_PARALLEL;
    uint64_t outstanding = 0;
    uint64_t ack_timeout = 0;
    uint64_t backoff = 0;
    uint64_t max_backoff = 0;
    uint32_t batches = 0;
    uint64_t granted = 0;
    uint32_t max_rows = 0;
    uint64_t max_bytes = 0;
    nx_consumer_policy(consumer.get(), &window, &outstanding, &ack_timeout, &backoff, &max_backoff);
    nx_consumer_grant(consumer.get(), &batches, &granted, &max_rows, &max_bytes);
    std::cout << "CONSUMER generation=" << nx_consumer_generation(consumer.get())
              << " state=" << state_name(nx_consumer_state(consumer.get()))
              << " window=" << (window == NX_ACK_SEQUENTIAL ? "sequential" : "parallel") << "/"
              << outstanding << " ack_timeout=" << ack_timeout << " credit=" << batches << "/"
              << granted << " max=" << max_rows << "/" << max_bytes << std::endl;
    for (const ReportedField &field : output) {
        std::cout << "OUTPUT FIELD " << field.line() << std::endl;
    }
    nx_producer_policy(producer.get(), &window, &outstanding, &ack_timeout, &backoff, &max_backoff);
    nx_producer_grant(producer.get(), &batches, &granted, &max_rows, &max_bytes);
    expect(max_rows > 0 && max_bytes > 0 && max_bytes <= granted,
           "the producer's batch limits fit its grant");
    std::cout << "PRODUCER generation=" << nx_producer_generation(producer.get())
              << " state=" << state_name(nx_producer_state(producer.get())) << " admission="
              << (nx_producer_admission(producer.get()) == NX_ADMISSION_OPEN ? "open" : "suspended")
              << " window=" << (window == NX_ACK_SEQUENTIAL ? "sequential" : "parallel") << "/"
              << outstanding << " ack_timeout=" << ack_timeout << " credit=" << batches << "/"
              << granted << std::endl;
    for (const ReportedField &field : input_reported) {
        std::cout << "INPUT FIELD " << field.line() << std::endl;
    }
    std::cout << "OPENED" << std::endl;

    // A wait for output that nothing produces ends by its deadline, and one cancelled from another
    // thread by its token; the reads they leave behind are the consumer's.
    expect(failure_kind([&] {
               Cancel expiring = with_deadline(expiring_millis);
               nx_delivery *nothing = nullptr;
               check(nx_consumer_next(consumer.get(), expiring.get(), &nothing));
           }) == NX_ERROR_DEADLINE,
           "an expired read reports its deadline");
    std::cout << "NEXT deadline" << std::endl;
    Cancel cancel(nx_cancel_new());
    nx_error_kind cancelled = NX_ERROR_INVALID_ARGUMENT;
    std::thread reader([&] {
        cancelled = failure_kind([&] {
            nx_delivery *nothing = nullptr;
            check(nx_consumer_next(consumer.get(), cancel.get(), &nothing));
        });
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    nx_cancel_trigger(cancel.get());
    reader.join();
    expect(cancelled == NX_ERROR_CANCELLED, "a cancelled read reports its cancellation");
    std::cout << "NEXT cancelled" << std::endl;

    // A batch built for another schema is refused before anything is sent, and the same batch
    // written as a stream by other tooling is refused by the server.
    std::vector<Row> rows = typed_rows();
    Batch other = build_batch(output_schema.get(), rows);
    expect(failure_kind([&] { submit(producer, other); }) == NX_ERROR_INVALID_ARGUMENT,
           "another schema is the host's argument");
    std::cout << "SUBMIT invalid argument" << std::endl;
    const uint8_t *foreign = nullptr;
    size_t foreign_len = 0;
    check(nx_batch_ipc(other.get(), &foreign, &foreign_len));
    uint64_t raw = 0;
    check(nx_producer_submit_ipc(producer.get(), foreign, foreign_len, nullptr, &raw));
    print_outcome(producer, raw);

    // The typed batch: its outcome waits for the application's acknowledgement.
    uint64_t first = submit(producer, build_batch(input_schema.get(), rows));
    std::cout << "SUBMITTED first" << std::endl;
    Delivery delivery = next(consumer);
    const uint8_t *relay = nullptr;
    size_t relay_len = 0;
    const uint8_t *fingerprint = nullptr;
    size_t fingerprint_len = 0;
    nx_delivery_source_relay(delivery.get(), &relay, &relay_len);
    bool branched = nx_delivery_branch_fingerprint(delivery.get(), &fingerprint, &fingerprint_len);
    std::cout << "DELIVERY relay=" << borrowed(relay, relay_len)
              << " members=" << nx_delivery_members(delivery.get())
              << " branch=" << (branched ? std::to_string(fingerprint_len) : "none")
              << " identity=" << identity(delivery.get()).size()
              << " reference=" << reference(delivery.get()).size() << std::endl;
    uint64_t pending_ids[4] = {0};
    bool pending_resolved[4] = {false};
    size_t pending_count = 0;
    check(nx_producer_pending(producer.get(), pending_ids, pending_resolved, 4, &pending_count));
    expect(pending_count == 1 && pending_ids[0] == first && !pending_resolved[0],
           "the only submission waits for its outcome");
    expect(failure_kind([&] {
               Cancel expiring = with_deadline(expiring_millis);
               nx_submission_outcome *early = nullptr;
               check(nx_producer_rejoin(producer.get(), first, expiring.get(), &early));
           }) == NX_ERROR_DEADLINE,
           "an unacknowledged submission has no outcome");
    std::cout << "PENDING first unresolved" << std::endl;
    Batch delivered = batch_of(delivery.get());
    std::vector<std::string> printed = batch_rows(delivered.get(), output);
    for (const std::string &line : printed) {
        std::cout << line << std::endl;
    }
    const uint8_t *batch_stream = nullptr;
    size_t batch_stream_len = 0;
    check(nx_batch_ipc(delivered.get(), &batch_stream, &batch_stream_len));
    expect(borrowed(batch_stream, batch_stream_len) == stream(delivery.get()),
           "the batch borrows the stream its delivery carried");

    // A retried attempt comes back with the same identity and a new reference; the first
    // reference is stale from then on.
    Cancel deadline = with_deadline(wait_millis);
    nx_settlement settlement = NX_SETTLEMENT_CONSUMER_ENDED;
    check(nx_delivery_retry(delivery.get(), deadline.get(), &settlement));
    std::cout << "RETRY " << settlement_name(settlement) << std::endl;
    Delivery again = next(consumer);
    expect(identity(again.get()) == identity(delivery.get()), "a retry keeps the identity");
    expect(reference(again.get()) != reference(delivery.get()), "a retry makes a new reference");
    std::cout << "REDELIVERED same identity new reference" << std::endl;
    std::cout << "ACK " << ack(delivery.get()) << std::endl;
    delivery.reset();

    // A reference retained here outlives the first, released on another thread, and reads and
    // settles the same attempt.
    Delivery retained(nx_delivery_retain(again.get()));
    Batch retained_batch = batch_of(again.get());
    std::string stream_before = stream(retained.get());
    std::thread releaser([moved = std::move(again)]() mutable { moved.reset(); });
    releaser.join();
    expect(stream(retained.get()) == stream_before, "a retained delivery keeps its stream");
    expect(batch_rows(retained_batch.get(), output) == printed,
           "a retained batch reads the same rows");
    std::cout << "ACK " << ack(retained.get()) << std::endl;
    retained.reset();
    retained_batch.reset();
    print_outcome(producer, first);

    // An application rejection finishes the batch through the emitter's message error policy.
    uint64_t second = submit(producer, build_batch(input_schema.get(), {plain_row(10, "beta")}));
    std::cout << "SUBMITTED second" << std::endl;
    Delivery rejected = next(consumer);
    const std::string reason = "application refused";
    deadline = with_deadline(wait_millis);
    check(nx_delivery_reject(rejected.get(), bytes(reason), reason.size(), deadline.get(),
                             &settlement));
    std::cout << "REJECT " << settlement_name(settlement) << std::endl;
    rejected.reset();
    print_outcome(producer, second);

    // Two outstanding batches use up the producer's credit, so a third waits for an outcome.
    Batch fifth_batch =
        build_batch(input_schema.get(), {plain_row(24, "acme"), plain_row(25, "acme")});
    uint64_t third =
        submit(producer, build_batch(input_schema.get(), {plain_row(20, "acme"), plain_row(21, "acme")}));
    std::cout << "SUBMITTED third" << std::endl;
    uint64_t fourth =
        submit(producer, build_batch(input_schema.get(), {plain_row(22, "acme"), plain_row(23, "acme")}));
    std::cout << "SUBMITTED fourth" << std::endl;
    expect(failure_kind([&] {
               Cancel expiring = with_deadline(expiring_millis);
               uint64_t beyond = 0;
               check(nx_producer_submit(producer.get(), fifth_batch.get(), expiring.get(), &beyond));
           }) == NX_ERROR_DEADLINE,
           "a batch beyond the credit waits");
    std::cout << "SUBMIT deadline" << std::endl;
    // A batch refused as busy is sent again after the ingestor's backoff, behind the batch
    // submitted after it, so the two outputs may arrive in either order; each keeps its own rows.
    std::vector<std::vector<uint32_t>> outputs;
    for (int index = 0; index < 2; index++) {
        Delivery output_delivery = next(consumer);
        outputs.push_back(batch_ids(batch_of(output_delivery.get()).get(), output));
        std::cout << "ACK " << ack(output_delivery.get()) << std::endl;
    }
    std::sort(outputs.begin(), outputs.end());
    expect(outputs == std::vector<std::vector<uint32_t>>{{20, 21}, {22, 23}},
           "a multiple-row batch keeps its rows");
    print_outcome(producer, third);
    print_outcome(producer, fourth);
    uint64_t fifth = submit(producer, fifth_batch);
    std::cout << "SUBMITTED fifth" << std::endl;
    {
        Delivery fifth_output = next(consumer);
        expect(batch_ids(batch_of(fifth_output.get()).get(), output) == std::vector<uint32_t>{24, 25},
               "a multiple-row batch keeps its rows");
        std::cout << "ACK " << ack(fifth_output.get()) << std::endl;
    }
    print_outcome(producer, fifth);

    // The scenario cuts the session while one delivery is held unacknowledged.
    Producer extra = open_producer(input.get(), 1, 65536);
    std::cout << "PRODUCER extra opened" << std::endl;
    uint64_t held = submit(producer, build_batch(input_schema.get(), {plain_row(30, "gamma")}));
    std::cout << "SUBMITTED held" << std::endl;
    Delivery held_delivery = next(consumer);
    std::string held_identity = identity(held_delivery.get());
    std::cout << "HOLDING" << std::endl;
    expect(failure_kind([&] {
               Cancel waiting = with_deadline(wait_millis);
               nx_delivery *nothing = nullptr;
               check(nx_consumer_next(consumer.get(), waiting.get(), &nothing));
           }) == NX_ERROR_INTERRUPTED,
           "a lost session interrupts the consumer");
    std::cout << "NEXT interrupted" << std::endl;
    expect(failure_kind([&] { ack(held_delivery.get()); }) == NX_ERROR_REJECTED,
           "a delivery of a lost session expired");
    std::cout << "ACK expired" << std::endl;
    held_delivery.reset();
    print_outcome(producer, held);
    auto waited_until = std::chrono::steady_clock::now() + std::chrono::milliseconds(wait_millis);
    while (nx_producer_state(extra.get()) == NX_ENDPOINT_ACTIVE) {
        if (std::chrono::steady_clock::now() > waited_until) {
            throw std::runtime_error("the extra producer stayed active after its session ended");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    deadline = with_deadline(wait_millis);
    check(nx_producer_close(extra.get(), deadline.get()));
    expect(nx_producer_state(extra.get()) == NX_ENDPOINT_CLOSED,
           "a producer closed during reconnect is closed");
    std::cout << "CLOSED extra producer" << std::endl;
    std::cout << "WAITING restore" << std::endl;

    // The restored consumer receives the held batch again, and the restored producer publishes.
    Delivery redelivered = next(consumer);
    expect(identity(redelivered.get()) == held_identity, "the held batch keeps its identity");
    std::cout << "REDELIVERED held same identity" << std::endl;
    std::cout << "ACK " << ack(redelivered.get()) << std::endl;
    redelivered.reset();
    uint64_t sixth = submit(producer, build_batch(input_schema.get(), {plain_row(40, "gamma")}));
    std::cout << "SUBMITTED sixth" << std::endl;
    {
        Delivery sixth_output = next(consumer);
        std::cout << "ACK " << ack(sixth_output.get()) << std::endl;
    }
    print_outcome(producer, sixth);
    std::cout << "STATE producer=" << state_name(nx_producer_state(producer.get()))
              << " extra=" << state_name(nx_producer_state(extra.get()))
              << " consumer=" << state_name(nx_consumer_state(consumer.get())) << std::endl;
    deadline = with_deadline(wait_millis);
    check(nx_consumer_close(consumer.get(), deadline.get()));
    check(nx_producer_close(producer.get(), deadline.get()));
    expect(nx_consumer_state(consumer.get()) == NX_ENDPOINT_CLOSED &&
               nx_producer_state(producer.get()) == NX_ENDPOINT_CLOSED,
           "closed handles read closed");
    std::cout << "CLOSED completed" << std::endl;
    if (!failures_.empty()) {
        throw std::runtime_error("the probe's checks failed: " + failures_);
    }
    std::cout << "CHECKS ok" << std::endl;
    std::cout << "PASS" << std::endl;
}

void run(const std::string &mode) {
    const std::string server = env("NERVIX_PROBE_GRPC_URI");
    const std::string username = env("NERVIX_PROBE_USERNAME");
    const std::string password = env("NERVIX_PROBE_PASSWORD");
    const std::string domain = env("NERVIX_PROBE_DOMAIN");

    nx_session *raw_session = nullptr;
    check(nx_session_connect(bytes(server), server.size(), bytes(domain), domain.size(),
                             bytes(username), username.size(), bytes(password), password.size(),
                             nullptr, &raw_session));
    Session session(raw_session);
    if (mode == "clock") {
        run_clock(session, domain);
        return;
    }
    if (mode == "io") {
        Endpoints(session, domain).run();
        return;
    }

    const std::string relay = env("NERVIX_PROBE_RELAY");
    const std::string subscription = env("NERVIX_PROBE_SUBSCRIPTION");
    const size_t expected_rows = std::stoul(env("NERVIX_PROBE_ROWS"));

    Outcome operation = execute(session, "SHOW CREATE RELAY " + relay + ";", nullptr);
    std::cout << "OPERATION " << disposition_name(nx_outcome_disposition(operation.get()))
              << std::endl;

    Outcome failed = execute(session, "CREATE RELAY;", nullptr);
    size_t diagnostics = nx_outcome_diagnostic_count(failed.get());
    std::string span = "none";
    if (diagnostics > 0) {
        const uint8_t *message = nullptr;
        size_t message_len = 0;
        bool has_span = false;
        uint32_t start = 0;
        uint32_t end = 0;
        check(nx_outcome_diagnostic(failed.get(), 0, &message, &message_len, &has_span, &start,
                                    &end));
        if (has_span) {
            span = std::to_string(start) + ".." + std::to_string(end);
        }
    }
    std::cout << "ERROR " << disposition_name(nx_outcome_disposition(failed.get()))
              << " diagnostics=" << diagnostics << " span=" << span << std::endl;

    Outcome opened =
        execute(session, "CREATE SUBSCRIPTION " + subscription + " TO " + relay + ";", nullptr);
    const uint8_t *opened_name = nullptr;
    size_t opened_name_len = 0;
    uint64_t generation = 0;
    if (!nx_outcome_subscription(opened.get(), &opened_name, &opened_name_len, &generation) ||
        generation == 0) {
        throw std::runtime_error("the subscribe command opened no subscription");
    }
    nx_schema *raw_schema = nullptr;
    check(nx_outcome_schema(opened.get(), &raw_schema));
    Schema schema(raw_schema);
    std::vector<Field> fields = fields_of(schema.get(), NX_PART_ROWS);
    std::vector<Field> key_fields = fields_of(schema.get(), NX_PART_BRANCH_KEY);
    for (const Field &field : fields) {
        std::cout << field.line("FIELD") << std::endl;
    }
    const uint8_t *branch = nullptr;
    size_t branch_len = 0;
    if (nx_schema_branch(schema.get(), &branch, &branch_len)) {
        std::cout << "BRANCH " << borrowed(branch, branch_len) << std::endl;
    }
    for (const Field &field : key_fields) {
        std::cout << field.line("KEY") << std::endl;
    }
    std::cout << "SUBSCRIBED" << std::endl;

    Cancel rows_deadline = with_deadline(120000);
    std::vector<Event> retained;
    std::vector<std::vector<std::string>> reported;
    size_t seen = 0;
    while (seen < expected_rows) {
        nx_event *raw_event = nullptr;
        check(nx_session_next_event(session.get(), rows_deadline.get(), &raw_event));
        Event event(raw_event);
        if (nx_event_kind_of(event.get()) != NX_EVENT_ROWS) {
            throw std::runtime_error("the subscription reported something other than rows first");
        }
        const uint8_t *name = nullptr;
        size_t name_len = 0;
        uint64_t event_generation = 0;
        nx_event_subscription(event.get(), &name, &name_len, &event_generation);
        if (borrowed(name, name_len) != subscription || event_generation != generation) {
            throw std::runtime_error("rows arrived for another subscription");
        }
        std::vector<std::string> lines = row_lines(event.get(), fields, key_fields);
        for (const std::string &line : lines) {
            std::cout << line << std::endl;
        }
        seen += lines.size();
        reported.push_back(lines);
        // Keep a second reference and let the first go with `event`.
        retained.emplace_back(nx_event_retain(event.get()));
    }

    for (size_t index = 0; index < retained.size(); ++index) {
        if (row_lines(retained[index].get(), fields, key_fields) != reported[index]) {
            throw std::runtime_error("a retained event reads differently than it did");
        }
    }
    std::thread releaser([moved = std::move(retained)]() mutable { moved.clear(); });
    releaser.join();
    check_cancellation(session);
    std::cout << "CHECKS ok" << std::endl;

    Outcome closed = execute(session, "DELETE SUBSCRIPTION " + subscription + ";", nullptr);
    std::cout << "CLOSED " << disposition_name(nx_outcome_disposition(closed.get())) << std::endl;
    std::cout << "PASS" << std::endl;
}

} // namespace

int main(int argc, char **argv) {
    try {
        run(argc > 1 ? argv[1] : "");
    } catch (const std::exception &error) {
        std::cerr << "probe failed: " << error.what() << std::endl;
        return 1;
    }
    return 0;
}
