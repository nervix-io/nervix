// The C++ probe of the shared Rust binding.
//
// It owns every handle through std::unique_ptr with the binding's release function, so no path
// leaks or double-releases a handle, and turns every returned error into an exception. It prints
// the same conformance report as every other probe. Run as `cpp-probe clock`, it attaches to the
// domain's running clock instead, reads the clock the attach reported before its first tick,
// follows the generation a STOP and START begin and the attachment restored after its session
// ends, and detaches.

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

void run(bool clock) {
    const std::string server = env("NERVIX_PROBE_GRPC_URI");
    const std::string username = env("NERVIX_PROBE_USERNAME");
    const std::string password = env("NERVIX_PROBE_PASSWORD");
    const std::string domain = env("NERVIX_PROBE_DOMAIN");

    nx_session *raw_session = nullptr;
    check(nx_session_connect(bytes(server), server.size(), bytes(domain), domain.size(),
                             bytes(username), username.size(), bytes(password), password.size(),
                             nullptr, &raw_session));
    Session session(raw_session);
    if (clock) {
        run_clock(session, domain);
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
        run(argc > 1 && std::string_view(argv[1]) == "clock");
    } catch (const std::exception &error) {
        std::cerr << "probe failed: " << error.what() << std::endl;
        return 1;
    }
    return 0;
}
