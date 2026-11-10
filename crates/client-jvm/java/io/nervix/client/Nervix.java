package io.nervix.client;

import java.lang.ref.Cleaner;
import java.lang.ref.Reference;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

/**
 * Java 21 client for Nervix, using the shared Rust session runtime through JNI.
 * Close handles promptly; Cleaner provides fallback cleanup. Close releases ownership; endpoint
 * end(Cancel) explicitly waits for the server to close the attachment. No Java monitor is held
 * while a host operation waits. Cancellation tokens may be triggered from another Java thread.
 * Unsigned 64-bit identities, counts, offsets and durations retain their bits in long; use
 * Long.compareUnsigned and Long.toUnsignedString. Fixed-width column bytes use native byte order.
 * Completion cursor and edit ranges are UTF-8 byte offsets, not Java UTF-16 indices.
 */
public final class Nervix {
    private Nervix() {}
    private static final Cleaner CLEANER = Cleaner.create();

    private interface Coded { int code(); }
    private static <T extends Enum<T> & Coded> T decode(T[] values, int code) {
        if (code > 0 && code <= values.length) {
            T value = values[code - 1];
            if (value.code() == code) return value;
        }
        throw new IllegalArgumentException("unknown native vocabulary code " + code);
    }
    private static <T extends Enum<T> & Coded> T optional(T[] values, int code) {
        return code == 0 ? null : decode(values, code);
    }
    /** State byte values returned by bulk column reads. */
    public enum CellState implements Coded {
        VALUE(1), NULL(2), REDACTED(3);
        private final int code;
        CellState(int code) { this.code = code; }
        public int code() { return code; }
        public static CellState fromCode(byte code) { return decode(values(), Byte.toUnsignedInt(code)); }
    }
    public enum TlsRequirement implements Coded {
        PREFERRED(1), REQUIRED(2);
        private final int code;
        TlsRequirement(int code) { this.code = code; }
        public int code() { return code; }
    }
    public enum FieldType implements Coded {
        U8(1), I8(2), U16(3), I16(4), U32(5), I32(6), U64(7), I64(8), F32(9), F64(10), BOOL(11), STRING(12), BYTES(13), DATETIME(14), FIXED_LIST(15), LIST(16);
        private final int code;
        FieldType(int code) { this.code = code; }
        public int code() { return code; }
        static FieldType of(int code) { return decode(values(), code); }
    }
    public enum Part implements Coded {
        ROWS(1), BRANCH(2);
        private final int code;
        Part(int code) { this.code = code; }
        public int code() { return code; }
        static Part of(int code) { return decode(values(), code); }
    }
    public enum Disposition implements Coded {
        COMPLETED(1), FAILED(2), NOT_LEADER(3), TRANSACTION_DETACHED(4), TRANSACTION_TAKEN_OVER(5), OUTCOME_UNKNOWN(6), EXECUTION_REFERENCE_CONFLICT(7), EXECUTION_REFERENCE_EXPIRED(8), PREVIEW_STALE(9);
        private final int code;
        Disposition(int code) { this.code = code; }
        public int code() { return code; }
        static Disposition of(int code) { return decode(values(), code); }
    }
    /** Why a resource upload failed. */
    public enum UploadFailure implements Coded {
        INVALID_STREAM(1), RESOURCE_NOT_DECLARED(2), SIZE_MISMATCH(3), QUOTA_EXCEEDED(4), INSTALLATION_FAILED(5), IDENTITY_CONFLICT(6);
        private final int code;
        UploadFailure(int code) { this.code = code; }
        public int code() { return code; }
    }
    public enum EventKind implements Coded {
        ROWS(1), DELIVERY_LOST(2), ROWS_SKIPPED(3), ENDED(4), INTERRUPTED(5), CONSUMER_OVERFLOW(6), RESTORATION_FAILED(7);
        private final int code;
        EventKind(int code) { this.code = code; }
        public int code() { return code; }
        static EventKind of(int code) { return decode(values(), code); }
    }
    /** A typed refusal to read graph state; an empty page has no refusal. */
    public enum InspectionRejection implements Coded {
        UNKNOWN_ENTITY(1), NOT_MATERIALIZED(2), INVALID_SELECTION(3), INVALID_LIMIT(4), STALE_CONTINUATION(5), OWNER_UNAVAILABLE(6), NOT_READY(7), DEADLINE(8), REPLY_TOO_LARGE(9);
        private final int code;
        InspectionRejection(int code) { this.code = code; }
        public int code() { return code; }
    }
    public enum InspectionStage implements Coded {
        LIVE(1), STORED(2);
        private final int code;
        InspectionStage(int code) { this.code = code; }
        public int code() { return code; }
        static InspectionStage of(int code) { return decode(values(), code); }
    }
    public enum InspectionUnitKind implements Coded {
        SINGLE(1), PARTITION(2), INSTANCE(3);
        private final int code;
        InspectionUnitKind(int code) { this.code = code; }
        public int code() { return code; }
        static InspectionUnitKind of(int code) { return decode(values(), code); }
    }
    /** Complete selected-state counts, including rows omitted from this page; longs retain unsigned bits. */
    public record InspectionCounts(long branches, long rows, long bytes) {}
    public enum ClockEventKind implements Coded {
        STATE(1), TICK(2), ENDED(3), INTERRUPTED(4), RESTORATION_FAILED(5);
        private final int code;
        ClockEventKind(int code) { this.code = code; }
        public int code() { return code; }
        static ClockEventKind of(int code) { return decode(values(), code); }
    }
    public enum ClockState implements Coded {
        STOPPED(1), UNINSTALLED(2), UNPACED(3), PACED(4);
        private final int code;
        ClockState(int code) { this.code = code; }
        public int code() { return code; }
        static ClockState of(int code) { return decode(values(), code); }
    }
    public enum EndReason implements Coded {
        RELAY_REMOVED(1), RELAY_CHANGED(2);
        private final int code;
        EndReason(int code) { this.code = code; }
        public int code() { return code; }
        static EndReason of(int code) { return decode(values(), code); }
    }
    public enum RestorationCause implements Coded {
        IN_TRANSACTION(1), INVALID_STATEMENT(2), NAME_IN_USE(3), INVALID_FILTER(4), UNAVAILABLE(5), REJECTED(6), FAILED(7);
        private final int code;
        RestorationCause(int code) { this.code = code; }
        public int code() { return code; }
        static RestorationCause of(int code) { return decode(values(), code); }
    }
    public enum ClockEndReason implements Coded {
        DOMAIN_REMOVED(1);
        private final int code;
        ClockEndReason(int code) { this.code = code; }
        public int code() { return code; }
        static ClockEndReason of(int code) { return decode(values(), code); }
    }
    public enum EndpointState implements Coded {
        ACTIVE(1), INTERRUPTED(2), RESTORING(3), REOPEN_REQUIRED(4), CLOSED(5);
        private final int code;
        EndpointState(int code) { this.code = code; }
        public int code() { return code; }
        static EndpointState of(int code) { return decode(values(), code); }
    }
    public enum Admission implements Coded {
        OPEN(1), SUSPENDED(2);
        private final int code;
        Admission(int code) { this.code = code; }
        public int code() { return code; }
        static Admission of(int code) { return decode(values(), code); }
    }
    public enum WindowKind implements Coded {
        SEQUENTIAL(1), PARALLEL(2);
        private final int code;
        WindowKind(int code) { this.code = code; }
        public int code() { return code; }
        static WindowKind of(int code) { return decode(values(), code); }
    }
    public enum Settlement implements Coded {
        CONFIRMED(1), STALE_REFERENCE(2), WRONG_CONSUMER(3), INVALID_REASON(4), CONSUMER_ENDED(5);
        private final int code;
        Settlement(int code) { this.code = code; }
        public int code() { return code; }
        static Settlement of(int code) { return decode(values(), code); }
    }
    public enum OpenRefusal implements Coded {
        DOMAIN_NOT_FOUND(1), DOMAIN_STOPPED(2), ENDPOINT_NOT_FOUND(3), NOT_CLIENT_ENDPOINT(4), ENDPOINT_UNAVAILABLE(5), SCHEMA_MISMATCH(6), TOO_MANY_ENDPOINTS(7), SESSION_CAPACITY_EXHAUSTED(8), NODE_CAPACITY_EXHAUSTED(9), INVALID_LIMITS(10), IN_TRANSACTION(11);
        private final int code;
        OpenRefusal(int code) { this.code = code; }
        public int code() { return code; }
        static OpenRefusal of(int code) { return decode(values(), code); }
    }
    public enum ReopenReason implements Coded {
        DOMAIN_STOPPED(1), ENDPOINT_REMOVED(2), SCHEMA_CHANGED(3), CONTRACT_CHANGED(4), GENERATION_CHANGED(5), PROTOCOL_VIOLATED(6), REFUSED(7);
        private final int code;
        ReopenReason(int code) { this.code = code; }
        public int code() { return code; }
        static ReopenReason of(int code) { return decode(values(), code); }
    }
    public enum SubmissionResult implements Coded {
        NOT_ADMITTED(1), COMPLETED(2), PROCESSING_FAILED(3), OUTCOME_UNKNOWN(4);
        private final int code;
        SubmissionResult(int code) { this.code = code; }
        public int code() { return code; }
        static SubmissionResult of(int code) { return decode(values(), code); }
    }
    public enum SubmissionRefusal implements Coded {
        INVALID_BATCH(1), SUSPENDED(2), BUSY(3), DRAINING(4), PRODUCER_ENDED(5), CREDIT_EXCEEDED(6);
        private final int code;
        SubmissionRefusal(int code) { this.code = code; }
        public int code() { return code; }
        static SubmissionRefusal of(int code) { return decode(values(), code); }
    }
    public enum BatchDefect implements Coded {
        MALFORMED(1), UNEXPECTED_MESSAGE(2), COMPRESSED(3), SCHEMA_MISMATCH(4), NOT_ONE_BATCH(5), TOO_MANY_ROWS(6), TOO_LARGE(7), INVALID_DATA(8);
        private final int code;
        BatchDefect(int code) { this.code = code; }
        public int code() { return code; }
        static BatchDefect of(int code) { return decode(values(), code); }
    }
    public enum ProcessingFailure implements Coded {
        ACK_TIMED_OUT(1), REJECTED(2);
        private final int code;
        ProcessingFailure(int code) { this.code = code; }
        public int code() { return code; }
        static ProcessingFailure of(int code) { return decode(values(), code); }
    }
    public enum Uncertainty implements Coded {
        INTERRUPTED(1), OWNER_LOST(2), SESSION_LOST(3);
        private final int code;
        Uncertainty(int code) { this.code = code; }
        public int code() { return code; }
        static Uncertainty of(int code) { return decode(values(), code); }
    }
    public enum CompletionStatus implements Coded {
        READY(1), MISSING_CONTEXT(2), STALE_CONTEXT(3), LOOKUP_FAILED(4);
        private final int code;
        CompletionStatus(int code) { this.code = code; }
        public int code() { return code; }
        static CompletionStatus of(int code) { return decode(values(), code); }
    }
    public enum CompletionKind implements Coded {
        TEXT(1), LOCAL_DIRECTORY_LOOKUP(2);
        private final int code;
        CompletionKind(int code) { this.code = code; }
        public int code() { return code; }
        static CompletionKind of(int code) { return decode(values(), code); }
    }
    public enum FailureKind implements Coded {
        INVALID_ARGUMENT(1), CONNECT(2), TRANSPORT(3), UNCERTAIN(4), REJECTED(5), DEADLINE(6), CANCELLED(7), OVERFLOW(8), PROTOCOL(9), TYPE(10), CLOSED(11), INTERRUPTED(12), REOPEN_REQUIRED(13);
        private final int code;
        FailureKind(int code) { this.code = code; }
        public int code() { return code; }
        static FailureKind of(int code) { return decode(values(), code); }
    }

    /** Base of classified host failures; command disposition remains an Outcome value. */
    public static class ClientException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final FailureKind kind;
        private final String executionReference;
        private final OpenRefusal openRefusal;
        ClientException(FailureKind kind, String message, String reference, int refusal) {
            super(message); this.kind = kind; this.executionReference = reference;
            this.openRefusal = optional(OpenRefusal.values(), refusal);
        }
        public FailureKind kind() { return kind; }
        public String executionReference() { return executionReference; }
        public OpenRefusal openRefusal() { return openRefusal; }
    }
    public static final class InvalidArgumentException extends ClientException {
        private static final long serialVersionUID = 1L;
        InvalidArgumentException(String message, String reference, int refusal) {
            super(FailureKind.INVALID_ARGUMENT, message, reference, refusal);
        }
    }
    public static final class ConnectException extends ClientException {
        private static final long serialVersionUID = 1L;
        ConnectException(String message, String reference, int refusal) {
            super(FailureKind.CONNECT, message, reference, refusal);
        }
    }
    public static final class TransportException extends ClientException {
        private static final long serialVersionUID = 1L;
        TransportException(String message, String reference, int refusal) {
            super(FailureKind.TRANSPORT, message, reference, refusal);
        }
    }
    public static final class UncertainOutcomeException extends ClientException {
        private static final long serialVersionUID = 1L;
        UncertainOutcomeException(String message, String reference, int refusal) {
            super(FailureKind.UNCERTAIN, message, reference, refusal);
        }
    }
    public static final class RejectedException extends ClientException {
        private static final long serialVersionUID = 1L;
        RejectedException(String message, String reference, int refusal) {
            super(FailureKind.REJECTED, message, reference, refusal);
        }
    }
    public static final class DeadlineException extends ClientException {
        private static final long serialVersionUID = 1L;
        DeadlineException(String message, String reference, int refusal) {
            super(FailureKind.DEADLINE, message, reference, refusal);
        }
    }
    public static final class CancelledException extends ClientException {
        private static final long serialVersionUID = 1L;
        CancelledException(String message, String reference, int refusal) {
            super(FailureKind.CANCELLED, message, reference, refusal);
        }
    }
    public static final class EventOverflowException extends ClientException {
        private static final long serialVersionUID = 1L;
        EventOverflowException(String message, String reference, int refusal) {
            super(FailureKind.OVERFLOW, message, reference, refusal);
        }
    }
    public static final class ProtocolException extends ClientException {
        private static final long serialVersionUID = 1L;
        ProtocolException(String message, String reference, int refusal) {
            super(FailureKind.PROTOCOL, message, reference, refusal);
        }
    }
    public static final class TypeMismatchException extends ClientException {
        private static final long serialVersionUID = 1L;
        TypeMismatchException(String message, String reference, int refusal) {
            super(FailureKind.TYPE, message, reference, refusal);
        }
    }
    public static final class ClosedException extends ClientException {
        private static final long serialVersionUID = 1L;
        ClosedException(String message, String reference, int refusal) {
            super(FailureKind.CLOSED, message, reference, refusal);
        }
    }
    public static final class ConsumerInterruptedException extends ClientException {
        private static final long serialVersionUID = 1L;
        ConsumerInterruptedException(String message, String reference, int refusal) {
            super(FailureKind.INTERRUPTED, message, reference, refusal);
        }
    }
    public static final class ReopenRequiredException extends ClientException {
        private static final long serialVersionUID = 1L;
        ReopenRequiredException(String message, String reference, int refusal) {
            super(FailureKind.REOPEN_REQUIRED, message, reference, refusal);
        }
    }

    /** State never references its Cleaner referent. Its monitor covers only native short calls. */
    private static final class State implements Runnable {
        private enum Phase { OPEN, CLOSING, CLOSED }
        private long pointer;
        private Phase phase = Phase.OPEN;
        private boolean active;
        State(long pointer) {
            if (pointer == 0) throw new IllegalArgumentException("a handle must be present");
            this.pointer = pointer;
        }
        private long present() {
            if (phase != Phase.OPEN) throw new ClosedException("the Java handle is closed", null, 0);
            return pointer;
        }
        private long enter() {
            long owned = present();
            if (active) throw new IllegalStateException("reentrant access to a native handle");
            active = true;
            return owned;
        }
        private void leave() {
            active = false;
            if (phase == Phase.CLOSING) release();
        }
        private void release() {
            long owned = pointer;
            pointer = 0;
            phase = Phase.CLOSED;
            Native.release(owned);
        }
        synchronized <T> T read(LongFunction<T> call) {
            long owned = enter();
            try { return call.apply(owned); } finally { leave(); }
        }
        synchronized void write(LongConsumer call) {
            long owned = enter();
            try { call.accept(owned); } finally { leave(); }
        }
        synchronized Lease lease() {
            long owned = enter();
            try {
                long retained = Native.retain(owned);
                try { return new Lease(retained); }
                catch (Throwable failure) { Native.release(retained); throw failure; }
            } finally { leave(); }
        }
        @Override public synchronized void run() {
            if (phase == Phase.OPEN) {
                // A class loader may call close again on this same thread during JNI marshalling.
                // Stop admission immediately; the last admitted call releases its live borrow.
                phase = Phase.CLOSING;
                if (!active) release();
            }
        }
    }
    private static final class Lease implements AutoCloseable {
        private long pointer;
        Lease(long pointer) { this.pointer = pointer; }
        // Cleanup after a failed buffer publication can race the root buffer's Cleaner.
        @Override public synchronized void close() {
            if (pointer != 0) { long owned = pointer; pointer = 0; Native.release(owned); }
        }
    }
    public abstract static class Handle implements AutoCloseable {
        private final State state;
        private final Cleaner.Cleanable cleanup;
        Handle(long pointer) {
            State owned;
            try { owned = new State(pointer); }
            catch (Throwable failure) {
                if (pointer != 0) Native.release(pointer);
                throw failure;
            }
            try { this.cleanup = CLEANER.register(this, owned); this.state = owned; }
            catch (Throwable failure) { owned.run(); throw failure; }
        }
        final <T> T read(LongFunction<T> call) {
            try { return state.read(call); } finally { Reference.reachabilityFence(this); }
        }
        final void write(LongConsumer call) {
            try { state.write(call); } finally { Reference.reachabilityFence(this); }
        }
        final Lease lease() {
            try { return state.lease(); } finally { Reference.reachabilityFence(this); }
        }
        final ByteBuffer directBuffer() {
            Lease retained = lease();
            try {
                ByteBuffer root = (ByteBuffer) Native.buffer(retained.pointer);
                // A direct view's attachment retains the root across slice/duplicate chains.
                // The Cleaner belongs to that root, which is never exposed as writable.
                CLEANER.register(root, retained::close);
                return root.asReadOnlyBuffer();
            } catch (Throwable failure) { retained.close(); throw failure; }
        }
        @Override public void close() { cleanup.clean(); }
    }
    private static Lease token(Cancel cancel) { return cancel == null ? new Lease(0) : cancel.lease(); }

    public record Varlen(long[] offsets, byte[] data) {}
    public record Subscription(String name, long generation) {}
    public record RestorationFailure(RestorationCause cause, String message, long waitNanos) {
        RestorationFailure(int cause, String message, long waitNanos) { this(RestorationCause.of(cause), message, waitNanos); }
    }
    public record Diagnostic(String message, long[] span) {}
    public record Backup(long bytes, byte[] digest) {}
    public record Restore(boolean dryRun, long domains, long resourceVersions, long models, boolean failed) {}
    public record PacedClock(long generation, long periodNanos, long skewNanos, long logicalOrigin, long utcAnchor, double rate) {}
    public record Tick(long generation, long id, long logicalBoundary, long authorityUtc, long servingLogical) {}
    public record AdmissionWindow(long earliestCenter, long latestCenter) {}
    public record Reopen(ReopenReason reason, OpenRefusal refusal) {
        static Reopen from(int[] values) {
            return values == null ? null : new Reopen(ReopenReason.of(values[0]), optional(OpenRefusal.values(), values[1]));
        }
    }
    public record FieldLevel(FieldType type, long length) {}
    public static final class Field {
        private final String name;
        private final List<FieldLevel> levels;
        private final boolean nullable;
        private final boolean sensitive;
        Field(String name, int[] kinds, long[] lengths, boolean nullable, boolean sensitive) {
            this.name = name; this.nullable = nullable; this.sensitive = sensitive;
            var values = new FieldLevel[kinds.length];
            for (int index = 0; index < kinds.length; index++) values[index] = new FieldLevel(FieldType.of(kinds[index]), lengths[index]);
            this.levels = List.of(values);
        }
        public String name() { return name; }
        public FieldType type() { return levels.getFirst().type(); }
        public List<FieldLevel> levels() { return levels; }
        public boolean nullable() { return nullable; }
        public boolean sensitive() { return sensitive; }
    }
    private static <T> List<T> values(Object[] array, Class<T> type) {
        return Arrays.stream(array).map(type::cast).toList();
    }
    public static final class EndpointDescription {
        private final long[] numbers;
        private final byte[] contract;
        private final String attachment;
        EndpointDescription(long[] numbers, byte[] contract, String attachment) {
            this.numbers = numbers; this.contract = contract; this.attachment = attachment;
        }
        public long generation() { return numbers[0]; }
        public long batches() { return numbers[1]; }
        public long bytes() { return numbers[2]; }
        public long maxBatchRows() { return numbers[3]; }
        public long maxBatchBytes() { return numbers[4]; }
        public WindowKind window() { return WindowKind.of(Math.toIntExact(numbers[5])); }
        public long outstanding() { return numbers[6]; }
        public long ackTimeoutNanos() { return numbers[7]; }
        public long retryBackoffNanos() { return numbers[8]; }
        public long retryMaxBackoffNanos() { return numbers[9]; }
        public byte[] contract() { return contract.clone(); }
        public String attachment() { return attachment; }
    }
    public static final class SubmissionInfo {
        private final SubmissionResult result;
        private final String message;
        private final SubmissionRefusal refusal;
        private final BatchDefect defect;
        private final ProcessingFailure failure;
        private final Uncertainty uncertainty;
        SubmissionInfo(int result, String message, int refusal, int defect, int failure, int uncertainty) {
            this.result = SubmissionResult.of(result); this.message = message;
            this.refusal = optional(SubmissionRefusal.values(), refusal); this.defect = optional(BatchDefect.values(), defect);
            this.failure = optional(ProcessingFailure.values(), failure); this.uncertainty = optional(Uncertainty.values(), uncertainty);
        }
        public SubmissionResult result() { return result; }
        public String message() { return message; }
        public SubmissionRefusal refusal() { return refusal; }
        public BatchDefect defect() { return defect; }
        public ProcessingFailure failure() { return failure; }
        public Uncertainty uncertainty() { return uncertainty; }
    }
    public record Pending(long id, SubmissionInfo outcome) {}
    public static final class Suggestion {
        private final String value;
        private final CompletionKind kind;
        private final long start;
        private final long end;
        private final String replacement;
        Suggestion(String value, int kind, long start, long end, String replacement) {
            this.value = value; this.kind = CompletionKind.of(kind); this.start = start; this.end = end; this.replacement = replacement;
        }
        public String value() { return value; }
        public CompletionKind kind() { return kind; }
        public long start() { return start; }
        public long end() { return end; }
        public String replacement() { return replacement; }
    }
    public static final class Cancel extends Handle {
        public Cancel() { super(Native.cancelNew()); }
        private Cancel(long pointer) { super(pointer); }
        public static Cancel deadline(double seconds) { return new Cancel(Native.cancelDeadline(seconds)); }
        public void trigger() { write(Native::cancelTrigger); }
    }
    public static final class ConnectOptions extends Handle {
        public ConnectOptions() { super(Native.optionsNew()); }
        public ConnectOptions auth(String username, String password) { write(p -> Native.optionsAuth(p, username, password)); return this; }
        public ConnectOptions tls(TlsRequirement requirement) { write(p -> Native.optionsTls(p, requirement.code())); return this; }
        public ConnectOptions caCertificate(byte[] pem) { write(p -> Native.optionsCa(p, Objects.requireNonNull(pem))); return this; }
        public ConnectOptions seeds(String... servers) { write(p -> Native.optionsSeeds(p, Objects.requireNonNull(servers))); return this; }
        public ConnectOptions connectTimeout(double seconds) { write(p -> Native.optionsTimeout(p, 1, seconds)); return this; }
        public ConnectOptions requestTimeout(double seconds) { write(p -> Native.optionsTimeout(p, 2, seconds)); return this; }
        public ConnectOptions retryTimeout(double seconds) { write(p -> Native.optionsTimeout(p, 3, seconds)); return this; }
        public ConnectOptions backupWaitTimeout(double seconds) { write(p -> Native.optionsTimeout(p, 4, seconds)); return this; }
    }
    public static final class Fields extends Handle {
        public Fields() { super(Native.fieldsNew()); }
        public Fields add(String name, FieldType type, long length, boolean nullable, boolean sensitive) {
            write(p -> Native.fieldsAdd(p, Objects.requireNonNull(name), type.code(), length, nullable, sensitive)); return this;
        }
        public Fields element(FieldType type, long length) { write(p -> Native.fieldsElement(p, type.code(), length)); return this; }
        public Schema schema() { return read(p -> new Schema(Native.fieldsSchema(p))); }
    }
    public static final class Schema extends Handle {
        private Schema(long pointer) { super(pointer); }
        public List<Field> fields(Part part) { return read(p -> values(Native.schemaFields(p, part.code()), Field.class)); }
        public String branch() { return read(Native::schemaBranch); }
    }
    public static final class BatchBuilder extends Handle {
        public BatchBuilder(Schema schema, int rows) { super(schema.read(p -> Native.builderNew(p, rows))); }
        public BatchBuilder states(int column, byte[] states) { write(p -> Native.builderStates(p, column, states)); return this; }
        public BatchBuilder offsets(int column, int level, long[] offsets) { write(p -> Native.builderOffsets(p, column, level, offsets)); return this; }
        public BatchBuilder fixed(int column, int level, byte[] data) { write(p -> Native.builderFixed(p, column, level, data)); return this; }
        public BatchBuilder varlen(int column, int level, long[] offsets, byte[] data) { write(p -> Native.builderVarlen(p, column, level, offsets, data)); return this; }
        public Batch finish() { return read(p -> new Batch(Native.builderFinish(p))); }
    }
    public static final class Batch extends Handle {
        private Batch(long pointer) { super(pointer); }
        public static Batch fromIpc(Schema schema, int rows, byte[] ipc) { return schema.read(p -> new Batch(Native.batchFromIpc(p, rows, ipc))); }
        public int rows() { return read(Native::batchRows); }
        public Schema schema() { return read(p -> new Schema(Native.batchSchema(p))); }
        public byte[] ipc() { return read(Native::batchIpc); }
        public ByteBuffer ipcBuffer() { return directBuffer(); }
        public int cells(int column, int level) { return read(p -> Native.batchCells(p, column, level)); }
        public byte[] states(int column) { return read(p -> Native.batchStates(p, column)); }
        public long[] offsets(int column, int level) { return read(p -> Native.batchOffsets(p, column, level)); }
        public byte[] fixed(int column, int level) { return read(p -> Native.batchFixed(p, column, level)); }
        public Varlen varlen(int column, int level) { return read(p -> (Varlen) Native.batchVarlen(p, column, level)); }
    }
    public static final class Session extends Handle {
        private Session(long pointer) { super(pointer); }
        public static Session connect(String server, String domain, ConnectOptions options, Cancel cancel) {
            try (var settings = options.lease(); var cancellation = token(cancel)) {
                return new Session(Native.connect(Objects.requireNonNull(server), domain, settings.pointer, cancellation.pointer));
            }
        }
        public static Session connect(String server, String domain) {
            try (var options = new ConnectOptions()) { return connect(server, domain, options, null); }
        }
        public Execution prepare(String query, Cancel cancel) {
            try (var session = lease(); var cancellation = token(cancel)) {
                return new Execution(Native.prepare(session.pointer, Objects.requireNonNull(query), cancellation.pointer));
            }
        }
        public Outcome execute(Execution execution, Cancel cancel) {
            try (var session = lease(); var prepared = execution.lease(); var cancellation = token(cancel)) {
                return new Outcome(Native.execute(session.pointer, prepared.pointer, cancellation.pointer));
            }
        }
        public Outcome command(String query, Cancel cancel) {
            try (var execution = prepare(query, cancel)) { return execute(execution, cancel); }
        }
        public Event nextEvent(Cancel cancel) {
            try (var session = lease(); var cancellation = token(cancel)) {
                return new Event(Native.nextEvent(session.pointer, cancellation.pointer));
            }
        }
        public ClockEvent nextClockEvent(Cancel cancel) {
            try (var session = lease(); var cancellation = token(cancel)) {
                return new ClockEvent(Native.nextClockEvent(session.pointer, cancellation.pointer));
            }
        }
        public DomainClock domainClock(String domain) {
            return read(p -> { long handle = Native.domainClock(p, domain); return handle == 0 ? null : new DomainClock(handle); });
        }
        public Suggestions suggest(String input, int cursor, int pageSize, String continuation, Cancel cancel) {
            try (var session = lease(); var cancellation = token(cancel)) {
                return new Suggestions(Native.suggest(session.pointer, input, cursor, pageSize, continuation, cancellation.pointer));
            }
        }
        public Producer openProducer(String domain, String ingestor, Fields fields, int batches, long bytes, Cancel cancel) {
            try (var session = lease(); var expected = fields.lease(); var cancellation = token(cancel)) {
                return new Producer(Native.producerOpen(session.pointer, domain, ingestor, expected.pointer, batches, bytes, cancellation.pointer));
            }
        }
        public Consumer openConsumer(String domain, String emitter, Fields fields, int batches, long bytes, Cancel cancel) {
            try (var session = lease(); var expected = fields.lease(); var cancellation = token(cancel)) {
                return new Consumer(Native.consumerOpen(session.pointer, domain, emitter, expected.pointer, batches, bytes, cancellation.pointer));
            }
        }
    }
    public static final class Execution extends Handle {
        private Execution(long pointer) { super(pointer); }
        public String reference() { return read(Native::executionReference); }
    }
    public static final class Outcome extends Handle {
        private Outcome(long pointer) { super(pointer); }
        public Disposition disposition() { return read(p -> Disposition.of(Native.outcomeDisposition(p))); }
        /** The typed upload failure, or null for a completed upload or another command. */
        public UploadFailure uploadFailure() { return read(p -> optional(UploadFailure.values(), Native.outcomeUploadFailure(p))); }
        public String message() { return read(Native::outcomeMessage); }
        public String executionReference() { return read(Native::outcomeReference); }
        public List<Diagnostic> diagnostics() { return read(p -> values(Native.outcomeDiagnostics(p), Diagnostic.class)); }
        public Subscription subscription() { return read(p -> (Subscription) Native.outcomeSubscription(p)); }
        public Schema schema() { return read(p -> { long handle = Native.outcomeSchema(p); return handle == 0 ? null : new Schema(handle); }); }
        public Backup backup() { return read(p -> (Backup) Native.outcomeBackup(p)); }
        public Restore restore() { return read(p -> (Restore) Native.outcomeRestore(p)); }
        public Inspection stateInspection() {
            return read(p -> { long handle = Native.outcomeInspection(p); return handle == 0 ? null : new Inspection(handle); });
        }
    }
    /** An independently owned typed state page or refusal, with the complete lossless JSON view. */
    public static final class Inspection extends Handle {
        private Inspection(long pointer) { super(pointer); }
        /** The graph domain, or null for a cluster license inspection. */
        public String domain() { return read(Native::inspectionDomain); }
        public String entityKind() { return read(Native::inspectionEntityKind); }
        /** The graph entity name, or null for a cluster license inspection. */
        public String entityName() { return read(Native::inspectionEntityName); }
        public String json() { return read(Native::inspectionJson); }
        public InspectionRejection rejection() { return read(p -> optional(InspectionRejection.values(), Native.inspectionRejection(p))); }
        public InspectionCounts counts() { return read(p -> (InspectionCounts) Native.inspectionCounts(p)); }
        public Long unitCount() { return read(p -> (Long) Native.inspectionUnitCount(p)); }
        public String continuation() { return read(Native::inspectionContinuation); }
        public List<InspectionUnit> units() {
            return read(p -> {
                int count = Native.inspectionUnitsLen(p);
                var units = new ArrayList<InspectionUnit>(count);
                for (int index = 0; index < count; index++) units.add(new InspectionUnit(Native.retain(p), index));
                return List.copyOf(units);
            });
        }
    }
    /** A unit metadata view owns its inspection independently of the originating handle. */
    public static final class InspectionUnit extends Handle {
        private final int unit;
        private InspectionUnit(long pointer, int unit) { super(pointer); this.unit = unit; }
        public InspectionUnitKind kind() { return read(p -> InspectionUnitKind.of(Native.inspectionUnitKind(p, unit))); }
        public Long index() { return read(p -> (Long) Native.inspectionUnitIndex(p, unit)); }
        public String owner() { return read(p -> Native.inspectionUnitOwner(p, unit)); }
        public List<String> replicas() { return read(p -> values(Native.inspectionUnitReplicas(p, unit), String.class)); }
        public String originNode() { return read(p -> Native.inspectionUnitOriginNode(p, unit)); }
        public long originIncarnation() { return read(p -> Native.inspectionUnitOriginIncarnation(p, unit)); }
        public byte[] originFingerprint() { return read(p -> Native.inspectionUnitOriginFingerprint(p, unit)); }
        public long originGeneration() { return read(p -> Native.inspectionUnitOriginGeneration(p, unit)); }
        public long originRevision() { return read(p -> Native.inspectionUnitOriginRevision(p, unit)); }
        public InspectionStage originStage() { return read(p -> InspectionStage.of(Native.inspectionUnitOriginStage(p, unit))); }
        public InspectionCounts counts() { return read(p -> (InspectionCounts) Native.inspectionUnitCounts(p, unit)); }
        public List<InspectionSection> sections() {
            return read(p -> {
                int count = Native.inspectionSectionsLen(p, unit);
                var sections = new ArrayList<InspectionSection>(count);
                for (int index = 0; index < count; index++) sections.add(new InspectionSection(Native.retain(p), unit, index));
                return List.copyOf(sections);
            });
        }
    }
    public static final class InspectionSection extends Handle {
        private final int unit;
        private final int section;
        private InspectionSection(long pointer, int unit, int section) { super(pointer); this.unit = unit; this.section = section; }
        public String name() { return read(p -> Native.inspectionSectionName(p, unit, section)); }
        public long omittedRows() { return read(p -> Native.inspectionSectionOmittedRows(p, unit, section)); }
        public Long watermarkLow() { return read(p -> { long[] columns = Native.inspectionSectionWatermarks(p, unit, section); return columns == null ? null : columns[0]; }); }
        public Long watermarkHigh() { return read(p -> { long[] columns = Native.inspectionSectionWatermarks(p, unit, section); return columns == null ? null : columns[1]; }); }
        public Schema schema() { return read(p -> new Schema(Native.inspectionSectionSchema(p, unit, section))); }
        public List<InspectionFrame> frames() {
            return read(p -> {
                int count = Native.inspectionFramesLen(p, unit, section);
                var frames = new ArrayList<InspectionFrame>(count);
                for (int index = 0; index < count; index++) frames.add(new InspectionFrame(Native.inspectionFrame(p, unit, section, index)));
                return List.copyOf(frames);
            });
        }
    }
    /** The verified frame and its schema survive their inspection; buffers also survive this handle's close. */
    public static final class InspectionFrame extends Handle {
        private InspectionFrame(long pointer) { super(pointer); }
        public long rowCount() { return read(Native::inspectionFrameRows); }
        public Schema schema() { return read(p -> new Schema(Native.inspectionFrameSchema(p))); }
        public ByteBuffer frame() { return directBuffer(); }
    }
    public static final class Event extends Handle {
        private Event(long pointer) { super(pointer); }
        public EventKind kind() { return read(p -> EventKind.of(Native.eventKind(p))); }
        public long rows() { return read(Native::eventRows); }
        public Subscription subscription() { return read(p -> (Subscription) Native.eventSubscription(p)); }
        public Schema schema() { return read(p -> new Schema(Native.eventSchema(p))); }
        public Batch batch() { return read(p -> new Batch(Native.eventBatch(p))); }
        public ByteBuffer frame() { return directBuffer(); }
        public EndReason endReason() { return read(p -> EndReason.of(Native.eventEnd(p))); }
        public RestorationFailure restorationFailure() { return read(p -> (RestorationFailure) Native.eventRestoration(p)); }
        public byte[] states(Part part, int column) { return read(p -> Native.eventStates(p, part.code(), column)); }
        public byte[] fixed(Part part, int column) { return read(p -> Native.eventFixed(p, part.code(), column)); }
        public Varlen varlen(Part part, int column) { return read(p -> (Varlen) Native.eventVarlen(p, part.code(), column)); }
        public byte[] cell(Part part, int row, int column) { return read(p -> Native.eventCell(p, part.code(), row, column)); }
    }
    public static final class ClockEvent extends Handle {
        private ClockEvent(long pointer) { super(pointer); }
        public ClockEventKind kind() { return read(p -> ClockEventKind.of(Native.clockEventKind(p))); }
        public String domain() { return read(Native::clockEventDomain); }
        public long generation() { return read(Native::clockEventGeneration); }
        public ClockState state() { return read(p -> ClockState.of(Native.clockEventState(p))); }
        public PacedClock paced() { return read(p -> (PacedClock) Native.clockEventPaced(p)); }
        public Tick tick() { return read(p -> (Tick) Native.clockEventTick(p)); }
        public ClockEndReason endReason() { return read(p -> ClockEndReason.of(Native.clockEventEnd(p))); }
        public RestorationFailure restorationFailure() { return read(p -> (RestorationFailure) Native.clockEventRestoration(p)); }
    }
    public static final class DomainClock extends Handle {
        private DomainClock(long pointer) { super(pointer); }
        public String domain() { return read(Native::clockDomain); }
        public long generation() { return read(Native::clockGeneration); }
        public ClockState state() { return read(p -> ClockState.of(Native.clockState(p))); }
        public PacedClock paced() { return read(p -> (PacedClock) Native.clockPaced(p)); }
        public Tick tick() { return read(p -> (Tick) Native.clockTick(p)); }
        public long logicalTimeAt(long utcNanos) { return read(p -> Native.clockLogical(p, utcNanos)); }
        public long wallNanosUntil(long utcNanos, long logicalNanos) { return read(p -> Native.clockWait(p, utcNanos, logicalNanos)); }
        public AdmissionWindow admissionWindow(long utcNanos) {
            return read(p -> { long[] range = Native.clockWindow(p, utcNanos); return range == null ? null : new AdmissionWindow(range[0], range[1]); });
        }
        public boolean admits(long utcNanos, long eventNanos) { return read(p -> Native.clockAdmits(p, utcNanos, eventNanos)); }
    }
    public static final class Suggestions extends Handle {
        private Suggestions(long pointer) { super(pointer); }
        public CompletionStatus status() { return read(p -> CompletionStatus.of(Native.suggestionsStatus(p))); }
        public String continuation() { return read(Native::suggestionsContinuation); }
        public List<Suggestion> candidates() { return read(p -> values(Native.suggestionsValues(p), Suggestion.class)); }
    }
    public static final class Producer extends Handle {
        private Producer(long pointer) { super(pointer); }
        public Schema schema() { return read(p -> new Schema(Native.producerSchema(p))); }
        public EndpointDescription description() { return read(p -> (EndpointDescription) Native.producerDescription(p)); }
        public EndpointState state() { return read(p -> EndpointState.of(Native.producerState(p))); }
        public Admission admission() { return read(p -> Admission.of(Native.producerAdmission(p))); }
        public Reopen reopen() { return read(p -> Reopen.from(Native.producerReopen(p))); }
        public List<Pending> pending() { return read(p -> values(Native.producerPending(p), Pending.class)); }
        public long submit(Batch batch, Cancel cancel) {
            try (var producer = lease(); var payload = batch.lease(); var cancellation = token(cancel)) {
                return Native.producerSubmit(producer.pointer, payload.pointer, cancellation.pointer);
            }
        }
        public long submitIpc(byte[] ipc, Cancel cancel) {
            try (var producer = lease(); var cancellation = token(cancel)) {
                return Native.producerSubmitIpc(producer.pointer, ipc, cancellation.pointer);
            }
        }
        public Submission rejoin(long id, Cancel cancel) {
            try (var producer = lease(); var cancellation = token(cancel)) {
                return new Submission(Native.producerRejoin(producer.pointer, id, cancellation.pointer));
            }
        }
        public Submission release(long id) {
            return read(p -> { long handle = Native.producerRelease(p, id); return handle == 0 ? null : new Submission(handle); });
        }
        public void end(Cancel cancel) {
            try (var producer = lease(); var cancellation = token(cancel)) { Native.producerClose(producer.pointer, cancellation.pointer); }
        }
    }
    public static final class Consumer extends Handle {
        private Consumer(long pointer) { super(pointer); }
        public Schema schema() { return read(p -> new Schema(Native.consumerSchema(p))); }
        public EndpointDescription description() { return read(p -> (EndpointDescription) Native.consumerDescription(p)); }
        public EndpointState state() { return read(p -> EndpointState.of(Native.consumerState(p))); }
        public Reopen reopen() { return read(p -> Reopen.from(Native.consumerReopen(p))); }
        public Delivery next(Cancel cancel) {
            try (var consumer = lease(); var cancellation = token(cancel)) {
                return new Delivery(Native.consumerNext(consumer.pointer, cancellation.pointer));
            }
        }
        public void end(Cancel cancel) {
            try (var consumer = lease(); var cancellation = token(cancel)) { Native.consumerClose(consumer.pointer, cancellation.pointer); }
        }
    }
    public static final class Delivery extends Handle {
        private Delivery(long pointer) { super(pointer); }
        public byte[] identity() { return read(Native::deliveryIdentity); }
        public byte[] reference() { return read(Native::deliveryReference); }
        public String sourceRelay() { return read(Native::deliverySource); }
        public byte[] branchFingerprint() { return read(Native::deliveryBranch); }
        public long members() { return read(Native::deliveryMembers); }
        public long executionNow() { return read(Native::deliveryNow); }
        public byte[] ipc() { return read(Native::deliveryIpc); }
        public ByteBuffer ipcBuffer() { return directBuffer(); }
        public Batch batch() { return read(p -> new Batch(Native.deliveryBatch(p))); }
        public Settlement ack(Cancel cancel) {
            try (var delivery = lease(); var cancellation = token(cancel)) { return Settlement.of(Native.deliveryAck(delivery.pointer, cancellation.pointer)); }
        }
        public Settlement retry(Cancel cancel) {
            try (var delivery = lease(); var cancellation = token(cancel)) { return Settlement.of(Native.deliveryRetry(delivery.pointer, cancellation.pointer)); }
        }
        public Settlement reject(String reason, Cancel cancel) {
            try (var delivery = lease(); var cancellation = token(cancel)) { return Settlement.of(Native.deliveryReject(delivery.pointer, reason, cancellation.pointer)); }
        }
    }
    public static final class Submission extends Handle {
        private Submission(long pointer) { super(pointer); }
        public SubmissionInfo info() { return read(p -> (SubmissionInfo) Native.submissionInfo(p)); }
    }
}
