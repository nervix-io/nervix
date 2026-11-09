package io.nervix.client.suite.support;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/** The cluster tier's bounded request/confirmation channel to its scenario's declared actions.
 *
 * <p>Owns the one numbered request outstanding at a time and the thread that reads its
 * confirmation. Depends on the exchange the scenario harness specifies: a request shares standard
 * output with the result lines, and the confirmation arrives on standard input once the scenario
 * performed the action. Must not know the cluster, the fault the scenario injects or the binding.
 *
 * <p>Sequences start at one and increase by one across the runner's entire cluster tier. Java
 * cannot wait for a pipe with a deadline, so one daemon thread reads confirmations, a line of at
 * most 256 bytes at a time, and hands each to the waiting request through a one-line queue. A
 * request waits at most 120 seconds for its exact confirmation; end of input, an oversized line or
 * a foreign confirmation fails it.
 */
public final class Actions {
    private static final String REFUSE_NEXT_OPENING = "refuse-next-subscription-opening";
    private static final String CLEAR_REFUSALS = "clear-subscription-opening-refusals";
    /** The longest request or confirmation line, its newline included. */
    private static final int LINE_BYTES = 256;
    private static final long WAIT_SECONDS = 120;
    private static final Actions SCENARIO = new Actions(System.in, System.out);

    private final InputStream input;
    private final PrintStream output;
    /** The reader holds at most one line here while a second waits in its own buffer. */
    private final BlockingQueue<Reply> replies = new ArrayBlockingQueue<>(1);
    private long sequence;
    private Thread reader;
    /** Why the reader stopped; every later request fails with it instead of waiting. */
    private String ended;

    private Actions(InputStream input, PrintStream output) {
        this.input = input;
        this.output = output;
    }

    /** One test's subscription-opening refusals; closing clears every refusal still unused. */
    public static final class Refusals implements AutoCloseable {
        private boolean armed;

        /** Arms every node to refuse its next subscription opening as unavailable. */
        public void refuseNextSubscriptionOpening() {
            SCENARIO.perform(REFUSE_NEXT_OPENING);
            armed = true;
        }

        /** Clears unused refusals, even after a failed assertion, so none reaches another test. */
        @Override public void close() {
            if (!armed) return;
            armed = false;
            SCENARIO.perform(CLEAR_REFUSALS);
        }
    }

    private sealed interface Reply {}

    private record Line(String text) implements Reply {}

    private record Ended(String reason) implements Reply {}

    private synchronized void perform(String action) {
        if (ended != null) throw new AssertionError(ended);
        sequence = Math.addExact(sequence, 1);
        String expected = "NERVIX-SUITE-CONFIRMED " + sequence + " " + action + "\n";
        if (reader == null) {
            reader = new Thread(this::read, "jvm-suite-actions");
            reader.setDaemon(true);
            reader.start();
        }
        // A test may have left output on the current line; the request starts its own.
        output.print("\nNERVIX-SUITE-ACTION " + sequence + " " + action + "\n");
        if (output.checkError()) throw new AssertionError("the scenario action request was not written");
        Reply reply;
        try {
            reply = replies.poll(WAIT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while awaiting the scenario's " + action, interrupted);
        }
        if (reply == null)
            throw new AssertionError("the scenario did not confirm " + action + " within " + WAIT_SECONDS + "s");
        if (reply instanceof Ended stopped) {
            ended = stopped.reason();
            throw new AssertionError(ended);
        }
        if (reply instanceof Line line && !line.text().equals(expected))
            throw new AssertionError("unexpected scenario action confirmation: " + line.text().strip());
    }

    /** Reads confirmation lines until input ends, a line outgrows its bound or reading fails. */
    private void read() {
        var line = new ByteArrayOutputStream(LINE_BYTES);
        try {
            while (true) {
                int value = input.read();
                if (value < 0) {
                    deliver(new Ended("the scenario closed its action channel without confirmation"));
                    return;
                }
                line.write(value);
                if (value == '\n') {
                    deliver(new Line(line.toString(StandardCharsets.US_ASCII)));
                    line.reset();
                } else if (line.size() >= LINE_BYTES) {
                    deliver(new Ended("the scenario's action confirmation exceeds " + LINE_BYTES + " bytes"));
                    return;
                }
            }
        } catch (IOException failure) {
            deliver(new Ended("the scenario action channel failed: " + failure));
        }
    }

    private void deliver(Reply reply) {
        try {
            replies.put(reply);
        } catch (InterruptedException interrupted) {
            // Only the JVM's exit stops this daemon; nothing waits for a reply it could not hand over.
            Thread.currentThread().interrupt();
        }
    }
}
