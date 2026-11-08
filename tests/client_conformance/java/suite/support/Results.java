package io.nervix.client.suite.support;

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;

/** Reports each JUnit test after its teardown, using the host suites' shared result contract. */
public final class Results implements TestExecutionListener {
    @Override public void executionFinished(TestIdentifier test, TestExecutionResult result) {
        if (!test.isTest()) return;
        String outcome = switch (result.getStatus()) {
            case SUCCESSFUL -> "passed";
            case ABORTED -> "skipped";
            case FAILED -> "failed";
        };
        report(test, outcome);
    }

    @Override public void executionSkipped(TestIdentifier test, String reason) {
        if (test.isTest()) report(test, "skipped");
    }

    private static void report(TestIdentifier test, String outcome) {
        String identity = test.getSource()
            .filter(MethodSource.class::isInstance)
            .map(MethodSource.class::cast)
            .map(source -> source.getClassName() + "#" + source.getMethodName())
            .orElse(test.getUniqueId());
        System.out.println("NERVIX-SUITE " + outcome + " " + identity);
    }
}
