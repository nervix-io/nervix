package io.nervix.client;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The class-loader regression needs a fresh JVM's JNI lookup caches, as its original runner did. */
@Tag("boundary")
public final class ReentrantClose {
    @Test
    void admittedGetterSurvivesReentrantClose() throws Exception {
        Path jar = Path.of(System.getenv("NERVIX_CLIENT_JVM_JAR"));
        String classpath = jar.getParent().resolve("tests").toString();
        var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Xcheck:jni", "-cp", classpath, "io.nervix.client.ReentrantCloseProcess", jar.toString())
            .inheritIO().start();
        try {
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "fresh-JVM regression did not finish");
            assertEquals(0, process.exitValue(), "the original class-loader assertions failed");
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(10, TimeUnit.SECONDS), "fresh JVM did not stop");
            }
        }
    }
}
