/**
 * The Java 21 edge of the Nervix client.
 *
 * <p><strong>Owns.</strong> Java handle lifetimes, JNI calls, typed exceptions and bulk arrays.
 * <p><strong>Depends on.</strong> JNI and the shared Rust host runtime and client vocabulary.
 * <p><strong>Must not know.</strong> Server execution, C ABI details or session recovery decisions.
 */
package io.nervix.client;
