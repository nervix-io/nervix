package io.nervix.client.suite.support;

import io.nervix.client.Nervix;

/** A fresh application needs only the packaged jar, with no JUnit or native-library path. */
public final class PackagingProbe {
    private PackagingProbe() {}
    public static void main(String[] args) {
        try (var fields = new Nervix.Fields()) {
            fields.add("id", Nervix.FieldType.U32, 0, false, false);
            try (var schema = fields.schema()) {
                if (schema.fields(Nervix.Part.ROWS).size() != 1) throw new AssertionError("packaged JNI did not load");
            }
        }
    }
}
