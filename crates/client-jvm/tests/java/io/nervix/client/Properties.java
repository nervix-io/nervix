package io.nervix.client;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Bounded valid cases driven by the registered Rust Bolero targets through an embedded JVM. */
public final class Properties {
    private Properties() {}
    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
    private static final class Input {
        private final byte[] bytes;
        private int cursor;
        Input(byte[] bytes) { this.bytes = bytes; }
        int next() { return cursor < bytes.length ? Byte.toUnsignedInt(bytes[cursor++]) : 0; }
        int count(int maximum) { return next() % maximum; }
        byte[] bytes(int count) {
            byte[] result = new byte[count];
            for (int index = 0; index < count; index++) result[index] = (byte) next();
            return result;
        }
    }
    private record Expected(String name, List<Nervix.FieldLevel> levels, boolean nullable, boolean sensitive) {}
    private static void sameFields(List<Expected> expected, Object[] actual) {
        require(actual.length == expected.size(), "field count");
        for (int index = 0; index < expected.size(); index++) {
            Expected wanted = expected.get(index); Nervix.Field got = (Nervix.Field) actual[index];
            require(got.name().equals(wanted.name()), "field name and order");
            require(got.levels().equals(wanted.levels()), "complete field type levels");
            require(got.type() == wanted.levels().getFirst().type(), "field head kind");
            require(got.nullable() == wanted.nullable(), "field nullability");
            require(got.sensitive() == wanted.sensitive(), "field sensitivity");
        }
    }
    public static void fieldsRoundTrip(byte[] bytes) {
        Input input = new Input(bytes); List<Expected> expected = new ArrayList<>();
        try (var fields = new Nervix.Fields()) {
            int count = 1 + input.count(4);
            for (int index = 0; index < count; index++) {
                int depth = input.count(5); var levels = new ArrayList<Nervix.FieldLevel>();
                for (int level = 0; level < depth; level++) {
                    boolean fixed = input.count(2) == 1;
                    levels.add(new Nervix.FieldLevel(fixed ? Nervix.FieldType.FIXED_LIST : Nervix.FieldType.LIST, fixed ? 1 + input.count(4) : 0));
                }
                levels.add(new Nervix.FieldLevel(Nervix.FieldType.values()[input.count(14)], 0));
                boolean nullable = input.count(2) == 1, sensitive = input.count(2) == 1;
                String name = "field_" + index;
                var first = levels.getFirst(); fields.add(name, first.type(), first.length(), nullable, sensitive);
                for (var level : levels.subList(1, levels.size())) fields.element(level.type(), level.length());
                expected.add(new Expected(name, List.copyOf(levels), nullable, sensitive));
            }
            try (var schema = fields.schema()) { sameFields(expected, schema.fields(Nervix.Part.ROWS).toArray()); }
            // Exercise the complete snapshot an endpoint open leases before releasing the monitor.
            long snapshot = fields.read(Native::retain);
            try {
                long schema = Native.fieldsSchema(snapshot);
                try { sameFields(expected, Native.schemaFields(schema, Nervix.Part.ROWS.code())); }
                finally { Native.release(schema); }
            } finally { Native.release(snapshot); }
        }
    }
    private record Column(byte[] states, long[] offsets, byte[] data, boolean variable, int leaf) {}
    private static int width(Nervix.FieldType type) {
        return switch (type) {
            case U8, I8, BOOL -> 1;
            case U16, I16 -> 2;
            case U32, I32, F32 -> 4;
            case U64, I64, F64, DATETIME -> 8;
            default -> throw new AssertionError("a fixed scalar has a width");
        };
    }
    private static Column scalar(Input input, Nervix.FieldType type, int rows, boolean nullable) {
        byte[] states = new byte[rows]; Arrays.fill(states, (byte) 1);
        boolean variable = type == Nervix.FieldType.STRING || type == Nervix.FieldType.BYTES;
        long[] offsets = variable ? new long[rows + 1] : null;
        var output = new java.io.ByteArrayOutputStream();
        String[] texts = {"", "a\u0000b", "héllo 世界 🚀"};
        for (int row = 0; row < rows; row++) {
            boolean missing = nullable && input.count(3) == 1;
            if (missing) states[row] = 2;
            byte[] value;
            if (variable) {
                value = missing ? new byte[0] : type == Nervix.FieldType.STRING
                    ? texts[input.count(texts.length)].getBytes(StandardCharsets.UTF_8) : input.bytes(input.count(17));
            } else {
                value = missing ? new byte[width(type)] : input.bytes(width(type));
                if (type == Nervix.FieldType.BOOL) value[0] &= 1;
            }
            output.writeBytes(value);
            if (variable) offsets[row + 1] = output.size();
        }
        return new Column(states, offsets, output.toByteArray(), variable, 0);
    }
    private static void compare(Nervix.Batch batch, int index, Column expected) {
        require(Arrays.equals(batch.states(index), expected.states()), "all row states");
        if (expected.variable()) {
            Nervix.Varlen value = batch.varlen(index, expected.leaf());
            require(Arrays.equals(value.offsets(), expected.offsets()), "all variable offsets");
            require(Arrays.equals(value.data(), expected.data()), "all variable bytes");
        } else {
            require(Arrays.equals(batch.fixed(index, expected.leaf()), expected.data()), "all fixed bits");
            if (expected.offsets() != null) require(Arrays.equals(batch.offsets(index, 0), expected.offsets()), "all list offsets");
        }
    }
    public static void columnsRoundTrip(byte[] bytes) {
        Input input = new Input(bytes); int rows = input.count(7); var columns = new ArrayList<Column>();
        var described = new ArrayList<Expected>();
        try (var fields = new Nervix.Fields()) {
            for (Nervix.FieldType type : Nervix.FieldType.values()) {
                if (type == Nervix.FieldType.LIST || type == Nervix.FieldType.FIXED_LIST) continue;
                boolean nullable = input.count(2) == 1, sensitive = input.count(2) == 1;
                String name = "field_" + columns.size();
                fields.add(name, type, 0, nullable, sensitive);
                described.add(new Expected(name, List.of(new Nervix.FieldLevel(type, 0)), nullable, sensitive));
                columns.add(scalar(input, type, rows, nullable));
            }
            fields.add("list", Nervix.FieldType.LIST, 0, false, false).element(Nervix.FieldType.I16, 0);
            described.add(new Expected("list", List.of(new Nervix.FieldLevel(Nervix.FieldType.LIST, 0), new Nervix.FieldLevel(Nervix.FieldType.I16, 0)), false, false));
            long[] offsets = new long[rows + 1];
            for (int row = 0; row < rows; row++) offsets[row + 1] = offsets[row] + input.count(5);
            byte[] present = new byte[rows]; Arrays.fill(present, (byte) 1);
            columns.add(new Column(present, offsets, input.bytes(Math.toIntExact(offsets[rows] * 2)), false, 1));
            int length = 1 + input.count(4);
            fields.add("array", Nervix.FieldType.FIXED_LIST, length, false, false).element(Nervix.FieldType.DATETIME, 0);
            described.add(new Expected("array", List.of(new Nervix.FieldLevel(Nervix.FieldType.FIXED_LIST, length), new Nervix.FieldLevel(Nervix.FieldType.DATETIME, 0)), false, false));
            columns.add(new Column(present, null, input.bytes(rows * length * 8), false, 1));
            try (var schema = fields.schema(); var builder = new Nervix.BatchBuilder(schema, rows)) {
                for (int index = 0; index < columns.size(); index++) {
                    Column column = columns.get(index); builder.states(index, column.states());
                    if (column.variable()) builder.varlen(index, column.leaf(), column.offsets(), column.data());
                    else {
                        if (column.offsets() != null) builder.offsets(index, 0, column.offsets());
                        builder.fixed(index, column.leaf(), column.data());
                    }
                }
                ByteBuffer view;
                byte[] ipc;
                try (var batch = builder.finish(); var copy = Nervix.Batch.fromIpc(schema, rows, batch.ipc()); var copiedSchema = copy.schema()) {
                    require(copy.rows() == rows && batch.rows() == rows, "complete row count");
                    sameFields(described, schema.fields(Nervix.Part.ROWS).toArray());
                    sameFields(described, copiedSchema.fields(Nervix.Part.ROWS).toArray());
                    for (int index = 0; index < columns.size(); index++) { compare(batch, index, columns.get(index)); compare(copy, index, columns.get(index)); }
                    require(Arrays.equals(batch.ipc(), copy.ipc()), "canonical IPC bytes");
                    view = batch.ipcBuffer().slice().asReadOnlyBuffer().duplicate(); ipc = batch.ipc();
                }
                byte[] retained = new byte[view.remaining()]; view.get(retained);
                require(Arrays.equals(ipc, retained), "derived direct buffer owns its batch after close");
            }
        }
    }
}
