package io.nervix.client;

/** Verifies admitted JNI calls survive close reentered from the library's class loader. */
public final class ReentrantClose {
    private ReentrantClose() {}
    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
    /** Class loading can call close again on the same thread while a JNI getter borrows state. */
    public static void main(String[] args) throws Exception {
        final class Loader extends java.net.URLClassLoader {
            AutoCloseable schema;
            Object state;
            java.lang.reflect.Field pointer;
            java.lang.reflect.Method read;
            Object rows;
            boolean armed;
            boolean closed;
            Loader(java.net.URL jar) {
                super(new java.net.URL[] {jar}, ClassLoader.getPlatformClassLoader());
            }
            @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (armed && name.equals("io.nervix.client.Nervix$Field")) {
                    armed = false;
                    try {
                        require(Thread.holdsLock(state), "class loading reenters the active handle monitor");
                        require(java.util.Arrays.stream(Thread.currentThread().getStackTrace()).anyMatch(frame ->
                            frame.getClassName().equals("io.nervix.client.Native") && frame.isNativeMethod()),
                            "the native getter still holds its borrow");
                        try {
                            read.invoke(schema, rows);
                            throw new AssertionError("nested native access succeeded");
                        } catch (java.lang.reflect.InvocationTargetException failure) {
                            require(failure.getCause() instanceof IllegalStateException,
                                "nested native access is refused while a borrow is live");
                        }
                        schema.close();
                        closed = true;
                        System.out.println("reentrant close: native borrow active; pointer after close="
                            + pointer.getLong(state));
                        // Keep a broken implementation from returning to a freed native borrow.
                        if (pointer.getLong(state) == 0) {
                            throw new ClassNotFoundException("close released the allocation during its native getter");
                        }
                    } catch (Exception failure) {
                        throw new ClassNotFoundException("reentrant close failed", failure);
                    }
                }
                return super.loadClass(name, resolve);
            }
        }
        var jar = java.nio.file.Path.of(args[0]).toUri().toURL();
        try (var loader = new Loader(jar)) {
            Class<?> fieldsType = loader.loadClass("io.nervix.client.Nervix$Fields");
            Class<?> fieldType = loader.loadClass("io.nervix.client.Nervix$FieldType");
            Object fields = fieldsType.getConstructor().newInstance();
            try {
                var add = fieldsType.getMethod("add", String.class, fieldType, long.class, boolean.class, boolean.class);
                add.invoke(fields, "first", fieldType.getField("U64").get(null), 0L, false, false);
                add.invoke(fields, "second", fieldType.getField("STRING").get(null), 0L, false, false);
                Object schema = fieldsType.getMethod("schema").invoke(fields);
                loader.schema = (AutoCloseable) schema;
                var state = schema.getClass().getSuperclass().getDeclaredField("state");
                state.setAccessible(true); loader.state = state.get(schema);
                loader.pointer = loader.state.getClass().getDeclaredField("pointer");
                loader.pointer.setAccessible(true);
                Class<?> partType = loader.loadClass("io.nervix.client.Nervix$Part");
                var read = schema.getClass().getMethod("fields", partType);
                Object rows = partType.getField("ROWS").get(null);
                loader.read = read; loader.rows = rows;
                loader.armed = true;
                var values = (java.util.List<?>) read.invoke(schema, rows);
                require(loader.closed && values.size() == 2, "the admitted getter completes after reentrant close");
                Class<?> field = loader.loadClass("io.nervix.client.Nervix$Field");
                require(field.getMethod("name").invoke(values.getFirst()).equals("first"), "first field survives close");
                require(field.getMethod("name").invoke(values.getLast()).equals("second"), "second field survives close");
                require(loader.pointer.getLong(loader.state) == 0, "the last admitted call releases the allocation");
                System.out.println("reentrant close: admitted getter finished; pointer released");
                try { read.invoke(schema, rows); throw new AssertionError("a new call on the closed schema succeeded"); }
                catch (java.lang.reflect.InvocationTargetException failure) {
                    require(failure.getCause().getClass().getName().equals("io.nervix.client.Nervix$ClosedException"),
                        "new calls observe typed closure");
                }
            } finally { ((AutoCloseable) fields).close(); }
        }
    }
}
