/* Ruby edge: owns immutable external-buffer flag composition, depends on Ruby's public C API,
 * and knows nothing about client sessions or the server. rb-sys's Rust enum cannot hold a
 * combination of flags; keep that bitmask operation in its declaring C language. */
#include <string.h>
#include <ruby.h>
#include <ruby/io/buffer.h>

VALUE nervix_ruby_readonly_buffer(void *base, size_t size)
{
    return rb_io_buffer_new(base, size,
        RB_IO_BUFFER_EXTERNAL | RB_IO_BUFFER_READONLY | RB_IO_BUFFER_LOCKED);
}

/* A new binary String owns its initialized output storage. It is not exposed to Ruby code
 * until the host has filled it synchronously, with the GVL held. */
VALUE nervix_ruby_binary_string(long length, unsigned char **bytes)
{
    VALUE string = rb_str_new(NULL, length);
    *bytes = (unsigned char *)RSTRING_PTR(string);
    memset(*bytes, 0, (size_t)length);
    return string;
}
