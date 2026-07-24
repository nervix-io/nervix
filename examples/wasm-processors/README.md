# WASM Processor Examples

Build the guest modules before loading the NSPL examples:

```bash
just wasm-processor-guests
```

`wasm-dual.nspl` assumes it is executed from the repository root so the resource
upload paths resolve to the built Rust and Go guest artifacts. Both WASM
processors declare multiple `TO` routes so the examples cover guest-emitted
batches for more than one output relay.

The Rust guest is built on the high-level `nervix-wasm-sdk` crate in
`crates/nervix-wasm-sdk`; the Go guest implements the raw C ABI documented in
`INTERFACE.md` directly.
