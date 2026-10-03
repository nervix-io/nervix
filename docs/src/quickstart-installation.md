# Installation

From a [clone of the Nervix repository](developing-nervix.md#clone-the-repository), install the
server, CLI, and NSPL formatter:

```bash
just install
```

The recipe fetches the required ONNX Runtime artifact from R2 or reuses its verified local copy.
An unavailable artifact fails installation; source compilation is a maintainer task.
Ensure Cargo's binary directory, `$HOME/.cargo/bin` by default, is on your `PATH`.

See [Installation](installation.md) for prerequisites, reproducible Git revisions, a three-node
Docker deployment, and the Kubernetes Operator installation.
