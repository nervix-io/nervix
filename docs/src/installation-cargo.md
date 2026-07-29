# Cargo Install From GitHub

Install the Nervix server and interactive CLI directly from the
[Nervix GitHub repository](https://github.com/nervix-io/nervix):

```bash
cargo install --locked \
  --git https://github.com/nervix-io/nervix.git \
  nervix-server nervix-cli
```

This builds both binaries from source and installs them into Cargo's binary directory, which is
`$HOME/.cargo/bin` by default. Ensure that directory is on your `PATH`.

The build requires Rust 1.97 or newer and the native build tools required by Nervix's dependencies.
The initial build can take several minutes.

Confirm that both commands are available:

```bash
nervix-server --help
nervix-cli --help
```

## Pin A Git Revision

The unpinned command installs the current default branch. For a reproducible installation, select a
reviewed commit:

```bash
cargo install --locked \
  --git https://github.com/nervix-io/nervix.git \
  --rev <commit-sha> \
  nervix-server nervix-cli
```

Run the same command with a different revision to upgrade. Add `--force` if Cargo reports that the
selected packages are already installed.

To remove both binaries:

```bash
cargo uninstall nervix-server nervix-cli
```
