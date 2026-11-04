# Cargo Install From GitHub

*Plexus, the Nervix graph definition language.*

Install the Nervix server, the interactive CLI, and the Plexus formatter directly from the
[Nervix GitHub repository](https://github.com/nervix-io/nervix):

```bash
cargo install --locked \
  --git https://github.com/nervix-io/nervix.git \
  nervix-server nervix-cli nervix-plexus-format
```

This builds the binaries from source and installs them into Cargo's binary directory, which is
`$HOME/.cargo/bin` by default. Ensure that directory is on your `PATH`.

The build requires Rust 1.99 or newer and the native build tools required by Nervix's dependencies.
The initial build can take several minutes.

Confirm that the commands are available:

```bash
nervix-server --help
nervix-cli --help
nervix-plexus-format --help
```

## Pin A Git Revision

The unpinned command installs the current default branch. For a reproducible installation, select a
reviewed commit:

```bash
cargo install --locked \
  --git https://github.com/nervix-io/nervix.git \
  --rev <commit-sha> \
  nervix-server nervix-cli nervix-plexus-format
```

Run the same command with a different revision to upgrade. Add `--force` if Cargo reports that the
selected packages are already installed.

## Native Session TLS

Provision a PEM certificate chain and matching private key for each node. Its leaf certificate
must cover the host in the advertised address, and clients need the issuing CA. Add these options
to the node's normal identity, storage and interconnect configuration:

On a fresh node, set `NERVIX_INIT_DEFAULT_USER_PASSWORD` to provision the `default` user. The CLI
prompts for this password when connecting.

```bash
nervix-server \
  --node-id node-1 \
  --allow-bootstrap \
  --interconnect-tls-ca /etc/nervix/cluster/ca.pem \
  --interconnect-tls-cert /etc/nervix/cluster/node-1.pem \
  --interconnect-tls-key /etc/nervix/cluster/node-1-key.pem \
  --grpc-mode https \
  --grpc-https-listen-addr 0.0.0.0:47390 \
  --grpc-https-advertise-addr nervix.example.com:47390 \
  --grpc-tls-cert /etc/nervix/session/node-1.pem \
  --grpc-tls-key /etc/nervix/session/node-1-key.pem
```

The corresponding variables are `NERVIX_GRPC_MODE`, `NERVIX_GRPC_HTTPS_LISTEN_ADDR`,
`NERVIX_GRPC_HTTPS_ADVERTISE_ADDR`, `NERVIX_GRPC_TLS_CERT`, and `NERVIX_GRPC_TLS_KEY`. The server
requires explicit material for HTTPS and refuses TLS files in HTTP mode. Unreadable, malformed or
mismatched material fails startup. To rotate the certificate, replace its files and restart the
node; neither the native listener nor the console watches certificate files.

Connect with the [CLI](client-tools-cli.md#connecting):

```bash
nervix-cli --server https://nervix.example.com:47390 --tls required --tls-ca-cert ./session-ca.pem
```

The console uses its own `--web-console-https-listen-addr`, `--web-console-tls-cert` and
`--web-console-tls-key`. When enabled, `--web-console-advertise-addr` must name its reachable
HTTPS host and port, so follower redirects lead browsers to the TLS listener.

## Uninstall

To remove the installed binaries:

```bash
cargo uninstall nervix-server nervix-cli nervix-plexus-format
```
