# Resources

`RESOURCE` is Nervix's versioned file-distribution primitive.

A resource is an uploaded directory tree that is stored and replicated across the cluster. It is used for binary or multi-file assets that runtime nodes need locally, such as:

- ONNX models
- protobuf descriptor trees
- CSV lookup tables
- TLS certificate bundles

This chapter describes the NSPL statements and their observable behavior. See
[Resource Versions And Bindings](./resource-versions.md) for the architecture behind them: how a
version is installed and completed on every node, what each binding loads and when, and how a
rebinding is validated, classified, and applied.

## Domain Ownership

A resource belongs to the domain it was created in, like every other entity. `CREATE RESOURCE`, `UPLOAD RESOURCE`, and `DESCRIBE RESOURCE` all act on the session's selected domain, and a model resolves a resource name only against versions completed in its own domain.

The same resource name in two domains is two independent resources: they have separate version sequences, separate stored content, and separate replica state. Referring to a name that was never created in the selected domain fails with `resource '<name>' does not exist`, even when another domain has it.

## Lifecycle

Resources are managed in two phases:

```nspl,ignore
CREATE [IF NOT EXISTS] RESOURCE <name>;
UPLOAD RESOURCE <name> VERSION '<local_directory>';
DESCRIBE RESOURCE <name>;
DESCRIBE RESOURCE <name> VERSION <n>;
REBIND RESOURCE <name> TO VERSION <n>|LATEST
  [FOR <kind> <name> [, <kind> <name> ...]];
```

`CREATE RESOURCE` registers the logical resource name.

With `IF NOT EXISTS`, creating an already-registered resource becomes a successful no-op instead of an error.

`UPLOAD RESOURCE` uploads a local directory recursively and returns the assigned numeric version
after every current live node incarnation has verified and atomically installed the same archive
digest. A command that references the version can run immediately through any live node after the
upload succeeds.

Clients give each administrative upload an identity and retain it across redirects and transport
retries. Reusing an identity for the same user, domain, and resource returns its existing assigned
version and terminal outcome. Reusing an identity with different archive bytes fails with a digest
conflict. Once the client has supplied the complete declared archive and the cluster admits it,
installation continues if that request disconnects. An interrupted partial body has not admitted a
complete upload and can be sent again with the same identity.

`DESCRIBE RESOURCE <name>` reports `latest` as the highest completed version, or `(none)` when no
upload has completed. Its `versions` list and version details include every version whose metadata
has been published, including a version whose upload is still applying or finished with failure.
This keeps incomplete upload diagnostics visible without making that version eligible for use.
Its `usages` section lists every model currently bound to the resource and its pinned version.

`DESCRIBE RESOURCE <name> VERSION <n>` shows the detailed state for one version, including:

- content checksums
- total file count and size
- each node's replica state, process incarnation, and installation diagnostics
- the models pinned to that specific version

Descriptions are observations. They are useful for diagnosis and recovery visibility, but a caller
does not poll them to finish an upload.

## Versioning

Versions are monotonically increasing integers assigned by the cluster leader per domain and
resource name. A resource binding may select only a completed version, and every binding names the
version it selects with a mandatory `VERSION <n>` or `VERSION LATEST` clause. An explicit
`VERSION <n>` fails while that upload is applying and remains unavailable if the upload fails.
`VERSION LATEST` selects the highest completed version, so a newer applying or failed version does
not displace the last usable one. A resource with no completed version cannot be bound.

`LATEST` is resolved when the statement is applied, and the stored model keeps the resolved number.
A standalone statement resolves it immediately. A statement queued in a transaction resolves it
provisionally when it is admitted, so the queued prefix can be validated, and again when `COMMIT`
applies it; a version that completes in between is the one `COMMIT` binds. The command result names
the version each `LATEST` resolved to, `SHOW CREATE` renders the stored number, and no later upload
moves an existing binding.

`latest` in `DESCRIBE RESOURCE` is the version `VERSION LATEST` would select at that moment.

## Rebinding Existing Models

`REBIND RESOURCE` moves existing bindings without changing the resource catalog:

```nspl,ignore
REBIND RESOURCE fraud_model TO VERSION LATEST;
REBIND RESOURCE fraud_model TO VERSION 3
  FOR INFERENCER score_model, HASH MAP scores_by_id, CLIENT model_store;
```

Without `FOR`, every usage in the selected domain is included. With `FOR`, every member is
kind-qualified and must exist and already bind that resource. The supported kinds are `VHOST`,
`CODEC`, `SIGNALING PROTOCOL`, `INFERENCER`, `WASM PROCESSOR`, `HASH MAP`, and `CLIENT`. Duplicate
members are rejected, and written member order has no semantic effect.

Nervix resolves the target from one captured planning snapshot, rebuilds every selected model,
runs the same model and external-file validation used by `CREATE`, and commits the resulting model
replacements as one mutation batch. If one selected model fails validation, none of the bindings
move. A stopped domain still stores the replacements without a runtime pause. An already-pinned
selection succeeds at `DYNAMIC`, reports every selected usage as `unchanged`, and writes no model.

`LATEST` follows the same timing as other model bindings: immediate for a standalone command,
provisional during transaction admission, and resolved again when `COMMIT` plans the queued
statement. The result reports the number of changed and selected usages, the effective quiesce
level, and one sorted `from`/`to` line per selected model.

```text
rebound 2 of 3 usage(s) of resource 'fraud_model' to version 3 (latest)
quiesce level: DOMAIN_PAUSE
- kind=client name=model_store from=3 to=3 unchanged
- kind=inferencer name=score_model from=1 to=3
- kind=hash_map name=scores_by_id from=2 to=3
```

Rotate a resource by uploading the replacement first and then moving its usages. The upload must
complete everywhere before the rebind can select it:

```nspl,ignore
UPLOAD RESOURCE fraud_model VERSION './models/2026-09-17';
REBIND RESOURCE fraud_model TO VERSION LATEST;
```

The rebinding runs at the level of the usages it moves. Rotating a VHOST certificate this way is
`DYNAMIC`: every node's HTTPS listener installs the new bundle before the command succeeds,
established connections keep their session, new connections present the new certificate, and
ingestion does not pause. The refresh also applies while the domain is stopped. An inferencer or
WASM processor usage pauses only that entity, and a protobuf codec, signaling protocol, hash map, or
client mount usage pauses the domain. A WASM processor moved to another version starts every branch
without guest state. If any node's listener cannot install the new certificate, the rebinding
fails; when none of its usages paused, as when it moves only VHOSTs, every usage keeps its previous
version.

## Upload Format

The client builds a deterministic tar archive of the directory's subdirectories and regular files,
skipping symbolic links and other special files, declares its exact size, and sends it in bounded
chunks. Every node verifies the archive against its digest and the per-version limits below before
it atomically installs the version, so a failed install leaves no partial version behind. An upload
never changes the version an existing binding uses. See
[Resource Versions And Bindings](./resource-versions.md#version-lifecycle) for installation,
transfer between nodes, and completion.

The per-version limits are configured with `NERVIX_RESOURCE_MAX_ARCHIVE_BYTES`, `NERVIX_RESOURCE_MAX_EXTRACTED_BYTES`, and `NERVIX_RESOURCE_MAX_FILE_COUNT`. Their defaults are 4 GiB, 16 GiB, and 1,000,000 files. Every node enforces them for each version it installs.

## TLS Bundles

The first runtime integration for resources is `VHOST` TLS. A VHOST presents the bundle version it
binds on the HTTPS listener of every node, and `REBIND RESOURCE` moves it to another version
without pausing the domain.

A TLS resource bundle must contain these files at its root:

- `tls.crt`
- `tls.key`
- `ca.crt`

This matches the common cert-manager layout.

<a id="client-config-mounts"></a>

## Client Config Mounts

Client configs can mount a resource version into a temporary directory at runtime and then refer to files inside that mount from normal key-value settings.

Declare the mount directly on the client:

```nspl,ignore
MOUNT <resource_name> VERSION <n>|LATEST
```

Every mount includes a version. A number binds that exact completed resource version. With
`VERSION LATEST`, Nervix resolves the highest completed version when the client definition is
applied and stores the resulting number. Later uploads and restarts therefore keep using the same
files, and `SHOW CREATE CLIENT` renders the stored number.

Inside other client config values, Nervix renders values with a lightweight Jinja-like template language. The mounted resource path is exposed as a template variable named after the mounted resource:

```nspl,ignore
{{ <resource_name> }}
```

Example:

```nspl
CREATE IF NOT EXISTS CLIENT kafka_tls
  TYPE KAFKA
  MOUNT dev_tls VERSION 1
  CONFIG {
    'bootstrap.servers' = '127.0.0.1:9094',
    'security.protocol' = 'ssl',
    'ssl.ca.location' = '{{ dev_tls }}/ca.pem'
  };
```

Nervix creates one temporary mount root per instantiated client and keeps it alive for the lifetime of the ingestor or emitter using that client.

## TLS Client Config Pattern

The general pattern for TLS-enabled external clients is:

1. upload a resource containing the PEM files you want to use
2. mount that resource on the client with `MOUNT <resource_name> VERSION <n>|LATEST`
3. reference mounted paths from the client config template

Example:

```nspl
CREATE IF NOT EXISTS RESOURCE dev_tls;
UPLOAD RESOURCE dev_tls VERSION './tls/dev';

CREATE IF NOT EXISTS CLIENT redis_tls
  TYPE REDIS
  POOL SIZE MIN 1 MAX 4
  MOUNT dev_tls VERSION 1
  CONFIG {
    'addr' = 'rediss://127.0.0.1:6380/',
    'tls_ca_file' = '{{ dev_tls }}/ca.pem'
  };
```

Common Nervix-managed TLS config keys:

- `tls_ca_file`: PEM-encoded CA bundle used to trust the remote server
- `tls_cert_file`: PEM-encoded client certificate for mTLS
- `tls_key_file`: PEM-encoded client private key for mTLS

When supported by the client type, `tls_cert_file` and `tls_key_file` must be supplied together.

Client types that currently support TLS-oriented configuration through mounted resources:

- Kafka
- HTTP
- Sentry
- OTEL
- Prometheus
- WebSockets
- MQTT
- NATS
- Pulsar
- RabbitMQ
- Redis
- SQS
- Syslog

`ZEROMQ` remains plain pass-through transport configuration and does not currently expose a Nervix-specific TLS helper surface.

Pulsar currently supports mounted `tls_ca_file` server trust configuration, but not mounted client certificate authentication.
