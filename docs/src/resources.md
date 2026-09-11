# Resources

`RESOURCE` is Nervix's versioned file-distribution primitive.

A resource is an uploaded directory tree that is stored and replicated across the cluster. It is used for binary or multi-file assets that runtime nodes need locally, such as:

- ONNX models
- protobuf descriptor trees
- CSV lookup tables
- TLS certificate bundles

## Domain Ownership

A resource belongs to the domain it was created in, like every other entity. `CREATE RESOURCE`, `UPLOAD RESOURCE`, and `DESCRIBE RESOURCE` all act on the session's selected domain, and a model resolves a resource name only against versions published in its own domain.

The same resource name in two domains is two independent resources: they have separate version sequences, separate stored content, and separate replica state. Referring to a name that was never created in the selected domain fails with `resource '<name>' does not exist`, even when another domain has it.

## Lifecycle

Resources are managed in two phases:

```nspl,ignore
CREATE [IF NOT EXISTS] RESOURCE <name>;
UPLOAD RESOURCE <name> VERSION '<local_directory>';
DESCRIBE RESOURCE <name>;
DESCRIBE RESOURCE <name> VERSION <n>;
```

`CREATE RESOURCE` registers the logical resource name.

With `IF NOT EXISTS`, creating an already-registered resource becomes a successful no-op instead of an error.

`UPLOAD RESOURCE` uploads a local directory recursively and returns the assigned numeric version after the leader has durably installed the archive and committed its publication metadata. Replication to the other live nodes continues asynchronously. The upload result distinguishes publication from cluster readiness, so a published version can briefly report `cluster_ready: false`.

Clients give each administrative upload an identity and retain it across redirects and transport retries. Reusing an identity for the same user, domain, and resource returns its existing assigned version and publication outcome. Reusing a completed identity with different archive bytes fails with a digest conflict. This prevents an uncertain response from allocating another version.

Callers that must wait for every live node use the resource-readiness API with their own timeout. A timeout is an ordinary `cluster_ready: false` result that still includes the published version. `DESCRIBE RESOURCE <name> VERSION <n>` provides the same readiness state for polling clients.

`DESCRIBE RESOURCE <name>` shows the resource and the list of published versions.

`DESCRIBE RESOURCE <name> VERSION <n>` shows the detailed state for one version, including:

- content checksums
- total file count and size
- whether the version is cluster-ready
- per-node replica state

## Versioning

Versions are monotonically increasing integers assigned by the cluster leader per domain and resource name.

There is no `latest` keyword in NSPL. If a model omits a version and chooses "latest" behavior, that behavior belongs to the model semantics, not to the resource system itself.

## Upload Format

The client builds a deterministic tar archive, declares its exact size, and sends it in bounded chunks. Internode replication also streams bounded chunks with HTTP/2 flow control; neither endpoint retains the complete archive in memory. A failed transfer restarts from the beginning on its next reconciliation attempt.

On each node, Nervix verifies the archive digest, enforces the staged-archive quota, then enforces extracted-byte and file-count quotas from tar headers before writing each entry. It writes into a staging tree, verifies the manifest, and atomically promotes the complete version. Failed and cancelled installs remove their staging trees, and startup removes staging trees abandoned by an interrupted process.

The per-version limits are configured with `NERVIX_RESOURCE_MAX_ARCHIVE_BYTES`, `NERVIX_RESOURCE_MAX_EXTRACTED_BYTES`, and `NERVIX_RESOURCE_MAX_FILE_COUNT`. Their defaults are 4 GiB, 16 GiB, and 1,000,000 files.

## TLS Bundles

The first runtime integration for resources is `VHOST` TLS.

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
MOUNT <resource_name>
```

Inside other client config values, Nervix renders values with a lightweight Jinja-like template language. The mounted resource path is exposed as a template variable named after the mounted resource:

```nspl,ignore
{{ <resource_name> }}
```

Example:

```nspl
CREATE IF NOT EXISTS CLIENT kafka_tls
  TYPE KAFKA
  MOUNT dev_tls
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
2. mount that resource on the client with `MOUNT <resource_name>`
3. reference mounted paths from the client config template

Example:

```nspl
CREATE IF NOT EXISTS RESOURCE dev_tls;
UPLOAD RESOURCE dev_tls VERSION './tls/dev';

CREATE IF NOT EXISTS CLIENT redis_tls
  TYPE REDIS
  POOL SIZE MIN 1 MAX 4
  MOUNT dev_tls
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
