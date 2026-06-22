# Resources

`RESOURCE` is Nervix's versioned file-distribution primitive.

A resource is an uploaded directory tree that is stored and replicated across the cluster. It is used for binary or multi-file assets that runtime nodes need locally, such as:

- ONNX models
- protobuf descriptor trees
- CSV lookup tables
- TLS certificate bundles

## Lifecycle

Resources are managed in two phases:

```nspl
CREATE [IF NOT EXISTS] RESOURCE <name>;
UPLOAD RESOURCE <name> VERSION '<local_directory>';
DESCRIBE RESOURCE <name>;
DESCRIBE RESOURCE <name> VERSION <n>;
```

`CREATE RESOURCE` registers the logical resource name.

With `IF NOT EXISTS`, creating an already-registered resource becomes a successful no-op instead of an error.

`UPLOAD RESOURCE` uploads a local directory recursively, assigns the next numeric version, and waits until that version is replicated to all alive nodes in the cluster.

`DESCRIBE RESOURCE <name>` shows the resource and the list of uploaded versions.

`DESCRIBE RESOURCE <name> VERSION <n>` shows the detailed state for one version, including:

- content checksums
- total file count and size
- whether the version is cluster-ready
- per-node replica state

## Versioning

Versions are monotonically increasing integers assigned by the cluster leader per resource name.

There is no `latest` keyword in NSPL. If a model omits a version and chooses "latest" behavior, that behavior belongs to the model semantics, not to the resource system itself.

## Upload Format

The client uploads a local directory, but Nervix transfers and stores it as a deterministic tar archive internally. The archive checksum is used as the resource content identity.

On each node, Nervix also unpacks the archive into a local directory tree so runtime code can resolve normal filesystem paths inside the resource.

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

```nspl
MOUNT <resource_name>
```

Inside other client config values, Nervix renders values with a lightweight Jinja-like template language. The mounted resource path is exposed as a template variable named after the mounted resource:

```nspl
{{ <resource_name> }}
```

Example:

```nspl
CREATE [IF NOT EXISTS] CLIENT kafka_tls
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
CREATE [IF NOT EXISTS] RESOURCE dev_tls;
UPLOAD RESOURCE dev_tls VERSION './tls/dev';

CREATE [IF NOT EXISTS] CLIENT redis_tls
  TYPE REDIS
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
- Prometheus
- WebSockets
- MQTT
- NATS
- Pulsar
- Kinesis
- RabbitMQ
- Redis
- SQS

`ZEROMQ` remains plain pass-through transport configuration and does not currently expose a Nervix-specific TLS helper surface.

Pulsar currently supports mounted `tls_ca_file` server trust configuration, but not mounted client certificate authentication.

Kinesis client configs can also use mounted `tls_ca_file` when targeting HTTPS-compatible local or private endpoints.
