# Cryptography

This chapter owns the cryptography Nervix runs: the boundary its own code takes cryptography from,
the algorithm policy, the register of non-security hashes, the inventory of cryptography outside
AWS-LC in the product graph, the `non_aws_lc_crypto` count, and what the later FIPS 140-3 migration
will refuse. The Cryptography section of `AGENTS.md` states the rules every change follows, and this
chapter defines them.

Nervix is preparing for FIPS 140-3; it is not a FIPS build. The preparation moves every
cryptographic decision Nervix owns onto one AWS-LC boundary while the stored shapes those decisions
reach are still cheap to change, so that the migration only has to replace the module underneath.
The rules bind every change from the moment they land, including work in flight on other branches:
a change that raises the count fails `just ratchet`.

## The Boundary

`nervix-crypto` is the single source of Nervix-authored cryptography. It is an engine-layer crate
built only on `aws-lc-rs` and `rustls`, and it knows no Nervix model, graph or connector. It
provides:

- the rustls `CryptoProvider`, built in one place and installed as the process default once, by
  purpose: the interconnect's fixed set of approved algorithms, and the provider of outward-facing
  TLS;
- digests used for security, such as SHA-256 and SHA-512;
- secure random values;
- password and key derivation.

The crate does not exist yet. Until it does, it is the target, and each authored use it will own is
debt the `non_aws_lc_crypto` count records. Nervix obtains that cryptography today as follows.

| Purpose | Where Nervix obtains it today |
| --- | --- |
| TLS provider | Three direct installs of AWS-LC's default provider: in the interconnect, in the connectors' shared TLS configuration and in the client core. The application's shared certificate/key constructor reads the installed default for native sessions and TLS VHOSTs. |
| Security digests | RustCrypto SHA-256, for the interconnect's trust-root fingerprint and the expression function `sha256()`. |
| Secure randomness | The operating-system generator of `rand_core`, for interconnect entropy and for password salts. |
| Password derivation | Argon2, for user passwords. |
| NATS user authentication | AWS-LC Ed25519 in the NATS connection owner; its nonce callback signs nkey and credentials-file authentication. This operation moves to the boundary when FIPS Prep 02 establishes it. |
| Other digests | BLAKE3, for backups, resources, checkpoints, archives, identities and fingerprints. No site is classified yet. |

## Algorithm Policy

Anything that protects confidentiality, integrity or authenticity uses an algorithm AWS-LC provides
and FIPS 140-3 approves, such as the SHA-2 family, HMAC, AES-GCM, PBKDF2, RSA and ECDSA signatures,
and ECDH over P-256 and P-384.

A non-approved hash serves only a non-security purpose. BLAKE3, MD5, SHA-1, xxHash and CRC are
non-approved. A purpose is non-security when its threat model has no adversary who can write the
hashed data: detecting corruption, identifying content, partitioning or detecting a change. A
purpose is a security purpose when the digest protects against tampering or establishes
authenticity, for example an integrity claim about an archive someone could modify, or a digest
compared with input an attacker supplies to authorise something. A security purpose takes SHA-256
from the boundary instead.

No change adds a dependency that brings a cryptography implementation other than AWS-LC into the
`nervix-server` product graph. A connector need that cannot be met is recorded on that connector's
epic, and the [inventory](#inventory-outside-aws-lc) lists what the product links today.

The product links one AWS-LC, so that FIPS brings one module. `aws-lc-rs` and librdkafka share it:
Nervix's fork of `rust-rdkafka` builds librdkafka's TLS against the `aws-lc-sys` that `aws-lc-rs`
links, with that crate's `ssl` feature, so an upgrade of `aws-lc-rs` that moves to another
`aws-lc-sys` moves the fork in the same change. A second `aws-lc-*-sys` crate, or a second version
of one, is a defect, and [the gate](#the-count) refuses it.

## Classification Register

A site that uses a non-approved hash records its non-security purpose where it is written, and this
register lists it with the site and the hash. The count recognises no recorded BLAKE3 purpose yet,
so it holds every BLAKE3 site as unclassified.

The NATS authentication wire codec in
`crates/connectors/nats/src/client/authentication.rs` uses CRC-16/XMODEM only to detect transcription
errors in a seed or public-key encoding, as the [NATS nkeys format](https://github.com/nats-io/nkeys/blob/main/strkey.go)
requires. It makes no authenticity decision from the checksum; AWS-LC signs the server nonce.

## Inventory Outside AWS-LC

The product graph is what the release image builds: `nervix-server` for
`x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`, with its normal and build dependencies.
Each row names cryptography that graph implements outside AWS-LC, the path that brings it from
`nervix-server`, what uses it and whether that use is a security purpose. A FIPS build refuses the
security rows until they move onto AWS-LC; the non-security rows stay.

Nervix's own crates bring:

| Crates | Path | Use | Purpose |
| --- | --- | --- | --- |
| `argon2`, with `blake2` | `nervix-server` | User password hashing | Security |
| `sha2` | `nervix-interconnect` | The trust-root fingerprint | Security |
| `sha2` | `nervix-vm` | The expression function `sha256()` | A digest users compute; moves to the boundary |
| `md5` | `nervix-vm` | The expression function `md5()` | Non-security checksum |
| `blake3` | `nervix-server`, `nervix-backup`, `nervix-consensus`, `nervix-models`, `nervix-roto` | Backup, resource, checkpoint and archive digests, identities and fingerprints | Unclassified |

Libraries bring:

| Crates | Path | Use | Purpose |
| --- | --- | --- | --- |
| `sha1`, `sha2`, `hmac`, `pbkdf2`, `md-5` | `nervix-connector-mongodb` → `mongodb` | SCRAM-SHA-1 and SCRAM-SHA-256 authentication | Security |
| `sha2`, `hmac`, `md-5` | `nervix-connector-postgres` → `sqlx` → `sqlx-postgres` | SCRAM-SHA-256 and MD5 password authentication | Security |
| `hkdf`, with `hmac` | `sqlx` → `sqlx-postgres` | Advisory-lock keys derived from lock names | Non-security |
| `sha2` | `sqlx` → `sqlx-core` | Migration checksums | Non-security |
| `sha1`, `sha2` | `nervix-connector-mysql` → `mysql_async` → `mysql_common` | `mysql_native_password` and `caching_sha2_password` authentication | Security |
| `sha2`, `hmac` | `nervix-connector-sqs` → `aws-config` → `aws-runtime` → `aws-sigv4` | SigV4 request signing | Security |
| `sha1` | `nervix-connector-sqs` → `aws-config` | SSO token cache file names | Non-security |
| `sha1`, `sha2`, `hmac`, `rsa` | `nervix-connector-iceberg` → `opendal` → `reqsign-core`, `reqsign-azure-storage`, `reqsign-google` | Object-storage request signing: S3 SigV4, Azure Shared Key and client certificates, and GCS service-account keys | Security |
| `sha1` | `opendal` → `reqsign-aws-v4` → `reqsign-aws-core` | SSO token cache file names | Non-security |
| `sha2`, `aes`, `cbc`, `pbkdf2`, `scrypt`, with `hmac` and `salsa20` | `reqsign-core` → `rsa` → `pkcs8` → `pkcs5` | Encrypted PKCS#8 private keys, and RSA's own digests | Security |
| `md-5` | `opendal` → `opendal-core`, `opendal-service-s3` | `Content-MD5` request headers | Non-security |
| `aes-gcm`, with `aes`, `ctr`, `ghash` and `polyval` | `nervix-connector-iceberg` → `iceberg` | Parquet modular encryption | Security |
| `ring` | `nervix-connector-iceberg` → `parquet` | Parquet modular encryption | Security |
| `des`, `rc2`, `cbc`, `hmac`, `sha1`, `sha2` | `nervix-connector-rabbitmq` → `lapin` → `amq-protocol` → `amq-protocol-tcp` → `tcp-stream` → `p12-keystore` | PKCS#12 client identities | Security |
| `sha1` | `tokio-tungstenite` → `tungstenite` | The WebSocket handshake's accept key | Non-security |
| `sha2` | `nervix-roto` → `roto` → `cranelift-codegen`; `nervix-wasm` → `wasmtime` → `wasmtime-environ` | Compilation cache keys and artifact checksums | Non-security |
| `chacha20`, `rand_chacha` | `rand`, through `chitchat`, `pulsar`, `apache-avro` and other libraries | The default generators of `rand` | Depends on the caller |

Native libraries that a crate of the graph builds from source are outside the count, which reads
crate names. librdkafka takes its TLS, the HMAC of SASL SCRAM and the signatures of OAUTHBEARER
client assertions from the product's one AWS-LC, and libcurl, which librdkafka uses for
OAUTHBEARER token requests, takes TLS and randomness from rustls on `aws-lc-rs`. They also bring:

| Code | Path | Use | Purpose |
| --- | --- | --- | --- |
| `rd_jitter`, librdkafka's `rand_r` generator seeded from the clock and the thread | `nervix-connector-kafka` → `rdkafka` → `rdkafka-sys` | SASL SCRAM client nonces | Security |
| MD5, SHA-256, SHA-512/256 and HMAC built into libcurl | `rdkafka-sys` → `curl-sys` | HTTP Digest authentication and AWS SigV4 signing | Unused: librdkafka's token requests ask for neither |

## The Count

`just ratchet` counts `non_aws_lc_crypto` and holds it at or below its entry in
`debt-baseline.json`, as [Architecture Rules And The Debt Ratchet](architecture-enforcement.md)
holds every count. The count has two parts.

**Authored uses.** The stable source parser reads every authored product file, inactive
configurations included. It skips test-only items, in-crate test files and test directories, and
`tools/` and the `arbitrary`, `benchmark`, `model-harness` and `test-environment` crates. A site is
a name a `use` declaration imports, an `extern crate` item, or a path of two or more segments,
inside a macro or outside one, that names:

- the `argon2`, `sha1`, `sha2`, `hmac` or `pbkdf2` crate, anywhere;
- the `blake3` crate, at every site, since the count recognises no classification yet;
- outside `crates/crypto`, the boundary's own directory: the `getrandom` crate, the
  operating-system generator `OsRng` of `rand_core` or of a crate that re-exports it, or
  `default_provider` in `aws_lc_rs` or in `rustls::crypto::aws_lc_rs`.

The first segment of a path is resolved through the imports and crate renames of its scope,
including what an enclosing module declares and a `self::` or `super::` path reaches, so a rename
does not hide a use. A glob import counts when it can bring a counted item into scope. Inside a
macro, whose tokens need not parse as Rust, each path joined by `::` counts once, at its first
segment, and a `$crate` path belongs to the macro. A one-segment name counts where its import names
it. The parser resolves no type, trait or cross-file re-export: a `crate::` path stays local, and a
use that reaches a counted crate only through another file's re-export counts at that re-export.

**Product dependencies.** The gate reads Cargo's resolved graph of `nervix-server` for both release
targets with `cargo tree`, which resolves features for the product alone, as the release build does.
Each normal or build dependency edge onto a crate on the list is one site, so a further dependent of
a crate the product already links counts too. The list, declared once in `nervix-ratchet gate`,
names crates implementing digests, message authentication, key derivation and password hashing,
ciphers and authenticated encryption, public-key algorithms and cryptographic random generators, and
complete cryptography libraries such as `ring` and OpenSSL. Crates that only define traits or
encodings, the entropy interfaces `getrandom` and `rand_core`, and non-cryptographic checksums are
not on it. AWS-LC is not on it either; a second copy of it is a defect, not debt.

The gate therefore refuses a product graph that does not link exactly one AWS-LC module: one
`aws-lc-sys`, or the FIPS module's `aws-lc-fips-sys`, in one version across both release targets.
An `aws-lc-rs` upgrade onto `aws-lc-sys` 0.47 that leaves the fork on 0.46 links two modules, and so
does `aws-lc-sys` beside `aws-lc-fips-sys`. The refusal names each module with the packages that
link it, so it says which dependency brought the second one, and no count or baseline admits it. A
graph without a module is refused too, since the product's cryptography comes from AWS-LC and such
a graph was not read correctly.

`just ratchet --show non_aws_lc_crypto` lists every site. An authored site shows its file, line and
source line, marked `(authored)`. A graph site points at the dependent's `Cargo.lock` entry and
shows the edge, marked `(graph)`. The baseline was measured when the rule landed, and a change may
lower the count but never raise it. `tools/nervix-lint/ratchet` tests pin both parts and the gate,
including an added dependency on `sha2` or `argon2`, a dependency that brings `ring`, and a second
AWS-LC module, whether a second version, the FIPS crate beside the default one or a module that
differs on one release target. The registered `typed-non-aws-lc-crypto` property checks every
generated authored location and the complete source and site representations.

## NATS Signing And The Preparation Boundary

NATS nkey and credentials-file authentication signs the server's nonce through AWS-LC Ed25519.
The connector uses async-nats' authentication callback, and the product does not enable that
driver's `nkeys` feature. The independent `nkeys` signer is a test-only interoperability oracle
and scenario fixture, outside the release product graph. Adding the callback therefore adds no
non-AWS-LC cryptography edge and leaves the count unchanged.

The signing operation currently lives in `crates/connectors/nats/src/client/authentication.rs`
while `nervix-crypto` is still a target. FIPS Prep 02 moves it into the boundary; wire decoding
continues to belong to the connector. This is preparation, with no FIPS-validation claim. The
pinned [aws-lc-rs Ed25519 implementation](https://github.com/aws/aws-lc-rs/blob/v1.18.2/aws-lc-rs/src/ed25519.rs)
marks signing as unsuitable for FIPS use, so the future FIPS mode must refuse these NATS methods
until its boundary has a validated signing service for the protocol. Token authentication has no
Nervix-owned nonce-signing operation.

## What The FIPS Migration Will Refuse

The migration enables the FIPS module underneath the boundary: `aws-lc-rs` with its FIPS feature on
`aws-lc-fips-sys`, librdkafka built against that same `aws-lc-fips-sys`, and the rustls FIPS
provider. The fork's build reads the metadata of whichever AWS-LC crate its backend selects, so
building librdkafka on the FIPS crate is a backend the fork adds, not a rewrite of its build. A
FIPS build refuses to start, and refuses a commit, when a resolved TLS policy names an algorithm
FIPS 140-3 does not approve. It refuses each security path of the inventory until that path moves
onto AWS-LC: password authentication through the MongoDB, Postgres and MySQL drivers, Kafka SASL
SCRAM while librdkafka draws its client nonce from `rd_jitter`, SQS and object-storage request
signing, RabbitMQ PKCS#12 client identities and Parquet encryption in Iceberg. Non-security uses,
the register's entries and the inventory's non-security and unused rows, are not refused.

Runtime checkpoint digests also identify retained Kafka start offers and immutable pending-seed
projections. Their purpose is non-security content checking: detecting changed, truncated or
mismatched transfer bodies inside authenticated node requests. The existing checkpoint digest
owner computes and verifies them under bounded execution and memory admission.
