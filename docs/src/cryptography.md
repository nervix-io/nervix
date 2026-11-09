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
using `aws-lc-rs` and `rustls` for cryptography, the primitive boundary for shared ownership and
initialization, and shared error-reporting contracts. It knows no Nervix model, graph or connector.
It owns the scope of:

- the rustls `CryptoProvider`, built in one place and installed as the process default once, by
  purpose: the interconnect's fixed set of approved algorithms, and the provider of outward-facing
  TLS;
- digests used for security, such as SHA-256 and SHA-512;
- secure random values;
- password and key derivation.

The crate owns TLS provider selection, SHA-256 and SHA-512 digests, secure randomness, ECDSA
P-256/SHA-256 and Ed25519 signing, and PBKDF2-HMAC-SHA-512 password derivation. Unclassified hashes
still have the counted call sites below.
The one-shot digests use `aws_lc_rs::digest`. Incremental SHA-256 wraps the maintained native
`SHA256_CTX` from the same AWS-LC module, keeping its state on the stack; the EVP context used by
`aws_lc_rs::digest::Context` allocates algorithm state. The direct `aws-lc-sys` pin moves together
with `aws-lc-rs` and librdkafka, preserving the single-module rule. With the boundary's `fips`
feature the context comes from `aws-lc-fips-sys`, the module `aws-lc-rs` then runs on, so such a
build references the ordinary module nowhere.

| Purpose | Where Nervix obtains it today |
| --- | --- |
| TLS provider | `nervix-crypto` installs the outward provider through rustls's process-default installation and supplies an independent checked interconnect provider. Connector TLS, native sessions and TLS VHOSTs retain the outward provider; SQS projects the boundary's typed outward choice into Smithy's mode, preserving the SDK's cipher filtering. |
| Security digests | `nervix_crypto::digest` obtains SHA-256 and SHA-512 from AWS-LC. The interconnect trust-root fingerprint and the expression function `sha256()` use this boundary. |
| Digest comparison | `nervix_crypto::digest::equal` compares equal-width digest arrays through AWS-LC's constant-time comparison. |
| ECDSA signatures and keys | `nervix_crypto::ecdsa` owns P-256/SHA-256 fixed `r‖s` signing and verification, PKCS#8 v1 key generation/import/export, and uncompressed public-point coordinates. |
| Backend status | `nervix_crypto::provider::module_mode` reports the linked AWS-LC module's runtime FIPS status as `ModuleMode`, independently of TLS algorithm selection. |
| Secure randomness | `nervix_crypto::random` obtains bytes and unsigned 64-bit values from AWS-LC's operating-system-seeded generator, for interconnect entropy and password salts. |
| Password derivation | `nervix_crypto::password` uses AWS-LC PBKDF2-HMAC-SHA-512 and its constant-time verifier. |
| NATS user authentication | `nervix-crypto` prepares the Ed25519 signing key and signs the nonce with AWS-LC. The NATS connector owns seed and credentials decoding, public-key encoding and the driver callback. |
| Other digests | BLAKE3, for the non-security content checks, identities, fingerprints, sampling and sketches in the classification register below. |

## Algorithm Policy

The boundary's random fill returns only a complete value. AWS-LC's `RAND_bytes` contract aborts
if sufficient entropy cannot be obtained, and otherwise returns success; it provides no partial,
default or deterministic substitute. The transport still accepts an environment-owned entropy
callback for deterministic simulation, without changing the production cryptographic generator.
Password credentials carry a 128-bit salt and a 64-byte digest as fixed byte arrays, with a typed algorithm and validated iteration count. No text password-hash format is stored.

The interconnect uses TLS 1.3 with `TLS_AES_256_GCM_SHA384` and
`TLS_AES_128_GCM_SHA256`, and the `secp256r1`, `secp384r1` and `X25519MLKEM768` key
exchange groups. It preserves AWS-LC's preference order among those members. The pinned rustls
0.23.46 classifies the hybrid through its first, ML-KEM component when the backend is in FIPS mode;
standalone X25519 and ChaCha20 are outside this set. Every client/server bundle checks its provider
against the fixed set and keeps TLS 1.3, mutual certificate authentication and its certificate clock.
This selects algorithms; the ordinary AWS-LC build is not a validated FIPS module.

Outward TLS keeps the full AWS-LC default algorithm set. Rustls installs that process default
once and retains a provider installed by the embedding host. The interconnect's independent
immutable provider is cached once per process, so neither install order nor outward host policy
changes its algorithm set. SQS retains Smithy's existing AWS-LC mode and its additional cipher
filter, rather than using a custom-provider hook that bypasses that filter. Protocol negotiation,
trust roots and client identities remain with their existing TLS owners.

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

The Credentials memory class has 19 MiB shared by password work and mounted NATS credentials
files. Each password operation charges its 64-byte verifier buffer; a file charges its complete
length until the temporary contents are cleared.

## Classification Register

A site that uses a non-approved hash records its non-security purpose where it is written, and this
register lists it with the site and the hash. A BLAKE3 operation, import or field records a Rustdoc
line beginning `Non-security BLAKE3:` followed by its reason. The stable source parser reads that
comment as a `doc` attribute. Ordinary body comments, string contents, empty reasons, and comments
on crates, modules, implementations, structs or enums do not classify their children. Nested items
and fields require their own reasons. The classification applies only to BLAKE3; it cannot excuse
another counted algorithm, entropy source or TLS-provider installation.

```rust
/// Non-security BLAKE3: checks transfer consistency; the interconnect authenticates the peer.
fn checkpoint_checksum(bytes: &[u8]) -> blake3::Hash {
    blake3::hash(bytes)
}
```

Review establishes the truth of the reason. The parser checks its written form and lexical owner,
not the threat model. A new security purpose moves that operation to the boundary's SHA-256 rather
than keeping its non-security classification.

All current authored BLAKE3 operations have the following non-security purposes. Classification
preserves their digest bytes, ordering, prefixes and stored representations.

| Owners | Purpose and trust boundary |
| --- | --- |
| `crates/backup/src/section.rs`: `SectionDigester` | Detect section corruption and mismatches against the manifest. The manifest is in the same archive, not a signature or trusted statement of origin. |
| `crates/client-core/src/backup.rs`: `DownloadFile`; `crates/web-console/src/backup_dialog/archive_download.rs`: `ArchiveReceipt` | Check download consistency against the selected server's summary. Session authentication and the configured transport establish the connection's trust. |
| `crates/client-core/src/restore.rs` and `crates/web-console/src/backup_dialog/restore_stream.rs`: `measure_archive` | Bind a restore attempt to the operator-selected file's bytes. The digest does not authenticate the file or authorize a restore. |
| `src/resource.rs`: archive staging, bundle construction, extraction and manifest checksums | Identify immutable content and detect mismatched copies. The authenticated user and committed catalog own the upload; the interconnect authenticates replicas. Archives and manifests are not signed. |
| `src/runtime/snapshot_staging.rs` and `snapshot_staging/encoding.rs` | Identify and measure an immutable staged artifact for its selected checkpoint or archive section. Staging ownership and the selected descriptor determine where it belongs. |
| `src/runtime/state_checkpoint_transfer.rs` | Compare fetched bytes with the exact descriptor selected over the authenticated interconnect. Peer and placement admission are independent. |
| `src/runtime/state_store/{generation,checkpoint_reader,checkpoint_stream,backup}.rs` | Check segmented checkpoint consistency against node-owned metadata during reading, publication and restore. The database and installation authority select the generation; the digest does not authenticate an adversarially rewritten store. |
| `crates/models/src/schema_fingerprint.rs` and `src/registry/graph.rs` | Identify schema-dependent state and domain-start generations. Configuration admission and schema validation happen independently. |
| `crates/models/src/window_model_digest.rs` | Identify a canonical window definition for state matching, not authorize it. |
| `crates/models/src/quiesce/impact.rs`: `BranchKeyFingerprint` | Identify a typed branch and derive its partition seed. The fingerprint neither encrypts nor authenticates branch values. |
| `crates/models/src/command.rs`: `CommandExecutionReference::derive_step`; `src/runtime/wasm_guest_state_reset.rs` | Derive bounded deterministic statement/reset identities. Command ownership and the selected branch/generation scope the operation; the digest is not a credential or secret capability. |
| `crates/models/src/udf.rs` and `crates/nervix-roto/src/lib.rs` | Identify admitted UDF source and check stored source/hash consistency. Declaration, compilation and execution policy decide what may execute; the hash is not a code signature. |
| `crates/consensus/src/transaction.rs` and `src/application/command_execution.rs` | Detect changed repetitions of an authenticated owner's request. Owner identity, reference admission and password verification are separate checks; digest equality grants no authority. |
| `crates/consensus/src/transaction_report.rs`, `src/application/transaction.rs` and `src/registry/execution_revision.rs` | Identify topology, planning inputs and scheduled revisions for deduplication, freshness and predecessor matching in the trusted control plane. |
| `src/application/{configured_choices,session_service}.rs` | Bind a page to its query, candidates and configuration revision. Cursors check freshness; authentication and candidate selection run independently. |
| `src/registry/ingestor_plan.rs` and `src/application/session/consumers.rs` | Detect changes to a client endpoint's schema and execution contract. Authentication, domain generations and endpoint admission remain separate. |
| `src/application/tls.rs` | Detect changes to operator-owned certificate/key files. Rustls validates and uses the material; the fingerprint does not validate certificates or authenticate peers. |
| `src/domain_clock_authority.rs` | Choose a clock authority deterministically from admitted live identities. Membership and clock authority validation are separate. |
| `src/application/subscription.rs`: `SubscriptionSampler::passes` | Distribute sampling draws from a sequence and branch key. This is not security randomness or an access-control decision. |
| `src/runtime/window_accumulator/sketches.rs`: `HyperLogLog::admit` | Distribute values into sketch registers for approximate cardinality. |
| `src/registry/scheduler.rs`: `RandomAssignmentPlanner`; `crates/consensus/src/storage/generators.rs`; `src/runtime/state_store/store_properties.rs`: `GeneratedCheckpoint::metadata` | Deterministic test placement and controlled generated fixture bytes, not security randomness. |

An archive or resource checksum is not a tamper-proof seal. Someone able to replace both the data
and its checksum can construct a different, self-consistent input. Parsing, path and quota checks,
authenticated command ownership, and transport security retain their own contracts. Operators
establish the provenance of files they restore. No current BLAKE3 operation requires a
security-digest migration under this trust model.

The inferencer model identity in `src/registry/execution_revision.rs` uses the boundary's SHA-256
over the resource, pinned version, file and ordered `STATE` declarations. This is a non-security
cache and placement identity; it neither authenticates model contents nor authorizes a resource.
The vocabulary carries only the resulting `InferencerModelFingerprint` bytes.

The expression function `md5()` in `crates/nervix-vm/src/runtime.rs` is a non-security checksum
over UTF-8 text. It establishes no authenticity, authorization or collision-resistance guarantee;
expressions needing collision resistance use the boundary-backed `sha256()` function.

The maintained format-only [NKey fork](https://github.com/nervix-io/nkeys), called by
`crates/connectors/nats/src/client/authentication.rs`, uses CRC-16/XMODEM only to detect transcription
errors in a seed or public-key encoding, as the [NATS nkeys format](https://github.com/nats-io/nkeys/blob/main/strkey.go)
requires. It makes no authenticity decision from the checksum; AWS-LC signs the server nonce.

`tests/web_console_restore_faults/unrecorded_failure.rs` uses BLAKE3 only to check that its controlled
test-generated archive bytes match the captured restore start. The fixture owns both inputs and
makes no authenticity or authorization decision. This test site is outside the authored product-use
scope of the count below.

## Inventory Outside AWS-LC

The product graph is what the release image builds: `nervix-server` for
`x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`, with its normal and build dependencies.
Each row names cryptography that graph implements outside AWS-LC, the path that brings it from
`nervix-server`, what uses it and whether that use is a security purpose. A FIPS build refuses the
security rows until they move onto AWS-LC; the non-security rows stay.

Nervix's own crates bring:

| Crates | Path | Use | Purpose |
| --- | --- | --- | --- |
| `md5` | `nervix-vm` | The expression function `md5()` | Non-security checksum |
| `blake3` | `nervix-server`, `nervix-backup`, `nervix-consensus`, `nervix-models`, `nervix-roto` | Content checks, identities, fingerprints, sampling and sketches recorded above | Non-security |

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
- the `blake3` crate, unless the operation, import or field records the non-security purpose
  described in the [classification register](#classification-register);
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
driver's `nkeys` feature. The organization-owned NKey fork is consumed with default features
disabled: only its format codec is present, with no signing, hashing or random backend. The driver
fork exposes borrowed credentials parsing independently of nonce signing. Both dependencies are
pinned to immutable commits. The published `nkeys` signer remains an independent test-only
interoperability oracle and scenario fixture, outside the release product graph. This arrangement
adds no non-AWS-LC cryptography edge. Seed decoding clears both its intermediate record and its
returned bytes; neither the codec nor credentials parser includes secret values in refusals.

The signing operation lives in `nervix_crypto::ed25519`.
`crates/connectors/nats/src/client/authentication.rs` owns wire decoding and maps the boundary's
typed errors into the driver's authentication callback. This is preparation, with no
FIPS-validation claim. The
pinned [aws-lc-rs Ed25519 implementation](https://github.com/aws/aws-lc-rs/blob/v1.18.2/aws-lc-rs/src/ed25519.rs)
marks signing as unsuitable for FIPS use, so the future FIPS mode must refuse these NATS methods
until its boundary has a validated signing service for the protocol. Token authentication has no
Nervix-owned nonce-signing operation.

## P-256 Signing And Private-Key Ownership

`nervix_crypto::ecdsa` uses AWS-LC's `ECDSA_P256_SHA256_FIXED_SIGNING` and
`ECDSA_P256_SHA256_FIXED`. Each signature is exactly 64 bytes: the 32-byte unsigned big-endian
`r` followed by the 32-byte `s`. AWS-LC hashes the complete message with SHA-256 and obtains
its signing nonce from its system-seeded generator; the caller supplies neither a digest nor a
nonce. Verification retains the typed backend refusal for malformed keys/signatures and altered
messages. `nervix_crypto::jws` owns the maintained JWS library's AWS-LC provider, ES256 signing and
verification, and RFC 7638 SHA-256 thumbprints. The [shared licensing protocol](./licensing-protocol.md)
owns its exact header/public-JWK profile, document shapes and trust policy.

Key generation and export return an unencrypted PKCS#8 v1 DER `Pkcs8PrivateKey`, whose maintained
AWS-LC document zeroizes its byte allocation on drop. `EcdsaP256KeyPair` owns the native key;
AWS-LC releases its private scalar through its cleansing allocator when the final native key
reference ends. Neither owner is cloneable or prints any key bytes in `Debug`. Callers borrowing
or copying private DER must protect and clear their own storage. Public extraction borrows the
backend's SEC 1 uncompressed point or returns its fixed-width `x` and `y` coordinates without
implementing another point encoder.

`digest::equal` uses AWS-LC's maintained constant-time primitive on equal-width arrays. Its work
does not depend on which digest byte differs; the width is public. `provider::module_mode` queries
the linked module with AWS-LC's runtime FIPS check. An approved algorithm or selected TLS suite
does not establish a validated backend or deployment. The ordinary Nervix build reports
`ModuleMode::Ordinary`. A service's FIPS qualification must select one consistent FIPS module
throughout its dependency graph and run these same vectors there. The boundary's `fips` feature
makes that selection beneath it: `aws-lc-rs` on `aws-lc-fips-sys`, the rustls FIPS provider, and
the native SHA-256 context on the same module, so the build links that module alone and
`module_mode` reports `ModuleMode::Fips`, which the boundary's own test asserts for each
configuration. The licensing service's FIPS configuration enables the feature; no Nervix build
does.

The boundary tests retain the complete P-256/SHA-256 section of the
[NIST CAVP FIPS 186-4 verification vectors](https://csrc.nist.gov/projects/cryptographic-algorithm-validation-program/digital-signatures),
including invalid signatures, changed messages and changed public keys. Registered bounded
properties cover message/signature bit changes and complete PKCS#8 round trips. These are algorithm
and representation checks; they do not claim a FIPS-qualified build. Cross-build signature
qualification belongs to the licensing service's FIPS lane.

The JWS adapter uses `jsonwebtoken` 11 with default features disabled and only `aws_lc_rs` enabled.
It installs that library's native AWS-LC provider once. A provider installed beforehand by an
external caller causes a typed refusal, rather than silently choosing the external default.
An exact permitted `unmodeled::sync::OnceLock` keeps only this process-initialization result;
it carries no licensing state or modeled protocol. Signing-key DER is owned by the maintained
library's zeroizing allocation and its `Debug` is redacted. Verification preserves the native typed
error beneath its context and disables generic JWT time/audience policy; callers supply their
own document validation. The protocol's `jose-jws`/`jose-b64` edges are format-only and enable no
cryptographic backend.

The consensus cluster identity stores its private DER in an owner that zeroizes on drop, alongside
the shared protocol's `PublicKey` and `KeyId`. Recovery re-derives both public values from each
private key. Observers receive a public projection. A leader coordinator receives `ClusterSigner`,
which wraps the existing `DocumentSigner`, exposes signing without key export, and prints only its
`kid` in `Debug`. The coordinator owns its leadership lifetime and supplies document limits and a
working-set reservation; signing runs on the bounded control executor with cancellation checks.
The maintained signer clears its owned private material. Raft persistence and replication carry
the identity; public backup archives carry only its source UUID as display metadata.

## What The FIPS Migration Will Refuse

The migration enables the FIPS module underneath the boundary: `aws-lc-rs` with its FIPS feature on
`aws-lc-fips-sys`, librdkafka built against that same `aws-lc-fips-sys`, and the rustls FIPS
provider. The boundary's `fips` feature already makes its own part of that selection, for
`aws-lc-rs`, rustls and its native digest; the migration enables it for `nervix-server` beside
librdkafka's backend. The fork's build reads the metadata of whichever AWS-LC crate its backend selects, so
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

## Password Credentials

`PASSWORD_ITERATIONS` is a committed cluster parameter: it promises the cost used when a new
user is created, rather than describing one node's capacity or a prerequisite for joining.
Its definition supplies the single default, 210,000, and accepts 1 through 1,000,000 iterations.
`PasswordCost` validates the same bound during construction, JSON decoding and archive decoding,
before the credentials executor can receive work. `PasswordCredential` has one current profile,
PBKDF2-HMAC-SHA-512, with the complete salt, digest and cost. Its debug representation excludes
salt and digest. Passwords themselves are never persisted.

The consensus publication retains the largest admitted verification cost. Creation-policy changes,
new users and restored users raise it; lowering the creation policy does not lower verification
work. An authentication captures that count and its optional user from one immutable publication.
Known, unknown and invalid names perform two native calls with the same total iteration work:
verification followed by padding, including one padding iteration. Padding protects users created
at a smaller cost. Command replay compares the exact credential admitted with its CREATE USER,
without replacing its salt or digest. Credential work keeps its isolated CPU class and reserves
the 64-byte allocation of the maintained verifier; native HMAC scratch lives on the worker's
bounded stack. Cancellation is checked before work and between the two authentication operations.

The ordinary build remains preparation for FIPS, not a validated FIPS module. The cryptographic
profile does not impose a new minimum password length.
