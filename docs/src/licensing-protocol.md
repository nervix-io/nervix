# Shared Licensing Protocol

`nervix-licensing-protocol` owns the signed contract shared by Nervix, the licensing service and
the test service. It is an engine: callers provide typed state, UTC instants and explicit work
bounds. It reads no clock, performs no I/O, and knows no graph, consensus or database type.
The service consumes this crate and `nervix-crypto` at a pinned Nervix git revision; it does not
copy their types or verification rules.

This chapter owns the shared representations, signature profile, trust derivation, condition
resolution and decision validation. [Cryptography](./cryptography.md) owns the AWS-LC boundary.
The cluster coordinator, persistence and enforcement integrations consume these outcomes at their
own boundaries; a verified signature alone never authorizes a graph operation.

## Documents And Bounds

The current payloads include decisions, operation-specific activation requests, stream tickets,
key-possession proofs, working-key authorizations, revocations, root successions, response envelopes,
challenge exchanges and the closed problem-details catalog. Fixed structures reject unknown fields;
timestamps use RFC 3339 `DateTime<Utc>`. Requests have one exact flat shape per operation. Create
and adoption refresh omit committed state; the three committed revision/time fields are present
together for an established refresh, release, rotation or stream ticket. Secret strings zeroize
on drop and redact `Debug`; transport and persistence owners also protect serialized copies.

`DocumentLimits` carries nonzero byte, signature-count and trust-document bounds from the caller.
Compact input is bounded before framing allocations, and trusted payload serialization uses the
bounded writer before the maintained signer runs. Root sets and signature sets are bounded by the
signature count; trust derivation is bounded by the trust-document count. HTTP owners must bound
their response body before deserializing an envelope. These are caller bounds, not commercial
policy defaults.

## Signature Profile

Compact JWS uses ES256 with fixed 64-byte `r || s` signatures. The protected header contains exactly
`alg`, `kid` and `typ`; even an extra member with a null value is refused. Public JWKs contain only
`kty: EC`, `crv: P-256` and canonical, fixed-width base64url `x` and `y`. A key identifier is its
RFC 7638 SHA-256 thumbprint, never a caller-selected label.

| Payload | Protected `typ` |
| --- | --- |
| Decision | `nervix.licensing.decision` |
| Activation request or key-possession proof | `nervix.licensing.request` |
| Stream ticket | `nervix.licensing.stream-ticket` |
| Working-key authorization | `nervix.licensing.trust.working-key` |
| Working-key revocation | `nervix.licensing.trust.revocation` |
| Root succession | `nervix.licensing.trust.succession` |

The format-only maintained `jose-jws`/`jose-b64` decoder exposes the original protected JSON so the
engine can enforce the exact header. It does not interpret payload JSON. `jsonwebtoken` with
only its AWS-LC backend verifies the original received signing input before deserializing that
payload. Expiry, not-before and audience claim checks are disabled there: licensing owns their
distinct typed fields and supplies their policy explicitly, without a hidden clock read.
Reformatting equal JSON invalidates an existing signature; verification never reserializes it.

A signature set contains compact members with exactly equal payload segments and distinct key
identifiers. Canonical unpadded base64url makes decoded-payload byte equality equivalent to segment
equality. A root threshold counts only distinct verified keys in that epoch, never the number of
members supplied. Invalid or unknown signatures cannot contribute to it.

## Trust And Provisioning

An embedded epoch is trusted. A verified threshold succession can add exactly the next epoch;
input order does not replace the chain. Working-key authorization requires a trusted epoch,
content-matching key identifier, supported protocol, ordered validity window and the decision use.
The window is inclusive and evaluated at the signed decision's `issued_at`.

Revocation is an independent forward-looking check. Applying a new decision refuses a revoked
signing key. Reload verifies the committed signature and authorization, preserves the decision,
and reports `signing_key_revoked`; `revoked_at` is informational. Trust state retains the original
accepted signed documents so its owner can store and reverify them, together with the embedded
bundle. A conflicting serial or root epoch fails closed.

`PRODUCTION_TRUST_BUNDLE` explicitly represents unprovisioned anchors until the production ceremony
supplies its reviewed output. An empty bundle grants no trust. This crate embeds no artificial
production roots and exposes no runtime configuration that replaces the server's anchors. The
launch owner installs actual compile-checked declarations before enabling production activation.
Test-service roots are explicit engine inputs; their availability in a server belongs to its
separate testing-only composition seam.

## Conditions And Resolution

The catalog declares the kind, scope and owner of every current name. Cluster quotas belong in
`usage`; relationship quotas, durations, sets and policy belong in `operating`.

| Name | Kind and scope | Owner |
| --- | --- | --- |
| `cluster.nodes` | quota, cluster | committed membership admission |
| `cluster.domains` | quota, cluster | domain admission |
| `license.activations.persistent` | quota, license | service activation capacity |
| `license.activations.ephemeral` | quota, license | service activation capacity |
| `activation.kinds` | persistent/ephemeral set, organization | service activation admission |
| `ephemeral.max_lifetime` | bounded duration, organization | activation creation |
| `refresh.interval` | bounded duration, organization | decision timing |
| `refresh.overdue_after` | bounded duration, organization | decision timing |
| `offline.allowance` | bounded/unbounded duration, organization | decision timing |
| `offline.on_lapse` | closed `freeze` policy, organization | cluster admission freeze |
| `transition.capability_removal_grace` | bounded duration, organization | transition arithmetic |
| `transition.quota_reduction_grace` | bounded duration, organization | transition arithmetic |

There are no current capability entries. A bounded quota of zero, an unlimited quota, and an
absent override are distinct. Plans provide every catalog name; overrides replace complete values
for only the names they contain. The service refuses unknown names. A receiving cluster retains
an unknown name's complete external JSON and ignores it during evaluation; a known name with an
invalid value is always refused. This extension behavior is part of the external contract.

Resolution validates the complete plan and partial overrides, applies overrides, derives status
from the supplied license/activation facts and instant, and hashes the complete result using
SHA-256 over typed serialization with fixed field order and sorted maps. Active usage is resolved
from the assignment. Every non-active status uses the free usage policy, except expired ephemeral
activations, whose usage is empty. Operating conditions remain authoritative in each state.
The free policy grants every current cluster quota without a limit and has no operating half.
An expired lifetime is a firm stop even if the license or activation also has another terminal state.

## Decision Acceptance

The verifier returns a private verified outcome. Validation then checks these rules against typed
caller inputs and returns `DecisionRejection` on failure:

1. The exact envelope, signature, authorization and apply/reload revocation rules above hold.
2. The document kind and protocol match this build.
3. The service equals the caller's compiled deployment address.
4. Activation, cluster, kind, creation, expiry, request and key identity match the committed binding.
   A pending key with checked generation `+1` is accepted only for completing that rotation.
5. Decision revision, conditions revision and issue time cannot move backwards. An equal decision
   revision is a no-op only for the identical logical decision; equal conditions revisions cannot
   label different conditions.
6. `issued_at <= refresh_after <= overdue_at <= valid_until` when bounded; term and ephemeral
   timing remain internally consistent.
7. Status and complete known conditions are valid. Acknowledged transitions have unique identities,
   known revisions, affected catalog entries and a deadline at or after their start.

Validation mutates nothing. Its owner keeps committed state on rejection and decides how to report
or retry. A request's logical idempotency digest excludes exactly `challenge` and `sent_at` and
includes every other field. It recursively sorts JSON object keys even when another dependency
enables serde_json's insertion-order feature, so service and product builds agree.

## Impact And Grace

`EffectiveUsage` carries the committed target and existing transitions. Impact assessment compares
the target with actual usage and the current effective allowance. Improvements and reductions that
already fit usage create no grant. An affected quota gets only the newly affected amount below the
previous target; earlier amounts retain their deadlines. Current quota entries share one transition
with one checked deadline and an explicit decision, tick or release trigger. Expiry creates none.

For example, reducing unlimited nodes to four while seven are in use protects three units. A later
reduction from four to three protects one further unit, keeping the first deadline. The effective
allowance is seven until the first deadline, four until the second, then three. An improvement caps
remaining grants at their protected existing usage, preventing expansion. Same-revision refreshes
create no transition. A deterministic UUIDv8 derives from the complete replicated impact inputs;
the engine reads neither randomness nor a clock. The caller owns retaining, acknowledging and
removing transitions, and feature owners decide what an over-limit state permits.

## Evidence

Focused tests cover positive and negative trust/decision rules, original-byte signature checks,
strict fixed structures, operation shapes and checked grant arithmetic. Six registered bounded
properties cover complete representations, resolution, malformed JWS, trust chains, impact and
request digests; their budgets and corpora are in the [property register](./property-testing-and-fuzzing.md).
The cryptographic boundary retains the NIST and cross-build P-256 vectors. These checks do not
claim a FIPS-qualified deployment. The shared engine has no product endpoint or concurrent state
machine, so Cucumber and concurrency-model execution belong to its later integration owners.
