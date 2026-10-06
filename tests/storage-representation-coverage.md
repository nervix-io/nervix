# Storage Representation Coverage

Every target is registered in `tests/bolero-targets.toml` for ordinary randomized/corpus checks and
sanitizer libFuzzer. The same bounded generators, production storage paths and complete assertions
run in both modes. A target writes through the storage engine its boundary owns when it has one:
consensus state, the registry and the runtime-state store use a temporary database, and the
resource store a temporary directory. Codecs without an engine run in memory. All retained inputs
contain synthetic data and describe the current shapes.

| Stored representation | Target | Complete oracle |
| --- | --- | --- |
| Consensus state records: schedules, domains with their clock mappings and authorities, users, resource versions, node statuses and uploads, cordons, admission fences, mutation leases, restore installations, transactions, commit plans, impact reports, command executions and the metadata with its retry fence | `consensus-state-records` | The recovered `StateMachineData` equals the stored one, records, derived indexes and metadata alike, for a revision written into an empty keyspace and for a second revision written as the changes from it, with records replaced and removed |
| Damaged consensus state | `consensus-state-corruption` | Recovery fails with `StorageFailure::InvalidState`, or recovers a state in which every record that names its key is stored under it, each resource version has at most one upload, and every transaction counts its pending and completed operations and numbers a failing one within those it accepted, and which stores and recovers again unchanged |
| Raft votes, log positions, log entries of every command, state metadata, snapshot manifests, log keys, section keys and sections | `consensus-raft-records` | Complete equality through the storage codec and its conversions; a log key names its entry's index and a section key its section |
| Malformed Raft records | `consensus-raft-records-malformed` | Arbitrary bytes fail with the typed storage failure, or decode to a value that stores back to the same record |
| Registry Models keyed by domain, kind and name | `registry-stored-models` | Complete Model equality, listed in key order and read by key, after reopening the registry while its journal serves them and after a flush moved them into on-disk tables |
| Damaged registry records | `registry-stored-models-corruption` | The listing fails with `DecodeKey`, `InvalidModelArchive`, `DeserializeValue` or `StoredModelKindMismatch`, or lists Models whose own keys are exactly the stored keys |
| Runtime-state physical keys, namespace prefixes, index keys and chunk prefixes | `runtime-state-keys` | A key decodes to its namespace, state, schema fingerprint, generation, Model kind and name and the fingerprint of its branch's canonical text; it lies under its own domain and namespace prefixes, and its index and chunk keys reach no other placement's |
| Malformed runtime-state keys | `runtime-state-keys-malformed` | Damaged or arbitrary keys fail with the store's typed failure, or decode to a placement whose own key is exactly the stored key |
| Handoff preparations, forced-recovery preparations and completions | `runtime-state-identity-records` | Every coordination identity, operation, node, incarnation, entity and fingerprint, and every checkpoint with its placement compared by archived bits; two transitions share a key exactly when they name the same handoff or recovered entity |
| Malformed identity records | `runtime-state-identity-records-malformed` | Arbitrary bytes fail with the typed decode failure, or decode to a record whose placements convert back and which stores back unchanged |
| Kafka offset, branch lifecycle, deduplicator and branch-aggregated metrics snapshots in the inline checkpoint envelope | `runtime-state-snapshots` | Each owner's complete oracle: offsets with their topic schedules, lifecycle keys with activity and incarnations, keyspace parts bit for bit in arrival order, and every metrics series |
| Malformed snapshot payloads | `runtime-state-snapshots-malformed` | Each kind's typed decode failure, or a value that stores back to the same payload |
| Inline and chunked checkpoints in the runtime-state store | `runtime-state-store-checkpoints` | The exact payload at its revision through the latest-snapshot read and the streaming reader, before and after the database reopens, at, beside and between the chunk boundaries |
| Sealed window processor state | `runtime-window-snapshots` | Every retained row's sequence, ingestion time, branch bits and input and argument values, the next sequence, the branch lifetime and every aggregate's typed state; another branch lifetime restores nothing |
| Malformed window snapshots | `runtime-window-snapshots-malformed` | A sealed window damaged once, or arbitrary bytes behind the snapshot magic, fail with the snapshot's typed failure, open nothing for the branch lifetime, or open a window that seals back to the same bytes |
| Resource store directories and manifests | `resource-store-layout` | Every installed version reads back its complete manifest and the archive built for it after every other installation, after staging cleanup and after another version's removal; the removed version alone fails with `ReadFile` |

The earlier storage targets remain the owners of their codecs: `registry-archived-models` for the
registry's Model archive, `consensus-archived-counts` and `runtime-window-archived-counts` for
archived counts, `restore-installation-storage` for staged restore records,
`restore-native-lifecycle` and `restore-native-kafka` for the lifecycle and Kafka offsets a restore
writes, and the WASM targets listed in the
[WASM representation coverage map](./wasm-representation-coverage.md) for guest checkpoints.

## Contract Boundaries

- A runtime-state key holds its branch's exact text, and its decoder exposes the fingerprint of that
  text. The properties compare both the complete encoded key and this projection; a fingerprint does
  not reconstruct branch fields.
- A concrete branch key holds only finite floats. A stored or remote key holding a non-finite float
  fails with `BranchKeyError::NonFiniteFloat`, and one holding a datetime that is not RFC 3339 text
  with `BranchKeyError::RemoteFieldValue`. Generated keys hold finite floats of every bit pattern,
  signed zeros and subnormals included, and datetimes with whole-minute offsets.
- A registry key and a runtime-state key are read only in the exact spelling the writer produces:
  canonical lower-case names, separators where the layout puts them, and no trailing bytes.
- A consensus state record whose value names its own identity is accepted only under that key, and
  the state keyspace holds no record that recovery does not read. A log record is accepted only
  under its own entry's index. A transaction is accepted only in a state its transitions reach,
  and an operation range only with its last operation at or after its first, in every stored and
  transmitted form.
- Values that name no key, such as counters, cordons, admission fences, mutation leases, restore
  installations, commit plan steps and report items, are checked by their codec and by the readers
  that assemble them: report assembly refuses a missing or inconsistent item with a typed error.
  A whole value copied under a sibling key of its own family is not detectable from storage alone.
- A resource name that begins with a dot is written with that dot as `%2E` in its directory's name.
  The generated names include `.`, `..`, `.staging`, `.1.staging` and version numbers.
- The consensus state targets read up to 64 KiB, because a revision holding records of every
  family reads about 38 KiB on average. The corruption target draws its damage from 64 leading
  bytes of its own.
- Generators bound every record family to three records, names to the name rule's 128 bytes,
  chunked payloads to three chunks, windows to three rows and branch keys to three fields of two
  levels of nesting. Volatile state, such as records in flight, ACKs, plans and attempts, is never
  generated or stored.
- These are serial representation properties. Storage engine durability, replication, snapshot
  transfer, catch-up and shutdown retain their Cucumber, Turmoil and Chaos evidence. No
  synchronization mechanism or ordering claim is added by this coverage.

The public scenario **Persisted models reload from the on-disk tables a node flushed them into**
covers the registry's on-disk read path on one- and three-node clusters.
