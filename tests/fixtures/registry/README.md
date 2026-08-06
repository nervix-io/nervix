# Registry compatibility fixtures

`emitter-before-publishing-modes.rkyv` was serialized by the repository at commit
`56bfd9bec5206abd6cce40cd4198113f842c8f92`, before emitter publishing modes were added. It is an
archived `StoredModelVersioned::Emitter` containing a Kafka emitter and has SHA-256 digest
`b8a5731af2a1b997e931db623e382300f06da551cbeb5fd828efd22b105447ff`.

`schema-before-publishing-modes.rkyv` was serialized by the same revision and contains an unchanged
`StoredModelVersioned::Schema`. Its SHA-256 digest is
`d198d5e814eb8f49a9085b79f3dc21df25e5449ddbc0d653dd947e7a10ad0fed`.

Fixtures in this directory must only be regenerated from their documented historical revisions.
Serializing a retained legacy type from the current layout would not test bytes users could
actually have persisted.
