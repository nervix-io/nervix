# Cluster chaos runner

The chaos runner exercises a packaged Nervix image entirely through public processes and protocols.
It does not build Nervix or use Cargo, local Nervix binaries, or the Rust scenario harness.

List the scenarios:

```bash
just chaos list
```

Run the three-node baseline against an image that has already been built or published:

```bash
just chaos run baseline --image nervix:debian
```

Run the same workload on one node:

```bash
just chaos run baseline --image nervix:debian --nodes 1
```

The controller resolves the supplied reference to its immutable local image ID before Compose
starts. If the reference is not local, it performs one bounded pull and then resolves the result.
The Compose file has no build directives. Every Nervix node and every disposable administration
container uses the resolved ID.

The host needs Bash, Docker with Compose, GNU `timeout`, OpenSSL, and jq. Kafka administration,
traffic, listener probes, metrics probes, and Nervix administration run in prebuilt containers.
The baseline provisions its Kafka topics explicitly, installs the checked-in NSPL graph with the
image's packaged `nervix-cli`, and reconstructs the accepted-input ledger from Kafka itself. It then
checks the ingestor consumer-group boundary and compares the output ledger by event ID and exact
content. Duplicate, missing, unexpected, and corrupt records are reported independently.

Each run writes bounded evidence under `target/chaos/<run-id>/`: the immutable image identity,
rendered Compose configuration, phase ledger, public cluster and placement output, listener and
metrics probes, broker offsets, accepted and observed traffic, verifier reports, logs, and container
inspection. The default fixture has 24 records and accepts at most 1,000 with `--records`.

All owned containers, networks, and volumes carry `io.nervix.chaos.run=<run-id>`. This label also
gives Pumba scenarios an exact future target selector; Nervix nodes additionally carry
`io.nervix.chaos.target=true`. Normal exit, failure, timeout, and catchable signals preserve
diagnostics and remove the labeled resources. If a controller is killed before its trap runs, use:

```bash
just chaos cleanup --run-id <run-id>
```

`--keep` retains resources for interactive diagnosis and prints the same cleanup command.

Run the external verifier and Compose contract checks directly with:

```bash
just chaos self-test
```
