# Cluster chaos runner

`just chaos` runs black-box cluster scenarios and suites against an already built Nervix image,
through Docker Compose, Pumba and the image's packaged CLI. It builds nothing and needs no Rust
toolchain.

[External Cluster Chaos](../../docs/src/cluster-chaos.md) documents the runner:

- its commands, the images it qualifies and the host it needs
- the deployment, and how each fault is injected and proven
- every scenario and its verdicts, including backups and restores under node and network faults
  and the measured staging volume of concurrent restores, and seeded experiments with their replay
- failure categories, budgets, artifacts and cleanup
- diagnostic images, the smoke and soak suites, and their CI

Keep that chapter current with every change to this directory.

```bash
just chaos list
just chaos run baseline --image nervix:debian
just chaos run restore-fault --image nervix:debian --fault leader-crash
just chaos run restore-volume --image nervix:debian
just chaos suite smoke --image ghcr.io/nervix-io/nervix@sha256:<digest>
just chaos replay target/chaos/<run-id>
just chaos cleanup --run-id <run-id>
just chaos self-test
```

## Diagnostic images

An image whose `io.nervix.diagnostic.selection` label declares a Deloxide selection runs every node
as a diagnostic node. Every run then fails unless the deadlock evidence of each process its nodes
started qualifies. [Diagnostic Images](../../docs/src/cluster-chaos.md#diagnostic-images) describes
how a run collects and judges that evidence.
