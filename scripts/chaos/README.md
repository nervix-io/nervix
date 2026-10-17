# Cluster chaos runner

`just chaos` runs black-box cluster scenarios and suites against an already built Nervix image,
through Docker Compose, Pumba and the image's packaged CLI. It builds nothing and needs no Rust
toolchain.

[External Cluster Chaos](../../docs/src/cluster-chaos.md) documents the runner:

- its commands, the images it qualifies and the host it needs
- the deployment, and how each fault is injected and proven
- every scenario and its verdicts, and seeded experiments with their replay
- failure categories, budgets, artifacts and cleanup
- diagnostic images, the smoke and soak suites, and their CI

Keep that chapter current with every change to this directory.

```bash
just chaos list
just chaos run baseline --image nervix:debian
just chaos suite smoke --image ghcr.io/nervix-io/nervix@sha256:<digest>
just chaos replay target/chaos/<run-id>
just chaos cleanup --run-id <run-id>
just chaos self-test
```
