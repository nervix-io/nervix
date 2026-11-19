# Branch-Owned Runtime

A Nervix graph can give each account its own branch of the same processing path. A named
parametrization defines which fields identify a branch, such as the account identifier. Each
branch holds its relay parts, buffers, tasks, timers and state. The parametrizer partition owns
branch membership and tells the parts when a branch starts or ends.

A record stays in the same parametrization and partition between its entry point and the
reingestor or emitter that ends the branch. The scheduler can place the units that serve that
partition on different cluster nodes.

The [repository rule](../../AGENTS.md#a-branch-owns-its-runtime) defines the ownership and partition
contracts. [Architecture Rules And The Debt Ratchet](architecture-enforcement.md) describes their
enforcement during the move to branch-owned runtime.
