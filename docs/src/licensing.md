# Licensing

Nervix is one program for everyone. Without a license it runs every base feature it has, such as
clusters of any number of nodes, any number of domains, backups and restores. A license switches on
optional capabilities on top of that. Licensing never limits a base feature: a cluster has only the
technical limits it has without a license.

This page describes licensing as it is today. For a license, write to
[licensing@nervix.io](mailto:licensing@nervix.io).

## Without A License

A cluster without a license runs under the free policy, which is built into Nervix:

- Every optional capability is disabled.
- Nodes, domains and every other base feature have no licensing limit.
- The cluster never contacts the licensing service and sends nothing about itself to anyone,
  including after a restart.

`DESCRIBE LICENSE` on any node shows `mode: free` and the state of every capability.

## What A License Adds

A license enables optional capabilities. `DESCRIBE LICENSE` lists every capability the running
Nervix knows, each `enabled` or `disabled`. The only capability today is `licensing.dummy`, which
changes nothing: a license that enables it shows that the cluster received and verified a decision
from the licensing service.

A license may also limit how much an enabled capability is used. Such a limit never applies to a
base feature, and lowering it below the current use comes with the grace period the license
states. No current capability has such a limit.

## Persistent And Ephemeral Activations

A cluster uses a license by activating it. The license states how many clusters may hold each kind
of activation at once.

- **Persistent** activation is for clusters that keep running, such as production. It lasts as
  long as the license is valid.
- **Ephemeral** activation is for development, continuous integration, testing and other short-lived
  clusters. It enables the same capabilities as a persistent one for a fixed lifetime, counted from
  the activation and never extended. When the lifetime ends, the cluster stops processing: its
  domains stop and `START` is refused, while its data and administrative access remain. Activating
  again lifts the stop, and the stopped domains then wait for an explicit `START`.

```plexus
ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-ABCDEFG2-your-secret';
ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-ABCDEFG2-your-secret';
```

## Staying Licensed

A licensed cluster's leader talks to the licensing service at `https://licensing.nervix.io`, the
address built into Nervix, over HTTPS. Every decision the service sends is signed, and the cluster
verifies it against the trust built into its release before applying it.

- The cluster refreshes its license on the schedule the license states, and the service notifies
  it of changes as they happen.
- When the service cannot be reached, the cluster keeps working. Once refreshes are overdue,
  `DESCRIBE LICENSE`, the licensing metrics and a warning notice say so. After the offline allowance
  the license states, the license lapses into a freeze: existing work keeps running under the last
  verified decision, and a capability may not take on new uses. Base features are not affected,
  and no current capability tracks its uses, so today a freeze refuses nothing. A successful
  refresh ends the freeze.
- When the license ends, or is suspended, revoked, transferred or released, the cluster returns to
  the free policy.

Licensing stops processing only when an ephemeral activation's lifetime ends, and it never removes a
node, a domain or data.

## Statements

| Statement | What it does |
| --- | --- |
| `ACTIVATE LICENSE PERSISTENT WITH KEY '…'` | Activates a license persistently. |
| `ACTIVATE LICENSE EPHEMERAL WITH KEY '…'` | Activates a license for a fixed lifetime. |
| `REFRESH LICENSE` | Asks the licensing service for a current decision now. |
| `RELEASE LICENSE` | Gives the activation back and returns the cluster to the free policy. It returns once the cluster has recorded the release and tells the service afterwards, so an outage of the service does not hold it up, unless a key rotation is still in progress. |
| `ROTATE LICENSE CLUSTER KEY` | Replaces the key that identifies the cluster to the service. |
| `DESCRIBE LICENSE` | Shows the mode, the policy, the decision and every capability. |

Every statement runs without a selected domain. [Cluster Licensing](node-settings.md#cluster-licensing)
describes each in detail, [Licensing Metrics](metrics-and-observability.md#licensing-metrics) the
metrics, and [Shared Licensing Protocol](licensing-protocol.md) the signed documents.
