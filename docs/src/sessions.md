#  Sessions

Nervix supports session-local commands over its session protocol.

These commands are not persisted in the registry:

```nspl
CREATE SUBSCRIPTION acme_notifications TO notifications WHERE tenant = 'acme';
CREATE SUBSCRIPTION sampled_telemetry TO telemetry DROPPING BATCH SAMPLE RATE 0.1 WHERE input.tenant = 'acme';
DELETE SUBSCRIPTION acme_notifications;
DESCRIBE RELAY notifications WHERE (tenant = 'acme');
```

Current session behavior:

- subscription creation validates that the referenced relay exists in the active runtime
- subscription names are unique within one connected session and may refer to relays in different domains
- `DELETE SUBSCRIPTION` resolves only the session-local subscription name, independent of the currently active domain
- subscribing to a relay collects records from all active branch groups for that relay
- subscriptions are read-only views; only an optional `WHERE` predicate is supported
- bare fields, `message.<field>`, and `input.<field>` all read the subscribed relay record;
  `output`, `branch`, and `relay_state` are unavailable
- optional `BATCH SAMPLE RATE <rate>` samples arrivals after `WHERE` has been evaluated
- `BLOCKING` delivery waits for the connected session transport queue, while `DROPPING` discards delivered events when that queue is full
- subscription events are delivered asynchronously to the connected client session
- runtime and server errors are also delivered asynchronously
- cluster membership updates are also delivered asynchronously

Sessions are runtime-facing protocol interactions, not part of the persisted namespace model.

## Transaction Binding

An NSPL transaction is replicated control-plane state, but its binding to a live session is
leader-local soft state. A session may bind one transaction, and a transaction may be bound to at
most one session. The session protocol can attach by transaction id; the authenticated user must
match the transaction owner. A later attach takes over the binding and the displaced session gets
an explicit takeover error on its next transaction operation.

A transaction also carries the domain it is bound to. Attaching adopts that domain as the session's
selected domain, so a session can never queue a statement for a different domain than the
transaction it holds. `USE` remains unavailable while a transaction is active.

The CLI, web console, and Rust client retain the transaction id and automatically attach after a
redirect or transport reconnect before replaying a command. An unclean transport loss, node loss,
or leadership change therefore leaves an open transaction intact until attach or idle expiry. A
client compares the attached progress with the status it last observed and does not repeat an
operation already recorded by the cluster. During election convergence, a client also retries a
bounded interval when a peer cannot yet advertise the new leader. A clean end of the session
preserves the existing interactive behavior by reverting a bound open transaction. Ending a
session never reverts a transaction whose replicated state is already `COMMITTING`; the leader
finishes it without a client.

Finished transaction outcomes remain available during tombstone retention. Attach during that
window reports `COMMITTED`, `FAILED`, `REVERTED`, or `EXPIRED` and includes structured commit
status plus the retained per-statement results. After retention, attach reports an unknown
transaction id. See
[Replicated NSPL Transactions](control-plane.md#replicated-nspl-transactions).
