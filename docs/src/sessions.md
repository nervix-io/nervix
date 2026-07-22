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
