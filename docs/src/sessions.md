#  Sessions

Nervix supports session-local commands over its session protocol.

These commands are not persisted in the registry:

```nspl
SUBSCRIBE SESSION TO notifications SET notifications.normalized = lower(notifications.tenant) UNSET notifications.user_id WHERE notifications.tenant = 'acme';
SUBSCRIBE SESSION TO telemetry DROPPING BATCH SAMPLE RATE 0.1 WHERE telemetry.tenant = 'acme';
UNSUBSCRIBE SESSION FROM notifications SET notifications.normalized = lower(notifications.tenant) UNSET notifications.user_id WHERE notifications.tenant = 'acme';
DESCRIBE RELAY notifications WHERE (tenant = 'acme');
```

Current session behavior:

- subscription creation validates that the referenced relay exists in the active runtime
- subscribing to a relay collects records from all active branch groups for that relay
- optional `SET` / `UNSET` / `WHERE` clauses run at the session subscription boundary and filter-map the delivered records
- optional `BATCH SAMPLE RATE <rate>` samples arrivals after `WHERE` has been evaluated
- `BLOCKING` delivery waits for the connected session transport queue, while `DROPPING` discards delivered events when that queue is full
- subscription events are delivered asynchronously to the connected client session
- runtime and server errors are also delivered asynchronously
- cluster membership updates are also delivered asynchronously

Sessions are runtime-facing protocol interactions, not part of the persisted namespace model.
