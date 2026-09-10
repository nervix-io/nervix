# Common

Common guides document features that cross several NSPL surfaces and are easiest to use when their
pieces are explained together. They complement the entity-focused manual chapters; they do not
introduce another model category.

- [Syslog](syslog.md) groups the predefined singleton syslog wire schema, codecs that use it,
  socket clients, ingestor and emitter transport behavior, TLS, framing, failure semantics, and
  limits.
- [Database Client Connection Pools](database-client-pools.md) groups the required `POOL SIZE MIN
  <n> MAX <n>` clause shared by Postgres, MySQL, MongoDB, and Redis clients: accepted values, how
  the bounds relate to connector `CONFIG`, inspection, and how a bound is changed.

Additional cross-cutting guides will live in this section.
