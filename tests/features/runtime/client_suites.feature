Feature: Language-level client suites
  Every host binding has a test suite written in its own language, in that language's standard test
  framework, that uses the binding as an application does. A suite's boundary tier needs no cluster
  and runs through `just test-client-<host>-api`; its cluster tier runs here, against a cluster whose
  session listener serves TLS, starting on a follower when the cluster has one. The harness holds
  the run to the tests `tests/client-suites-inventory.toml` registers for the host's cluster tier:
  every one ran once and passed, nothing was skipped, and nothing else ran.

  The graph is the cross-language conformance graph's shape: a client ingestor whose output reaches
  a client emitter, and a second client ingestor that feeds a relay no emitter reads, which the
  suite subscribes to. The suite creates what a test changes, such as a paced domain it stops and
  starts, or a relay it drops, itself.

  Scenario Outline: A <runtime> client's suite passes its cluster tests on <cluster_size> nodes
    Given client grpc transport is configured with mode "https"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA suite_row (
        id U32,
        tenant STRING,
        amount I64,
        ratio F64,
        flag BOOL,
        note STRING OPTIONAL,
        raw BYTES OPTIONAL,
        at DATETIME,
        tags <tags_type>,
        grid <grid_type>,
        secret STRING SENSITIVE
      );
      CREATE SCHEMA suite_out_row (
        id U32,
        tenant STRING,
        amount I64,
        ratio F64,
        flag BOOL,
        note STRING OPTIONAL,
        raw BYTES OPTIONAL,
        at DATETIME,
        tags <tags_type>,
        grid <grid_type>,
        secret STRING SENSITIVE,
        echo U32
      );
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY typed_rows SCHEMA suite_row BRANCHED BY by_tenant;
      CREATE RELAY watched_rows SCHEMA suite_row BRANCHED BY by_tenant;
      CREATE CLIENT PRODUCER INGESTOR typed_in
        FROM SCHEMA suite_row
          MODE ACK SEQUENTIAL ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO typed_rows
          INHERIT ALL
          BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT CONSUMER EMITTER typed_out FROM typed_rows
        TO SCHEMA suite_out_row
          MODE ACK SEQUENTIAL ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT ALL EXCEPT secret
        SET secret = leak_sensitive(input.secret), echo = input.id
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT PRODUCER INGESTOR watched_in
        FROM SCHEMA suite_row
          MODE ACK PARALLEL MAX 8 ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO watched_rows
          INHERIT ALL
          BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    When the "<runtime>" client suite runs its cluster tests
    Then within "600s" the client suite passes every registered cluster test

    @client_conformance_toolchain @client_suite_python @long_running @slots_1
    Examples: CPython over the Python package
      | runtime | cluster_size | tags_type   | grid_type     |
      | python  | 1            | VEC<STRING> | ARRAY<I16, 2> |
      | python  | 3            | VEC<STRING> | ARRAY<I16, 2> |
