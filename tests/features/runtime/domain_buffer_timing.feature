Feature: Domain-paced branch buffering
  @domain_buffer_timing
  Scenario Outline: Paced branch collection and flush follow logical time while Immediate and source idle remain physical
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA timed_event (
        tenant STRING,
        sequence I64,
        path STRING
      );
      CREATE WIRE JSON SCHEMA timed_event_wire MODE STRICT (
        tenant string,
        sequence integer,
        path string
      );
      CREATE CODEC timed_event_codec
        FROM WIRE JSON SCHEMA timed_event_wire
        TO SCHEMA timed_event;
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY fast_ingested SCHEMA timed_event BRANCHED BY by_tenant;
      CREATE RELAY observed SCHEMA timed_event BRANCHED BY by_tenant;
      CREATE VHOST edge fast-buffering-{{test_id}}.example.com;
      CREATE ENDPOINT fast_buffering_endpoint
        ON edge
        PATH '/events'
        TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR fast_buffering_source
        FROM fast_buffering_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING timed_event_codec
        TIMESTAMP NOW
        TO fast_ingested
          INHERIT ALL
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH EACH 1s MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE REINGESTOR fast_buffering_reingestor
        FROM fast_ingested
        COLLECT FOR 1s MAX BATCH SIZE 1MiB
        TO observed
          INHERIT ALL
          BRANCHED BY by_tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION observed_subscription TO observed;
      START AT '2000-01-01T00:00:00Z' TIME RATE 20.0;
      """
    And http payload is posted to host "fast-buffering-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1,"path":"fast"}
      """
    And http payload is posted to host "fast-buffering-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":1,"path":"fast"}
      """
    And http payload is posted to host "fast-buffering-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2,"path":"fast"}
      """
    And http payload is posted to host "fast-buffering-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":2,"path":"fast"}
      """
    # The caller deliberately resumes after the receive budget. The independent reader must
    # still collect all rows before 800ms, below either one-second physical cadence.
    Then within "800ms" the relay subscription receives payloads with its caller delayed by "1s"
      """
      key={"tenant":"alpha"} payload={"path":"fast","sequence":1,"tenant":"alpha"}
      key={"tenant":"alpha"} payload={"path":"fast","sequence":2,"tenant":"alpha"}
      key={"tenant":"beta"} payload={"path":"fast","sequence":1,"tenant":"beta"}
      key={"tenant":"beta"} payload={"path":"fast","sequence":2,"tenant":"beta"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @domain_buffer_timing
  Scenario Outline: Immediate flush keeps its physical minimum in a slow historical domain
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA timed_event (
        tenant STRING,
        sequence I64,
        path STRING
      );
      CREATE WIRE JSON SCHEMA timed_event_wire MODE STRICT (
        tenant string,
        sequence integer,
        path string
      );
      CREATE CODEC timed_event_codec
        FROM WIRE JSON SCHEMA timed_event_wire
        TO SCHEMA timed_event;
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY immediate_ingested SCHEMA timed_event BRANCHED BY by_tenant;
      CREATE RELAY observed SCHEMA timed_event BRANCHED BY by_tenant;
      CREATE VHOST edge immediate-buffering-{{test_id}}.example.com;
      CREATE ENDPOINT immediate_buffering_endpoint
        ON edge
        PATH '/events'
        TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR immediate_buffering_source
        FROM immediate_buffering_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING timed_event_codec
        TIMESTAMP NOW
        TO immediate_ingested
          INHERIT ALL
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION immediate_buffering_junction
        FROM immediate_ingested
        BRANCHED BY by_tenant
        TO observed
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION observed_subscription TO observed;
      START AT '2000-01-01T00:00:00Z' TIME RATE 0.0001;
      """
    And http payload is posted to host "immediate-buffering-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1,"path":"immediate"}
      """
    And http payload is posted to host "immediate-buffering-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":1,"path":"immediate"}
      """
    # Keep the original receive deadline while this caller waits beyond it. At rate 0.0001,
    # a wrongly logical 100us batching minimum would already require one physical second.
    Then within "500ms" the relay subscription receives payloads with its caller delayed by "1s"
      """
      key={"tenant":"alpha"} payload={"path":"immediate","sequence":1,"tenant":"alpha"}
      key={"tenant":"beta"} payload={"path":"immediate","sequence":1,"tenant":"beta"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @domain_buffer_timing
  Scenario Outline: Reingestor collection keeps separate logical deadlines for slow branches
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA timed_event (
        tenant STRING,
        sequence I64,
        path STRING
      );
      CREATE WIRE JSON SCHEMA timed_event_wire MODE STRICT (
        tenant string,
        sequence integer,
        path string
      );
      CREATE CODEC timed_event_codec
        FROM WIRE JSON SCHEMA timed_event_wire
        TO SCHEMA timed_event;
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY collect_ingested SCHEMA timed_event BRANCHED BY by_tenant;
      CREATE RELAY observed SCHEMA timed_event BRANCHED BY by_tenant;
      CREATE VHOST edge collect-buffering-{{test_id}}.example.com;
      CREATE ENDPOINT collect_buffering_endpoint
        ON edge
        PATH '/events'
        TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR collect_buffering_source
        FROM collect_buffering_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING timed_event_codec
        TIMESTAMP NOW
        TO collect_ingested
          INHERIT ALL
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE REINGESTOR slow_collection_reingestor
        FROM collect_ingested
        COLLECT FOR 100us MAX BATCH SIZE 1MiB
        TO observed
          INHERIT ALL
          BRANCHED BY by_tenant
          FLUSH EACH 1h MAX BATCH SIZE 1B
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION observed_subscription TO observed;
      START AT '2000-01-01T00:00:00Z' TIME RATE 0.0001;
      """
    And http payloads are posted concurrently to host "collect-buffering-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1,"path":"collect"}
      {"tenant":"beta","sequence":1,"path":"collect"}
      """
    Then within "2s" the relay subscription receives payloads no sooner than "300ms" after they were published
      """
      {"path":"collect","sequence":1,"tenant":"alpha"}
      {"path":"collect","sequence":1,"tenant":"beta"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @domain_buffer_timing @retained_task_handles
  Scenario Outline: Retained task handles force-flush interleaved branches under slow domain time
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA timed_event (
        tenant STRING,
        sequence I64,
        path STRING
      );
      CREATE WIRE JSON SCHEMA timed_event_wire MODE STRICT (
        tenant string,
        sequence integer,
        path string
      );
      CREATE CODEC timed_event_codec
        FROM WIRE JSON SCHEMA timed_event_wire
        TO SCHEMA timed_event;
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY held_ingested SCHEMA timed_event BRANCHED BY by_tenant;
      CREATE RELAY gate_ingested SCHEMA timed_event BRANCHED BY by_tenant;
      CREATE VHOST edge held-buffering-{{test_id}}.example.com;
      CREATE ENDPOINT held_buffering_endpoint
        ON edge
        PATH '/events'
        TYPE HTTP;
      CREATE ENDPOINT gate_buffering_endpoint
        ON edge
        PATH '/gate'
        TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR held_buffering_source
        FROM held_buffering_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING timed_event_codec
        TIMESTAMP NOW
        TO held_ingested
          INHERIT ALL
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH EACH 1h MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR gate_buffering_source
        FROM gate_buffering_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING timed_event_codec
        TIMESTAMP NOW
        TO gate_ingested
          INHERIT ALL
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION held_subscription TO held_ingested WHERE sequence > 0;
      START AT '2000-01-01T00:00:00Z' TIME RATE 0.0001;
      """
    And http payload is posted to host "held-buffering-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1,"path":"held"}
      """
    And http payload is posted to host "held-buffering-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":1,"path":"held"}
      """
    And http payload is posted to host "held-buffering-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2,"path":"held"}
      """
    And http payload is posted to host "held-buffering-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":2,"path":"held"}
      """
    When these Plexus commands are executed
      """
      DESCRIBE INGESTOR held_buffering_source;
      """
    Then the last command output contains
      """
      ready: true
      """
    And the last command output contains
      """
      transient error: -
      """
    When these Plexus commands are executed
      """
      ALTER HTTP ENDPOINT INGESTOR gate_buffering_source SET QUIESCE BUFFER MAX SIZE 2MiB;
      """
    Then within "10s" the relay subscription receives payloads
      """
      {"path":"held","sequence":1,"tenant":"alpha"}
      {"path":"held","sequence":1,"tenant":"beta"}
      {"path":"held","sequence":2,"tenant":"alpha"}
      {"path":"held","sequence":2,"tenant":"beta"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
