Feature: Emitter batch lifecycle
  Buffered rows and prepared payloads belong to the configuration and concrete branch that
  admitted them. Lifecycle changes preserve their membership, acknowledgements and execution
  snapshot until delivery, or discard them when their exact branch lifetime ends.

  Background:
    Given the production sticky scheduler is configured

  @emitter_batch_lifecycle
  Scenario: The final node drains a partial batch and confirms its source before shutdown
    Given Kafka is running
    And graceful shutdown drain is enabled
    And drain timeout is configured as "30s"
    And a 1 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "batch_shutdown_in_{{test_id}}" exists with 1 partitions
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_source CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR source
        FROM kafka_source TOPIC 'batch_shutdown_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'batch_shutdown_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 50ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT destination CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE ATTACHED ZEROMQ PUSH EMITTER batched FROM events
        TO destination MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 100ms
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH EACH 1h MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "batch_shutdown_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "batch_shutdown_in_{{test_id}}"
      """
      [{"seq":1},{"seq":2}]
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node reports "messages_total" "received" relay "events" totalling "2"
    And within "2s" Kafka consumer group "batch_shutdown_group_{{test_id}}" next offset for topic "batch_shutdown_in_{{test_id}}" partition 0 is "below 1"
    When node "node-1" begins stopping
    Then within "30s" the observed broker receives exactly these payloads
      """
      [{"seq":1},{"seq":2}]
      """
    And within "30s" Kafka consumer group "batch_shutdown_group_{{test_id}}" next offset for topic "batch_shutdown_in_{{test_id}}" partition 0 is "at least 1"
    When node "node-1" is stopped while timing shutdown
    Then the last cluster operation completes within "30s"
    When node "node-1" is started
    Then node "node-1" eventually observes a stable leader

  @emitter_batch_lifecycle
  Scenario: Relocating an emitter drains its partial batch before its replacement accepts work
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge batch-move-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR source
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT destination CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE ATTACHED ZEROMQ PUSH EMITTER batched FROM events
        TO destination MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 100ms
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH EACH 1h MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    When http payload is posted to host "batch-move-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1},{"seq":2}]
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node reports "messages_total" "received" relay "events" totalling "2"
    When these Plexus commands are executed on the leader node
      """
      RELOCATE EMITTER batched ONTO NODE node-2 IGNORE PREFERENCES;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    And within "10s" the observed broker receives exactly these payloads
      """
      [{"seq":1},{"seq":2}]
      """
    When http payload is posted to host "batch-move-{{test_id}}.example.com" path "/events"
      """
      [{"seq":3},{"seq":4}]
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node contains
      """
      messages_total received relay=events physical_node=node-2 total=2
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER ZEROMQ PUSH EMITTER batched SET FLUSH IMMEDIATE;
      """
    Then within "10s" the observed broker receives exactly these payloads
      """
      [{"seq":3},{"seq":4}]
      """

  @emitter_batch_lifecycle
  Scenario Outline: Materialized waits retain branch-local values when their partial batches flush
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, tenant STRING );
      CREATE SCHEMA output_event ( seq I64, tenant STRING, label STRING );
      CREATE WIRE JSON SCHEMA output_wire MODE STRICT (
        seq integer, tenant string, label string
      );
      CREATE WIRE JSON CODEC output_codec FROM WIRE SCHEMA output_wire TO SCHEMA output_event;
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_configuration ( tenant STRING, label STRING );
      CREATE JAQ JSON CODEC configuration_codec TO SCHEMA tenant_configuration
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY settings SCHEMA tenant_configuration BRANCHED BY by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST edge batch-state-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE ENDPOINT configuration_ingress ON edge PATH '/settings' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR source
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TIMESTAMP NOW
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR configuration_source
        FROM configuration_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING configuration_codec
        TIMESTAMP NOW
        TO settings INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT destination CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE ATTACHED ZEROMQ PUSH EMITTER batched FROM events
        USING MATERIALIZED STATE settings REQUIRED WAIT
        TO destination MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 100ms
          ENCODE USING output_codec
        INHERIT ALL SET label = relay_state.settings.label
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH EACH 1h MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to host "batch-state-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"tenant":"acme"},{"seq":2,"tenant":"acme"}]
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node reports "messages_total" "received" relay "events" totalling "2"
    When http payload is posted to host "batch-state-{{test_id}}.example.com" path "/events"
      """
      [{"seq":3,"tenant":"beta"},{"seq":4,"tenant":"beta"}]
      """
    And http payload is posted to host "batch-state-{{test_id}}.example.com" path "/settings"
      """
      {"tenant":"beta","label":"beta-ready"}
      """
    Then within "10s" node "node-1" eventually reports materialized state for relay "settings" containing
      """
      key={"tenant":"beta"} payload={"label":"beta-ready","tenant":"beta"}
      """
    When http payload is posted to host "batch-state-{{test_id}}.example.com" path "/settings"
      """
      {"tenant":"acme","label":"acme-ready"}
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node reports "messages_total" "received" relay "events" totalling "4"
    When http payload is posted to host "batch-state-{{test_id}}.example.com" path "/settings"
      """
      {"tenant":"acme","label":"acme-later"}
      """
    Then within "10s" node "node-1" eventually reports materialized state for relay "settings" containing
      """
      key={"tenant":"acme"} payload={"label":"acme-later","tenant":"acme"}
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER ZEROMQ PUSH EMITTER batched SET FLUSH IMMEDIATE;
      """
    Then within "10s" the observed broker receives exactly these payloads
      """
      [{"seq":1,"tenant":"acme","label":"acme-ready"},{"seq":2,"tenant":"acme","label":"acme-ready"}]
      [{"seq":3,"tenant":"beta","label":"beta-ready"},{"seq":4,"tenant":"beta","label":"beta-ready"}]
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batch_lifecycle
  Scenario Outline: Branch eviction discards buffered batch members before a later flush
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, tenant STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer, tenant string );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m MAX INSTANCES 1 EVICT LRU;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge batch-lifecycle-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR source
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
          INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT destination CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE ATTACHED ZEROMQ PUSH EMITTER batched FROM events
        TO destination MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 100ms
          ENCODE USING event_codec
        INHERIT ALL
        <batch_clause>
        FLUSH EACH 1h MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      RELOCATE RELAY events ONTO NODE <relay_owner> IGNORE PREFERENCES;
      RELOCATE EMITTER batched ONTO NODE <emitter_owner> IGNORE PREFERENCES;
      """
    When http payload is posted to host "batch-lifecycle-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"tenant":"acme"},{"seq":2,"tenant":"acme"}]
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node reports "messages_total" "received" relay "events" totalling "2"
    When http payload is posted to host "batch-lifecycle-{{test_id}}.example.com" path "/events"
      """
      [{"seq":3,"tenant":"beta"},{"seq":4,"tenant":"beta"}]
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node reports "messages_total" "received" relay "events" totalling "4"
    And within "10s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY events WHERE (tenant = 'acme');
      """
    When http payload is posted to host "batch-lifecycle-{{test_id}}.example.com" path "/events"
      """
      [{"seq":5,"tenant":"acme"},{"seq":6,"tenant":"acme"}]
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node reports "messages_total" "received" relay "events" totalling "6"
    And within "10s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY events WHERE (tenant = 'beta');
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER ZEROMQ PUSH EMITTER batched SET FLUSH IMMEDIATE;
      """
    Then within "10s" the observed broker receives exactly these payloads
      """
      <expected_payload_1>
      <expected_payload_2>
      """

    Examples:
      | cluster_size | relay_owner | emitter_owner | batch_clause                        | expected_payload_1                                    | expected_payload_2        |
      | 1            | node-1      | node-1        | BATCH MAX MESSAGES 10 MAX SIZE 1KiB | [{"seq":5,"tenant":"acme"},{"seq":6,"tenant":"acme"}] |                           |
      | 3            | node-2      | node-1        | BATCH MAX MESSAGES 10 MAX SIZE 1KiB | [{"seq":5,"tenant":"acme"},{"seq":6,"tenant":"acme"}] |                           |
      | 1            | node-1      | node-1        |                                     | {"seq":5,"tenant":"acme"}                             | {"seq":6,"tenant":"acme"} |
      | 3            | node-2      | node-1        |                                     | {"seq":5,"tenant":"acme"}                             | {"seq":6,"tenant":"acme"} |

  @emitter_batch_lifecycle
  Scenario Outline: Each source retains its own branches beside an unbranched input
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, tenant STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer, tenant string );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH first_tenant SCHEMA tenant_key TTL 5m PARTITIONS 2 MAX INSTANCES 1 EVICT LRU;
      CREATE BRANCH second_tenant SCHEMA tenant_key TTL 5m MAX INSTANCES 1 EVICT LRU;
      CREATE RELAY plain_events SCHEMA event UNBRANCHED;
      CREATE RELAY first_events SCHEMA event BRANCHED BY first_tenant;
      CREATE RELAY second_events SCHEMA event BRANCHED BY second_tenant;
      CREATE VHOST edge batch-source-retention-{{test_id}}.example.com;
      CREATE ENDPOINT plain_ingress ON edge PATH '/plain' TYPE HTTP;
      CREATE ENDPOINT first_ingress ON edge PATH '/first' TYPE HTTP;
      CREATE ENDPOINT second_ingress ON edge PATH '/second' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR plain_source
        FROM plain_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO plain_events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR first_source
        FROM first_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO first_events INHERIT ALL BRANCHED BY first_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR second_source
        FROM second_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO second_events INHERIT ALL BRANCHED BY second_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT destination CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE ATTACHED ZEROMQ PUSH EMITTER batched
        FROM plain_events, first_events, second_events
        TO destination MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 100ms
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH EACH 1h MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to host "batch-source-retention-{{test_id}}.example.com" path "/first"
      """
      [{"seq":1,"tenant":"acme"},{"seq":2,"tenant":"acme"}]
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node reports "messages_total" "received" relay "first_events" totalling "2"
    When http payload is posted to host "batch-source-retention-{{test_id}}.example.com" path "/second"
      """
      [{"seq":3,"tenant":"acme"},{"seq":4,"tenant":"acme"}]
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node reports "messages_total" "received" relay "second_events" totalling "2"
    When http payload is posted to host "batch-source-retention-{{test_id}}.example.com" path "/plain"
      """
      [{"seq":7,"tenant":"plain"},{"seq":8,"tenant":"plain"}]
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node reports "messages_total" "received" relay "plain_events" totalling "2"
    When http payload is posted to host "batch-source-retention-{{test_id}}.example.com" path "/first"
      """
      [{"seq":9,"tenant":"eta"},{"seq":10,"tenant":"eta"}]
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node reports "messages_total" "received" relay "first_events" totalling "4"
    When http payload is posted to host "batch-source-retention-{{test_id}}.example.com" path "/first"
      """
      [{"seq":5,"tenant":"beta"},{"seq":6,"tenant":"beta"}]
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node reports "messages_total" "received" relay "first_events" totalling "6"
    When these Plexus commands are executed on the leader node
      """
      ALTER ZEROMQ PUSH EMITTER batched SET FLUSH IMMEDIATE;
      """
    Then within "10s" the observed broker receives exactly these payloads
      """
      [{"seq":3,"tenant":"acme"},{"seq":4,"tenant":"acme"}]
      [{"seq":7,"tenant":"plain"},{"seq":8,"tenant":"plain"}]
      [{"seq":9,"tenant":"eta"},{"seq":10,"tenant":"eta"}]
      [{"seq":5,"tenant":"beta"},{"seq":6,"tenant":"beta"}]
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batch_lifecycle
  Scenario Outline: An emitter expires a partial branch batch before a later flush
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, tenant STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer, tenant string );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 400s;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge batch-expiry-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR source
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TIMESTAMP NOW
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT destination CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE ATTACHED ZEROMQ PUSH EMITTER batched FROM events
        TO destination MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 100ms
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH EACH 1h MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 20.0;
      """
    When http payload is posted to host "batch-expiry-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"tenant":"acme"},{"seq":2,"tenant":"acme"}]
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node reports "messages_total" "received" relay "events" totalling "2"
    # The partial batch stays unpublished throughout its TTL and the maintenance scan interval.
    And the observed broker does not receive a payload within "55s"
    When these Plexus commands are executed on the leader node
      """
      ALTER ZEROMQ PUSH EMITTER batched SET FLUSH IMMEDIATE;
      """
    Then the observed broker does not receive a payload within "2s"
    When http payload is posted to host "batch-expiry-{{test_id}}.example.com" path "/events"
      """
      [{"seq":3,"tenant":"acme"},{"seq":4,"tenant":"acme"}]
      """
    Then within "10s" the observed broker receives exactly these payloads
      """
      [{"seq":3,"tenant":"acme"},{"seq":4,"tenant":"acme"}]
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batch_lifecycle
  Scenario Outline: An expired materialized-state wait cannot publish when state later arrives
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, tenant STRING );
      CREATE SCHEMA output_event ( seq I64, tenant STRING, label STRING );
      CREATE WIRE JSON SCHEMA output_wire MODE STRICT (
        seq integer, tenant string, label string
      );
      CREATE WIRE JSON CODEC output_codec FROM WIRE SCHEMA output_wire TO SCHEMA output_event;
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_configuration ( tenant STRING, label STRING );
      CREATE JAQ JSON CODEC configuration_codec TO SCHEMA tenant_configuration
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 400s;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY settings SCHEMA tenant_configuration BRANCHED BY by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST edge batch-wait-expiry-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE ENDPOINT configuration_ingress ON edge PATH '/settings' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR source
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TIMESTAMP NOW
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR configuration_source
        FROM configuration_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING configuration_codec
        TIMESTAMP NOW
        TO settings INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT destination CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE ATTACHED ZEROMQ PUSH EMITTER batched FROM events
        USING MATERIALIZED STATE settings REQUIRED WAIT
        TO destination MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 100ms
          ENCODE USING output_codec
        INHERIT ALL SET label = relay_state.settings.label
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 20.0;
      """
    When http payload is posted to host "batch-wait-expiry-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"tenant":"acme"},{"seq":2,"tenant":"acme"}]
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node reports "messages_total" "received" relay "events" totalling "2"
    And the observed broker does not receive a payload within "55s"
    When http payload is posted to host "batch-wait-expiry-{{test_id}}.example.com" path "/settings"
      """
      {"tenant":"acme","label":"ready"}
      """
    Then within "10s" node "node-1" eventually reports materialized state for relay "settings" containing
      """
      key={"tenant":"acme"}
      """
    When http payload is posted to host "batch-wait-expiry-{{test_id}}.example.com" path "/events"
      """
      [{"seq":3,"tenant":"acme"},{"seq":4,"tenant":"acme"}]
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node reports "messages_total" "received" relay "events" totalling "4"
    And within "10s" the observed broker receives exactly these payloads
      """
      [{"seq":3,"tenant":"acme","label":"ready"},{"seq":4,"tenant":"acme","label":"ready"}]
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batch_lifecycle
  Scenario Outline: Batch policy changes drain partial output under the policy that admitted it
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "batch_policy_in_{{test_id}}" exists with 1 partitions
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_source CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR source
        FROM kafka_source TOPIC 'batch_policy_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'batch_policy_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 50ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT destination CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE ATTACHED ZEROMQ PUSH EMITTER batched FROM events
        TO destination MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 100ms
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH EACH 1h MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "batch_policy_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "batch_policy_in_{{test_id}}"
      """
      [{"seq":1},{"seq":2}]
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node reports "messages_total" "received" relay "events" totalling "2"
    And within "2s" Kafka consumer group "batch_policy_group_{{test_id}}" next offset for topic "batch_policy_in_{{test_id}}" partition 0 is "below 1"
    Given the next pending entity drain in domain "{{domain}}" is forced to time out
    When these Plexus commands fail with "timed out draining domain"
      """
      ALTER ZEROMQ PUSH EMITTER batched SET BATCH MAX MESSAGES 1 MAX SIZE 1KiB;
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER batched;
      """
    Then the last command output contains
      """
      BATCH MAX MESSAGES 10 MAX SIZE 1KiB
      """
    Given client "owner" is connected to the leader node
    When client "owner" executes these Plexus commands
      """
      BEGIN;
      ALTER ZEROMQ PUSH EMITTER batched SET BATCH MAX MESSAGES 1 MAX SIZE 1KiB;
      DESCRIBE TRANSACTION OPERATION 1 FORMAT JSON;
      """
    Then the last command output contains
      """
      EMITTER_BATCH_POLICY
      """
    When client "owner" executes these Plexus commands
      """
      COMMIT;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    And within "10s" the observed broker receives exactly these payloads
      """
      [{"seq":1},{"seq":2}]
      """
    And within "10s" Kafka consumer group "batch_policy_group_{{test_id}}" next offset for topic "batch_policy_in_{{test_id}}" partition 0 is "at least 1"
    When Kafka message is published to topic "batch_policy_in_{{test_id}}"
      """
      [{"seq":3},{"seq":4}]
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node reports "messages_total" "received" relay "events" totalling "4"
    When these Plexus commands are executed on the leader node
      """
      ALTER ZEROMQ PUSH EMITTER batched DROP BATCH;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    And within "10s" the observed broker receives exactly these payloads
      """
      [{"seq":3}]
      [{"seq":4}]
      """
    And within "10s" Kafka consumer group "batch_policy_group_{{test_id}}" next offset for topic "batch_policy_in_{{test_id}}" partition 0 is "at least 2"
    When these Plexus commands are executed on the leader node
      """
      ALTER ZEROMQ PUSH EMITTER batched SET FLUSH IMMEDIATE;
      """
    And Kafka message is published to topic "batch_policy_in_{{test_id}}"
      """
      [{"seq":5},{"seq":6}]
      """
    Then within "10s" the observed broker receives exactly these payloads
      """
      {"seq":5}
      {"seq":6}
      """
    And within "10s" Kafka consumer group "batch_policy_group_{{test_id}}" next offset for topic "batch_policy_in_{{test_id}}" partition 0 is "at least 3"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batch_lifecycle
  Scenario Outline: A stopped slow domain replays unresolved partial work after restart
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    And Kafka topic "batch_stop_in_{{test_id}}" exists with 1 partitions
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_source CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR source
        FROM kafka_source TOPIC 'batch_stop_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'batch_stop_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 50ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TIMESTAMP NOW
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT destination CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE ATTACHED ZEROMQ PUSH EMITTER batched FROM events
        TO destination MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 100ms
          ENCODE USING event_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH EACH 1h MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 0.01;
      """
    Then Kafka consumer group "batch_stop_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "batch_stop_in_{{test_id}}"
      """
      [{"seq":1},{"seq":2}]
      """
    Then within "10s" DESCRIBE EMITTER "batched" on the leader node reports "messages_total" "received" relay "events" totalling "2"
    When emitter "batched" enters stall mode
    And these Plexus commands are executed on the leader node
      """
      STOP;
      """
    Then the observed broker does not receive a payload within "1s"
    And within "2s" Kafka consumer group "batch_stop_group_{{test_id}}" next offset for topic "batch_stop_in_{{test_id}}" partition 0 is "below 1"
    When emitter "batched" leaves stall mode
    And these Plexus commands are executed on the leader node
      """
      ALTER ZEROMQ PUSH EMITTER batched SET FLUSH IMMEDIATE;
      START;
      """
    Then within "10s" the observed broker receives exactly these payloads
      """
      [{"seq":1},{"seq":2}]
      """
    And within "10s" Kafka consumer group "batch_stop_group_{{test_id}}" next offset for topic "batch_stop_in_{{test_id}}" partition 0 is "at least 1"
    When Kafka message is published to topic "batch_stop_in_{{test_id}}"
      """
      [{"seq":3},{"seq":4}]
      """
    Then within "10s" the observed broker receives exactly these payloads
      """
      [{"seq":3},{"seq":4}]
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
