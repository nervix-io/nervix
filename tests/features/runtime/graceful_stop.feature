@graceful_stop
Feature: A domain STOP lets the work already inside the domain finish
  A STOP first stops the domain's intake, then waits, with no time limit of its own, until every
  message already inside the domain has come to rest, and only then ends the domain's tasks. A
  publish attempt already on the wire runs to its outcome. STOP ENFORCE AFTER forces the stop
  through after a stated time and lists what it cut; STOP CANCEL AFTER calls the request off
  after a stated time. Several stop requests share one stop.

  @graceful_stop_publish_on_the_wire
  Scenario Outline: A STOP waits for a publish on the wire and stops once every record in motion is delivered on <cluster_size> nodes
    Given Kafka is running
    And HTTP receiver "slow" is running
    And HTTP receiver "slow" answers unscripted requests with "hold response until released"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "stop_wire_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, id STRING );
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 1h;
      CREATE RELAY outgoing SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR ingestor
        FROM kafka_main TOPIC 'stop_wire_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'stop_wire_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO outgoing INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT slow_api CONFIG {
        'endpoint' = '{{http_receiver.slow}}', 'timeout_ms' = 120000
      };
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        ATTACHED
        TO slow_api METHOD 'POST' PATH concat('/events/', input.tenant, '/', input.id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "stop_wire_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "stop_wire_{{test_id}}"
      """
      [{"tenant":"acme","id":"1"},{"tenant":"globex","id":"2"},{"tenant":"acme","id":"3"},{"tenant":"globex","id":"4"}]
      """
    Then HTTP receiver "slow" eventually receives at least 1 request
    When these Plexus commands begin executing in the background
      """
      STOP;
      """
    # The destination holds the publish on the wire, so the stop keeps waiting for it.
    Then the background Plexus execution is still running after "3s"
    When HTTP receiver "slow" releases its held responses with "respond 204"
    Then within "60s" the background Plexus execution reaches a successful outcome
    And the last command output contains
      """
      stopped domain '{{domain}}'
      """
    And the last command output does not contain
      """
      cut
      """
    And HTTP receiver "slow" eventually captures one request that is
      """
      POST /events/acme/1
      """
    And HTTP receiver "slow" eventually captures one request that is
      """
      POST /events/globex/2
      """
    And HTTP receiver "slow" eventually captures one request that is
      """
      POST /events/acme/3
      """
    And HTTP receiver "slow" eventually captures one request that is
      """
      POST /events/globex/4
      """
    And HTTP receiver "slow" has captured exactly 4 requests
    And within "10s" Kafka consumer group "stop_wire_group_{{test_id}}" next offset for topic "stop_wire_{{test_id}}" partition 0 is "at least 1"
    # The source's position was confirmed before the domain stopped, so a restart sends nothing again.
    When these Plexus commands are executed on the leader node
      """
      START;
      """
    Then Kafka consumer group "stop_wire_group_{{test_id}}" eventually has 1 consumers
    And HTTP receiver "slow" has captured exactly 4 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @graceful_stop_route_buffer
  Scenario Outline: A STOP flushes an ingestor's route buffer whatever its cadence, so a source without acknowledgements loses nothing on <cluster_size> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "stop_route_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "stop_route_out_{{test_id}}" exists with 1 partitions
    And Kafka topic "stop_route_out_{{test_id}}" is observed
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, id STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, id string );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 1h;
      CREATE RELAY outgoing SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR unacked
        FROM kafka_main TOPIC 'stop_route_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'stop_route_group_{{test_id}}' START AT EARLIEST
          MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO outgoing INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH EACH 1h MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER published FROM outgoing
        ATTACHED
        TO kafka_main TOPIC 'stop_route_out_{{test_id}}'
          MODE ACK PARALLEL MAX 16 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "stop_route_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "stop_route_in_{{test_id}}"
      """
      [{"tenant":"acme","id":"u1"},{"tenant":"globex","id":"u2"},{"tenant":"acme","id":"u3"},{"tenant":"globex","id":"u4"}]
      """
    # The source has handed the records over and confirmed nothing back; they rest in the
    # ingestor's route buffer, whose cadence is an hour.
    Then within "30s" Kafka consumer group "stop_route_group_{{test_id}}" next offset for topic "stop_route_in_{{test_id}}" partition 0 is "at least 1"
    When these Plexus commands are executed on the leader node
      """
      STOP;
      """
    Then the last command output contains
      """
      stopped domain '{{domain}}'
      """
    And within "30s" the observed broker receives exactly these payloads
      """
      {"tenant":"acme","id":"u1"}
      {"tenant":"globex","id":"u2"}
      {"tenant":"acme","id":"u3"}
      {"tenant":"globex","id":"u4"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # A deduplicator's keys and a window's rows have a saved form, so a STOP writes them and a START
  # continues from them: nothing in motion at the stop is lost, and nothing is applied twice.
  @graceful_stop_stateful_processors_continue
  Scenario Outline: STOP then START across a deduplicator and a window continues without losing a message or applying one twice on <cluster_size> nodes
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA reading ( tenant STRING, id STRING, value I64 );
      CREATE SCHEMA history ( tenant STRING, samples I64, total I64 );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT ( tenant string, id string, value integer );
      CREATE WIRE JSON CODEC reading_codec FROM WIRE SCHEMA reading_wire TO SCHEMA reading;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 1h;
      CREATE RELAY readings SCHEMA reading BRANCHED BY by_tenant;
      CREATE RELAY unique_readings SCHEMA reading BRANCHED BY by_tenant;
      CREATE RELAY histories SCHEMA history BRANCHED BY by_tenant;
      CREATE VHOST edge stop-state-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/readings';
      CREATE HTTP ENDPOINT INGESTOR reading_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING reading_codec
        TO readings INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH EACH 20s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR unique_ids
        FROM readings
        DEDUPLICATE ON input.id
        MAX TIME 1h
        BRANCHED BY by_tenant
        TO unique_readings INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE WINDOW PROCESSOR collection_window FROM unique_readings
        WIDTH 3 MESSAGES STEP 3 MESSAGES BRANCHED BY by_tenant
        TO histories SET tenant = FIRST(input.tenant), samples = COUNT(input.value),
          total = SUM(input.value)
        ON MESSAGE ERROR LOG;
      START;
      """
    When http payload is posted to node "node-1" with host "stop-state-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","id":"a1","value":10}
      """
    And http payload is posted to node "node-1" with host "stop-state-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"globex","id":"g1","value":100}
      """
    And http payload is posted to node "node-1" with host "stop-state-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","id":"a2","value":20}
      """
    And http payload is posted to node "node-1" with host "stop-state-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"globex","id":"g2","value":200}
      """
    # The records rest in the ingestor's route buffer, whose cadence the stop does not wait for:
    # it flushes them through the deduplicator into the window, which retains two rows per branch.
    When these Plexus commands are executed on the leader node
      """
      STOP;
      """
    Then the last command output contains
      """
      stopped domain '{{domain}}': every message in motion finished
      """
    # The rows rest in the window's saved state; only their acknowledgements, which nothing saves
    # yet, are cut.
    And the last command output contains
      """
      window_processor 'collection_window': acknowledgements pending for rows resting in a window (4)
      """
    When these Plexus commands are executed on the leader node
      """
      START;
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION recovered_histories TO histories;
      """
    # A repeated id is one the restored deduplicator already passed, so it is not applied again.
    And http payload is posted to node "node-1" with host "stop-state-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","id":"a1","value":10}
      """
    And http payload is posted to node "node-1" with host "stop-state-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"acme","id":"a3","value":30}
      """
    And http payload is posted to node "node-1" with host "stop-state-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"globex","id":"g3","value":300}
      """
    # Each window holds exactly the three distinct readings of its branch: a lost row or a
    # repeated one would change its total.
    Then within "90s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "samples":3 | "total":60
      key={"tenant":"globex"} | "samples":3 | "total":600
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @graceful_stop_processor_and_emitter_buffers
  Scenario Outline: A STOP flushes a processor's collector and route buffer and an emitter's buffer, delivering both sources exactly once on <cluster_size> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "stop_acked_{{test_id}}" exists with 1 partitions
    And Kafka topic "stop_unacked_{{test_id}}" exists with 1 partitions
    And Kafka topic "stop_out_{{test_id}}" exists with 1 partitions
    And Kafka topic "stop_out_{{test_id}}" is observed
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, id STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, id string );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 1h;
      CREATE RELAY incoming SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY outgoing SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR acked
        FROM kafka_main TOPIC 'stop_acked_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'stop_acked_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO incoming INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC INGESTOR unacked
        FROM kafka_main TOPIC 'stop_unacked_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'stop_unacked_group_{{test_id}}' START AT EARLIEST
          MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO incoming INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION forward FROM incoming COLLECT FOR 1h MAX BATCH SIZE 1MiB
        BRANCHED BY by_tenant
        TO outgoing INHERIT ALL FLUSH EACH 1h MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      CREATE KAFKA TOPIC EMITTER published FROM outgoing
        ATTACHED
        TO kafka_main TOPIC 'stop_out_{{test_id}}'
          MODE ACK PARALLEL MAX 16 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        FLUSH EACH 1h MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "stop_acked_group_{{test_id}}" eventually has 1 consumers
    And Kafka consumer group "stop_unacked_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "stop_acked_{{test_id}}"
      """
      [{"tenant":"acme","id":"a1"},{"tenant":"globex","id":"a2"},{"tenant":"acme","id":"a3"},{"tenant":"globex","id":"a4"}]
      """
    And Kafka message is published to topic "stop_unacked_{{test_id}}"
      """
      [{"tenant":"globex","id":"u1"},{"tenant":"acme","id":"u2"},{"tenant":"globex","id":"u3"},{"tenant":"acme","id":"u4"}]
      """
    # Every record reached the junction, whose collector and route buffer wait an hour, so they
    # all rest inside the domain when it is stopped.
    Then within "30s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "incoming" across physical nodes totals 8
    When these Plexus commands are executed on the leader node
      """
      STOP;
      """
    Then the last command output contains
      """
      stopped domain '{{domain}}'
      """
    And within "30s" the observed broker receives exactly these payloads
      """
      {"tenant":"acme","id":"a1"}
      {"tenant":"globex","id":"a2"}
      {"tenant":"acme","id":"a3"}
      {"tenant":"globex","id":"a4"}
      {"tenant":"globex","id":"u1"}
      {"tenant":"acme","id":"u2"}
      {"tenant":"globex","id":"u3"}
      {"tenant":"acme","id":"u4"}
      """
    And within "10s" Kafka consumer group "stop_acked_group_{{test_id}}" next offset for topic "stop_acked_{{test_id}}" partition 0 is "at least 1"
    # The acknowledged source confirmed its position before the stop ended, so a restart sends
    # nothing again and nothing arrives twice.
    When these Plexus commands are executed on the leader node
      """
      START;
      """
    Then Kafka consumer group "stop_acked_group_{{test_id}}" eventually has 1 consumers
    And the observed broker does not receive a payload within "3s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @graceful_stop_enforced_by_a_second_request
  Scenario Outline: A second STOP ENFORCE AFTER from another session ends a stop that waits for a publish that never completes on <cluster_size> nodes
    Given Kafka is running
    And HTTP receiver "stuck" is running
    And HTTP receiver "stuck" answers unscripted requests with "hold response until released"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "stop_enforce_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, id STRING );
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 1h;
      CREATE RELAY outgoing SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR ingestor
        FROM kafka_main TOPIC 'stop_enforce_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'stop_enforce_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO outgoing INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT stuck_api CONFIG {
        'endpoint' = '{{http_receiver.stuck}}', 'timeout_ms' = 600000
      };
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        ATTACHED
        TO stuck_api METHOD 'POST' PATH concat('/events/', input.tenant, '/', input.id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "stop_enforce_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "stop_enforce_{{test_id}}"
      """
      [{"tenant":"acme","id":"1"}]
      """
    Then HTTP receiver "stuck" eventually receives at least 1 request
    When these Plexus commands begin executing in the background
      """
      STOP;
      """
    Then the background Plexus execution is still running after "3s"
    # The domain and the cluster status show the stop, its request and what it waits for.
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      status: stopping
      """
    And the last command output contains
      """
      emitter 'published': a publish on the wire
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - {{domain}} status=Stopping
      """
    And the last command output contains
      """
      - {{domain}} requests: '
      """
    And the last command output contains
      """
      emitter 'published': a publish on the wire
      """
    When these Plexus commands are executed on the leader node
      """
      STOP ENFORCE AFTER 0s;
      """
    Then the last command output contains
      """
      enforced
      """
    And the last command output contains
      """
      published
      """
    And within "60s" the background Plexus execution reaches a successful outcome
    # The first request was plain; its answer says another request enforced the stop, and what
    # that cut.
    And the last command output contains
      """
      enforced
      """
    And the last command output contains
      """
      published
      """
    And HTTP receiver "stuck" eventually sees the client abandon at least 1 unfinished response
    # The cut record was answered "not processed", so the acknowledged source sends it again.
    And within "2s" Kafka consumer group "stop_enforce_group_{{test_id}}" next offset for topic "stop_enforce_{{test_id}}" partition 0 is "below 1"
    When HTTP receiver "stuck" releases its held responses with "respond 204"
    And these Plexus commands are executed on the leader node
      """
      START;
      """
    Then HTTP receiver "stuck" eventually receives at least 2 requests
    And within "30s" Kafka consumer group "stop_enforce_group_{{test_id}}" next offset for topic "stop_enforce_{{test_id}}" partition 0 is "at least 1"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @graceful_stop_enforced_at_once
  Scenario Outline: STOP ENFORCE AFTER 0s ends the domain at once and lists the cut publish on <cluster_size> nodes
    Given Kafka is running
    And HTTP receiver "stuck" is running
    And HTTP receiver "stuck" answers unscripted requests with "hold response until released"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "stop_now_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, id STRING );
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR ingestor
        FROM kafka_main TOPIC 'stop_now_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'stop_now_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO outgoing INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT stuck_api CONFIG {
        'endpoint' = '{{http_receiver.stuck}}', 'timeout_ms' = 600000
      };
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        ATTACHED
        TO stuck_api METHOD 'POST' PATH concat('/events/', input.tenant, '/', input.id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "stop_now_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "stop_now_{{test_id}}"
      """
      [{"tenant":"acme","id":"1"}]
      """
    Then HTTP receiver "stuck" eventually receives at least 1 request
    When these Plexus commands are executed on the leader node
      """
      STOP ENFORCE AFTER 0s;
      """
    Then the last command output contains
      """
      enforced
      """
    And the last command output contains
      """
      published
      """
    And HTTP receiver "stuck" eventually sees the client abandon at least 1 unfinished response
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      status: stopped
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @graceful_stop_cancelled
  Scenario Outline: STOP CANCEL AFTER calls the stop off and the domain runs on with nothing cut on <cluster_size> nodes
    Given Kafka is running
    And HTTP receiver "slow" is running
    And HTTP receiver "slow" answers unscripted requests with "hold response until released"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "stop_cancel_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, id STRING );
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR ingestor
        FROM kafka_main TOPIC 'stop_cancel_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'stop_cancel_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO outgoing INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT slow_api CONFIG {
        'endpoint' = '{{http_receiver.slow}}', 'timeout_ms' = 600000
      };
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        ATTACHED
        TO slow_api METHOD 'POST' PATH concat('/events/', input.tenant, '/', input.id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "stop_cancel_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "stop_cancel_{{test_id}}"
      """
      [{"tenant":"acme","id":"1"}]
      """
    Then HTTP receiver "slow" eventually receives at least 1 request
    # The stop was called off, so the statement did not stop the domain: its answer says so,
    # names what the stop was waiting for, and nothing was cut.
    When these Plexus commands fail with "cancelled"
      """
      STOP CANCEL AFTER 2s;
      """
    Then the last command error contains
      """
      published
      """
    And the last command error does not contain
      """
      cut
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      status: running
      """
    When HTTP receiver "slow" releases its held responses with "respond 204"
    Then within "30s" Kafka consumer group "stop_cancel_group_{{test_id}}" next offset for topic "stop_cancel_{{test_id}}" partition 0 is "at least 1"
    # Intake resumed with the domain, so a record sent after the cancelled stop is delivered too.
    When Kafka message is published to topic "stop_cancel_{{test_id}}"
      """
      [{"tenant":"acme","id":"2"}]
      """
    Then HTTP receiver "slow" eventually captures one request that is
      """
      POST /events/acme/2
      """
    And HTTP receiver "slow" captured one request that is
      """
      POST /events/acme/1
      """
    And HTTP receiver "slow" has captured exactly 2 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @graceful_stop_cancel_withdraws_one_request
  Scenario Outline: A cancelled request is withdrawn alone while another STOP still waits, and the stop goes on on <cluster_size> nodes
    Given Kafka is running
    And HTTP receiver "slow" is running
    And HTTP receiver "slow" answers unscripted requests with "hold response until released"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "stop_withdraw_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, id STRING );
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR ingestor
        FROM kafka_main TOPIC 'stop_withdraw_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'stop_withdraw_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO outgoing INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT slow_api CONFIG {
        'endpoint' = '{{http_receiver.slow}}', 'timeout_ms' = 600000
      };
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        ATTACHED
        TO slow_api METHOD 'POST' PATH concat('/events/', input.tenant, '/', input.id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "stop_withdraw_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "stop_withdraw_{{test_id}}"
      """
      [{"tenant":"acme","id":"1"}]
      """
    Then HTTP receiver "slow" eventually receives at least 1 request
    When these Plexus commands begin executing in the background
      """
      STOP;
      """
    Then the background Plexus execution is still running after "2s"
    When these Plexus commands fail with "cancelled"
      """
      STOP CANCEL AFTER 1s;
      """
    Then the background Plexus execution is still running after "2s"
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      status: stopping
      """
    When HTTP receiver "slow" releases its held responses with "respond 204"
    Then within "60s" the background Plexus execution reaches a successful outcome
    And the last command output contains
      """
      stopped domain '{{domain}}'
      """
    And within "10s" Kafka consumer group "stop_withdraw_group_{{test_id}}" next offset for topic "stop_withdraw_{{test_id}}" partition 0 is "at least 1"
    And HTTP receiver "slow" has captured exactly 1 request

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @graceful_stop_endpoints_withdrawn
  Scenario Outline: A stopping domain withdraws its endpoints while a request it admitted goes on, and a called-off stop publishes them again on <cluster_size> nodes
    Given HTTP receiver "slow" is running
    And HTTP receiver "slow" answers unscripted requests with "hold response until released"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( id STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( id string );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE VHOST edge stop-endpoint-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT acknowledged ON edge PATH '/acked';
      CREATE HTTP ENDPOINT INGESTOR intake FROM ingress MODE NO ACK
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR acknowledged_intake FROM acknowledged
        MODE ACK MAX 8 ACK TIMEOUT 5m
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT slow_api CONFIG {
        'endpoint' = '{{http_receiver.slow}}', 'timeout_ms' = 600000
      };
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        ATTACHED
        TO slow_api METHOD 'POST' PATH concat('/events/', input.id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    # The endpoint admits the request, which waits for its acknowledgement while the destination
    # holds its publish, so the stop waits too.
    And endpoint request "admitted" is posted on node "<request_node>" host "stop-endpoint-{{test_id}}.example.com" path "/acked"
      """
      {"id":"before"}
      """
    Then HTTP receiver "slow" eventually receives at least 1 request
    When these Plexus commands begin executing in the background
      """
      STOP CANCEL AFTER 20s;
      """
    Then the background Plexus execution is still running after "3s"
    # Intake ended with the stop and the domain's endpoints are withdrawn, so a request that
    # arrives now finds no route.
    And within "10s" node "<request_node>" answers http payload for host "stop-endpoint-{{test_id}}.example.com" path "/events" as not routed
      """
      {"id":"during"}
      """
    # The stop owns the domain until it ends or is called off, so a START is refused meanwhile.
    When these Plexus commands fail with "is stopping"
      """
      START;
      """
    # The stop is called off: the domain runs on and publishes its endpoints again.
    Then the background Plexus execution fails with "cancelled"
    And within "30s" node "<request_node>" accepts http payload for host "stop-endpoint-{{test_id}}.example.com" path "/events"
      """
      {"id":"after"}
      """
    # The request admitted before the stop kept its intake through the withdrawal, so it gets its
    # real answer once its message finishes.
    When HTTP receiver "slow" releases its held responses with "respond 204"
    Then endpoint request "admitted" answers HTTP 200 with "completed" cause "none"
    And HTTP receiver "slow" eventually captures one request that is
      """
      POST /events/after
      """
    And HTTP receiver "slow" captured one request that is
      """
      POST /events/before
      """
    And HTTP receiver "slow" captured no request with request line "POST /events/during"

    Examples:
      | cluster_size | request_node |
      | 1            | node-1       |
      | 3            | node-3       |

  @graceful_stop_reorderer_rows_cut
  Scenario Outline: A STOP does not wait for rows a reorderer holds; it lists them as cut and the source sends them again after START on <cluster_size> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "stop_reorder_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, seq I64 );
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 1h;
      CREATE RELAY incoming SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY ordered SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR ingestor
        FROM kafka_main TOPIC 'stop_reorder_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'stop_reorder_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO incoming INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE REORDERER order_rows FROM incoming BY input.seq MAX TIME 10m
        BRANCHED BY by_tenant TO ordered INHERIT ALL FLUSH EACH 1h MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION ordered_rows TO ordered;
      START;
      """
    Then Kafka consumer group "stop_reorder_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "stop_reorder_{{test_id}}"
      """
      [{"tenant":"acme","seq":2},{"tenant":"globex","seq":4},{"tenant":"acme","seq":1},{"tenant":"globex","seq":3}]
      """
    Then within "30s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "incoming" across physical nodes totals 4
    # The reorderer holds the rows for up to ten minutes. The stop does not wait for that timer:
    # the rows cannot be saved yet, so they are cut, and the answer names their holder.
    When these Plexus commands are executed on the leader node
      """
      STOP;
      """
    Then the last command output contains
      """
      stopped domain '{{domain}}'
      """
    And the last command output contains
      """
      cannot be saved yet
      """
    And the last command output contains
      """
      reorderer 'order_rows'
      """
    And within "2s" Kafka consumer group "stop_reorder_group_{{test_id}}" next offset for topic "stop_reorder_{{test_id}}" partition 0 is "below 1"
    When these Plexus commands are executed on the leader node
      """
      START;
      """
    Then Kafka consumer group "stop_reorder_group_{{test_id}}" eventually has 1 consumers
    # The cut rows were answered "not processed", so the source sends all four again: the relay's
    # cumulative count doubles.
    And within "30s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "incoming" across physical nodes totals 8

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @graceful_stop_correlator_records_cut
  Scenario Outline: A STOP does not wait for records a correlator holds; it lists them as cut on <cluster_size> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "stop_left_{{test_id}}" exists with 1 partitions
    And Kafka topic "stop_right_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, id STRING );
      CREATE SCHEMA pair ( tenant STRING, id STRING, matched STRING );
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 1h;
      CREATE RELAY left_events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY right_events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY pairs SCHEMA pair BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR left_ingestor
        FROM kafka_main TOPIC 'stop_left_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'stop_left_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO left_events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC INGESTOR right_ingestor
        FROM kafka_main TOPIC 'stop_right_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'stop_right_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO right_events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CORRELATOR match_pairs
        LEFT FROM left_events
        RIGHT FROM right_events
        CORRELATE WHERE left.id = right.id
        MATCH EARLIEST
        MAX TIME 10m
        ON CORRELATION TIMEOUT DROP, DROP
        BRANCHED BY by_tenant
        TO pairs SET tenant = left.tenant, id = left.id, matched = right.id
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION matched_pairs TO pairs;
      START;
      """
    Then Kafka consumer group "stop_left_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "stop_left_{{test_id}}"
      """
      [{"tenant":"acme","id":"l1"},{"tenant":"globex","id":"l2"}]
      """
    Then within "30s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "left_events" across physical nodes totals 2
    # No right record arrives, so the correlator keeps both left records waiting for a partner.
    When these Plexus commands are executed on the leader node
      """
      STOP;
      """
    Then the last command output contains
      """
      cannot be saved yet
      """
    And the last command output contains
      """
      correlator 'match_pairs'
      """
    And within "2s" Kafka consumer group "stop_left_group_{{test_id}}" next offset for topic "stop_left_{{test_id}}" partition 0 is "below 1"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @graceful_stop_required_wait_cut
  Scenario Outline: A STOP does not wait for messages a REQUIRED WAIT retains; it lists them as cut on <cluster_size> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "stop_wait_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, id STRING );
      CREATE SCHEMA tenant_setting ( tenant STRING, limit_value I64 );
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 1h;
      CREATE RELAY incoming SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY settings SCHEMA tenant_setting BRANCHED BY by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY resolved SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR ingestor
        FROM kafka_main TOPIC 'stop_wait_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'stop_wait_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO incoming INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION resolve FROM incoming
        BRANCHED BY by_tenant
        USING MATERIALIZED STATE settings REQUIRED WAIT
        TO resolved INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION resolved_events TO resolved;
      START;
      """
    Then Kafka consumer group "stop_wait_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "stop_wait_{{test_id}}"
      """
      [{"tenant":"acme","id":"1"},{"tenant":"globex","id":"2"}]
      """
    Then within "30s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "incoming" across physical nodes totals 2
    # No setting exists for either tenant, so the junction retains both messages on REQUIRED WAIT.
    When these Plexus commands are executed on the leader node
      """
      STOP;
      """
    Then the last command output contains
      """
      cannot be saved yet
      """
    And the last command output contains
      """
      junction 'resolve'
      """
    And within "2s" Kafka consumer group "stop_wait_group_{{test_id}}" next offset for topic "stop_wait_{{test_id}}" partition 0 is "below 1"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @graceful_stop_unavailable_destination
  Scenario Outline: A STOP does not wait for a destination that stays unavailable; the attempt on the wire runs to its outcome and the undelivered records are cut on <cluster_size> nodes
    Given Kafka is running
    And HTTP receiver "down" is running
    And HTTP receiver "down" answers unscripted requests with "respond 503"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "stop_down_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, id STRING );
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR ingestor
        FROM kafka_main TOPIC 'stop_down_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'stop_down_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO outgoing INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT down_api CONFIG {
        'endpoint' = '{{http_receiver.down}}', 'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        ATTACHED
        TO down_api METHOD 'POST' PATH concat('/events/', input.tenant, '/', input.id)
          MODE ACK RETRY POLICY BACKOFF 1s MAX 1m WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "stop_down_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "stop_down_{{test_id}}"
      """
      [{"tenant":"acme","id":"1"}]
      """
    Then HTTP receiver "down" eventually receives at least 1 request
    # The destination refuses every attempt, so the emitter keeps the record waiting for a retry,
    # which the stop does not wait for.
    When these Plexus commands are executed on the leader node
      """
      STOP;
      """
    Then the last command output contains
      """
      cannot be saved yet
      """
    And the last command output contains
      """
      emitter 'published'
      """
    And within "2s" Kafka consumer group "stop_down_group_{{test_id}}" next offset for topic "stop_down_{{test_id}}" partition 0 is "below 1"
    # The destination recovers, and the acknowledged source delivers the cut record again.
    Given HTTP receiver "down" answers unscripted requests with "respond 204"
    When these Plexus commands are executed on the leader node
      """
      START;
      """
    Then within "30s" Kafka consumer group "stop_down_group_{{test_id}}" next offset for topic "stop_down_{{test_id}}" partition 0 is "at least 1"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @graceful_stop_already_stopped
  Scenario Outline: A STOP of a domain that is already stopped is refused, while the STOP that stopped it answered on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      START;
      STOP;
      """
    Then the last command output contains
      """
      stopped domain '{{domain}}': every message in motion finished
      """
    When these Plexus commands fail with "is already stopped"
      """
      STOP;
      """
    Then the last command error contains
      """
      domain '{{domain}}' is already stopped
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
