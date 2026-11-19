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
    And HTTP receiver "slow" eventually captures requests that are
      """
      POST /events/acme/1
      POST /events/globex/2
      POST /events/acme/3
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
    And within "5s" the observed broker receives exactly 8 messages

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
    Then HTTP receiver "slow" eventually captures requests that are
      """
      POST /events/acme/1
      POST /events/acme/2
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
        BRANCHED BY by_tenant TO ordered INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
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
    And within "30s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "incoming" across physical nodes totals 4

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
    When HTTP receiver "down" answers unscripted requests with "respond 204"
    And these Plexus commands are executed on the leader node
      """
      START;
      """
    Then within "30s" Kafka consumer group "stop_down_group_{{test_id}}" next offset for topic "stop_down_{{test_id}}" partition 0 is "at least 1"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
