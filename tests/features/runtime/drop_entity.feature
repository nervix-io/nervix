@drop_entity
Feature: DROP removes an entity at once, discards what it holds and reports it

  A change whose diff only removes entities pauses and drains nothing. Every node disconnects each
  dropped entity from its relays first, destroys it at once with what it holds, releases its share
  of every held message as done, and the statement's answer reports what was discarded.

  @drop_entity_unavailable_destination
  Scenario Outline: DROP EMITTER removes an emitter whose destination is down at once, while another consumer of its relay keeps receiving, on <cluster_size> nodes
    Given Kafka is running
    And HTTP receiver "down" is running
    And HTTP receiver "down" answers unscripted requests with "respond 503"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "drop_down_{{test_id}}" exists with 1 partitions
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
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 1h;
      CREATE RELAY outgoing SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY copied SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR ingestor
        FROM kafka_main TOPIC 'drop_down_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'drop_down_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO outgoing INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT down_api CONFIG {
        'endpoint' = '{{http_receiver.down}}', 'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        ATTACHED
        TO down_api METHOD 'POST' PATH concat('/events/', input.tenant, '/', input.id)
          MODE ACK RETRY POLICY BACKOFF 1s MAX 1m WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION copy FROM outgoing BRANCHED BY by_tenant
        TO copied INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION copied_events TO copied;
      START;
      """
    Then Kafka consumer group "drop_down_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "drop_down_{{test_id}}"
      """
      [{"tenant":"acme","id":"1"},{"tenant":"globex","id":"2"}]
      """
    Then HTTP receiver "down" eventually receives at least 1 request
    And within "30s" the relay subscription receives payloads
      """
      {"id":"1","tenant":"acme"}
      {"id":"2","tenant":"globex"}
      """
    # The destination refuses every attempt, so the emitter keeps both records waiting for a
    # retry, and the source's acknowledgement waits for the emitter's share.
    And within "2s" Kafka consumer group "drop_down_group_{{test_id}}" next offset for topic "drop_down_{{test_id}}" partition 0 is "below 1"
    # The drop disconnects the emitter and destroys it at once: it waits for no retry and no pause.
    # A second session runs the drop, so the subscription stays open across it.
    Given client "operator" is connected to the leader node
    When client "operator" selects domain "{{domain}}"
    And within "15s" client "operator" executes these Plexus commands
      """
      DROP EMITTER published;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    And the last command output contains
      """
      emitter 'published' discarded
      """
    And the last command output contains
      """
      undelivered records (2)
      """
    # The emitter's share of each record counts as done, and the junction delivered its own, so the
    # source hears "acknowledged".
    And within "15s" Kafka consumer group "drop_down_group_{{test_id}}" next offset for topic "drop_down_{{test_id}}" partition 0 is "at least 1"
    When Kafka message is published to topic "drop_down_{{test_id}}"
      """
      [{"tenant":"globex","id":"3"},{"tenant":"acme","id":"4"}]
      """
    Then within "15s" the relay subscription receives payloads
      """
      {"id":"3","tenant":"globex"}
      {"id":"4","tenant":"acme"}
      """
    And within "15s" Kafka consumer group "drop_down_group_{{test_id}}" next offset for topic "drop_down_{{test_id}}" partition 0 is "at least 2"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @drop_entity_reorderer_rows
  Scenario Outline: DROP REORDERER discards the rows it holds at once and the source hears them acknowledged on <cluster_size> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "drop_reorder_{{test_id}}" exists with 1 partitions
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
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 1h;
      CREATE RELAY incoming SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY ordered SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY copied SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR ingestor
        FROM kafka_main TOPIC 'drop_reorder_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'drop_reorder_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO incoming INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE REORDERER order_rows FROM incoming BY input.seq MAX TIME 10m
        BRANCHED BY by_tenant TO ordered INHERIT ALL FLUSH EACH 1h MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
      CREATE JUNCTION copy FROM incoming BRANCHED BY by_tenant
        TO copied INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION copied_rows TO copied;
      START;
      """
    Then Kafka consumer group "drop_reorder_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "drop_reorder_{{test_id}}"
      """
      [{"tenant":"acme","seq":2},{"tenant":"globex","seq":4},{"tenant":"acme","seq":1},{"tenant":"globex","seq":3}]
      """
    Then within "30s" the relay subscription receives payloads
      """
      {"seq":2,"tenant":"acme"}
      {"seq":4,"tenant":"globex"}
      {"seq":1,"tenant":"acme"}
      {"seq":3,"tenant":"globex"}
      """
    # The reorderer holds the rows for up to ten minutes, so the source's acknowledgement waits.
    And within "2s" Kafka consumer group "drop_reorder_group_{{test_id}}" next offset for topic "drop_reorder_{{test_id}}" partition 0 is "below 1"
    # A second session runs the drop, so the subscription stays open across it.
    Given client "operator" is connected to the leader node
    When client "operator" selects domain "{{domain}}"
    And within "15s" client "operator" executes these Plexus commands
      """
      DROP REORDERER order_rows;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    And the last command output contains
      """
      reorderer 'order_rows' discarded
      """
    And the last command output contains
      """
      held rows (4)
      """
    And within "15s" Kafka consumer group "drop_reorder_group_{{test_id}}" next offset for topic "drop_reorder_{{test_id}}" partition 0 is "at least 1"
    # Nothing is sent again: the relay subscription receives no copy of the four rows.
    And the relay subscription does not receive a payload within "3s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @drop_entity_correlator_records
  Scenario Outline: DROP CORRELATOR discards the records waiting for a partner at once on <cluster_size> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "drop_left_{{test_id}}" exists with 1 partitions
    And Kafka topic "drop_right_{{test_id}}" exists with 1 partitions
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
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 1h;
      CREATE RELAY left_events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY right_events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY pairs SCHEMA pair BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR left_ingestor
        FROM kafka_main TOPIC 'drop_left_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'drop_left_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO left_events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC INGESTOR right_ingestor
        FROM kafka_main TOPIC 'drop_right_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'drop_right_group_{{test_id}}' START AT EARLIEST
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
    Then Kafka consumer group "drop_left_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "drop_left_{{test_id}}"
      """
      [{"tenant":"acme","id":"l1"},{"tenant":"globex","id":"l2"}]
      """
    Then within "30s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "left_events" across physical nodes totals 2
    # No right record arrives, so the correlator keeps both left records waiting for a partner.
    And within "2s" Kafka consumer group "drop_left_group_{{test_id}}" next offset for topic "drop_left_{{test_id}}" partition 0 is "below 1"
    # A second session runs the drop, so the subscription stays open across it.
    Given client "operator" is connected to the leader node
    When client "operator" selects domain "{{domain}}"
    And within "15s" client "operator" executes these Plexus commands
      """
      DROP CORRELATOR match_pairs;
      """
    Then the last command output contains
      """
      correlator 'match_pairs' discarded
      """
    And the last command output contains
      """
      waiting records (2)
      """
    And within "15s" Kafka consumer group "drop_left_group_{{test_id}}" next offset for topic "drop_left_{{test_id}}" partition 0 is "at least 1"
    And the relay subscription does not receive a payload within "3s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @drop_entity_required_wait
  Scenario Outline: DROP JUNCTION discards the messages its REQUIRED WAIT retains at once on <cluster_size> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "drop_wait_{{test_id}}" exists with 1 partitions
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
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 1h;
      CREATE RELAY incoming SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY settings SCHEMA tenant_setting BRANCHED BY by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY resolved SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR ingestor
        FROM kafka_main TOPIC 'drop_wait_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'drop_wait_group_{{test_id}}' START AT EARLIEST
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
    Then Kafka consumer group "drop_wait_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "drop_wait_{{test_id}}"
      """
      [{"tenant":"acme","id":"1"},{"tenant":"globex","id":"2"}]
      """
    Then within "30s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "incoming" across physical nodes totals 2
    # No setting exists for either tenant, so the junction retains both messages on REQUIRED WAIT.
    And within "2s" Kafka consumer group "drop_wait_group_{{test_id}}" next offset for topic "drop_wait_{{test_id}}" partition 0 is "below 1"
    # A second session runs the drop, so the subscription stays open across it.
    Given client "operator" is connected to the leader node
    When client "operator" selects domain "{{domain}}"
    And within "15s" client "operator" executes these Plexus commands
      """
      DROP JUNCTION resolve;
      """
    Then the last command output contains
      """
      junction 'resolve' discarded
      """
    And the last command output contains
      """
      waiting messages (2)
      """
    And within "15s" Kafka consumer group "drop_wait_group_{{test_id}}" next offset for topic "drop_wait_{{test_id}}" partition 0 is "at least 1"
    And the relay subscription does not receive a payload within "3s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @drop_entity_window_rows
  Scenario Outline: DROP WINDOW PROCESSOR discards the rows resting in its window at once on <cluster_size> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "drop_window_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, value I64 );
      CREATE SCHEMA summary ( tenant STRING, samples I64 );
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 1h;
      CREATE RELAY incoming SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY summaries SCHEMA summary BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR ingestor
        FROM kafka_main TOPIC 'drop_window_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'drop_window_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO incoming INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR counted FROM incoming
        WIDTH 3 MESSAGES STEP 3 MESSAGES BRANCHED BY by_tenant
        TO summaries SET tenant = FIRST(input.tenant), samples = COUNT(input.value)
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION summary_rows TO summaries;
      START;
      """
    Then Kafka consumer group "drop_window_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "drop_window_{{test_id}}"
      """
      [{"tenant":"acme","value":1},{"tenant":"globex","value":2},{"tenant":"acme","value":3},{"tenant":"globex","value":4}]
      """
    Then within "30s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "incoming" across physical nodes totals 4
    # Each window needs three rows, so both windows retain their two rows and their acknowledgements.
    And within "2s" Kafka consumer group "drop_window_group_{{test_id}}" next offset for topic "drop_window_{{test_id}}" partition 0 is "below 1"
    # A second session runs the drop, so the subscription stays open across it.
    Given client "operator" is connected to the leader node
    When client "operator" selects domain "{{domain}}"
    And within "15s" client "operator" executes these Plexus commands
      """
      DROP WINDOW PROCESSOR counted;
      """
    Then the last command output contains
      """
      window_processor 'counted' discarded
      """
    And the last command output contains
      """
      rows resting in a window (4)
      """
    And within "15s" Kafka consumer group "drop_window_group_{{test_id}}" next offset for topic "drop_window_{{test_id}}" partition 0 is "at least 1"
    And the relay subscription does not receive a payload within "3s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @drop_entity_reingestor_buffers
  Scenario Outline: DROP REINGESTOR discards the messages its route buffers hold at once and the source hears them acknowledged on <cluster_size> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "drop_reingest_{{test_id}}" exists with 1 partitions
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
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 1h;
      CREATE RELAY incoming SCHEMA event UNBRANCHED;
      CREATE RELAY regrouped SCHEMA event BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR ingestor
        FROM kafka_main TOPIC 'drop_reingest_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'drop_reingest_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO incoming INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE REINGESTOR regroup FROM incoming
        TO regrouped INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH EACH 1h MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION regrouped_events TO regrouped;
      START;
      """
    Then Kafka consumer group "drop_reingest_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "drop_reingest_{{test_id}}"
      """
      [{"tenant":"acme","id":"1"},{"tenant":"globex","id":"2"}]
      """
    Then within "30s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "incoming" across physical nodes totals 2
    # The reingestor's route holds both messages for an hour, so the source's acknowledgement waits.
    And within "2s" Kafka consumer group "drop_reingest_group_{{test_id}}" next offset for topic "drop_reingest_{{test_id}}" partition 0 is "below 1"
    # A second session runs the drop, so the subscription stays open across it.
    Given client "operator" is connected to the leader node
    When client "operator" selects domain "{{domain}}"
    And within "15s" client "operator" executes these Plexus commands
      """
      DROP REINGESTOR regroup;
      """
    Then the last command output contains
      """
      reingestor 'regroup' discarded
      """
    And the last command output contains
      """
      unprocessed messages (2)
      """
    And within "15s" Kafka consumer group "drop_reingest_group_{{test_id}}" next offset for topic "drop_reingest_{{test_id}}" partition 0 is "at least 1"
    # The held messages are destroyed, not flushed: the subscription receives neither of them.
    And the relay subscription does not receive a payload within "3s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @drop_entity_generator
  Scenario Outline: DROP GENERATOR ends a running generator at once on <cluster_size> nodes
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA amount ( tenant STRING, amount I64 );
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 1h;
      CREATE RELAY amounts SCHEMA amount BRANCHED BY by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY totals SCHEMA amount BRANCHED BY by_tenant;
      CREATE GENERATOR tenant_totals
        USING MATERIALIZED STATE amounts
        EACH 100ms
        BRANCHED BY by_tenant
        TO totals
          SET tenant = relay_state.amounts.tenant,
              amount = relay_state.amounts.amount
          FLUSH EACH 1h MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG;
      START;
      """
    Given client "operator" is connected to the leader node
    When client "operator" selects domain "{{domain}}"
    And within "15s" client "operator" executes these Plexus commands
      """
      DROP GENERATOR tenant_totals;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    And the last command output contains
      """
      generator 'tenant_totals' discarded nothing, state removed
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @drop_entity_configuration
  Scenario: A parametrizer and a signaling protocol are refused while something names them and dropped once nothing does
    Given a 1 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING );
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 1h;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge drop-config-{{test_id}}.example.com;
      CREATE SIGNALING PROTOCOL subscribe_first
        FORMAT JSON
        ON CONNECT
        SEND JAQ '{method: "SUBSCRIBE", id: 1}'
        WAIT JAQ '.id == 1 and .result == null' FAIL JAQ '.error'
        TIMEOUT 5s;
      CREATE WEBSOCKETS ENDPOINT stream_endpoint
        ON edge
        PATH '/ws' WITH SIGNALING PROTOCOL subscribe_first;
      """
    When these Plexus commands fail with "cannot delete model 'by_tenant'"
      """
      DROP PARAMETRIZER by_tenant;
      """
    And these Plexus commands fail with "cannot delete model 'subscribe_first'"
      """
      DROP SIGNALING PROTOCOL subscribe_first;
      """
    When these Plexus commands are executed on the leader node
      """
      DROP RELAY events;
      DROP PARAMETRIZER by_tenant;
      DROP ENDPOINT stream_endpoint;
      DROP SIGNALING PROTOCOL subscribe_first;
      """
    Then the last command output contains
      """
      dropped model 'subscribe_first'
      """

  @drop_entity_relay_batches
  Scenario Outline: A transaction that drops an ingestor, its relay and the emitter reading it discards the batches the relay holds at once on <cluster_size> nodes
    Given Kafka is running
    And HTTP receiver "down" is running
    And HTTP receiver "down" answers unscripted requests with "respond 503"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "drop_relay_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, id STRING );
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA event UNBRANCHED CAPACITY 1;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR ingestor
        FROM kafka_main TOPIC 'drop_relay_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'drop_relay_group_{{test_id}}' START AT EARLIEST
          MODE ACK PARALLEL MAX 16 BATCH TIMEOUT 100ms ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
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
    Then Kafka consumer group "drop_relay_group_{{test_id}}" eventually has 1 consumers
    # Each Kafka message becomes one batch. The emitter retries the first one, so the relay keeps
    # the batches after it: one in the emitter's queue, the rest in the relay's own buffer.
    When Kafka message is published to topic "drop_relay_{{test_id}}"
      """
      [{"tenant":"acme","id":"1"}]
      """
    And Kafka message is published to topic "drop_relay_{{test_id}}"
      """
      [{"tenant":"acme","id":"2"}]
      """
    And Kafka message is published to topic "drop_relay_{{test_id}}"
      """
      [{"tenant":"acme","id":"3"}]
      """
    And Kafka message is published to topic "drop_relay_{{test_id}}"
      """
      [{"tenant":"acme","id":"4"}]
      """
    # The emitter retries the first batch it took, so the relay and the emitter's queue hold what
    # came after it.
    Then HTTP receiver "down" eventually receives at least 2 requests
    When within "15s" these Plexus commands complete on the leader node
      """
      BEGIN;
      DROP EMITTER published;
      DROP INGESTOR ingestor;
      DROP RELAY outgoing;
      COMMIT;
      """
    Then the last command output contains
      """
      emitter 'published' discarded
      """
    And the last command output contains
      """
      relay 'outgoing' discarded
      """
    And Kafka consumer group "drop_relay_group_{{test_id}}" eventually has 0 consumers

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @drop_entity_retained_payloads
  Scenario Outline: An ingestor dropped inside a domain pause discards the payloads it retained for the pause on <cluster_size> nodes
    Given HTTP receiver "held" is running
    And HTTP receiver "held" answers unscripted requests with "hold response until released"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY replayed SCHEMA event UNBRANCHED;
      CREATE VHOST edge drop-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR incoming
        FROM intake MODE ACK MAX 8 ACK TIMEOUT 2m
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND AFTER RESUME DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT held_api CONFIG {
        'endpoint' = '{{http_receiver.held}}', 'timeout_ms' = 120000
      };
      CREATE HTTP REQUEST EMITTER published FROM events
        ATTACHED
        TO held_api METHOD 'POST' PATH '/events'
          MODE ACK RETRY POLICY BACKOFF 1s MAX 1m WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When endpoint request "first" is posted on node "node-1" host "drop-{{test_id}}.example.com" path "/events"
      """
      {"value":1}
      """
    Then HTTP receiver "held" eventually receives at least 1 request
    # A new input of the emitter changes the topology, which pauses the domain, and its drain waits
    # for the publish the receiver holds. The ingestor dropped in the same run lies inside that
    # pause, so it retains what arrives meanwhile.
    When these Plexus commands begin executing in the background as "drop"
      """
      BEGIN;
      ALTER HTTP REQUEST EMITTER published ADD FROM replayed;
      DROP INGESTOR incoming;
      COMMIT;
      """
    Then background Plexus execution "drop" is still running after "2s"
    And within "30s" node "node-1" eventually reports describe ingestor "incoming" as "status: quiesced"
    When endpoint request "waiting" is posted on node "node-1" host "drop-{{test_id}}.example.com" path "/events"
      """
      {"value":2}
      """
    Then within "15s" node "node-1" eventually reports describe ingestor "incoming" as "nervix_ingestor_quiesce_buffered_records: 1"
    When endpoint request "also_waiting" is posted on node "node-1" host "drop-{{test_id}}.example.com" path "/events"
      """
      {"value":3}
      """
    Then within "15s" node "node-1" eventually reports describe ingestor "incoming" as "nervix_ingestor_quiesce_buffered_records: 2"
    When HTTP receiver "held" releases its held responses with "respond 204"
    Then within "60s" background Plexus execution "drop" reaches a successful outcome
    And the last command output contains
      """
      ingestor 'incoming' discarded retained payloads (2), state removed
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @drop_entity_recreated_empty
  Scenario: A deduplicator dropped while a node is down and created again starts empty on every node
    Given Kafka is running
    And runtime replication is configured with replica count 2 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And Kafka topic "drop_dedup_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA txn ( tenant STRING, transaction_id STRING );
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA txn
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 1h;
      CREATE RELAY incoming SCHEMA txn BRANCHED BY by_tenant;
      CREATE RELAY unique_txns SCHEMA txn BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR ingestor
        FROM kafka_main TOPIC 'drop_dedup_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'drop_dedup_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO incoming INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR dedup_txns FROM incoming
        DEDUPLICATE ON input.transaction_id
        MAX TIME 10m
        BRANCHED BY by_tenant
        TO unique_txns INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION unique_rows TO unique_txns;
      START;
      """
    Then Kafka consumer group "drop_dedup_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "drop_dedup_{{test_id}}"
      """
      [{"tenant":"acme","transaction_id":"t1"},{"tenant":"globex","transaction_id":"t2"}]
      """
    Then within "30s" the relay subscription receives payloads
      """
      {"tenant":"acme","transaction_id":"t1"}
      {"tenant":"globex","transaction_id":"t2"}
      """
    # Every node keeps a copy of the deduplicator's keys: its owner and both replicas.
    When node "node-3" is stopped
    Then within "15s" these Plexus commands complete on node "node-1"
      """
      DROP DEDUPLICATOR dedup_txns;
      """
    And the last command output contains
      """
      deduplicator 'dedup_txns' discarded nothing
      """
    When node "node-3" is started
    # The session that ran the drop is the one the subscription reads from now.
    Then within "60s" these Plexus commands complete on node "node-1"
      """
      CREATE DEDUPLICATOR dedup_txns FROM incoming
        DEDUPLICATE ON input.transaction_id
        MAX TIME 10m
        BRANCHED BY by_tenant
        TO unique_txns INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION unique_rows_again TO unique_txns;
      """
    When Kafka message is published to topic "drop_dedup_{{test_id}}"
      """
      [{"tenant":"acme","transaction_id":"t1"},{"tenant":"globex","transaction_id":"t2"}]
      """
    # The new deduplicator remembers no key of the dropped one, wherever it runs, so both repeated
    # keys pass again.
    Then within "30s" the relay subscription receives payloads
      """
      {"tenant":"acme","transaction_id":"t1"}
      {"tenant":"globex","transaction_id":"t2"}
      """
