@entity_swap_failure
Feature: A failed entity swap keeps the running domain
  A planned change replaces only the entities it changes on each node. When that replacement fails
  on a node, the node keeps the entities it was replacing running with their state and what they
  hold, the change reports its failure with its cause, and no record is negatively acknowledged,
  dropped or sent again. The node applies the committed change once its swap can succeed.

  @entity_swap_failure_alteration
  Scenario Outline: An alteration whose swap fails keeps the previous deduplicator and loses no record
    Given Kafka is running
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "swap_acked_{{test_id}}" exists with 1 partitions
    And Kafka topic "swap_unacked_{{test_id}}" exists with 1 partitions
    And Kafka topic "swap_output_{{test_id}}" exists with 1 partitions
    And Kafka topic "swap_output_{{test_id}}" is observed
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA swap_event ( event_id STRING, tenant STRING, content STRING );
      CREATE WIRE JSON SCHEMA swap_event_wire MODE STRICT (
        event_id string,
        tenant string,
        content string
      );
      CREATE WIRE JSON CODEC swap_event_codec
        FROM WIRE SCHEMA swap_event_wire
        TO SCHEMA swap_event;
      CREATE SCHEMA swap_tenant ( tenant STRING );
      CREATE PARAMETRIZER by_swap_tenant SCHEMA swap_tenant TTL 1h;
      CREATE RELAY swap_events SCHEMA swap_event BRANCHED BY by_swap_tenant;
      CREATE RELAY swap_unique_events SCHEMA swap_event BRANCHED BY by_swap_tenant;
      CREATE KAFKA CLIENT swap_kafka CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}'
      };
      CREATE KAFKA TOPIC INGESTOR swap_acked
        FROM swap_kafka TOPIC 'swap_acked_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'swap_acked_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m
          RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING swap_event_codec
        TO swap_events
          INHERIT ALL
          BRANCHED BY by_swap_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC INGESTOR swap_unacked
        FROM swap_kafka TOPIC 'swap_unacked_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'swap_unacked_{{test_id}}' START AT EARLIEST
          MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING swap_event_codec
        TO swap_events
          INHERIT ALL
          BRANCHED BY by_swap_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR swap_dedup FROM swap_events
        DEDUPLICATE ON input.event_id
        MAX TIME 10m
        BRANCHED BY by_swap_tenant
        TO swap_unique_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE KAFKA TOPIC EMITTER swap_output
        FROM swap_unique_events
        ATTACHED
        TO swap_kafka TOPIC 'swap_output_{{test_id}}'
          MODE ACK PARALLEL MAX 16 ACK TIMEOUT 5m
          RETRY POLICY BACKOFF 200ms MAX 5s
          ENCODE USING swap_event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "deduplicator" "swap_dedup" is saved as placeholder "dedup_owner"
    # Every entity runs on the deduplicator's node, so no record reaches it as a routed delivery
    # from another node. A routed delivery that meets a swap there is still refused, which loses a
    # MODE NO ACK record; Relay Capacity 02 (https://app.clickup.com/t/86bd1z226) removes that
    # refusal, and this scenario regains a cross-node placement there.
    When these Plexus commands are executed on the leader node
      """
      RELOCATE INGESTOR swap_acked, INGESTOR swap_unacked, RELAY swap_events, RELAY swap_unique_events, EMITTER swap_output ONTO NODE {{dedup_owner}} IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "ingestor" "swap_acked" is saved as placeholder "acked_owner"
    And the last cluster status owner for scheduled "ingestor" "swap_unacked" is saved as placeholder "unacked_owner"
    And the last cluster status owner for scheduled "relay" "swap_events" is saved as placeholder "input_relay_owner"
    And the last cluster status owner for scheduled "relay" "swap_unique_events" is saved as placeholder "output_relay_owner"
    And the last cluster status owner for scheduled "emitter" "swap_output" is saved as placeholder "emitter_owner"
    And placeholder "acked_owner" equals placeholder "dedup_owner"
    And placeholder "unacked_owner" equals placeholder "dedup_owner"
    And placeholder "input_relay_owner" equals placeholder "dedup_owner"
    And placeholder "output_relay_owner" equals placeholder "dedup_owner"
    And placeholder "emitter_owner" equals placeholder "dedup_owner"
    When these Kafka messages are rapidly published to topic "swap_acked_{{test_id}}"
      """
      {"event_id":"a-1","tenant":"acme","content":"c-a-1"}
      {"event_id":"a-2","tenant":"beta","content":"c-a-2"}
      {"event_id":"a-3","tenant":"acme","content":"c-a-3"}
      {"event_id":"a-4","tenant":"beta","content":"c-a-4"}
      """
    And these Kafka messages are rapidly published to topic "swap_unacked_{{test_id}}"
      """
      {"event_id":"u-1","tenant":"beta","content":"c-u-1"}
      {"event_id":"u-2","tenant":"acme","content":"c-u-2"}
      {"event_id":"u-3","tenant":"beta","content":"c-u-3"}
      {"event_id":"u-4","tenant":"acme","content":"c-u-4"}
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"event_id":"a-1","tenant":"acme","content":"c-a-1"}
      {"event_id":"a-2","tenant":"beta","content":"c-a-2"}
      {"event_id":"a-3","tenant":"acme","content":"c-a-3"}
      {"event_id":"a-4","tenant":"beta","content":"c-a-4"}
      {"event_id":"u-1","tenant":"beta","content":"c-u-1"}
      {"event_id":"u-2","tenant":"acme","content":"c-u-2"}
      {"event_id":"u-3","tenant":"beta","content":"c-u-3"}
      {"event_id":"u-4","tenant":"acme","content":"c-u-4"}
      """
    # The swap on the deduplicator's node fails once the deduplicator has handed its branches over,
    # before its replacement starts, and keeps failing until the fault is cleared. Records arrive
    # while the swap holds there.
    Given every entity swap in domain "{{domain}}" on node "{{dedup_owner}}" fails while replacing deduplicator "swap_dedup"
    And the entity swap in domain "{{domain}}" on node "{{dedup_owner}}" pauses while replacing deduplicator "swap_dedup"
    When these Plexus commands begin executing in the background
      """
      ALTER DEDUPLICATOR swap_dedup SET DEDUPLICATE ON input.content;
      """
    Then the entity swap in domain "{{domain}}" on node "{{dedup_owner}}" reaches its replacement pause
    When these Kafka messages are rapidly published to topic "swap_acked_{{test_id}}"
      """
      {"event_id":"a-5","tenant":"acme","content":"c-a-5"}
      {"event_id":"a-6","tenant":"beta","content":"c-a-6"}
      """
    And these Kafka messages are rapidly published to topic "swap_unacked_{{test_id}}"
      """
      {"event_id":"u-5","tenant":"beta","content":"c-u-5"}
      {"event_id":"u-6","tenant":"acme","content":"c-u-6"}
      """
    And the entity swap in domain "{{domain}}" on node "{{dedup_owner}}" is released from its replacement pause
    Then the background Plexus execution fails with "injected entity-level schedule apply failure"
    And within "30s" the observed broker receives exactly these payloads
      """
      {"event_id":"a-5","tenant":"acme","content":"c-a-5"}
      {"event_id":"a-6","tenant":"beta","content":"c-a-6"}
      {"event_id":"u-5","tenant":"beta","content":"c-u-5"}
      {"event_id":"u-6","tenant":"acme","content":"c-u-6"}
      """
    # The previous deduplicator still runs with its state: it keys on event_id, remembers what each
    # branch saw before the change, and keeps the branches apart.
    When these Kafka messages are rapidly published to topic "swap_acked_{{test_id}}"
      """
      {"event_id":"a-1","tenant":"acme","content":"c-repeat-a-1"}
      {"event_id":"a-1","tenant":"beta","content":"c-a-1-beta"}
      {"event_id":"a-7","tenant":"beta","content":"c-a-2"}
      """
    And these Kafka messages are rapidly published to topic "swap_unacked_{{test_id}}"
      """
      {"event_id":"u-1","tenant":"beta","content":"c-repeat-u-1"}
      {"event_id":"u-7","tenant":"acme","content":"c-u-2"}
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"event_id":"a-1","tenant":"beta","content":"c-a-1-beta"}
      {"event_id":"a-7","tenant":"beta","content":"c-a-2"}
      {"event_id":"u-7","tenant":"acme","content":"c-u-2"}
      """
    # Once its swap can succeed, the node applies the committed change. A later change completes
    # only when every node runs the newest revision, which includes the earlier one.
    When entity swaps in domain "{{domain}}" on node "{{dedup_owner}}" stop failing
    And these Plexus commands are executed on the leader node
      """
      ALTER DEDUPLICATOR swap_dedup SET MAX TIME 9m;
      """
    And these Kafka messages are rapidly published to topic "swap_acked_{{test_id}}"
      """
      {"event_id":"a-8","tenant":"acme","content":"c-after"}
      {"event_id":"a-9","tenant":"acme","content":"c-after"}
      """
    And these Kafka messages are rapidly published to topic "swap_unacked_{{test_id}}"
      """
      {"event_id":"u-8","tenant":"beta","content":"c-after"}
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"event_id":"a-8","tenant":"acme","content":"c-after"}
      {"event_id":"u-8","tenant":"beta","content":"c-after"}
      """
    And cluster observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually totals 11
      """
      domain="{{domain}}"
      ingestor="swap_acked"
      outcome="acknowledged"
      """
    And cluster observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually totals 0
      """
      domain="{{domain}}"
      ingestor="swap_acked"
      outcome="failed"
      """
    And cluster observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually totals 0
      """
      domain="{{domain}}"
      ingestor="swap_acked"
      outcome="timed_out"
      """
    And cluster observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually totals 0
      """
      domain="{{domain}}"
      ingestor="swap_acked"
      outcome="shutdown"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @entity_swap_failure_mqtt_input
  Scenario Outline: An MQTT alteration whose swap fails keeps the previous broker session and every publish queued in it
    Given MQTT is running
    And a <cluster_size> node nervix cluster is started
    And an MQTT session ingestor named "events" runs with client id "swap_{{test_id}}"
    When these Plexus statements are executed
      """
      CREATE SUBSCRIPTION swap_output TO output;
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "ingestor" "events" is saved as placeholder "ingestor_owner"
    When MQTT QoS 1 message is published to topic "session_{{test_id}}"
      """
      {"user_id":71}
      """
    Then the relay subscription receives a payload
      """
      "user_id":71
      """
    # The alteration keeps the client id and changes the topic, so the previous input and its
    # replacement use one broker session. The swap on the ingestor's node fails once the ingestor's
    # units have stopped, before the replacement starts, and keeps failing until the fault is
    # cleared. A publish arrives while the swap holds there, and the broker queues it in the
    # stopped session.
    Given every entity swap in domain "{{domain}}" on node "{{ingestor_owner}}" fails while replacing ingestor "events"
    And the entity swap in domain "{{domain}}" on node "{{ingestor_owner}}" pauses while replacing ingestor "events"
    When these Plexus commands begin executing in the background
      """
      ALTER MQTT TOPIC INGESTOR events
        SET FROM broker TOPIC 'replacement_{{test_id}}'
        SESSION PERSISTENT QOS 1 MODE NO ACK ON QUIESCE SUSPEND;
      """
    Then the entity swap in domain "{{domain}}" on node "{{ingestor_owner}}" reaches its replacement pause
    When MQTT QoS 2 message is published directly to broker topic "session_{{test_id}}"
      """
      {"user_id":72}
      """
    And the entity swap in domain "{{domain}}" on node "{{ingestor_owner}}" is released from its replacement pause
    Then the background Plexus execution fails with "injected entity-level schedule apply failure"
    # The previous ingestor is back with its session and reads what the broker queued in it.
    And within "30s" the relay subscription receives a payload
      """
      "user_id":72
      """
    When MQTT QoS 1 message is published to topic "session_{{test_id}}"
      """
      {"user_id":73}
      """
    Then the relay subscription receives a payload
      """
      "user_id":73
      """
    And the relay subscription does not receive a payload within "1s"
    # Once its swap can succeed, the node applies the newest change, and the replacement reads its
    # own topic only.
    When entity swaps in domain "{{domain}}" on node "{{ingestor_owner}}" stop failing
    And these Plexus statements are executed
      """
      ALTER MQTT TOPIC INGESTOR events
        SET FROM broker TOPIC 'final_{{test_id}}'
        SESSION PERSISTENT QOS 1 MODE NO ACK ON QUIESCE SUSPEND;
      """
    Then within "30s" DESCRIBE INGESTOR "events" on the leader node contains
      """
      ready: true
      """
    When MQTT QoS 1 message is published to topic "session_{{test_id}}"
      """
      {"user_id":74}
      """
    And MQTT QoS 1 message is published to topic "final_{{test_id}}"
      """
      {"user_id":75}
      """
    Then the relay subscription receives a payload
      """
      "user_id":75
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @entity_swap_failure_pulsar_input
  Scenario Outline: A Pulsar alteration whose swap fails keeps the previous owned subscription and every message on it
    Given Pulsar is running
    And a <cluster_size> node nervix cluster is started
    And Pulsar subscription "preceding_{{test_id}}" of topic "preceding_{{test_id}}" is provisioned
    And Pulsar subscription "replacement_{{test_id}}" of topic "replacement_{{test_id}}" is provisioned
    And a Pulsar domain graph reads through "{{pulsar_addr}}"
    And Pulsar ingestor "events" reads subscription "preceding_{{test_id}}" of topic "preceding_{{test_id}}" declared "OWNED BY INGESTOR START AT EARLIEST TYPE SHARED"
    Then within "30s" Pulsar subscription "preceding_{{test_id}}" of topic "preceding_{{test_id}}" has 1 consumer
    When these Plexus statements are executed
      """
      CREATE SUBSCRIPTION swap_output TO output;
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "ingestor" "events" is saved as placeholder "ingestor_owner"
    When Pulsar message is published to topic "preceding_{{test_id}}"
      """
      {"user_id":81}
      """
    Then within "30s" the relay subscription receives a payload
      """
      "user_id":81
      """
    # The replacement reads another subscription, so the alteration retires the owned one. The swap
    # on the ingestor's node fails once the ingestor's units have stopped, before the replacement
    # starts. A message arrives while the swap holds there, and the subscription keeps it.
    Given every entity swap in domain "{{domain}}" on node "{{ingestor_owner}}" fails while replacing ingestor "events"
    And the entity swap in domain "{{domain}}" on node "{{ingestor_owner}}" pauses while replacing ingestor "events"
    When these Plexus commands begin executing in the background
      """
      ALTER PULSAR TOPIC INGESTOR events
        SET FROM broker TOPIC 'replacement_{{test_id}}' SUBSCRIPTION 'replacement_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST TYPE SHARED
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s ON QUIESCE SUSPEND;
      """
    Then the entity swap in domain "{{domain}}" on node "{{ingestor_owner}}" reaches its replacement pause
    When Pulsar message is published to topic "preceding_{{test_id}}"
      """
      {"user_id":82}
      """
    And the entity swap in domain "{{domain}}" on node "{{ingestor_owner}}" is released from its replacement pause
    Then the background Plexus execution fails with "injected entity-level schedule apply failure"
    # The previous ingestor is back on its subscription and reads on from where it stopped: what
    # arrived meanwhile, once, and nothing it had already read.
    And within "30s" the relay subscription receives a payload
      """
      "user_id":82
      """
    And the relay subscription does not receive a payload within "1s"
    And within "30s" Pulsar subscription "preceding_{{test_id}}" of topic "preceding_{{test_id}}" has 1 consumer
    # Once its swap can succeed, the node applies the newest change and removes the subscription
    # only the previous input read.
    When entity swaps in domain "{{domain}}" on node "{{ingestor_owner}}" stop failing
    And these Plexus statements are executed
      """
      ALTER PULSAR TOPIC INGESTOR events
        SET FROM broker TOPIC 'replacement_{{test_id}}' SUBSCRIPTION 'replacement_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST TYPE SHARED
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 3s ON QUIESCE SUSPEND;
      """
    Then within "30s" Pulsar subscription "replacement_{{test_id}}" of topic "replacement_{{test_id}}" has 1 consumer
    And within "60s" Pulsar topic "preceding_{{test_id}}" has no durable subscription "preceding_{{test_id}}"
    When Pulsar message is published to topic "replacement_{{test_id}}"
      """
      {"user_id":83}
      """
    Then within "30s" the relay subscription receives a payload
      """
      "user_id":83
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
