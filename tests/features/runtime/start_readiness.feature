@start_readiness
Feature: A START that waits for a node keeps its domain running
  START commits the domain running, then waits until every live node has prepared the revision,
  started the domain's ingestors and reported it ready. A node that is not ready yet keeps START
  waiting, past the readiness deadline if need be, while the domain runs and takes input on every
  node that started it. START never stops the domain again, so nothing the domain took in is cut
  or delivered twice.

  @start_readiness_held_node
  Scenario Outline: A START waiting for a node that is not ready keeps the domain running and delivers every record once on <cluster_size> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "start_ready_acked_{{test_id}}" exists with 1 partitions
    And Kafka topic "start_ready_unacked_{{test_id}}" exists with 1 partitions
    And Kafka topic "start_ready_out_{{test_id}}" exists with 1 partitions
    And Kafka topic "start_ready_out_{{test_id}}" is observed
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
      CREATE KAFKA TOPIC INGESTOR acked
        FROM kafka_main TOPIC 'start_ready_acked_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'start_ready_acked_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO outgoing INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC INGESTOR unacked
        FROM kafka_main TOPIC 'start_ready_unacked_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'start_ready_unacked_group_{{test_id}}' START AT EARLIEST
          MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO outgoing INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER published FROM outgoing
        ATTACHED
        TO kafka_main TOPIC 'start_ready_out_{{test_id}}'
          MODE ACK PARALLEL MAX 16 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And runtime readiness for domain "{{domain}}" on node "<held_node>" is paused
    And these Plexus commands begin executing in the background
      """
      START;
      """
    # The held node prepared the revision and started its ingestors before the hold, so the domain
    # runs on every node and takes input from both sources while START waits for that node.
    Then within "60s" node "<held_node>" reaches its runtime readiness pause for domain "{{domain}}"
    And Kafka consumer group "start_ready_acked_group_{{test_id}}" eventually has 1 consumers
    And Kafka consumer group "start_ready_unacked_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "start_ready_acked_{{test_id}}"
      """
      [{"tenant":"acme","id":"a1"},{"tenant":"globex","id":"a2"},{"tenant":"acme","id":"a3"},{"tenant":"globex","id":"a4"}]
      """
    And Kafka message is published to topic "start_ready_unacked_{{test_id}}"
      """
      [{"tenant":"globex","id":"u1"},{"tenant":"acme","id":"u2"},{"tenant":"globex","id":"u3"},{"tenant":"acme","id":"u4"}]
      """
    Then within "60s" the observed broker receives exactly these payloads
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
    # A scenario cluster's readiness deadline is 40 seconds: its 10-second node-unavailability
    # timeout plus the 30-second readiness propagation bound. START cannot answer while the node is
    # held; past the deadline it applies the revision again and keeps waiting, and the domain runs
    # on.
    And the background Plexus execution is still running after "45s"
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      status: running
      """
    When runtime readiness for domain "{{domain}}" on node "<held_node>" is released
    Then within "120s" the background Plexus execution reaches a successful outcome
    And the last command output contains
      """
      starting domain '{{domain}}'
      """
    # Nothing was cut while START waited: both sources go on, and no record the domain already
    # delivered arrives again.
    When Kafka message is published to topic "start_ready_acked_{{test_id}}"
      """
      [{"tenant":"globex","id":"a5"},{"tenant":"acme","id":"a6"}]
      """
    And Kafka message is published to topic "start_ready_unacked_{{test_id}}"
      """
      [{"tenant":"acme","id":"u5"},{"tenant":"globex","id":"u6"}]
      """
    Then within "60s" the observed broker receives exactly these payloads
      """
      {"tenant":"globex","id":"a5"}
      {"tenant":"acme","id":"a6"}
      {"tenant":"acme","id":"u5"}
      {"tenant":"globex","id":"u6"}
      """
    # The acknowledged source's position covers both of its messages, so it sends neither again.
    And within "30s" Kafka consumer group "start_ready_acked_group_{{test_id}}" next offset for topic "start_ready_acked_{{test_id}}" partition 0 is "at least 2"

    Examples:
      | cluster_size | held_node |
      | 1            | node-1    |
      | 3            | node-3    |
