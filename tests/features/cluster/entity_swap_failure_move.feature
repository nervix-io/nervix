@entity_swap_failure
Feature: A move whose swap fails holds the moved unit until every node applied it
  A planned move commits the new owner only after the unit drained, froze and its state was staged
  on the destination. When a node then cannot apply the move, the move answers which node and why,
  the moved unit's records wait at its gate, and nothing is negatively acknowledged, dropped or sent
  again. The unit resumes on its new owner, with its state, once every node applied the move.

  @entity_swap_failure_move
  Scenario Outline: A move whose swap fails on the <side> keeps the deduplicator's records waiting and loses none
    Given runtime replication is configured with replica count 0 and snapshot interval "10m"
    And Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And Kafka topic "move_acked_{{test_id}}" exists with 1 partitions
    And Kafka topic "move_unacked_{{test_id}}" exists with 1 partitions
    And Kafka topic "move_output_{{test_id}}" exists with 1 partitions
    And Kafka topic "move_output_{{test_id}}" is observed
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these Plexus commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA move_event ( event_id STRING, tenant STRING, content STRING );
      CREATE WIRE JSON SCHEMA move_event_wire MODE STRICT (
        event_id string,
        tenant string,
        content string
      );
      CREATE WIRE JSON CODEC move_event_codec
        FROM WIRE SCHEMA move_event_wire
        TO SCHEMA move_event;
      CREATE SCHEMA move_tenant ( tenant STRING );
      CREATE BRANCH by_move_tenant SCHEMA move_tenant TTL 1h;
      CREATE RELAY move_events SCHEMA move_event BRANCHED BY by_move_tenant;
      CREATE RELAY move_unique_events SCHEMA move_event BRANCHED BY by_move_tenant;
      CREATE KAFKA CLIENT move_kafka CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}'
      };
      CREATE KAFKA TOPIC INGESTOR move_acked
        FROM move_kafka TOPIC 'move_acked_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'move_acked_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m
          RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING move_event_codec
        TO move_events
          INHERIT ALL
          BRANCHED BY by_move_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC INGESTOR move_unacked
        FROM move_kafka TOPIC 'move_unacked_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'move_unacked_{{test_id}}' START AT EARLIEST
          MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING move_event_codec
        TO move_events
          INHERIT ALL
          BRANCHED BY by_move_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR move_dedup FROM move_events
        DEDUPLICATE ON input.event_id
        MAX TIME 10m
        BRANCHED BY by_move_tenant
        TO move_unique_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE KAFKA TOPIC EMITTER move_output
        FROM move_unique_events
        ATTACHED
        TO move_kafka TOPIC 'move_output_{{test_id}}'
          MODE ACK PARALLEL MAX 16 ACK TIMEOUT 5m
          RETRY POLICY BACKOFF 200ms MAX 5s
          ENCODE USING move_event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And these Plexus commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    When these Kafka messages are rapidly published to topic "move_acked_{{test_id}}"
      """
      {"event_id":"a-1","tenant":"acme","content":"c-a-1"}
      {"event_id":"a-2","tenant":"beta","content":"c-a-2"}
      {"event_id":"a-3","tenant":"acme","content":"c-a-3"}
      {"event_id":"a-4","tenant":"beta","content":"c-a-4"}
      """
    And these Kafka messages are rapidly published to topic "move_unacked_{{test_id}}"
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
    # The swap on <side> node <failing_node> fails where it replaces the deduplicator, and keeps
    # failing until the fault is cleared. Records arrive while the swap holds there.
    Given every entity swap in domain "{{domain}}" on node "<failing_node>" fails while replacing deduplicator "move_dedup"
    And the entity swap in domain "{{domain}}" on node "<failing_node>" pauses while replacing deduplicator "move_dedup"
    When these Plexus commands begin executing in the background
      """
      RELOCATE DEDUPLICATOR move_dedup ONTO NODE node-2 IGNORE PREFERENCES;
      """
    Then the entity swap in domain "{{domain}}" on node "<failing_node>" reaches its replacement pause
    When these Kafka messages are rapidly published to topic "move_acked_{{test_id}}"
      """
      {"event_id":"a-5","tenant":"acme","content":"c-a-5"}
      {"event_id":"a-6","tenant":"beta","content":"c-a-6"}
      """
    And these Kafka messages are rapidly published to topic "move_unacked_{{test_id}}"
      """
      {"event_id":"u-5","tenant":"beta","content":"c-u-5"}
      {"event_id":"u-6","tenant":"acme","content":"c-u-6"}
      """
    And the entity swap in domain "{{domain}}" on node "<failing_node>" is released from its replacement pause
    Then the background Plexus execution fails with "injected entity-level schedule apply failure"
    And the last command error contains
      """
      <failing_node>
      """
    # The moved unit stays held while node <failing_node> has not applied the move: the hold ends
    # only when that node applies it, which cannot happen before the fault is cleared below.
    And the observed broker does not receive a payload within "5s"
    When entity swaps in domain "{{domain}}" on node "<failing_node>" stop failing
    Then within "60s" the observed broker receives exactly these payloads
      """
      {"event_id":"a-5","tenant":"acme","content":"c-a-5"}
      {"event_id":"a-6","tenant":"beta","content":"c-a-6"}
      {"event_id":"u-5","tenant":"beta","content":"c-u-5"}
      {"event_id":"u-6","tenant":"acme","content":"c-u-6"}
      """
    When these Plexus commands are executed on the active session
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=deduplicator name=move_dedup owner=node-2
      """
    # The deduplicator moved with its state: it remembers what each branch saw before the move and
    # keeps the branches apart.
    When these Kafka messages are rapidly published to topic "move_acked_{{test_id}}"
      """
      {"event_id":"a-1","tenant":"acme","content":"c-repeat-a-1"}
      {"event_id":"a-1","tenant":"beta","content":"c-a-1-beta"}
      {"event_id":"a-7","tenant":"beta","content":"c-a-7"}
      """
    And these Kafka messages are rapidly published to topic "move_unacked_{{test_id}}"
      """
      {"event_id":"u-1","tenant":"beta","content":"c-repeat-u-1"}
      {"event_id":"u-7","tenant":"acme","content":"c-u-7"}
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"event_id":"a-1","tenant":"beta","content":"c-a-1-beta"}
      {"event_id":"a-7","tenant":"beta","content":"c-a-7"}
      {"event_id":"u-7","tenant":"acme","content":"c-u-7"}
      """
    And cluster observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually totals 9
      """
      domain="{{domain}}"
      ingestor="move_acked"
      outcome="acknowledged"
      """
    And cluster observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually totals 0
      """
      domain="{{domain}}"
      ingestor="move_acked"
      outcome="failed"
      """
    And cluster observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually totals 0
      """
      domain="{{domain}}"
      ingestor="move_acked"
      outcome="timed_out"
      """
    And cluster observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually totals 0
      """
      domain="{{domain}}"
      ingestor="move_acked"
      outcome="shutdown"
      """

    Examples:
      | side        | failing_node |
      | source      | node-1       |
      | destination | node-2       |
