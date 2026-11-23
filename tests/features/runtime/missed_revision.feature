@missed_revision
Feature: A node plans each domain's change from the revision that domain runs
  A node applies the committed schedule one domain after another. When an attempt fails part way,
  the domains it already brought to the new revision run it, while the node still records the
  earlier revision as the one it applied. Such a domain then takes the node's next change from the
  revision it runs, in place: its running work is neither cut nor rebuilt, and no record is
  negatively acknowledged, dropped or sent again.

  @missed_revision_in_place
  Scenario Outline: A domain a failed attempt moved forward takes the next change in place
    Given Kafka is running
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "lag_acked_{{test_id}}" exists with 1 partitions
    And Kafka topic "lag_unacked_{{test_id}}" exists with 1 partitions
    And Kafka topic "lag_output_{{test_id}}" exists with 1 partitions
    And Kafka topic "lag_output_{{test_id}}" is observed
    And the active domain is saved as placeholder "flowing_domain"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{flowing_domain}};
      CREATE SCHEMA lag_event ( event_id STRING, tenant STRING, content STRING );
      CREATE WIRE JSON SCHEMA lag_event_wire MODE STRICT (
        event_id string,
        tenant string,
        content string
      );
      CREATE WIRE JSON CODEC lag_event_codec
        FROM WIRE SCHEMA lag_event_wire
        TO SCHEMA lag_event;
      CREATE SCHEMA lag_tenant ( tenant STRING );
      CREATE PARAMETRIZER by_lag_tenant SCHEMA lag_tenant TTL 1h;
      CREATE RELAY lag_events SCHEMA lag_event BRANCHED BY by_lag_tenant;
      CREATE RELAY lag_checked_events SCHEMA lag_event BRANCHED BY by_lag_tenant;
      CREATE RELAY lag_unique_events SCHEMA lag_event BRANCHED BY by_lag_tenant;
      CREATE KAFKA CLIENT lag_kafka CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}'
      };
      CREATE KAFKA TOPIC INGESTOR lag_acked
        FROM lag_kafka TOPIC 'lag_acked_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'lag_acked_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m
          RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING lag_event_codec
        TO lag_events
          INHERIT ALL
          BRANCHED BY by_lag_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC INGESTOR lag_unacked
        FROM lag_kafka TOPIC 'lag_unacked_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'lag_unacked_{{test_id}}' START AT EARLIEST
          MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING lag_event_codec
        TO lag_events
          INHERIT ALL
          BRANCHED BY by_lag_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION lag_check FROM lag_events
        BRANCHED BY by_lag_tenant
        TO lag_checked_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE DEDUPLICATOR lag_dedup FROM lag_checked_events
        DEDUPLICATE ON input.event_id
        MAX TIME 10m
        BRANCHED BY by_lag_tenant
        TO lag_unique_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE KAFKA TOPIC EMITTER lag_output
        FROM lag_unique_events
        ATTACHED
        TO lag_kafka TOPIC 'lag_output_{{test_id}}'
          MODE ACK PARALLEL MAX 16 ACK TIMEOUT 5m
          RETRY POLICY BACKOFF 200ms MAX 5s
          ENCODE USING lag_event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    # The second domain sorts after the first, so a node applies it after the first one.
    Given the active domain is "{{flowing_domain}}_late"
    And the active domain is saved as placeholder "failing_domain"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{failing_domain}};
      CREATE SCHEMA late_event ( event_id STRING, content STRING );
      CREATE RELAY late_events SCHEMA late_event UNBRANCHED;
      CREATE RELAY late_unique_events SCHEMA late_event UNBRANCHED;
      CREATE DEDUPLICATOR late_dedup
        FROM late_events
        DEDUPLICATE ON input.event_id
        MAX TIME 10m
        UNBRANCHED
        TO late_unique_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    Given the active domain is "{{flowing_domain}}"
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "deduplicator" "lag_dedup" is saved as placeholder "lagging_node"
    # Every entity of both domains runs on one node, the lagging node: the second domain's swap
    # fails there, and the first domain's records never travel between nodes. A routed delivery
    # that meets a swap is still refused, which loses a MODE NO ACK record; Relay Capacity 02
    # (https://app.clickup.com/t/86bd1z226) removes that refusal.
    When these Plexus commands are executed on the leader node
      """
      RELOCATE INGESTOR lag_acked, INGESTOR lag_unacked, RELAY lag_events, RELAY lag_checked_events, RELAY lag_unique_events, JUNCTION lag_check, EMITTER lag_output ONTO NODE {{lagging_node}} IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "ingestor" "lag_acked" is saved as placeholder "acked_owner"
    And the last cluster status owner for scheduled "ingestor" "lag_unacked" is saved as placeholder "unacked_owner"
    And the last cluster status owner for scheduled "junction" "lag_check" is saved as placeholder "junction_owner"
    And the last cluster status owner for scheduled "relay" "lag_checked_events" is saved as placeholder "checked_relay_owner"
    And the last cluster status owner for scheduled "emitter" "lag_output" is saved as placeholder "emitter_owner"
    And placeholder "acked_owner" equals placeholder "lagging_node"
    And placeholder "unacked_owner" equals placeholder "lagging_node"
    And placeholder "junction_owner" equals placeholder "lagging_node"
    And placeholder "checked_relay_owner" equals placeholder "lagging_node"
    And placeholder "emitter_owner" equals placeholder "lagging_node"
    Given the active domain is "{{failing_domain}}"
    When these Plexus commands are executed on the leader node
      """
      RELOCATE DEDUPLICATOR late_dedup ONTO NODE {{lagging_node}} IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "deduplicator" "late_dedup" is saved as placeholder "late_dedup_owner"
    And placeholder "late_dedup_owner" equals placeholder "lagging_node"
    Given the active domain is "{{flowing_domain}}"
    When these Kafka messages are rapidly published to topic "lag_acked_{{test_id}}"
      """
      {"event_id":"a-1","tenant":"acme","content":"c-a-1"}
      {"event_id":"a-2","tenant":"beta","content":"c-a-2"}
      """
    And these Kafka messages are rapidly published to topic "lag_unacked_{{test_id}}"
      """
      {"event_id":"u-1","tenant":"beta","content":"c-u-1"}
      {"event_id":"u-2","tenant":"acme","content":"c-u-2"}
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"event_id":"a-1","tenant":"acme","content":"c-a-1"}
      {"event_id":"a-2","tenant":"beta","content":"c-a-2"}
      {"event_id":"u-1","tenant":"beta","content":"c-u-1"}
      {"event_id":"u-2","tenant":"acme","content":"c-u-2"}
      """
    # The node cannot apply a change of the second domain: its swap there keeps failing, so the
    # node keeps the revision before that change as the one it applied.
    Given every entity swap in domain "{{failing_domain}}" on node "{{lagging_node}}" fails while replacing deduplicator "late_dedup"
    And the active domain is "{{failing_domain}}"
    When these Plexus commands begin executing in the background
      """
      ALTER DEDUPLICATOR late_dedup SET DEDUPLICATE ON input.content;
      """
    Then the background Plexus execution fails with "injected entity-level schedule apply failure"
    # The node's attempt at the next revision applies this change to the first domain and then
    # fails on the second one. The attempt fails as a whole, so the node still records the
    # revision before both changes as applied, while the first domain runs this change.
    Given the active domain is "{{flowing_domain}}"
    When these Plexus commands begin executing in the background
      """
      ALTER DEDUPLICATOR lag_dedup SET DEDUPLICATE ON input.content;
      """
    Then the background Plexus execution fails with "injected entity-level schedule apply failure"
    # The first domain runs the change on the lagging node: its deduplicator now keys on the
    # content, branch by branch.
    When these Kafka messages are rapidly published to topic "lag_acked_{{test_id}}"
      """
      {"event_id":"a-3","tenant":"acme","content":"c-shared"}
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"event_id":"a-3","tenant":"acme","content":"c-shared"}
      """
    When these Kafka messages are rapidly published to topic "lag_unacked_{{test_id}}"
      """
      {"event_id":"u-3","tenant":"acme","content":"c-shared"}
      {"event_id":"u-4","tenant":"beta","content":"c-shared"}
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"event_id":"u-4","tenant":"beta","content":"c-shared"}
      """
    # The next change of the first domain closes its deduplicator's input. Records that arrive
    # meanwhile wait in the junction in front of it, as work the domain holds while the lagging
    # node applies the change.
    Given the entity gate for domain "{{flowing_domain}}" pauses after engagement
    When these Plexus commands begin executing in the background
      """
      ALTER DEDUPLICATOR lag_dedup SET DEDUPLICATE ON input.event_id;
      """
    Then the entity gate pause for domain "{{flowing_domain}}" is reached
    When these Kafka messages are rapidly published to topic "lag_acked_{{test_id}}"
      """
      {"event_id":"a-5","tenant":"acme","content":"c-a-5"}
      """
    And these Kafka messages are rapidly published to topic "lag_unacked_{{test_id}}"
      """
      {"event_id":"u-5","tenant":"beta","content":"c-u-5"}
      {"event_id":"u-6","tenant":"acme","content":"c-u-6"}
      """
    Then cluster observability metric "nervix_ingestor_read_payloads_total" with labels eventually totals 4
      """
      domain="{{flowing_domain}}"
      ingestor="lag_acked"
      """
    And cluster observability metric "nervix_ingestor_read_payloads_total" with labels eventually totals 6
      """
      domain="{{flowing_domain}}"
      ingestor="lag_unacked"
      """
    # The lagging node runs the first domain at the revision its failed attempt installed, which is
    # not the revision it records as applied. It takes this change in place: the held records go
    # on once the change has been applied, and the node still fails on the second domain.
    When the entity gate pause for domain "{{flowing_domain}}" is released
    Then the background Plexus execution fails with "injected entity-level schedule apply failure"
    And within "30s" the observed broker receives exactly these payloads
      """
      {"event_id":"a-5","tenant":"acme","content":"c-a-5"}
      {"event_id":"u-5","tenant":"beta","content":"c-u-5"}
      {"event_id":"u-6","tenant":"acme","content":"c-u-6"}
      """
    # Once its swap of the second domain can succeed, the node applies the newest revision of
    # both. A later change completes only when every node runs the newest revision.
    When entity swaps in domain "{{failing_domain}}" on node "{{lagging_node}}" stop failing
    Given the active domain is "{{failing_domain}}"
    When these Plexus commands are executed on the leader node
      """
      ALTER DEDUPLICATOR late_dedup SET MAX TIME 9m;
      """
    Given the active domain is "{{flowing_domain}}"
    When these Kafka messages are rapidly published to topic "lag_acked_{{test_id}}"
      """
      {"event_id":"a-7","tenant":"beta","content":"c-a-7"}
      """
    And these Kafka messages are rapidly published to topic "lag_unacked_{{test_id}}"
      """
      {"event_id":"u-7","tenant":"beta","content":"c-u-7"}
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"event_id":"a-7","tenant":"beta","content":"c-a-7"}
      {"event_id":"u-7","tenant":"beta","content":"c-u-7"}
      """
    And cluster observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually totals 5
      """
      domain="{{flowing_domain}}"
      ingestor="lag_acked"
      outcome="acknowledged"
      """
    And cluster observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually totals 0
      """
      domain="{{flowing_domain}}"
      ingestor="lag_acked"
      outcome="failed"
      """
    And cluster observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually totals 0
      """
      domain="{{flowing_domain}}"
      ingestor="lag_acked"
      outcome="timed_out"
      """
    And cluster observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually totals 0
      """
      domain="{{flowing_domain}}"
      ingestor="lag_acked"
      outcome="shutdown"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
