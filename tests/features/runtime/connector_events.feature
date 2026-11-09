Feature: Connector-declared events

  Scenario Outline: Redis publishes without subscribers stay delivered and count their declared event
    Given Redis is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR events_in
        FROM SCHEMA event MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s
          RETRY POLICY BACKOFF 100ms MAX 1s ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 1 MAX 2
        CONFIG { 'addr' = '{{redis_addr}}' };
      CREATE REDIS PUBSUB EMITTER events_out FROM events
        TO redis_main CHANNEL 'declared_events_{{test_id}}' MODE NO ACK
          RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE EMITTER "events_out" on the leader node contains
      """
      destination:
      """
    And the last command output owner is saved as placeholder "events_owner"
    And node "{{events_owner}}" observability metric "nervix_emitter_connector_events_total" with labels eventually equals 0
      """
      emitter="events_out"
      connector="REDIS"
      entity="PUBSUB"
      event="published_without_subscribers"
      """
    Given client "publisher" is connected to the leader node
    When client "publisher" opens producer "events" on ingestor "events_in" expecting fields "seq I64"
    And producer "events" submits batch "unobserved" with rows
      | seq |
      | 1   |
      | 2   |
      | 3   |
    Then batch "unobserved" completes
    And node "{{events_owner}}" observability metric "nervix_emitter_connector_events_total" with labels eventually equals 3
      """
      emitter="events_out"
      connector="REDIS"
      entity="PUBSUB"
      event="published_without_subscribers"
      """
    And within "30s" DESCRIBE EMITTER "events_out" on the leader node contains
      """
      event="published_without_subscribers"} total=3 wall_rate_per_sec=
      """
    And the last command output contains
      """
      messages_total sent relay=events physical_node={{events_owner}} total=3
      """
    Given Redis channel "declared_events_{{test_id}}" is observed
    Then Redis channel "declared_events_{{test_id}}" eventually has 1 subscribers
    When producer "events" submits batch "observed" with rows
      | seq |
      | 4   |
    Then batch "observed" completes
    And the observed broker receives a payload
      """
      {"seq":4}
      """
    And within "30s" DESCRIBE EMITTER "events_out" on the leader node contains
      """
      messages_total sent relay=events physical_node={{events_owner}} total=4
      """
    And node "{{events_owner}}" observability metric "nervix_emitter_connector_events_total" with labels eventually equals 3
      """
      emitter="events_out"
      connector="REDIS"
      entity="PUBSUB"
      event="published_without_subscribers"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
