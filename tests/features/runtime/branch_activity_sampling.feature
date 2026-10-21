Feature: Branch activity sampling

  @branch_activity_sampling
  Scenario Outline: Activity sampled after an awaited record keeps each logical branch alive
    Given branched relay expiration scan interval is configured as "6s"
    And runtime replication is configured with replica count 0 and snapshot interval "30s"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    When these Plexus statements are executed on the leader node
      """
      START AT NOW TIME RATE <time_rate>;

      CREATE SCHEMA notification (
        user_id I64,
        event_seq I64
      );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        event_seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE SCHEMA correlated_notification (
        user_id I64,
        left_seq I64,
        right_seq I64
      );

      CREATE SCHEMA user_id_branch ( user_id I64 );

      CREATE BRANCH by_correlated_users SCHEMA user_id_branch TTL <branch_ttl>;

      CREATE RELAY left_events SCHEMA notification BRANCHED BY by_correlated_users;

      CREATE RELAY right_events SCHEMA notification BRANCHED BY by_correlated_users;

      CREATE RELAY correlated_events SCHEMA correlated_notification BRANCHED BY by_correlated_users;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT left_endpoint
        ON edge
        PATH '/left'
        TYPE HTTP;

      CREATE ENDPOINT right_endpoint
        ON edge
        PATH '/right'
        TYPE HTTP;

      CREATE HTTP ENDPOINT INGESTOR left_ingestor
        FROM left_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TIMESTAMP NOW
        TO left_events
        INHERIT ALL
        BRANCHED BY by_correlated_users
        SET user_id = message.user_id
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE HTTP ENDPOINT INGESTOR right_ingestor
        FROM right_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TIMESTAMP NOW
        TO right_events
        INHERIT ALL
        BRANCHED BY by_correlated_users
        SET user_id = message.user_id
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE CORRELATOR match_users
        LEFT FROM left_events
        RIGHT FROM right_events
        CORRELATE WHERE left.user_id = right.user_id
        MATCH EARLIEST
        MAX TIME 10m
        ON CORRELATION TIMEOUT DROP, DROP
        BRANCHED BY by_correlated_users
        TO correlated_events
        SET user_id = left.user_id,
          left_seq = left.event_seq,
          right_seq = right.event_seq
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;

      CREATE SUBSCRIPTION correlated_events_subscription TO correlated_events;
      """
    And these Plexus statements are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "correlator" "match_users" is saved as placeholder "correlator_owner"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/left"
      """
      {"user_id":42,"event_seq":1}
      """
    Then within "20s" node "node-1" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY left_events WHERE (user_id = 42);
      """
    And within "30s" node "{{correlator_owner}}" observability metric "nervix_messages_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      target_kind="CORRELATOR"
      target="match_users"
      direction="received"
      relay="left_events"
      """
    And within "30s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY left_events WHERE (user_id = 42);
      """
    And within "30s" node "node-1" observability metric "nervix_branch_instances" with labels eventually equals 0
      """
      domain="{{domain}}"
      branch="by_correlated_users"
      physical_node_id="node-1"
      """
    And within "30s" node "{{correlator_owner}}" observability metric "nervix_branch_instances" with labels eventually equals 0
      """
      domain="{{domain}}"
      branch="by_correlated_users"
      physical_node_id="{{correlator_owner}}"
      """
    # Stagger the initial arrivals across a scan: one branch's expiration cannot establish the
    # other branch's lifetime. Both initial records must expire before the fresh records arrive.
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/left"
      """
      {"user_id":7,"event_seq":2}
      """
    Then within "20s" node "node-1" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY left_events WHERE (user_id = 7);
      """
    And within "30s" node "{{correlator_owner}}" observability metric "nervix_messages_total" with labels eventually equals 2
      """
      domain="{{domain}}"
      target_kind="CORRELATOR"
      target="match_users"
      direction="received"
      relay="left_events"
      """
    And within "30s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY left_events WHERE (user_id = 7);
      """
    And within "30s" node "node-1" observability metric "nervix_branch_instances" with labels eventually equals 0
      """
      domain="{{domain}}"
      branch="by_correlated_users"
      physical_node_id="node-1"
      """
    And within "30s" node "{{correlator_owner}}" observability metric "nervix_branch_instances" with labels eventually equals 0
      """
      domain="{{domain}}"
      branch="by_correlated_users"
      physical_node_id="{{correlator_owner}}"
      """
    # All initial branch owners have retired before either fresh left record is posted.
    # Logical retention and physical scan latency remain separate from input readiness.
    When physical time passes for "4500ms"
    And http payloads are posted concurrently to host "http-{{test_id}}.example.com" path "/left"
      """
      {"user_id":42,"event_seq":11}
      {"user_id":7,"event_seq":13}
      """
    # Both branches receive their matching input before waiting for either subscription result.
    # A delayed result for one branch must not consume the other branch's retention interval.
    And physical time passes for "3s"
    And http payloads are posted concurrently to host "http-{{test_id}}.example.com" path "/right"
      """
      {"user_id":42,"event_seq":12}
      {"user_id":7,"event_seq":14}
      """
    Then within "10s" the relay subscription receives exactly one payload for each fragment set
      """
      "left_seq":11,"right_seq":12,"user_id":42
      "left_seq":13,"right_seq":14,"user_id":7
      """

    Examples:
      | cluster_size | time_rate | branch_ttl |
      | 1            | 2.0       | 6s         |
      | 1            | 0.5       | 1500ms     |
      | 3            | 2.0       | 6s         |
      | 3            | 0.5       | 1500ms     |
