Feature: Branch activity sampling

  @branch_activity_sampling
  Scenario Outline: Activity sampled after an awaited record keeps each logical branch alive
    Given branched relay expiration scan interval is configured as "6s"
    And runtime replication is configured with replica count 0 and snapshot interval "30s"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    When these NSPL commands are executed on the leader node
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

      CREATE INGESTOR left_ingestor
        FROM ENDPOINT left_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TIMESTAMP NOW
        TO left_events
        INHERIT ALL
        BRANCHED BY by_correlated_users
        SET user_id = message.user_id
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE INGESTOR right_ingestor
        FROM ENDPOINT right_endpoint MODE NO_ACK SEQUENTIAL
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
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/left"
      """
      {"user_id":42,"event_seq":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/left"
      """
      {"user_id":7,"event_seq":2}
      """
    Then within "20s" node "node-1" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY left_events WHERE (user_id = 42);
      """
    # Retention is enforced by a physical maintenance scan whose latency is separate from the
    # logical expiration boundary. The observed expiration therefore marks a scan boundary, and
    # idling towards the next one leaves the supervisors waiting for input across it.
    And within "30s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY left_events WHERE (user_id = 42);
      """
    When physical time passes for "4500ms"
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/left"
      """
      {"user_id":42,"event_seq":11}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/left"
      """
      {"user_id":7,"event_seq":13}
      """
    # A record accepted after a long wait must be recorded at its own arrival, so both interleaved
    # branches keep their pending correlation state across the scan that follows it.
    And physical time passes for "3s"
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/right"
      """
      {"user_id":42,"event_seq":12}
      """
    Then the relay subscription receives a payload
      """
      "left_seq":11,"right_seq":12,"user_id":42
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/right"
      """
      {"user_id":7,"event_seq":14}
      """
    Then the relay subscription receives a payload
      """
      "left_seq":13,"right_seq":14,"user_id":7
      """

    Examples:
      | cluster_size | time_rate | branch_ttl |
      | 1            | 2.0       | 6s         |
      | 1            | 0.5       | 1500ms     |
      | 3            | 2.0       | 6s         |
      | 3            | 0.5       | 1500ms     |
