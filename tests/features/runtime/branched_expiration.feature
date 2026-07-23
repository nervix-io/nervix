Feature: Branched branch expiration
  Scenario Outline: Subscription survives reingestor branch expiration and re-creation
    Given branched relay expiration scan interval is configured as "100ms"
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA user_id_branch TTL 500ms;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE IF NOT EXISTS BRANCH by_reproject_notifications SCHEMA user_id_branch TTL 500ms;
        CREATE RELAY reingested_notifications SCHEMA notification BRANCHED BY by_reproject_notifications;
        CREATE RELAY projected_notifications SCHEMA notification BRANCHED BY by_reproject_notifications;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TIMESTAMP NOW
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE REINGESTOR reproject_notifications FROM notifications
        TO reingested_notifications
        INHERIT ALL
        BRANCHED BY by_reproject_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        CREATE DEDUPLICATOR passthrough FROM reingested_notifications
        DEDUPLICATE ON input.user_id
        MAX TIME 10m
        BRANCHED BY by_reproject_notifications
        TO projected_notifications
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        CREATE SUBSCRIPTION projected_notifications_subscription TO projected_notifications;
        START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    Then within "5s" node "node-1" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY projected_notifications WHERE (user_id = 42);
      """
    And within "5s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY projected_notifications WHERE (user_id = 42);
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    Then within "5s" node "node-1" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY projected_notifications WHERE (user_id = 42);
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Paced branch expiration follows domain logical time
    Given branched relay expiration scan interval is configured as "100ms"
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 10ms;
      """
    When these NSPL commands are executed on the leader node
      """
      START AT NOW TIME RATE 0.01;

      CREATE SCHEMA notification (
        user_id I64
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );

      CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA user_id_branch TTL 200ms;

      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;

      CREATE RELAY projected_notifications SCHEMA notification BRANCHED BY by_http_notifications;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;

      CREATE INGESTOR http_notifications
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TIMESTAMP NOW
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR passthrough FROM notifications
        DEDUPLICATE ON input.user_id
        MAX TIME 10m
        BRANCHED BY by_http_notifications
        TO projected_notifications
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42}
      """
    Then within "5s" node "node-1" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY projected_notifications WHERE (user_id = 42);
      """
    And within "30s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY projected_notifications WHERE (user_id = 42);
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: Correlator branch expiration drops pending correlation state
    Given branched relay expiration scan interval is configured as "100ms"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        user_id I64
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );

      CREATE IF NOT EXISTS BRANCH by_correlated_users SCHEMA user_id_branch TTL 500ms;

      CREATE RELAY left_events SCHEMA notification BRANCHED BY by_correlated_users;

      CREATE RELAY right_events SCHEMA notification BRANCHED BY by_correlated_users;

      CREATE RELAY correlated_events SCHEMA notification BRANCHED BY by_correlated_users;

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
        DECODE USING notification_codec
        TO left_events
        INHERIT ALL
        BRANCHED BY by_correlated_users
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE INGESTOR right_ingestor
        FROM ENDPOINT right_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO right_events
        INHERIT ALL
        BRANCHED BY by_correlated_users
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
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
        SET user_id = left.user_id
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;

      CREATE SUBSCRIPTION correlated_events_subscription TO correlated_events;

      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/left"
      """
      {"user_id":42}
      """
    Then within "5s" node "node-1" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY left_events WHERE (user_id = 42);
      """
    And within "30s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY left_events WHERE (user_id = 42);
      """
    Then the relay subscription does not receive a payload within "500ms"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/right"
      """
      {"user_id":42}
      """
    Then the relay subscription does not receive a payload within "1s"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/left"
      """
      {"user_id":7}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/right"
      """
      {"user_id":7}
      """
    Then the relay subscription receives a payload
      """
      "user_id":7
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Deduplicator suppression survives branch expiration and re-creation
    Given branched relay expiration scan interval is configured as "100ms"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        user_id I64
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );

      CREATE IF NOT EXISTS BRANCH by_suppressed_users SCHEMA user_id_branch TTL 500ms;

      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_suppressed_users;

      CREATE RELAY projected_notifications SCHEMA notification BRANCHED BY by_suppressed_users;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;

      CREATE INGESTOR http_notifications
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_suppressed_users
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR suppress_users FROM notifications
        DEDUPLICATE ON input.user_id
        MAX TIME 10m
        BRANCHED BY by_suppressed_users
        TO projected_notifications
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;

      CREATE SUBSCRIPTION projected_notifications_subscription TO projected_notifications;

      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    Then within "30s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY notifications WHERE (user_id = 42);
      """
    And the relay subscription does not receive a payload within "500ms"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42}
      """
    Then the relay subscription does not receive a payload within "1s"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":7}
      """
    Then the relay subscription receives a payload
      """
      "user_id":7
      """

    # Replicated-state reattach is node-local: the in-memory store lives on the
    # processor's owner. Cross-node behavior is covered by the schedule
    # movement scenario in cluster_scheduling.feature.
    Examples:
      | cluster_size |
      | 1            |

  Scenario Outline: Reorderer LRU eviction drops the least recently used branch task
    Given branched relay expiration scan interval is configured as "100ms"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        user_id I64
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );

      CREATE IF NOT EXISTS BRANCH by_limited_users SCHEMA user_id_branch TTL 5m MAX INSTANCES 1 EVICT LRU;

      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_limited_users;

      CREATE RELAY ordered_notifications SCHEMA notification BRANCHED BY by_limited_users;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;

      CREATE INGESTOR http_notifications
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_limited_users
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE REORDERER order_users
        FROM notifications
        BY input.user_id
        MAX TIME 2s
        BRANCHED BY by_limited_users
        TO ordered_notifications
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;

      CREATE SUBSCRIPTION ordered_notifications_subscription TO ordered_notifications;

      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":7}
      """
    Then the relay subscription receives a payload
      """
      "user_id":7
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
