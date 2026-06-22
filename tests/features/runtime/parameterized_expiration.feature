Feature: Parameterized branch expiration
  Scenario Outline: Subscription survives reingestor branch expiration and re-creation
    Given parameterized relay expiration scan interval is configured as "100ms"
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

      CREATE JSON WIRE SCHEMA notification_wire (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE RELAY notifications SCHEMA notification PARAMETERIZED BY user_id_branch;
      CREATE RELAY reingested_notifications SCHEMA notification PARAMETERIZED BY user_id_branch;
      CREATE RELAY projected_notifications SCHEMA notification PARAMETERIZED BY user_id_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 500ms
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        TIMESTAMP NOW
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE REINGESTOR reproject_notifications
        FROM notifications
        TO reingested_notifications
        PARAMETERIZED BY user_id_branch VALUES { user_id = reingested_notifications.user_id } TTL 500ms
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      CREATE DEDUPLICATOR passthrough
        FROM reingested_notifications
        TO projected_notifications PARAMETERIZED BY user_id_branch
        DEDUPLICATE ON reingested_notifications.user_id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO projected_notifications;
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
    Given parameterized relay expiration scan interval is configured as "100ms"
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

      CREATE JSON WIRE SCHEMA notification_wire (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE RELAY notifications SCHEMA notification PARAMETERIZED BY user_id_branch;
      CREATE RELAY projected_notifications SCHEMA notification PARAMETERIZED BY user_id_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 200ms
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        TIMESTAMP NOW
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR passthrough
        FROM notifications
        TO projected_notifications PARAMETERIZED BY user_id_branch
        DEDUPLICATE ON notifications.user_id
        MAX TIME 10m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
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
