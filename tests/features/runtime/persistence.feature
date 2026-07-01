Feature: Runtime persistence
  Scenario Outline: Persisted rules are reapplied after a full cluster restart
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
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
        CREATE IF NOT EXISTS BRANCH by_http_notifications BY user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_http_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        START;
      """
    When the cluster is restarted
    And these NSPL commands are executed on the leader node
      """
      SHOW CREATE INGESTOR http_notifications;
      """
    Then the last command output contains
      """
      CREATE INGESTOR http_notifications
      """
    Then within "5s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY notifications WHERE (user_id = 901);
      """
    And node "node-1" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":901}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":901}
      """
    Then within "5s" node "node-1" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY notifications WHERE (user_id = 901);
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
