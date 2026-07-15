Feature: Redis TLS resource mounts
  Scenario Outline: Redis ingestor consumes over native TLS with a mounted resource directory
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has dev TLS resource directory "dev_tls"
    When these NSPL commands are executed
      """
      CREATE RESOURCE dev_tls;
      """
    And these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE dev_tls VERSION "{{dev_tls}}";
      """
    And these NSPL commands are executed
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
        CREATE IF NOT EXISTS BRANCH by_redis_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_redis_notifications;
        CREATE CLIENT redis_tls
        TYPE REDIS
        MOUNT dev_tls
        CONFIG {
          'addr' = 'rediss://127.0.0.1:6380/',
          'tls_ca_file' = '{{dev_tls}}/ca.pem'
        };
        CREATE INGESTOR redis_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_redis_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM REDIS PUBSUB redis_tls
        CHANNEL notifications_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "10s" repeatedly publishing Redis message to channel "notifications_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
