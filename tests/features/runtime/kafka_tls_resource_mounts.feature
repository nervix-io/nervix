Feature: Kafka TLS resource mounts
  Scenario Outline: Kafka ingestor consumes over native TLS with a mounted resource directory
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
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE CLIENT kafka_tls
        TYPE KAFKA
        MOUNT dev_tls
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9094',
          'security.protocol' = 'ssl',
          'ssl.ca.location' = '{{dev_tls}}/ca.pem'
        };
        CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_kafka_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_tls
        TOPIC tls_notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP tls_notifications_group_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "10s" repeatedly publishing Kafka message to topic "tls_notifications_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
