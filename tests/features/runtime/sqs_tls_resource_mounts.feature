Feature: SQS TLS resource mounts
  Scenario Outline: SQS ingestor consumes over TLS with a mounted resource directory
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has dev TLS resource directory "dev_tls"
    And TLS SQS queue "notifications_{{test_id}}" exists
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

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT sqs_tls
        TYPE SQS
        MOUNT dev_tls
        CONFIG {
          'endpoint' = 'https://127.0.0.1:9325',
          'region' = 'us-east-1',
          'tls_ca_file' = '{{dev_tls}}/ca.pem'
        };

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE INGESTOR sqs_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM SQS sqs_tls
        QUEUE notifications_{{test_id}}
        INSTANCES 1
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    Then within "10s" repeatedly publishing TLS SQS message to queue "notifications_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
