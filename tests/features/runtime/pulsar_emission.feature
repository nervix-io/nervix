Feature: Pulsar emission
  Scenario Outline: Pulsar emitter publishes JSON payloads from a relay
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar topic "notifications_out_{{test_id}}" is observed
    When these NSPL commands are executed
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
        CREATE IF NOT EXISTS BRANCH by_pulsar_ingress SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_pulsar_ingress;
        CREATE CLIENT pulsar_main
        TYPE PULSAR
        CONFIG {
          'addr' = 'pulsar://127.0.0.1:6650'
        };
        CREATE INGESTOR pulsar_ingress
        FROM PULSAR pulsar_main TOPIC notifications_in_{{test_id}} SUBSCRIPTION nervix_pulsar_emission_{{test_id}} INSTANCES 1 MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_pulsar_ingress
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE EMITTER pulsar_notifications FROM notifications ENCODE USING notification_codec TO PULSAR pulsar_main TOPIC notifications_out_{{test_id}}
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And emitter "pulsar_notifications" enters stall mode
    And Pulsar message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "5s" DESCRIBE EMITTER "pulsar_notifications" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "pulsar_notifications" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
