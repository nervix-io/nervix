Feature: Redis emission
  Scenario Outline: Redis emitter publishes JSON payloads from a relay
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Redis channel "notifications_out_{{test_id}}" is observed
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
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        FROM MQTT mqtt_ingress TOPIC notifications_in_{{test_id}} MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE CLIENT redis_main
        TYPE REDIS
        CONFIG {
          'addr' = 'redis://127.0.0.1:6379/'
        };
        CREATE EMITTER redis_notifications FROM notifications ENCODE USING notification_codec TO REDIS PUBSUB redis_main CHANNEL notifications_out_{{test_id}}
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And emitter "redis_notifications" enters stall mode
    And MQTT message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "5s" DESCRIBE EMITTER "redis_notifications" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "redis_notifications" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
