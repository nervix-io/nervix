Feature: ZeroMQ emission
  Scenario Outline: ZeroMQ emitter publishes JSON payloads from a relay
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
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
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications BY user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_ingress
        TOPIC notifications_in_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE CLIENT zeromq_main
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };
        CREATE EMITTER zeromq_notifications
        FROM notifications
        ENCODE USING notification_codec
        TO ZEROMQ zeromq_main ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        START;
      """
    And emitter "zeromq_notifications" enters stall mode
    And MQTT message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "5s" DESCRIBE EMITTER "zeromq_notifications" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "zeromq_notifications" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: ZeroMQ emitter filter-map reads materialized relay state
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        source STRING
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        source string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_state_source BY user_id_branch TTL 5m;
        CREATE RELAY state_notifications
        SCHEMA notification BRANCHED BY by_state_source
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
        CREATE IF NOT EXISTS BRANCH by_notifications_source BY user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_notifications_source;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT state_ingress
        ON edge
        PATH '/state'
        TYPE HTTP;
        CREATE ENDPOINT notifications_ingress
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR state_source
        TO state_notifications
        DECODE USING notification_codec
        BRANCHED BY by_state_source VALUES { user_id = state_notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT state_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE INGESTOR notifications_source
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_notifications_source VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT notifications_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE CLIENT zeromq_main
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };
        CREATE EMITTER zeromq_notifications
        FROM notifications
        ENCODE USING notification_codec
        TO ZEROMQ zeromq_main
        SET notifications.source = state_notifications.source ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/state"
      """
      {"user_id":42,"source":"state"}
      """
    Then within "5s" node "node-1" eventually reports materialized state for relay "state_notifications" containing
      """
      key={"user_id":42} payload={"source":"state","user_id":42}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42,"source":"input"}
      """
    Then the observed broker receives a payload
      """
      {"source":"state","user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
