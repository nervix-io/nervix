Feature: ZeroMQ emission
  Scenario Outline: ZeroMQ emitter publishes JSON payloads from a relay
    Given MQTT is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE MQTT CLIENT mqtt_ingress
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'notifications_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE ZEROMQ CLIENT zeromq_main
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };
        CREATE ZEROMQ PUSH EMITTER zeromq_notifications FROM notifications TO zeromq_main MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        source STRING
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        source string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_state_source SCHEMA user_id_branch TTL 5m;
        CREATE RELAY state_notifications
        SCHEMA notification BRANCHED BY by_state_source
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_state_source;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT state_ingress
        ON edge
        PATH '/state'
        TYPE HTTP;
        CREATE ENDPOINT notifications_ingress
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE HTTP ENDPOINT INGESTOR state_source
        FROM state_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO state_notifications
        INHERIT ALL
        BRANCHED BY by_state_source
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE HTTP ENDPOINT INGESTOR notifications_source
        FROM notifications_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_state_source
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE ZEROMQ CLIENT zeromq_main
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };
        CREATE ZEROMQ PUSH EMITTER zeromq_notifications
        FROM notifications
        USING MATERIALIZED STATE state_notifications REQUIRED WAIT TO zeromq_main MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL EXCEPT source
        SET source = relay_state.state_notifications.source
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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

  # The emitter's client connects to an address whose port refuses every connection, so the driver
  # retries the connection until the client's connect_timeout_ms ends the attempt.
  @zeromq_emitter_open_deadline @deloxide_stress
  Scenario Outline: A connect-mode ZeroMQ emitter whose peer never accepts is reported within connect_timeout_ms and delivers once the peer appears
    Given a <cluster_size> node nervix cluster is started
    And the ZeroMQ emission endpoint refuses connections
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge zeromq-open-{{test_id}}.example.com;
      CREATE ENDPOINT notifications_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM notifications_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT zeromq_main CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false',
        'connect_timeout_ms' = '1000'
      };
      CREATE ZEROMQ PUSH EMITTER zeromq_out FROM notifications
        TO zeromq_main MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 2s
        ENCODE USING notification_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "zeromq-open-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":42}
      """
    Then within "20s" DESCRIBE EMITTER "zeromq_out" on the leader node contains
      """
      connecting to the ZeroMQ peer did not finish within connect_timeout_ms 1s
      """
    When a ZeroMQ peer starts listening on the emission endpoint
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
