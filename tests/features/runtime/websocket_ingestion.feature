Feature: Websocket endpoint ingestion
  Scenario Outline: Websocket endpoint ingestor delivers JSON payloads to a subscribed relay
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
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
        CREATE IF NOT EXISTS BRANCH by_ws_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_ws_notifications;
        CREATE VHOST edge ws-{{test_id}}.example.com;
        CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS;
        CREATE INGESTOR ws_notifications
        FROM ENDPOINT ws_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_ws_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And websocket message is published to host "ws-{{test_id}}.example.com" path "/ws"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Secure websocket endpoint ingestor delivers JSON payloads to a subscribed relay
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has TLS resource directory "tls_bundle" for hosts "ws-{{test_id}}.example.com"
    When these NSPL commands are executed
      """
      CREATE RESOURCE tls_bundle;
      """
    And these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE tls_bundle VERSION "{{tls_bundle}}";
      """
    And these NSPL commands are executed
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
      CREATE IF NOT EXISTS BRANCH by_ws_notifications SCHEMA user_id_branch TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_ws_notifications;
      CREATE VHOST edge ws-{{test_id}}.example.com WITH TLS tls_bundle;
      CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS;
      CREATE INGESTOR ws_notifications
        FROM ENDPOINT ws_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_ws_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And secure websocket message is published to host "ws-{{test_id}}.example.com" path "/ws" using CA from resource directory "tls_bundle"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
