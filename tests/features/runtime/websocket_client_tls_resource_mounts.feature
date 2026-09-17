Feature: Websocket client TLS resource mounts
  Scenario Outline: Websocket client keeps its pinned TLS mount through upload and restart
    Given the HTTP mock server is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has dev TLS resource directory "dev_tls"
    And node "node-1" has resource directory "dev_tls_v2" containing
      """
      {
        "ca.pem": "this is not a certificate\n"
      }
      """
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
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_ws_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_ws_notifications;
        CREATE CLIENT ws_tls
        TYPE WEBSOCKETS
        MOUNT dev_tls VERSION 1
        CONFIG {
          'endpoint' = '{{mock_wss_addr}}/ws/{{test_id}}',
          'tls_ca_file' = '{{dev_tls}}/ca.pem'
        };
        CREATE INGESTOR ws_notifications
        FROM WEBSOCKETS ws_tls MODE NO_ACK SEQUENTIAL
        ON QUIESCE DROP DECODE USING notification_codec
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
    Then the relay subscription receives a payload
      """
      {"user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'
    When these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE dev_tls VERSION "{{dev_tls_v2}}";
      """
    When the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE CLIENT ws_tls;
      """
    Then the last command output contains
      """
      MOUNT dev_tls VERSION 1
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      """
    When the websocket client test server sends a payload
      """
      {"user_id":43}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":43}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
