Feature: Protobuf codec
  Scenario Outline: HTTP endpoint ingestor decodes protobuf through JAQ transformation
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "notification.proto": "syntax = \"proto3\";\npackage nervix.test;\n\nmessage Notification {\n  uint32 user_id = 1;\n  string tenant = 2;\n  string payload = 3;\n}\n"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE proto_bundle;
      UPLOAD RESOURCE proto_bundle VERSION '{{proto_dir}}';
      """
    Then the last command output contains
      """
      uploaded resource version 1
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        user_id I64,
        payload STRING
      );
        CREATE CODEC notification_codec
        FROM PROTOBUF
        USING RESOURCE proto_bundle VERSION 1
        CONFIG {'file' = 'notification.proto', 'include' = '.'}
        MESSAGE 'nervix.test.Notification'
        TO SCHEMA notification
        WITH JAQ TRANSFORMATION '{user_id: .user_id, payload: .payload}';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And protobuf payload fixture "notification" is posted to host "http-{{test_id}}.example.com" path "/ingest"
    Then the relay subscription receives a payload
      """
      {"payload":"aligned","user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
