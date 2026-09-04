Feature: WebSocket client ingestor scheduling

  Scenario: Creating another runtime node does not move a WebSocket client ingestor
    Given the HTTP mock server is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT ws_main
        TYPE WEBSOCKETS
        CONFIG {
          'endpoint' = '{{mock_ws_addr}}/ws/{{test_id}}'
        };
      CREATE INGESTOR ws_notifications
        FROM WEBSOCKETS ws_main MODE NO_ACK SEQUENTIAL
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    When the websocket client test server sends a payload
      """
      {"user_id":101}
      """
    Then the relay subscription receives a payload
      """
      "user_id":101
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "ingestor" "ws_notifications" is saved as placeholder "original_ws_owner"
    When these NSPL commands are executed on the leader node
      """
      CREATE RELAY copied_notifications SCHEMA notification UNBRANCHED;
      CREATE JUNCTION notification_copy
        FROM notifications
        UNBRANCHED
        TO copied_notifications
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      """
    Then within "5s" node "node-1" eventually reports scheduled "ingestor" "ws_notifications" owner equals placeholder "original_ws_owner"
    When the websocket client test server sends a payload
      """
      {"user_id":102}
      """
    Then the relay subscription receives a payload
      """
      "user_id":102
      """

  Scenario: Uncordoning a cluster node does not move a WebSocket client ingestor
    Given the HTTP mock server is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CORDON NODE node-1;
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT ws_main
        TYPE WEBSOCKETS
        CONFIG {
          'endpoint' = '{{mock_ws_addr}}/ws/{{test_id}}'
        };
      CREATE INGESTOR ws_notifications
        FROM WEBSOCKETS ws_main MODE NO_ACK SEQUENTIAL
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "ingestor" "ws_notifications" is saved as placeholder "original_ws_owner"
    When these NSPL commands are executed on the leader node
      """
      UNCORDON NODE node-1;
      CREATE RELAY uncordon_reconciliation_probe SCHEMA notification UNBRANCHED;
      """
    Then within "5s" node "node-2" eventually reports scheduled "ingestor" "ws_notifications" owner equals placeholder "original_ws_owner"
    When the websocket client test server sends a payload
      """
      {"user_id":201}
      """
    Then the relay subscription receives a payload
      """
      "user_id":201
      """

  Scenario: A soft placement default change does not move a WebSocket client ingestor
    Given the HTTP mock server is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT ws_main
        TYPE WEBSOCKETS
        CONFIG {
          'endpoint' = '{{mock_ws_addr}}/ws/{{test_id}}'
        };
      CREATE INGESTOR ws_notifications
        FROM WEBSOCKETS ws_main MODE NO_ACK SEQUENTIAL
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "ingestor" "ws_notifications" is saved as placeholder "original_ws_owner"
    When these NSPL commands are executed on the leader node
      """
      CORDON NODE {{original_ws_owner}};
      ALTER DOMAIN SET PLACEMENT SUGGEST SEPARATION;
      """
    Then the last command output contains
      """
      planned relocations: 0
      """
    And within "5s" node "node-2" eventually reports scheduled "ingestor" "ws_notifications" owner equals placeholder "original_ws_owner"
    When the websocket client test server sends a payload
      """
      {"user_id":301}
      """
    Then the relay subscription receives a payload
      """
      "user_id":301
      """

  Scenario: Draining its owner moves a WebSocket client ingestor and reconnects its session
    Given the HTTP mock server is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT ws_main
        TYPE WEBSOCKETS
        CONFIG {
          'endpoint' = '{{mock_ws_addr}}/ws/{{test_id}}'
        };
      CREATE INGESTOR ws_notifications
        FROM WEBSOCKETS ws_main MODE NO_ACK SEQUENTIAL
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "ingestor" "ws_notifications" is saved as placeholder "original_ws_owner"
    When the websocket client test server sends a payload
      """
      {"user_id":401}
      """
    Then the relay subscription receives a payload
      """
      "user_id":401
      """
    When these NSPL commands are executed on the leader node
      """
      DRAIN NODE {{original_ws_owner}};
      """
    Then within "10s" node "node-2" eventually reports scheduled "ingestor" "ws_notifications" owner different from placeholder "original_ws_owner"
    When the websocket client test server sends a payload
      """
      {"user_id":402}
      """
    Then the relay subscription receives a payload
      """
      "user_id":402
      """
