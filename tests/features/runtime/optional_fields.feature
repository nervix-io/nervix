Feature: Optional fields
  Scenario Outline: HTTP endpoint ingestor preserves optional fields when omitted or null
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        tenant STRING,
        active BOOL OPTIONAL,
        amount I64 OPTIONAL,
        raw STRING OPTIONAL
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        tenant string,
        active boolean OPTIONAL,
        amount integer OPTIONAL,
        raw string OPTIONAL
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = notifications.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      <payload>
      """
    Then the relay subscription receives a payload
      """
      <expected_fragment>
      """
    And the last relay subscription payload contains key fragment '<expected_key>'
    And the last relay subscription payload does not contain "active\""
    And the last relay subscription payload does not contain "amount\""
    And the last relay subscription payload does not contain "raw\""

    Examples:
      | cluster_size | replica_count | payload                                                  | expected_fragment | expected_key      |
      | 1            | 0             | {"tenant":"acme"}                                        | "tenant":"acme"   | {"tenant":"acme"} |
      | 3            | 0             | {"tenant":"acme"}                                        | "tenant":"acme"   | {"tenant":"acme"} |
      | 3            | 1             | {"tenant":"acme"}                                        | "tenant":"acme"   | {"tenant":"acme"} |
      | 1            | 0             | {"tenant":"beta","active":null,"amount":null,"raw":null} | "tenant":"beta"   | {"tenant":"beta"} |
      | 3            | 0             | {"tenant":"beta","active":null,"amount":null,"raw":null} | "tenant":"beta"   | {"tenant":"beta"} |
      | 3            | 1             | {"tenant":"beta","active":null,"amount":null,"raw":null} | "tenant":"beta"   | {"tenant":"beta"} |

  Scenario Outline: Kafka ingestor preserves optional fields when omitted or null
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        tenant STRING,
        active BOOL OPTIONAL,
        amount I64 OPTIONAL,
        raw STRING OPTIONAL
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        tenant string,
        active boolean OPTIONAL,
        amount integer OPTIONAL,
        raw string OPTIONAL
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = notifications.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        INSTANCES 1
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    When Kafka message is published to topic "notifications_{{test_id}}"
      """
      <payload>
      """
    Then Kafka consumer group "nervix_cucumber_{{test_id}}" eventually has 1 consumers
    And the relay subscription receives a payload
      """
      <expected_fragment>
      """
    And the last relay subscription payload contains key fragment '<expected_key>'
    And the last relay subscription payload does not contain "active\""
    And the last relay subscription payload does not contain "amount\""
    And the last relay subscription payload does not contain "raw\""

    Examples:
      | cluster_size | replica_count | payload                                                  | expected_fragment | expected_key      |
      | 1            | 0             | {"tenant":"acme"}                                        | "tenant":"acme"   | {"tenant":"acme"} |
      | 3            | 0             | {"tenant":"acme"}                                        | "tenant":"acme"   | {"tenant":"acme"} |
      | 3            | 1             | {"tenant":"acme"}                                        | "tenant":"acme"   | {"tenant":"acme"} |
      | 1            | 0             | {"tenant":"beta","active":null,"amount":null,"raw":null} | "tenant":"beta"   | {"tenant":"beta"} |
      | 3            | 0             | {"tenant":"beta","active":null,"amount":null,"raw":null} | "tenant":"beta"   | {"tenant":"beta"} |
      | 3            | 1             | {"tenant":"beta","active":null,"amount":null,"raw":null} | "tenant":"beta"   | {"tenant":"beta"} |

  Scenario Outline: Websocket endpoint ingestor preserves optional fields when omitted or null
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        tenant STRING,
        active BOOL OPTIONAL,
        amount I64 OPTIONAL,
        raw STRING OPTIONAL
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        tenant string,
        active boolean OPTIONAL,
        amount integer OPTIONAL,
        raw string OPTIONAL
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification;

      CREATE VHOST edge ws-{{test_id}}.example.com;

      CREATE ENDPOINT ws_notifications_endpoint
        ON edge
        PATH '/ws'
        TYPE WEBSOCKETS;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR ws_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = notifications.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ws_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    And websocket message is published to host "ws-{{test_id}}.example.com" path "/ws"
      """
      <payload>
      """
    Then the relay subscription receives a payload
      """
      <expected_fragment>
      """
    And the last relay subscription payload contains key fragment '<expected_key>'
    And the last relay subscription payload does not contain "active\""
    And the last relay subscription payload does not contain "amount\""
    And the last relay subscription payload does not contain "raw\""

    Examples:
      | cluster_size | replica_count | payload                                                  | expected_fragment | expected_key      |
      | 1            | 0             | {"tenant":"acme"}                                        | "tenant":"acme"   | {"tenant":"acme"} |
      | 3            | 0             | {"tenant":"acme"}                                        | "tenant":"acme"   | {"tenant":"acme"} |
      | 3            | 1             | {"tenant":"acme"}                                        | "tenant":"acme"   | {"tenant":"acme"} |
      | 1            | 0             | {"tenant":"beta","active":null,"amount":null,"raw":null} | "tenant":"beta"   | {"tenant":"beta"} |
      | 3            | 0             | {"tenant":"beta","active":null,"amount":null,"raw":null} | "tenant":"beta"   | {"tenant":"beta"} |
      | 3            | 1             | {"tenant":"beta","active":null,"amount":null,"raw":null} | "tenant":"beta"   | {"tenant":"beta"} |

  Scenario Outline: ZeroMQ ingestor preserves optional fields when omitted or null
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        tenant STRING,
        active BOOL OPTIONAL,
        amount I64 OPTIONAL,
        raw STRING OPTIONAL
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        tenant string,
        active boolean OPTIONAL,
        amount integer OPTIONAL,
        raw string OPTIONAL
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT zeromq_main
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_ingest_addr}}',
          'bind' = 'true'
        };

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE INGESTOR zeromq_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY tenant_branch VALUES { tenant = notifications.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ZEROMQ zeromq_main
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    And ZeroMQ message is published
      """
      <payload>
      """
    Then the relay subscription receives a payload
      """
      <expected_fragment>
      """
    And the last relay subscription payload contains key fragment '<expected_key>'
    And the last relay subscription payload does not contain "active\""
    And the last relay subscription payload does not contain "amount\""
    And the last relay subscription payload does not contain "raw\""

    Examples:
      | cluster_size | replica_count | payload                                                  | expected_fragment | expected_key      |
      | 1            | 0             | {"tenant":"acme"}                                        | "tenant":"acme"   | {"tenant":"acme"} |
      | 3            | 0             | {"tenant":"acme"}                                        | "tenant":"acme"   | {"tenant":"acme"} |
      | 3            | 1             | {"tenant":"acme"}                                        | "tenant":"acme"   | {"tenant":"acme"} |
      | 1            | 0             | {"tenant":"beta","active":null,"amount":null,"raw":null} | "tenant":"beta"   | {"tenant":"beta"} |
      | 3            | 0             | {"tenant":"beta","active":null,"amount":null,"raw":null} | "tenant":"beta"   | {"tenant":"beta"} |
      | 3            | 1             | {"tenant":"beta","active":null,"amount":null,"raw":null} | "tenant":"beta"   | {"tenant":"beta"} |
