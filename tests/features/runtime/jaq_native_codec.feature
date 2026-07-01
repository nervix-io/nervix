Feature: JAQ native codec
  Scenario Outline: HTTP endpoint ingestor decodes JAQ native payload formats
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        payload STRING
      );
        CREATE CODEC notification_codec
        FROM <format>
        TO SCHEMA notification
        WITH JAQ TRANSFORMATION '<transformation>';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_http_notifications BY user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_http_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        SUBSCRIBE SESSION TO notifications;
        START;
      """
    And JAQ native payload fixture "<payload_fixture>" is posted to host "http-{{test_id}}.example.com" path "/ingest"
    Then the relay subscription receives a payload
      """
      {"payload":"aligned","user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count | format | payload_fixture           | transformation                                                                                                 |
      | 1            | 0             | JSON   | json_wrapped_notification | .payload                                                                                                       |
      | 3            | 0             | JSON   | json_wrapped_notification | .payload                                                                                                       |
      | 1            | 0             | YAML   | yaml_wrapped_notification | .payload                                                                                                       |
      | 3            | 0             | YAML   | yaml_wrapped_notification | .payload                                                                                                       |
      | 1            | 0             | TOML   | toml_wrapped_notification | .payload                                                                                                       |
      | 3            | 0             | TOML   | toml_wrapped_notification | .payload                                                                                                       |
      | 1            | 0             | XML    | xml_wrapped_notification  | {user_id: (.c[] \| select(.t == "user_id").c[0] \| tonumber), payload: (.c[] \| select(.t == "payload").c[0])} |
      | 3            | 0             | XML    | xml_wrapped_notification  | {user_id: (.c[] \| select(.t == "user_id").c[0] \| tonumber), payload: (.c[] \| select(.t == "payload").c[0])} |
      | 1            | 0             | CBOR   | cbor_wrapped_notification | .payload                                                                                                       |
      | 3            | 0             | CBOR   | cbor_wrapped_notification | .payload                                                                                                       |

  Scenario Outline: Kafka emitter serializes XML through JAQ native codec
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        payload STRING
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        payload string
      );
        CREATE CODEC json_notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE CODEC xml_notification_codec
        FROM XML
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS
          ON EMITTING '{t: "notification", c: [{t: "user_id", c: [(.user_id | tostring)]}, {t: "payload", c: [.payload]}]}';
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_http_notifications BY user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING json_notification_codec
        BRANCHED BY by_http_notifications VALUES { user_id = notifications.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };
        CREATE EMITTER kafka_notifications
        FROM notifications
        ENCODE USING xml_notification_codec
        TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
        START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42,"payload":"aligned"}
      """
    Then the observed broker receives a payload
      """
      user_id>42
      """
    And the last observed broker payload contains
      """
      notification>
      /user_id>
      payload>aligned
      /payload>
      /notification>
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Ingestor rejects emitting-only JAQ native codec
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail with "codec 'xml_notification_codec' cannot be used for decoding because it does not declare an ON INGESTION transformation"
      """
      CREATE SCHEMA notification (
        user_id I64,
        payload STRING
      );

      CREATE CODEC xml_notification_codec
        FROM XML
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS
          ON EMITTING '{t: "notification", c: [{t: "user_id", c: [(.user_id | tostring)]}, {t: "payload", c: [.payload]}]}';

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE VHOST edge http-invalid-{{test_id}}.example.com;

      CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;

      CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING xml_notification_codec
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Emitter rejects ingestion-only JAQ native codec
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail with "codec 'json_notification_codec' cannot be used for encoding because it does not declare an ON EMITTING transformation"
      """
      CREATE SCHEMA notification (
        user_id I64,
        payload STRING
      );

      CREATE CODEC json_notification_codec
        FROM JSON
        TO SCHEMA notification
        WITH JAQ TRANSFORMATION '.';

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE EMITTER kafka_notifications
        FROM notifications
        ENCODE USING json_notification_codec
        TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
