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
        WITH JAQ TRANSFORMATIONS ON INGESTION '{user_id: .user_id, payload: .payload}';
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
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
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

  Scenario Outline: Kafka emitter and ingestor keep every digit of an F64 through a protobuf codec
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "reading.proto": "syntax = \"proto3\";\npackage nervix.test;\n\nmessage Reading {\n  int64 sensor = 1;\n  double level = 2;\n  repeated double levels = 3;\n}\n"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "readings_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE proto_bundle;
      UPLOAD RESOURCE proto_bundle VERSION '{{proto_dir}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA reading ( sensor I64, level F64, levels <levels_type> );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT ( sensor integer, level number, levels array );
      CREATE CODEC exact_reading_codec FROM WIRE JSON SCHEMA reading_wire TO SCHEMA reading;
      CREATE CODEC transforming_reading_codec
        FROM PROTOBUF
        USING RESOURCE proto_bundle VERSION 1
        CONFIG {'file' = 'reading.proto', 'include' = '.'}
        MESSAGE 'nervix.test.Reading'
        TO SCHEMA reading
        WITH JAQ TRANSFORMATIONS
          ON INGESTION '{sensor: .sensor, level: .level, levels: .levels}'
          ON EMITTING '.';
      CREATE RELAY readings SCHEMA reading UNBRANCHED;
      CREATE RELAY echoed_readings SCHEMA reading UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT readings_endpoint ON edge PATH '/readings' TYPE HTTP;
      CREATE INGESTOR http_readings
        FROM ENDPOINT readings_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING exact_reading_codec
        TO readings
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE EMITTER emitted_readings FROM readings
        TO KAFKA kafka_main TOPIC readings_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING transforming_reading_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INGESTOR kafka_readings
        FROM KAFKA kafka_main TOPIC readings_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_readings_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING transforming_reading_codec
        TO echoed_readings
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION echoed_subscription TO echoed_readings;
      START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/readings"
      """
      {"sensor":7,"level":1.4000000000000001,"levels":[0.9999999999999999,90.33333333333333]}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "sensor":7 | "level":1.4000000000000001 | "levels":[0.9999999999999999,90.33333333333333]
      """

    Examples:
      | cluster_size | levels_type |
      | 1            | VEC<F64>    |
      | 3            | VEC<F64>    |
