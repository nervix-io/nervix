@mqtt_publish_properties
Feature: MQTT publications carry the envelope the statement prepares
  Scenario Outline: A retained MQTT QOS <qos> publication reaches a late subscriber with its properties
    Given MQTT is running
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MQTT 5 topic "mqtt_properties_out_{{test_id}}" is observed with its complete envelope
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE WIRE JSON CODEC notification_codec
        FROM WIRE SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MQTT CLIENT mqtt_input CONFIG {
        'addr' = '{{mqtt_addr}}', 'client_id' = 'properties-in-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR notification_input
        FROM mqtt_input TOPIC 'mqtt_properties_in_{{test_id}}'
        SESSION CLEAN QOS 0 MODE NO ACK ON QUIESCE DROP
        DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE MQTT CLIENT mqtt_output CONFIG {
        'addr' = '{{mqtt_addr}}', 'client_id' = 'properties-out-{{test_id}}'
      };
      CREATE MQTT TOPIC EMITTER notification_output
        FROM notifications TO mqtt_output TOPIC 'mqtt_properties_out_{{test_id}}'
        QOS <qos> MODE <mode> RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING notification_codec
        RETAIN PROPERTIES {
          'content_type' = 'application/json',
          'message_expiry' = (600 AS U32),
          'response_topic' = 'responses/{{test_id}}',
          'correlation_data' = hex_decode('00ff10')
        }
        INHERIT ALL
        INVOKE write_header('trace', 'first'), write_header('trace', 'second')
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And MQTT message is published to topic "mqtt_properties_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the MQTT 5 observer receives the complete publication with retain false
    Given MQTT 5 topic "mqtt_properties_out_{{test_id}}" is observed with its complete envelope
    Then the MQTT 5 observer receives the complete publication with retain true
    And SHOW CREATE EMITTER on the leader node renders these clauses
      | emitter             | clause                              |
      | notification_output | RETAIN                              |
      | notification_output | 'content_type' = 'application/json' |
      | notification_output | 'response_topic' = 'responses/      |
    And within "30s" DESCRIBE EMITTER "notification_output" on the leader node contains
      """
      RETAIN
      """

    Examples:
      | nodes | qos | mode                              |
      | 1     | 0   | NO ACK                            |
      | 3     | 0   | NO ACK                            |
      | 1     | 1   | ACK SEQUENTIAL ACK TIMEOUT 5s     |
      | 3     | 1   | ACK SEQUENTIAL ACK TIMEOUT 5s     |
      | 1     | 2   | ACK PARALLEL MAX 2 ACK TIMEOUT 5s |
      | 3     | 2   | ACK PARALLEL MAX 2 ACK TIMEOUT 5s |

  Scenario Outline: MQTT publish properties require their declared exact types
    Given MQTT is running
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE WIRE JSON CODEC notification_codec
        FROM WIRE SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MQTT CLIENT mqtt_output CONFIG {
        'addr' = '{{mqtt_addr}}', 'client_id' = 'properties-types-{{test_id}}'
      };
      """
    When these Plexus commands fail with "<required_type>"
      """
      CREATE MQTT TOPIC EMITTER refused_property
        FROM notifications TO mqtt_output TOPIC 'mqtt_properties_out_{{test_id}}'
        QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING notification_codec
        PROPERTIES {'<property>' = <value>}
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes | property         | value | required_type |
      | 1     | message_expiry   | 600   | U32           |
      | 3     | message_expiry   | 600   | U32           |
      | 1     | content_type     | 42    | STRING        |
      | 3     | content_type     | 42    | STRING        |
      | 1     | correlation_data | 'hex' | BYTES         |
      | 3     | correlation_data | 'hex' | BYTES         |

  Scenario Outline: An MQTT retry sends the same retained properties and repeated user properties
    Given MQTT is running
    And an MQTT publication endpoint is configured for retry
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MQTT 5 topic "mqtt_properties_retry_{{test_id}}" is observed with its complete envelope
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE WIRE JSON CODEC notification_codec
        FROM WIRE SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MQTT CLIENT mqtt_input CONFIG {
        'addr' = '{{mqtt_addr}}', 'client_id' = 'retry-in-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR notification_input
        FROM mqtt_input TOPIC 'mqtt_properties_in_{{test_id}}'
        SESSION CLEAN QOS 0 MODE NO ACK ON QUIESCE DROP
        DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE MQTT CLIENT mqtt_output CONFIG {
        'addr' = '{{mqtt_publication_addr}}', 'client_id' = 'retry-out-{{test_id}}'
      };
      CREATE MQTT TOPIC EMITTER notification_output
        FROM notifications TO mqtt_output TOPIC 'mqtt_properties_retry_{{test_id}}'
        QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING notification_codec
        RETAIN PROPERTIES {
          'content_type' = 'application/json',
          'message_expiry' = (600 AS U32),
          'response_topic' = 'responses/{{test_id}}',
          'correlation_data' = hex_decode('00ff10')
        }
        INHERIT ALL
        INVOKE write_header('trace', 'first'), write_header('trace', 'second')
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And MQTT message is published to topic "mqtt_properties_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the MQTT publication retry carries the identical prepared envelope
    And the MQTT 5 observer receives the complete publication with retain false

    Examples:
      | nodes |
      | 1     |
      | 3     |

  Scenario Outline: MQTT property bytes reject an oversized packet before sending its valid neighbour
    Given MQTT is running
    And an MQTT publication endpoint is configured for size
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE SCHEMA rejected_notification (user_id I64, error_code STRING, error_message STRING);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE WIRE JSON CODEC notification_codec
        FROM WIRE SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE RELAY rejections SCHEMA rejected_notification UNBRANCHED;
      CREATE MQTT CLIENT mqtt_input CONFIG {
        'addr' = '{{mqtt_addr}}', 'client_id' = 'size-in-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR notification_input
        FROM mqtt_input TOPIC 'mqtt_properties_in_{{test_id}}'
        SESSION CLEAN QOS 0 MODE NO ACK ON QUIESCE DROP
        DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE MQTT CLIENT mqtt_output CONFIG {
        'addr' = '{{mqtt_publication_addr}}', 'client_id' = 'size-out-{{test_id}}'
      };
      CREATE MQTT TOPIC EMITTER notification_output
        FROM notifications TO mqtt_output TOPIC 'mqtt_properties_size_{{test_id}}'
        QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING notification_codec
        PROPERTIES {
          'content_type' = IF input.user_id = 41 THEN repeat('x', 1024) ELSE 'application/json' END
        }
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejections
          SET user_id = input.user_id, error_code = error.code, error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejections_subscription TO rejections;
      START;
      """
    # Completing a warm-up publication establishes that the emitter has consumed CONNACK.
    And MQTT message is published to topic "mqtt_properties_in_{{test_id}}"
      """
      {"user_id":40}
      """
    Then the next MQTT publication at the endpoint has user_id 40
    When MQTT message is published to topic "mqtt_properties_in_{{test_id}}"
      """
      {"user_id":41}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "user_id":41 | "error_code":"external" | maximum packet size of 1024 bytes
      """
    When MQTT message is published to topic "mqtt_properties_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the next MQTT publication at the endpoint has user_id 42

    Examples:
      | nodes |
      | 1     |
      | 3     |
