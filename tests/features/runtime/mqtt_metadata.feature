@mqtt_metadata @deloxide_stress
Feature: MQTT publish metadata
  An MQTT TOPIC ingestor exposes each delivered publish as typed metadata.<field> values: the topic
  it was delivered on, its retain, qos and duplicate flags, and its content_type, response_topic,
  correlation_data and message_expiry MQTT 5 properties. A property the publisher did not set reads
  as null. read_header and read_headers read the publish's MQTT 5 user properties.

  Scenario Outline: An MQTT ingestor reads the delivered packet's topic, flags and MQTT 5 properties as metadata
    Given MQTT is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA reading ( user_id I64 );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT ( user_id integer );
      CREATE CODEC reading_codec FROM WIRE JSON SCHEMA reading_wire TO SCHEMA reading;
      CREATE SCHEMA delivered_reading (
        user_id I64,
        topic STRING OPTIONAL,
        retained BOOL OPTIONAL,
        qos U8 OPTIONAL,
        duplicate BOOL OPTIONAL,
        content_type STRING OPTIONAL,
        response_topic STRING OPTIONAL,
        correlation_hex STRING OPTIONAL,
        expiry U32 OPTIONAL,
        no_properties BOOL
      );
      CREATE RELAY readings SCHEMA delivered_reading UNBRANCHED;
      CREATE MQTT CLIENT mqtt_main CONFIG { 'addr' = '{{mqtt_addr}}' };
      CREATE MQTT TOPIC INGESTOR mqtt_readings
        FROM mqtt_main TOPIC 'readings_{{test_id}}/+' <subscription>
        ON QUIESCE DROP DECODE USING reading_codec
        TO readings INHERIT ALL
        SET topic = metadata.topic,
          retained = metadata.retain,
          qos = metadata.qos,
          duplicate = metadata.duplicate,
          content_type = metadata.content_type,
          response_topic = metadata.response_topic,
          correlation_hex = hex_encode(metadata.correlation_data),
          expiry = metadata.message_expiry,
          no_properties = is_null(metadata.content_type) AND is_null(metadata.response_topic)
            AND is_null(metadata.correlation_data) AND is_null(metadata.message_expiry)
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION readings_subscription TO readings;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "mqtt_readings" on the leader node contains
      """
      ready: true
      """
    # The broker counts a message's expiry down while it holds the message, so the scenario reads the
    # leading digits of the 86400 seconds the publisher wrote.
    When an MQTT message is published to topic "readings_{{test_id}}/kitchen" with these MQTT 5 properties
      | payload          | {"user_id":42}   |
      | qos              | <published_qos>  |
      | retain           | true             |
      | content_type     | application/json |
      | response_topic   | replies/kitchen  |
      | correlation_data | request-7        |
      | message_expiry   | 86400            |
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "user_id":42 | "topic":"readings_{{test_id}}/kitchen" | "retained":true | "qos":<delivered_qos> | "duplicate":false | "content_type":"application/json" | "response_topic":"replies/kitchen" | "correlation_hex":"726571756573742d37" | "expiry":86 | "no_properties":false
      """
    When an MQTT message is published to topic "readings_{{test_id}}/hall" with these MQTT 5 properties
      | payload | {"user_id":43} |
      | qos     | 0              |
      | retain  | false          |
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "user_id":43 | "topic":"readings_{{test_id}}/hall" | "retained":false | "qos":0 | "duplicate":false | "no_properties":true
      """

    Examples:
      | cluster_size | subscription                                                                                     | published_qos | delivered_qos |
      | 1            | SESSION CLEAN QOS 1 MODE NO ACK                                                                  | 1             | 1             |
      | 3            | SESSION CLEAN QOS 1 MODE NO ACK                                                                  | 1             | 1             |
      | 1            | SESSION PERSISTENT QOS 2 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms | 2             | 2             |
      | 3            | SESSION PERSISTENT QOS 2 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms | 2             | 2             |
      | 1            | SESSION PERSISTENT QOS 2 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms | 1             | 1             |
      | 3            | SESSION PERSISTENT QOS 2 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms | 1             | 1             |

  Scenario Outline: A wildcard MQTT ingestor routes each publish by the topic it was delivered on
    Given MQTT is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA reading ( user_id I64 );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT ( user_id integer );
      CREATE CODEC reading_codec FROM WIRE JSON SCHEMA reading_wire TO SCHEMA reading;
      CREATE RELAY kitchen_readings SCHEMA reading UNBRANCHED;
      CREATE MQTT CLIENT mqtt_main CONFIG { 'addr' = '{{mqtt_addr}}' };
      CREATE MQTT TOPIC INGESTOR mqtt_readings
        FROM mqtt_main TOPIC 'routed_{{test_id}}/+' SESSION CLEAN QOS 1 MODE NO ACK
        ON QUIESCE DROP DECODE USING reading_codec
        TO kitchen_readings INHERIT ALL
        WHERE metadata.topic = 'routed_{{test_id}}/kitchen'
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION kitchen_subscription TO kitchen_readings;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "mqtt_readings" on the leader node contains
      """
      ready: true
      """
    When an MQTT message is published to topic "routed_{{test_id}}/hall" with these MQTT 5 properties
      | payload | {"user_id":1} |
      | qos     | 1             |
    And an MQTT message is published to topic "routed_{{test_id}}/kitchen" with these MQTT 5 properties
      | payload | {"user_id":2} |
      | qos     | 1             |
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "user_id":2
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An MQTT ingestor reads user properties as headers in the order the publisher wrote them
    Given MQTT is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA reading ( user_id I64 );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT ( user_id integer );
      CREATE CODEC reading_codec FROM WIRE JSON SCHEMA reading_wire TO SCHEMA reading;
      CREATE SCHEMA tagged_reading (
        user_id I64,
        tenant STRING OPTIONAL,
        no_tenant BOOL,
        tenant_matches_its_spelling BOOL,
        first_route STRING OPTIONAL,
        last_route STRING OPTIONAL,
        routes I64
      );
      CREATE RELAY readings SCHEMA tagged_reading UNBRANCHED;
      CREATE MQTT CLIENT mqtt_main CONFIG { 'addr' = '{{mqtt_addr}}' };
      CREATE MQTT TOPIC INGESTOR mqtt_readings
        FROM mqtt_main TOPIC 'tagged_{{test_id}}' SESSION CLEAN QOS 1 MODE NO ACK
        ON QUIESCE DROP DECODE USING reading_codec
        TO readings INHERIT ALL
        SET tenant = read_header('tenant'),
          no_tenant = is_null(read_header('tenant')),
          tenant_matches_its_spelling = is_null(read_header('Tenant')),
          first_route = first(read_headers('route')),
          last_route = last(read_headers('route')),
          routes = count(read_headers('route'))
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION readings_subscription TO readings;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "mqtt_readings" on the leader node contains
      """
      ready: true
      """
    When an MQTT message is published to topic "tagged_{{test_id}}" with these MQTT 5 properties
      | payload       | {"user_id":7} |
      | qos           | 1             |
      | user_property | route=second  |
      | user_property | tenant=acme   |
      | user_property | route=first   |
      | user_property | route=third   |
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "user_id":7 | "tenant":"acme" | "no_tenant":false | "tenant_matches_its_spelling":true | "first_route":"second" | "last_route":"third" | "routes":3
      """
    When an MQTT message is published to topic "tagged_{{test_id}}" with these MQTT 5 properties
      | payload | {"user_id":8} |
      | qos     | 1             |
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "user_id":8 | "no_tenant":true | "routes":0
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # The faulted emitter holds the publish's acknowledgement, so the broker still owes it when the
  # connection ends. Released, the emitter lets the ingestor acknowledge on the lost connection, which
  # is how the host learns of the loss; it reconnects, and the persistent session delivers the publish
  # again with DUP set.
  Scenario Outline: A publish the broker delivers again reads metadata.duplicate as true
    Given MQTT is running
    And a stallable MQTT endpoint is configured
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA reading ( user_id I64 );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT ( user_id integer );
      CREATE CODEC reading_codec FROM WIRE JSON SCHEMA reading_wire TO SCHEMA reading;
      CREATE SCHEMA delivered_reading ( user_id I64, duplicate BOOL OPTIONAL );
      CREATE RELAY readings SCHEMA delivered_reading UNBRANCHED;
      CREATE RELAY forwarded SCHEMA reading UNBRANCHED;
      CREATE MQTT CLIENT mqtt_ingress CONFIG { 'addr' = '{{mqtt_stallable_addr}}' };
      CREATE MQTT CLIENT mqtt_out CONFIG { 'addr' = '{{mqtt_addr}}' };
      CREATE MQTT TOPIC INGESTOR mqtt_readings
        FROM mqtt_ingress TOPIC 'redelivered_{{test_id}}' SESSION PERSISTENT QOS 1
        MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE DROP DECODE USING reading_codec
        TO readings INHERIT ALL
        SET duplicate = metadata.duplicate
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        TO forwarded INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE MQTT TOPIC EMITTER mqtt_forward FROM forwarded TO mqtt_out TOPIC 'redelivered_out_{{test_id}}'
        MODE QOS 0 RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING reading_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION readings_subscription TO readings;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "mqtt_readings" on the leader node contains
      """
      ready: true
      """
    When emitter "mqtt_forward" enters fault mode
    And an MQTT message is published to topic "redelivered_{{test_id}}" with these MQTT 5 properties
      | payload | {"user_id":61} |
      | qos     | 1              |
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "user_id":61 | "duplicate":false
      """
    When the stallable endpoint "mqtt" resets its connections
    And emitter "mqtt_forward" leaves fault mode
    Then within "60s" the relay subscription receives payloads containing all fragments
      """
      "user_id":61 | "duplicate":true
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A publish a buffering quiesce retains keeps its metadata and user properties
    Given MQTT is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the entity gate for domain "{{domain}}" pauses after engagement
    When these Plexus commands are executed
      """
      CREATE SCHEMA reading ( user_id I64 );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT ( user_id integer );
      CREATE CODEC reading_codec FROM WIRE JSON SCHEMA reading_wire TO SCHEMA reading;
      CREATE CODEC reading_codec_v2 FROM WIRE JSON SCHEMA reading_wire TO SCHEMA reading;
      CREATE SCHEMA delivered_reading (
        user_id I64,
        topic STRING OPTIONAL,
        content_type STRING OPTIONAL,
        tenant STRING OPTIONAL
      );
      CREATE RELAY readings SCHEMA delivered_reading UNBRANCHED;
      CREATE MQTT CLIENT mqtt_main CONFIG { 'addr' = '{{mqtt_addr}}' };
      CREATE MQTT TOPIC INGESTOR mqtt_readings
        FROM mqtt_main TOPIC 'held_{{test_id}}/+' SESSION CLEAN QOS 1 MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST DECODE USING reading_codec
        TO readings INHERIT ALL
        SET topic = metadata.topic,
          content_type = metadata.content_type,
          tenant = read_header('tenant')
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION readings_subscription TO readings;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "mqtt_readings" on the leader node contains
      """
      ready: true
      """
    When these Plexus commands begin executing in the background
      """
      ALTER MQTT TOPIC INGESTOR mqtt_readings SET DECODE USING reading_codec_v2;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    And within "30s" DESCRIBE INGESTOR "mqtt_readings" on the leader node contains
      """
      quiesce state: entity hold
      """
    When an MQTT message is published to topic "held_{{test_id}}/kitchen" with these MQTT 5 properties
      | payload       | {"user_id":52}   |
      | qos           | 1                |
      | content_type  | application/json |
      | user_property | tenant=acme      |
    Then within "30s" DESCRIBE INGESTOR "mqtt_readings" on the leader node contains
      """
      nervix_ingestor_quiesce_buffered_records: 1
      """
    When the entity gate pause for domain "{{domain}}" is released
    Then the background Plexus execution succeeds
    And within "30s" the relay subscription receives payloads containing all fragments
      """
      "user_id":52 | "topic":"held_{{test_id}}/kitchen" | "content_type":"application/json" | "tenant":"acme"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
