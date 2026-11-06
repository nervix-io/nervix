Feature: CBOR wire schemas use CBOR values
  A declared CBOR byte string crosses an HTTP ingestor and a broker emitter as bytes. Its
  neighbouring fields retain their CBOR integer, float and text representations.

  @cbor_wire_schema
  Scenario Outline: A CBOR byte string crosses the graph without a text interchange
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "cbor_wire_{{test_id}}" exists with 1 partitions
    And Kafka topic "cbor_wire_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA event (uid U64, temperature F64, occurred_at DATETIME, body BYTES);
      CREATE WIRE CBOR SCHEMA event_wire MODE STRICT (
        uid integer, temperature float, occurred_at text, body bytes
      );
      CREATE CODEC event_codec FROM WIRE CBOR SCHEMA event_wire TO SCHEMA event
        ENCODE occurred_at AS RFC3339;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge cbor-wire-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/ingest' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC EMITTER kafka_events FROM events
        TO kafka_main TOPIC 'cbor_wire_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION events_subscription TO events;
      START;
      """
    When http CBOR payload with byte string field "body" is posted to host "cbor-wire-{{test_id}}.example.com" path "/ingest"
      """
      {"uid":7,"temperature":1.25,"occurred_at":"2024-02-29T12:34:56Z","body":"AP8="}
      """
    Then within "10s" the relay subscription receives a payload
      """
      {"body":"AP8=","occurred_at":"2024-02-29T12:34:56+00:00","temperature":1.25,"uid":7}
      """
    And within "30s" the observed broker receives one CBOR event with byte string "AP8="
    When http CBOR payload with NaN float field "temperature" is posted to host "cbor-wire-{{test_id}}.example.com" path "/ingest"
      """
      {"uid":7,"temperature":1.25,"occurred_at":"2024-02-29T12:34:56Z","body":"AP8="}
      """
    Then within "30s" the observed broker receives one CBOR event with NaN float
    When http CBOR payload with invalid "tagged item" at field "body" is posted to host "cbor-wire-{{test_id}}.example.com" path "/ingest"
      """
      {"uid":7,"temperature":1.25,"occurred_at":"2024-02-29T12:34:56Z","body":"AP8="}
      """
    Then within "10s" the active session observes a server error containing
      """
      codec 'event_codec' failed to parse field 'body': CBOR tag is not a schemaful value
      """
    When http CBOR payload with invalid "non-text map key" at field "body" is posted to host "cbor-wire-{{test_id}}.example.com" path "/ingest"
      """
      {"uid":7,"temperature":1.25,"occurred_at":"2024-02-29T12:34:56Z","body":"AP8="}
      """
    Then within "10s" the active session observes a server error containing
      """
      codec 'event_codec' failed to parse field 'body': CBOR map key must be text
      """
    When http CBOR payload with invalid "repeated key" at field "body" is posted to host "cbor-wire-{{test_id}}.example.com" path "/ingest"
      """
      {"uid":7,"temperature":1.25,"occurred_at":"2024-02-29T12:34:56Z","body":"AP8="}
      """
    Then within "10s" the active session observes a server error containing
      """
      codec 'event_codec' failed to parse field 'body': CBOR map has duplicate key 'body'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
