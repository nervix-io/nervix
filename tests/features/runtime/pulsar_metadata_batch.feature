@pulsar_message_metadata @pulsar_batch_metadata @deloxide_stress
Feature: Pulsar batch member metadata
  Background:
    Given Pulsar is running

  Scenario Outline: Pulsar batch members retain their own sequence ids and complete metadata
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar subscription "batch_{{test_id}}" of topic "batch_{{test_id}}" is provisioned
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA observed (
        seq I64, topic STRING OPTIONAL, message_id STRING OPTIONAL,
        publish_time DATETIME OPTIONAL, event_time DATETIME OPTIONAL, key STRING OPTIONAL,
        event_missing BOOL, key_missing BOOL, ordering_hex STRING,
        producer STRING OPTIONAL, sequence_id I64 OPTIONAL, redelivery_count I64 OPTIONAL,
        property STRING OPTIONAL
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA observed UNBRANCHED;
      CREATE PULSAR CLIENT source CONFIG { 'addr' = '{{pulsar_addr}}' };
      CREATE PULSAR TOPIC INGESTOR records FROM source TOPIC 'batch_{{test_id}}'
        SUBSCRIPTION 'batch_{{test_id}}' PROVISIONED TYPE SHARED MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING event_codec TIMESTAMP AT metadata.publish_time
        FILTER WHERE payload.bytes > (0 AS U64)
        TO events INHERIT ALL
          SET topic = metadata.topic, message_id = metadata.message_id,
              publish_time = metadata.publish_time, event_time = metadata.event_time,
              event_missing = is_null(metadata.event_time), key_missing = is_null(metadata.key),
              key = metadata.key, ordering_hex = coalesce(hex_encode(metadata.ordering_key), 'absent'),
              producer = metadata.producer_name, sequence_id = metadata.sequence_id,
              redelivery_count = metadata.redelivery_count, property = read_header('Case')
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      """
    And a Pulsar batch with distinct member metadata is published to topic "batch_{{test_id}}"
    And these Plexus commands are executed
      """
      START;
      """
    Then within "60s" the relay subscription receives payloads containing all fragments
      """
      "seq":1 | "topic":"persistent://public/default/batch_{{test_id}}" | "message_id":"{{pulsar_batch_id_1}}" | "publish_time":"2026-01-01T00:00:00.123+00:00" | "event_time":"2026-01-01T00:00:00+00:00" | "event_missing":false | "key":"batch-key" | "key_missing":false | "ordering_hex":"00ff" | "producer":"{{pulsar_batch_producer}}" | "sequence_id":41 | "redelivery_count":0 | "property":"kept"
      "seq":2 | "topic":"persistent://public/default/batch_{{test_id}}" | "producer":"{{pulsar_batch_producer}}" | "message_id":"{{pulsar_batch_id_2}}" | "publish_time":"2026-01-01T00:00:00.123+00:00" | "event_time":"1970-01-01T00:00:00+00:00" | "event_missing":false | "key":"" | "key_missing":false | "ordering_hex":"" | "sequence_id":42 | "redelivery_count":0 | "property":"kept"
      "seq":3 | "topic":"persistent://public/default/batch_{{test_id}}" | "producer":"{{pulsar_batch_producer}}" | "message_id":"{{pulsar_batch_id_3}}" | "publish_time":"2026-01-01T00:00:00.123+00:00" | "event_time":"2026-01-01T00:00:00.456+00:00" | "event_missing":false | "key_missing":true | "ordering_hex":"absent" | "sequence_id":99 | "redelivery_count":0 | "property":"kept"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
