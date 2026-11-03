@pulsar_message_metadata @deloxide_stress
Feature: Pulsar transport metadata values
  Background:
    Given Pulsar is running

  Scenario Outline: A Pulsar <mode_name> ingestor projects the complete message metadata and properties
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar subscription "values_{{test_id}}" of topic "values_{{test_id}}" is provisioned
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA observed (
        seq I64, topic STRING OPTIONAL, message_id STRING OPTIONAL,
        publish_time DATETIME OPTIONAL, event_time DATETIME OPTIONAL, key STRING OPTIONAL,
        event_missing BOOL, key_missing BOOL,
        ordering_hex STRING, producer STRING OPTIONAL, sequence_id I64 OPTIONAL,
        redelivery_count I64 OPTIONAL, property STRING OPTIONAL
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA observed UNBRANCHED;
      CREATE PULSAR CLIENT source CONFIG { 'addr' = '{{pulsar_addr}}' };
      CREATE PULSAR TOPIC INGESTOR records FROM source TOPIC 'values_{{test_id}}'
        SUBSCRIPTION 'values_{{test_id}}' PROVISIONED MODE <mode>
        ON QUIESCE SUSPEND DECODE USING event_codec TIMESTAMP AT metadata.publish_time
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
    And these Pulsar metadata records are published to topic "values_{{test_id}}"
      """
      [
        {"payload":"{\"seq\":1}","key":"Key-A","ordering_key":[0,255],"event_time":1767225600123},
        {"payload":"{\"seq\":2}","key":null,"ordering_key":null,"event_time":null},
        {"payload":"{\"seq\":3}","key":"","ordering_key":[],"event_time":0}
      ]
      """
    And these Plexus commands are executed
      """
      START;
      """
    Then within "60s" the relay subscription receives payloads containing all fragments
      """
      "seq":1 | "topic":"persistent://public/default/values_{{test_id}}" | "message_id":"{{pulsar_message_id_1}}" | "publish_time":"{{pulsar_publish_time_1}}" | "event_time":"2026-01-01T00:00:00.123+00:00" | "event_missing":false | "key":"Key-A" | "key_missing":false | "ordering_hex":"00ff" | "producer":"{{pulsar_producer_name}}" | "sequence_id":{{pulsar_sequence_id_1}} | "redelivery_count":0 | "property":"kept"
      "seq":2 | "message_id":"{{pulsar_message_id_2}}" | "publish_time":"{{pulsar_publish_time_2}}" | "event_missing":true | "key_missing":true | "ordering_hex":"absent" | "sequence_id":{{pulsar_sequence_id_2}} | "redelivery_count":0
      "seq":3 | "message_id":"{{pulsar_message_id_3}}" | "publish_time":"{{pulsar_publish_time_3}}" | "event_time":"1970-01-01T00:00:00+00:00" | "event_missing":false | "key":"" | "key_missing":false | "ordering_hex":"" | "sequence_id":{{pulsar_sequence_id_3}} | "redelivery_count":0
      """

    Examples:
      | cluster_size | mode_name      | mode                                                                                     |
      | 1            | NO ACK         | NO ACK                                                                                   |
      | 3            | NO ACK         | NO ACK                                                                                   |
      | 1            | ACK SEQUENTIAL | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s                         |
      | 3            | ACK SEQUENTIAL | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s                         |
      | 1            | ACK PARALLEL   | ACK PARALLEL MAX 8 BATCH TIMEOUT 500ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s |
      | 3            | ACK PARALLEL   | ACK PARALLEL MAX 8 BATCH TIMEOUT 500ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s |

  Scenario Outline: Pulsar event time flushes historical windows separately for two branches
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar subscription "windows_{{test_id}}" of topic "windows_{{test_id}}" is provisioned
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( tenant STRING, value I64 );
      CREATE SCHEMA timed ( tenant STRING, value I64, event_time DATETIME OPTIONAL );
      CREATE SCHEMA summary ( tenant STRING, total I64, event_time DATETIME OPTIONAL );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY events SCHEMA timed BRANCHED BY by_tenant;
      CREATE RELAY summaries SCHEMA summary BRANCHED BY by_tenant;
      CREATE PULSAR CLIENT source CONFIG { 'addr' = '{{pulsar_addr}}' };
      CREATE PULSAR TOPIC INGESTOR records FROM source TOPIC 'windows_{{test_id}}'
        SUBSCRIPTION 'windows_{{test_id}}' PROVISIONED MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING event_codec TIMESTAMP AT metadata.event_time
        TO events INHERIT ALL SET event_time = metadata.event_time
          BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR historical FROM events
        WIDTH 24h DURATION STEP 24h DURATION BRANCHED BY by_tenant
        TO summaries SET tenant = FIRST(input.tenant), total = SUM(input.value), event_time = FIRST(input.event_time)
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION observations TO summaries;
      """
    And these Pulsar metadata records are published to topic "windows_{{test_id}}"
      """
      [
        {"payload":"{\"tenant\":\"acme\",\"value\":10}","event_time":1767225600123},
        {"payload":"{\"tenant\":\"beta\",\"value\":100}","event_time":1767225601456}
      ]
      """
    And these Plexus commands are executed
      """
      START;
      """
    Then within "60s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "total":10 | "event_time":"2026-01-01T00:00:00.123+00:00"
      key={"tenant":"beta"} | "total":100 | "event_time":"2026-01-01T00:00:01.456+00:00"
      """
    When these Pulsar metadata records are published to topic "windows_{{test_id}}"
      """
      [
        {"payload":"{\"tenant\":\"acme\",\"value\":20}","event_time":1767312000123},
        {"payload":"{\"tenant\":\"beta\",\"value\":200}","event_time":1767312001456}
      ]
      """
    Then within "60s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "total":20 | "event_time":"2026-01-02T00:00:00.123+00:00"
      key={"tenant":"beta"} | "total":200 | "event_time":"2026-01-02T00:00:01.456+00:00"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A Pulsar redelivery keeps its message identity and reports the broker's count
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar subscription "redelivery_{{test_id}}" of topic "redelivery_{{test_id}}" is provisioned
    And Pulsar subscription "out_{{test_id}}" of topic "out_{{test_id}}" is provisioned
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA observed ( seq I64, message_id STRING OPTIONAL, redelivery_count I64 OPTIONAL );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA observed UNBRANCHED;
      CREATE PULSAR CLIENT source CONFIG { 'addr' = '{{pulsar_addr}}' };
      CREATE PULSAR TOPIC INGESTOR records FROM source TOPIC 'redelivery_{{test_id}}'
        SUBSCRIPTION 'redelivery_{{test_id}}' PROVISIONED
        MODE ACK SEQUENTIAL ACK TIMEOUT 2s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL
          SET message_id = metadata.message_id, redelivery_count = metadata.redelivery_count
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ATTACHED PULSAR TOPIC EMITTER forwarded FROM events
        TO source TOPIC 'out_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 2s RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING event_codec INHERIT seq
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      START;
      """
    And emitter "forwarded" enters fault mode
    And these Pulsar metadata records are published to topic "redelivery_{{test_id}}"
      """
      [{"payload":"{\"seq\":1}"}]
      """
    Then within "60s" the relay subscription receives payloads containing all fragments
      """
      "seq":1 | "message_id":"{{pulsar_message_id_1}}" | "redelivery_count":0
      "seq":1 | "message_id":"{{pulsar_message_id_1}}" | "redelivery_count":1
      """
    When emitter "forwarded" leaves fault mode
    Then within "60s" Pulsar subscription "redelivery_{{test_id}}" of topic "redelivery_{{test_id}}" holds 0 unacknowledged messages

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
