@kafka_record_metadata @deloxide_stress
Feature: Kafka record keys, timestamps and tombstones
  Background:
    Given Kafka is running

  Scenario Outline: Kafka broker event time makes historical windows due independently in two branches
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "event_time_{{test_id}}" exists with 1 partitions
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA event ( tenant STRING, value I64 );
      CREATE SCHEMA timed_event ( tenant STRING, value I64, event_time DATETIME OPTIONAL );
      CREATE SCHEMA summary ( tenant STRING, total I64, first_time DATETIME OPTIONAL );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY events SCHEMA timed_event BRANCHED BY by_tenant;
      CREATE RELAY summaries SCHEMA summary BRANCHED BY by_tenant;
      CREATE KAFKA CLIENT source CONFIG { 'bootstrap.servers' = '{{kafka_addr}}', 'auto.offset.reset' = 'earliest' };
      CREATE KAFKA TOPIC INGESTOR records FROM source TOPIC 'event_time_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'event_time_{{test_id}}' MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING event_codec TIMESTAMP AT metadata.timestamp
        TO events INHERIT ALL SET event_time = metadata.timestamp
          BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR historical FROM events
        WIDTH 24h DURATION STEP 24h DURATION BRANCHED BY by_tenant
        TO summaries SET tenant = FIRST(input.tenant), total = SUM(input.value), first_time = FIRST(input.event_time)
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION observations TO summaries;
      """
    And these Kafka records are published to topic "event_time_{{test_id}}"
      """
      [
        {"key":[1],"payload":"{\"tenant\":\"acme\",\"value\":10}","timestamp":"2026-01-01T00:00:00.123Z"},
        {"key":[2],"payload":"{\"tenant\":\"beta\",\"value\":100}","timestamp":"2026-01-01T00:00:01.456Z"}
      ]
      """
    And these Plexus statements are executed on the leader node
      """
      START;
      """
    Then within "60s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "total":10 | "first_time":"2026-01-01T00:00:00.123+00:00"
      key={"tenant":"beta"} | "total":100 | "first_time":"2026-01-01T00:00:01.456+00:00"
      """
    When these Kafka records are published to topic "event_time_{{test_id}}"
      """
      [
        {"key":[1],"payload":"{\"tenant\":\"acme\",\"value\":20}","timestamp":"2026-01-02T00:00:00.123Z"},
        {"key":[2],"payload":"{\"tenant\":\"beta\",\"value\":200}","timestamp":"2026-01-02T00:00:01.456Z"}
      ]
      """
    Then within "60s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "total":20 | "first_time":"2026-01-02T00:00:00.123+00:00"
      key={"tenant":"beta"} | "total":200 | "first_time":"2026-01-02T00:00:01.456+00:00"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A Kafka <mode_name> ingestor projects each record's key, timestamp and type
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "metadata_{{test_id}}" exists with 1 partitions
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA observed ( seq I64, key_hex STRING, record_time DATETIME OPTIONAL, time_type STRING OPTIONAL, tombstone BOOL );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA observed UNBRANCHED;
      CREATE KAFKA CLIENT source CONFIG { 'bootstrap.servers' = '{{kafka_addr}}', 'auto.offset.reset' = 'earliest' };
      CREATE KAFKA TOPIC INGESTOR records FROM source TOPIC 'metadata_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'metadata_{{test_id}}' MODE <mode>
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL
          SET key_hex = coalesce(hex_encode(metadata.key), 'absent'),
              record_time = metadata.timestamp, time_type = metadata.timestamp_type,
              tombstone = metadata.tombstone
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      """
    And these Kafka records are published to topic "metadata_{{test_id}}"
      """
      [
        {"key":[0,255],"payload":"{\"seq\":1}","timestamp":"2026-01-01T00:00:00.123Z"},
        {"key":null,"payload":"{\"seq\":2}","timestamp":"2026-01-01T00:00:01.456Z"},
        {"key":[],"payload":"{\"seq\":3}","timestamp":"2026-01-01T00:00:02.789Z"}
      ]
      """
    And these Plexus statements are executed on the leader node
      """
      START;
      """
    Then within "60s" the relay subscription receives payloads containing all fragments
      """
      "seq":1 | "key_hex":"00ff" | "record_time":"2026-01-01T00:00:00.123+00:00" | "time_type":"create_time" | "tombstone":false
      "seq":2 | "key_hex":"absent" | "record_time":"2026-01-01T00:00:01.456+00:00" | "time_type":"create_time" | "tombstone":false
      "seq":3 | "key_hex":"" | "record_time":"2026-01-01T00:00:02.789+00:00" | "time_type":"create_time" | "tombstone":false
      """

    Examples:
      | cluster_size | mode_name      | mode                                                                                     |
      | 1            | NO ACK         | NO ACK                                                                                   |
      | 3            | NO ACK         | NO ACK                                                                                   |
      | 1            | ACK SEQUENTIAL | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s                         |
      | 3            | ACK SEQUENTIAL | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s                         |
      | 1            | ACK PARALLEL   | ACK PARALLEL MAX 8 BATCH TIMEOUT 500ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s |
      | 3            | ACK PARALLEL   | ACK PARALLEL MAX 8 BATCH TIMEOUT 500ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s |

  Scenario Outline: A Kafka ingestor distinguishes a tombstone from an empty value
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "tombstones_{{test_id}}" exists with 1 partitions
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA event ( marker STRING );
      CREATE SCHEMA observed ( marker STRING, record_offset I64, tombstone BOOL );
      CREATE CODEC empty_codec FROM TOML TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '{marker: "seen"}';
      CREATE RELAY events SCHEMA observed UNBRANCHED;
      CREATE KAFKA CLIENT source CONFIG { 'bootstrap.servers' = '{{kafka_addr}}', 'auto.offset.reset' = 'earliest' };
      CREATE KAFKA TOPIC INGESTOR records FROM source TOPIC 'tombstones_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'tombstones_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING empty_codec
        TO events INHERIT ALL SET record_offset = metadata.offset, tombstone = metadata.tombstone
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      """
    And these Kafka records are published to topic "tombstones_{{test_id}}"
      """
      [
        {"key":[1],"payload":null,"timestamp":"2026-01-01T00:00:00Z"},
        {"key":[1],"payload":"","timestamp":"2026-01-01T00:00:01Z"},
        {"key":[1],"payload":"value=3","timestamp":"2026-01-01T00:00:02Z"}
      ]
      """
    And these Plexus statements are executed on the leader node
      """
      START;
      """
    Then within "60s" the relay subscription receives payloads containing all fragments
      """
      "marker":"seen" | "record_offset":0 | "tombstone":true
      "marker":"seen" | "record_offset":1 | "tombstone":false
      "marker":"seen" | "record_offset":2 | "tombstone":false
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
