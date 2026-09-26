Feature: Emitter batch containers
  An emitter that declares BATCH MAX MESSAGES and MAX SIZE publishes the eligible records of one
  buffered batch as batch payloads instead of one payload per record. Each payload is one value in
  the codec's own format: an array for JSON, CBOR, YAML and Avro, a single batch key for TOML, a
  single batch root element for XML, the codec's BATCH MESSAGE for protobuf and one RFC 5424 frame
  for syslog. An ON EMITTING BATCH transformation may replace that container with any single value
  of the format. A candidate whose encoding reaches MAX SIZE is halved and re-encoded, a record that
  alone still exceeds it is rejected, and a batch transformation that fails rejects every member of
  its batch with one shared error reference.

  @emitter_batch_containers
  Scenario Outline: Every format publishes one batch container for a compatible run
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "event.proto": "syntax = \"proto3\";\npackage nervix.test;\n\nmessage Event {\n  int64 seq = 1;\n  string note = 2;\n}\n\nmessage EventBatch {\n  repeated Event events = 1;\n}\n\nmessage EventEnvelope {\n  uint32 count = 1;\n  repeated Event records = 2;\n}\n"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "containers_{{test_id}}" exists with 1 partitions
    And Kafka topic "containers_{{test_id}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE proto_bundle;
      UPLOAD RESOURCE proto_bundle VERSION '{{proto_dir}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE WIRE JSON SCHEMA event_json_wire MODE STRICT ( seq integer, note string );
      CREATE WIRE CBOR SCHEMA event_cbor_wire MODE STRICT ( seq integer, note string );
      CREATE WIRE AVRO SCHEMA event_avro_wire MODE STRICT ( seq long, note string );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC json_wire_codec FROM WIRE JSON SCHEMA event_json_wire TO SCHEMA event;
      CREATE CODEC cbor_wire_codec FROM WIRE CBOR SCHEMA event_cbor_wire TO SCHEMA event;
      CREATE CODEC avro_wire_codec FROM WIRE AVRO SCHEMA event_avro_wire TO SCHEMA event;
      CREATE CODEC json_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON EMITTING '.';
      CREATE CODEC yaml_codec FROM YAML TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON EMITTING '.';
      CREATE CODEC toml_codec FROM TOML TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON EMITTING '.';
      CREATE CODEC cbor_codec FROM CBOR TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON EMITTING '.';
      CREATE CODEC xml_codec FROM XML TO SCHEMA event
        WITH JAQ TRANSFORMATIONS
          ON EMITTING '{t: "event", a: {seq: (.seq | tostring), note: .note}}';
      CREATE CODEC proto_codec
        FROM PROTOBUF
        USING RESOURCE proto_bundle VERSION 1
        CONFIG {'file' = 'event.proto', 'include' = '.'}
        MESSAGE 'nervix.test.Event'
        BATCH MESSAGE 'nervix.test.EventBatch'
        TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON EMITTING '{seq: .seq, note: .note}';
      CREATE CODEC proto_envelope_codec
        FROM PROTOBUF
        USING RESOURCE proto_bundle VERSION 1
        CONFIG {'file' = 'event.proto', 'include' = '.'}
        MESSAGE 'nervix.test.Event'
        BATCH MESSAGE 'nervix.test.EventEnvelope'
        TO SCHEMA event
        WITH JAQ TRANSFORMATIONS
          ON EMITTING '{seq: .seq, note: .note}'
          ON EMITTING BATCH '{count: length, records: .}';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE EMITTER record_json FROM events
        TO KAFKA kafka_main TOPIC containers_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING json_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER batch_json_wire FROM events
        TO KAFKA kafka_main TOPIC containers_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING json_wire_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER batch_cbor_wire FROM events
        TO KAFKA kafka_main TOPIC containers_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING cbor_wire_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER batch_avro_wire FROM events
        TO KAFKA kafka_main TOPIC containers_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING avro_wire_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER batch_json FROM events
        TO KAFKA kafka_main TOPIC containers_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING json_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER batch_yaml FROM events
        TO KAFKA kafka_main TOPIC containers_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING yaml_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER batch_toml FROM events
        TO KAFKA kafka_main TOPIC containers_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING toml_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER batch_cbor FROM events
        TO KAFKA kafka_main TOPIC containers_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING cbor_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER batch_xml FROM events
        TO KAFKA kafka_main TOPIC containers_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING xml_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER batch_proto FROM events
        TO KAFKA kafka_main TOPIC containers_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING proto_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER batch_proto_envelope FROM events
        TO KAFKA kafka_main TOPIC containers_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING proto_envelope_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"note":"a"},{"seq":2,"note":"b"},{"seq":3,"note":"c"}]
      """
    Then within "30s" the observed broker receives exactly these encoded payloads
      """
      text:{"seq": 1, "note": "a"}
      text:{"seq": 2, "note": "b"}
      text:{"seq": 3, "note": "c"}
      text:[{"seq":1,"note":"a"},{"seq":2,"note":"b"},{"seq":3,"note":"c"}]
      hex:83bf6373657101646e6f74656161ffbf6373657102646e6f74656162ffbf6373657103646e6f74656163ff
      hex:0602026104026206026300
      text:[{"seq": 1, "note": "a"}, {"seq": 2, "note": "b"}, {"seq": 3, "note": "c"}]
      hex:5b7b7365713a20312c206e6f74653a20617d2c207b7365713a20322c206e6f74653a20627d2c207b7365713a20332c206e6f74653a20637d5d0a
      hex:5b5b62617463685d5d0a736571203d20310a6e6f7465203d202261220a0a5b5b62617463685d5d0a736571203d20320a6e6f7465203d202262220a0a5b5b62617463685d5d0a736571203d20330a6e6f7465203d202263220a
      hex:83a26373657101646e6f74656161a26373657102646e6f74656162a26373657103646e6f74656163
      hex:3c62617463683e3c6576656e74207365713d223122206e6f74653d2261222f3e3c6576656e74207365713d223222206e6f74653d2262222f3e3c6576656e74207365713d223322206e6f74653d2263222f3e3c2f62617463683e
      hex:0a0508011201610a0508021201620a050803120163
      hex:0803120508011201611205080212016212050803120163
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batch_transformations
  Scenario Outline: ON EMITTING BATCH maps, wraps and expands the array of member values
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "transformed_{{test_id}}" exists with 1 partitions
    And Kafka topic "transformed_{{test_id}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA rich_event ( seq I64, tags <tags_type>, blob BYTES );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA rich_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC identity_codec FROM JSON TO SCHEMA rich_event
        WITH JAQ TRANSFORMATIONS ON EMITTING '.' ON EMITTING BATCH '.';
      CREATE CODEC map_codec FROM JSON TO SCHEMA rich_event
        WITH JAQ TRANSFORMATIONS
          ON EMITTING '.'
          ON EMITTING BATCH 'map({id: .seq, first_tag: .tags[0]})';
      CREATE CODEC envelope_codec FROM JSON TO SCHEMA rich_event
        WITH JAQ TRANSFORMATIONS
          ON EMITTING '{id: .seq, blob: .blob}'
          ON EMITTING BATCH '{schema_version: 2, count: length, records: .}';
      CREATE CODEC expansion_codec FROM JSON TO SCHEMA rich_event
        WITH JAQ TRANSFORMATIONS
          ON EMITTING '{id: .seq}'
          ON EMITTING BATCH '[.[], .[]]';
      CREATE RELAY events SCHEMA rich_event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE EMITTER identity_batches FROM events
        TO KAFKA kafka_main TOPIC transformed_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING identity_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER mapped_batches FROM events
        TO KAFKA kafka_main TOPIC transformed_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING map_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER enveloped_batches FROM events
        TO KAFKA kafka_main TOPIC transformed_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING envelope_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER expanded_batches FROM events
        TO KAFKA kafka_main TOPIC transformed_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING expansion_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"tags":["x","y"],"blob":"AP8="},{"seq":2,"tags":[],"blob":""}]
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      [{"seq": 1, "tags": ["x", "y"], "blob": "AP8="}, {"seq": 2, "tags": [], "blob": ""}]
      [{"id": 1, "first_tag": "x"}, {"id": 2, "first_tag": null}]
      {"schema_version": 2, "count": 2, "records": [{"id": 1, "blob": "AP8="}, {"id": 2, "blob": ""}]}
      [{"id": 1}, {"id": 2}, {"id": 1}, {"id": 2}]
      """

    Examples:
      | cluster_size | tags_type   |
      | 1            | VEC<STRING> |
      | 3            | VEC<STRING> |

  @emitter_batch_round_trip
  Scenario Outline: Batch payloads read back as their members through a Nervix ingestor
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "event.proto": "syntax = \"proto3\";\npackage nervix.test;\n\nmessage Event {\n  int64 seq = 1;\n  string note = 2;\n}\n\nmessage EventBatch {\n  repeated Event events = 1;\n}\n"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "json_batches_{{test_id}}" exists with 1 partitions
    And Kafka topic "proto_batches_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE proto_bundle;
      UPLOAD RESOURCE proto_bundle VERSION '{{proto_dir}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE SCHEMA echoed_event ( seq I64, note STRING, path STRING );
      CREATE CODEC json_batch_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]' ON EMITTING '.';
      CREATE CODEC proto_batch_codec
        FROM PROTOBUF
        USING RESOURCE proto_bundle VERSION 1
        CONFIG {'file' = 'event.proto', 'include' = '.'}
        MESSAGE 'nervix.test.Event'
        BATCH MESSAGE 'nervix.test.EventBatch'
        TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON EMITTING '{seq: .seq, note: .note}';
      CREATE CODEC proto_batch_reader
        FROM PROTOBUF
        USING RESOURCE proto_bundle VERSION 1
        CONFIG {'file' = 'event.proto', 'include' = '.'}
        MESSAGE 'nervix.test.EventBatch'
        TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.events[] | {seq: .seq, note: .note}';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY echoed SCHEMA echoed_event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING json_batch_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE EMITTER json_batches FROM events
        TO KAFKA kafka_main TOPIC json_batches_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING json_batch_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER proto_batches FROM events
        TO KAFKA kafka_main TOPIC proto_batches_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING proto_batch_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INGESTOR json_batch_source
        FROM KAFKA kafka_main TOPIC json_batches_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_json_batches_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING json_batch_codec
        TO echoed
        INHERIT ALL
        SET path = 'json'
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR proto_batch_source
        FROM KAFKA kafka_main TOPIC proto_batches_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_proto_batches_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING proto_batch_reader
        TO echoed
        INHERIT ALL
        SET path = 'protobuf'
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION echoed_subscription TO echoed;
      START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"note":"a"},{"seq":2,"note":"b \"quoted\""},{"seq":3,"note":"ü"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":1 | "note":"a" | "path":"json"
      "seq":2 | "note":"b \"quoted\"" | "path":"json"
      "seq":3 | "note":"ü" | "path":"json"
      "seq":1 | "note":"a" | "path":"protobuf"
      "seq":2 | "note":"b \"quoted\"" | "path":"protobuf"
      "seq":3 | "note":"ü" | "path":"protobuf"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batch_subdivision
  Scenario Outline: A candidate that reaches MAX SIZE is halved and re-encoded until it fits
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "subdivided_{{test_id}}" exists with 1 partitions
    And Kafka topic "subdivided_{{test_id}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE SCHEMA rejected_event (
        seq I64,
        error_code STRING,
        error_message STRING,
        operation STRING
      );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC padded_pairs_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS
          ON EMITTING 'if .seq == 7 then error("unencodable") else {seq: .seq, note: .note} end'
          ON EMITTING BATCH 'if length == 2 then {pad: "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", records: .} else . end';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE EMITTER subdivided FROM events
        TO KAFKA kafka_main TOPIC subdivided_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING padded_pairs_codec
        INHERIT ALL
        BATCH MAX MESSAGES 3 MAX SIZE 80B
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq,
              error_code = error.code,
              error_message = error.message,
              operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START;
      """
    # Three members encode to 75 bytes and fit. Two members are padded past the limit, so the
    # candidate [4, 5, 6] halves to [4, 5], which is larger still, and halves again to [4]. The
    # sixth record alone exceeds the limit, and the seventh never becomes a member.
    And http payload is posted to host "http-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"note":"a"},{"seq":2,"note":"b"},{"seq":3,"note":"c"},{"seq":4,"note":"d"},{"seq":5,"note":"e"},{"seq":6,"note":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"},{"seq":7,"note":"g"}]
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      [{"seq": 1, "note": "a"}, {"seq": 2, "note": "b"}, {"seq": 3, "note": "c"}]
      [{"seq": 4, "note": "d"}]
      [{"seq": 5, "note": "e"}]
      """
    And within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":6 | "error_code":"validation" | "operation":"encode" | emitter 'subdivided' codec 'padded_pairs_codec' JSON payload exceeds MAX SIZE 80B
      "seq":7 | "operation":"encode" | emitter 'subdivided' failed to encode record
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_batch_transformation_failures
  Scenario Outline: A failed batch transformation rejects every member with one shared reference
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "failed_batches_{{test_id}}" exists with 1 partitions
    And Kafka topic "failed_batches_{{test_id}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE SCHEMA rejected_event (
        seq I64,
        error_reference STRING,
        error_code STRING,
        error_message STRING,
        operation STRING
      );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC failing_codec FROM <format> TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON EMITTING '.' ON EMITTING BATCH '<program>';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY rejected_events SCHEMA rejected_event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE EMITTER failing_batches FROM events
        TO KAFKA kafka_main TOPIC failed_batches_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING failing_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_events
          SET seq = input.seq,
              error_reference = error.reference,
              error_code = error.code,
              error_message = error.message,
              operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_events_subscription TO rejected_events;
      START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/events"
      """
      [{"seq":1,"note":"a"},{"seq":2,"note":"b"},{"seq":3,"note":"c"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments that share one "error_reference"
      """
      "seq":1 | "error_code":"<code>" | "operation":"encode" | emitter 'failing_batches' codec 'failing_codec' <diagnostic> for a batch of 3 messages
      "seq":2 | "error_code":"<code>" | "operation":"encode" | emitter 'failing_batches' codec 'failing_codec' <diagnostic> for a batch of 3 messages
      "seq":3 | "error_code":"<code>" | "operation":"encode" | emitter 'failing_batches' codec 'failing_codec' <diagnostic> for a batch of 3 messages
      """
    And the observed broker does not receive a payload within "2s"

    Examples:
      | cluster_size | format | program          | code       | diagnostic                                      |
      | 1            | JSON   | empty            | evaluation | ON EMITTING BATCH produced no output            |
      | 1            | JSON   | .[]              | evaluation | ON EMITTING BATCH produced more than one output |
      | 1            | JSON   | error("private") | evaluation | ON EMITTING BATCH evaluation failed             |
      | 1            | TOML   | .                | validation | cannot write the batch as TOML                  |
      | 3            | JSON   | empty            | evaluation | ON EMITTING BATCH produced no output            |
      | 3            | JSON   | .[]              | evaluation | ON EMITTING BATCH produced more than one output |
      | 3            | JSON   | error("private") | evaluation | ON EMITTING BATCH evaluation failed             |
      | 3            | TOML   | .                | validation | cannot write the batch as TOML                  |

  @emitter_batch_syslog
  Scenario Outline: A syslog batch is one RFC 5424 frame per run of records sharing a header
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "syslog_frames_{{test_id}}" exists with 1 partitions
    And Kafka topic "syslog_frames_{{test_id}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA syslog_event (
        facility U8,
        severity U8,
        hostname STRING OPTIONAL,
        message STRING
      );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA syslog_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY syslog_events SCHEMA syslog_event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR http_events
        FROM ENDPOINT events_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO syslog_events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE EMITTER syslog_frames FROM syslog_events
        TO KAFKA kafka_main TOPIC syslog_frames_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING syslog_codec
        INHERIT ALL
        BATCH MAX MESSAGES 10 MAX SIZE 1KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/events"
      """
      [{"facility":16,"severity":6,"hostname":"app-01","message":"order accepted"},{"facility":16,"severity":6,"hostname":"app-01","message":"order shipped"},{"facility":16,"severity":3,"hostname":"app-01","message":"order failed"},{"facility":16,"severity":6,"hostname":"app-01","message":"order closed"}]
      """
    Then within "30s" the observed broker receives exactly these encoded payloads
      """
      hex:3c3133343e31202d206170702d3031202d202d202d202d205b223c3133343e31202d206170702d3031202d202d202d202d206f72646572206163636570746564222c223c3133343e31202d206170702d3031202d202d202d202d206f726465722073686970706564225d
      hex:3c3133313e31202d206170702d3031202d202d202d202d205b223c3133313e31202d206170702d3031202d202d202d202d206f72646572206661696c6564225d
      hex:3c3133343e31202d206170702d3031202d202d202d202d205b223c3133343e31202d206170702d3031202d202d202d202d206f7264657220636c6f736564225d
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
