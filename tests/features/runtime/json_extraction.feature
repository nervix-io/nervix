Feature: Typed JSON extraction
  Scenario Outline: JSON_VALUE reads declared scalar and collection types from one document
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA raw_event (
        id STRING,
        doc STRING
      );
      CREATE SCHEMA extracted_event (
        id STRING,
        count I64 OPTIONAL,
        level U8 OPTIONAL,
        offset I8 OPTIONAL,
        ratio F64 OPTIONAL,
        label STRING OPTIONAL,
        active BOOL OPTIONAL,
        tags <string_vec> OPTIONAL,
        point <point_type> OPTIONAL,
        matrix <matrix_type> OPTIONAL,
        second_items <integer_vec> OPTIONAL,
        odd U16 OPTIONAL,
        note_state STRING
      );
      CREATE SCHEMA extraction_error (
        input_id STRING,
        error_message STRING
      );
      CREATE CODEC raw_event_batch_codec
        FROM JSON
        TO SCHEMA raw_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY raw_events SCHEMA raw_event UNBRANCHED;
      CREATE RELAY extracted_events SCHEMA extracted_event UNBRANCHED;
      CREATE RELAY extraction_errors SCHEMA extraction_error UNBRANCHED;
      CREATE VHOST edge json-extraction-{{test_id}}.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING raw_event_batch_codec
        TO raw_events
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION extract_events
        FROM raw_events
        UNBRANCHED
        TO extracted_events
          SET id = input.id,
              count = JSON_VALUE(input.doc, '$.count' AS I64),
              level = JSON_VALUE(input.doc, '$.level' AS U8),
              offset = JSON_VALUE(input.doc, '$.offset' AS I8),
              ratio = JSON_VALUE(input.doc, '$.ratio' AS F64),
              label = JSON_VALUE(input.doc, '$.label' AS STRING),
              active = JSON_VALUE(input.doc, '$.active' AS BOOL),
              tags = JSON_VALUE(input.doc, '$.tags' AS <string_vec>),
              point = JSON_VALUE(input.doc, '$.point' AS <point_type>),
              matrix = JSON_VALUE(input.doc, '$.matrix' AS <matrix_type>),
              second_items = JSON_VALUE(input.doc, '$.orders[1].items' AS <integer_vec>),
              odd = JSON_VALUE(input.doc, '$["odd key"]' AS U16),
              note_state = CASE
                WHEN NOT JSON_EXISTS(input.doc, '$.note') THEN 'absent'
                WHEN is_null(JSON_VALUE(input.doc, '$.note' AS STRING)) THEN 'null'
                ELSE 'present'
              END
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO extraction_errors
          SET input_id = input.id,
              error_message = error.message;
      CREATE SUBSCRIPTION extracted_events_subscription TO extracted_events;
      CREATE SUBSCRIPTION extraction_errors_subscription TO extraction_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "json-extraction-{{test_id}}.example.com" path "/events"
      """
      [{"id":"full","doc":"{\"count\":9007199254740993,\"level\":255,\"offset\":-128,\"ratio\":0.25,\"label\":\"caf\\u00e9 \\\"q\\\" \\\\ \\ud83d\\ude00\",\"active\":true,\"tags\":[\"a\",\"b\"],\"point\":[1.5,-2],\"matrix\":[[1,2],[3]],\"orders\":[{\"items\":[1]},{\"items\":[2,3]}],\"odd key\":65535,\"note\":null}"},{"id":"sparse","doc":"{\"count\":1,\"note\":\"hi\",\"orders\":5}"},{"id":"empty","doc":"{}"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"full" | "count":9007199254740993 | "level":255 | "offset":-128 | "ratio":0.25 | "label":"café \"q\" \\ 😀" | "active":true | "tags":["a","b"] | "point":[1.5,-2.0] | "matrix":[[1,2],[3]] | "second_items":[2,3] | "odd":65535 | "note_state":"null"
      "id":"sparse" | "count":1 | "note_state":"present"
      "id":"empty" | "note_state":"absent"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count | string_vec  | point_type    | matrix_type   | integer_vec |
      | 1            | 0             | VEC<STRING> | ARRAY<F32, 2> | VEC<VEC<I32>> | VEC<I64>    |
      | 3            | 0             | VEC<STRING> | ARRAY<F32, 2> | VEC<VEC<I32>> | VEC<I64>    |

  Scenario Outline: Strict extraction names each defect and TRY_JSON_VALUE yields a typed null
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA raw_document (
        id STRING,
        doc STRING
      );
      CREATE SCHEMA strict_reading (
        id STRING,
        count I64 OPTIONAL,
        level U8 OPTIONAL,
        tags <integer_vec> OPTIONAL,
        point <point_type> OPTIONAL
      );
      CREATE SCHEMA tolerant_reading (
        id STRING,
        count I64 OPTIONAL,
        count_read BOOL,
        level_read BOOL,
        tags_read BOOL,
        point_read BOOL
      );
      CREATE SCHEMA reading_error (
        input_id STRING,
        error_message STRING
      );
      CREATE CODEC raw_document_batch_codec
        FROM JSON
        TO SCHEMA raw_document
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY raw_documents SCHEMA raw_document UNBRANCHED;
      CREATE RELAY strict_readings SCHEMA strict_reading UNBRANCHED;
      CREATE RELAY tolerant_readings SCHEMA tolerant_reading UNBRANCHED;
      CREATE RELAY reading_errors SCHEMA reading_error UNBRANCHED;
      CREATE VHOST edge json-defects-{{test_id}}.example.com;
      CREATE ENDPOINT document_ingress ON edge PATH '/documents' TYPE HTTP;
      CREATE INGESTOR document_source
        FROM ENDPOINT document_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING raw_document_batch_codec
        TO raw_documents
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION read_documents
        FROM raw_documents
        UNBRANCHED
        TO strict_readings
          SET id = input.id,
              count = JSON_VALUE(input.doc, '$.count' AS I64),
              level = JSON_VALUE(input.doc, '$.level' AS U8),
              tags = JSON_VALUE(input.doc, '$.tags' AS <integer_vec>),
              point = JSON_VALUE(input.doc, '$.point' AS <point_type>)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO reading_errors
          SET input_id = input.id,
              error_message = error.message
        TO tolerant_readings
          SET id = input.id,
              count = TRY_JSON_VALUE(input.doc, '$.count' AS I64),
              count_read = NOT is_null(TRY_JSON_VALUE(input.doc, '$.count' AS I64)),
              level_read = NOT is_null(TRY_JSON_VALUE(input.doc, '$.level' AS U8)),
              tags_read = NOT is_null(TRY_JSON_VALUE(input.doc, '$.tags' AS <integer_vec>)),
              point_read = NOT is_null(TRY_JSON_VALUE(input.doc, '$.point' AS <point_type>))
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO reading_errors
          SET input_id = input.id,
              error_message = error.message;
      CREATE SUBSCRIPTION strict_readings_subscription TO strict_readings;
      CREATE SUBSCRIPTION tolerant_readings_subscription TO tolerant_readings;
      CREATE SUBSCRIPTION reading_errors_subscription TO reading_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "json-defects-{{test_id}}.example.com" path "/documents"
      """
      [{"id":"valid","doc":"{\"count\":7,\"level\":200,\"tags\":[1,2],\"point\":[0.5,1]}"},{"id":"malformed","doc":"{\"count\":7,"},{"id":"text-count","doc":"{\"count\":\"7\"}"},{"id":"fractional","doc":"{\"count\":1.5}"},{"id":"too-large","doc":"{\"level\":256}"},{"id":"huge","doc":"{\"count\":18446744073709551616}"},{"id":"bad-element","doc":"{\"tags\":[1,\"x\"]}"},{"id":"null-element","doc":"{\"tags\":[1,null]}"},{"id":"long-point","doc":"{\"point\":[1,2,3]}"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"valid" | "count":7 | "level":200 | "tags":[1,2] | "point":[0.5,1.0]
      "input_id":"malformed" | cast_failed: JSON_VALUE document is not valid JSON
      "input_id":"text-count" | cast_failed: JSON_VALUE found a JSON string at $.count where I64 is declared
      "input_id":"fractional" | cast_failed: JSON_VALUE found a fractional JSON number at $.count where I64 is declared
      "input_id":"too-large" | cast_failed: JSON_VALUE number at $.level does not fit U8
      "input_id":"huge" | cast_failed: JSON_VALUE number at $.count does not fit I64
      "input_id":"bad-element" | cast_failed: JSON_VALUE found a JSON string in $.tags where I64 elements are declared
      "input_id":"null-element" | cast_failed: JSON_VALUE found JSON null in $.tags where I64 elements are declared
      "input_id":"long-point" | cast_failed: JSON_VALUE array at $.point has 3 elements where 2 are declared
      "id":"valid" | "count":7 | "count_read":true | "level_read":true | "tags_read":true | "point_read":true
      "id":"malformed" | "count_read":false | "level_read":false | "tags_read":false | "point_read":false
      "id":"text-count" | "count_read":false | "level_read":false
      "id":"fractional" | "count_read":false
      "id":"too-large" | "count_read":false | "level_read":false
      "id":"huge" | "count_read":false
      "id":"bad-element" | "tags_read":false
      "id":"null-element" | "tags_read":false
      "id":"long-point" | "point_read":false
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count | integer_vec | point_type    |
      | 1            | 0             | VEC<I64>    | ARRAY<F64, 2> |
      | 3            | 0             | VEC<I64>    | ARRAY<F64, 2> |

  Scenario Outline: A conditional arm reads only the documents of the messages that select it
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA routed_document (
        id STRING,
        kind STRING,
        doc STRING
      );
      CREATE SCHEMA routed_number (
        id STRING,
        number I64 OPTIONAL,
        tolerant_number I64 OPTIONAL
      );
      CREATE SCHEMA routed_error (
        input_id STRING,
        error_message STRING
      );
      CREATE CODEC routed_document_batch_codec
        FROM JSON
        TO SCHEMA routed_document
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY routed_documents SCHEMA routed_document UNBRANCHED;
      CREATE RELAY routed_numbers SCHEMA routed_number UNBRANCHED;
      CREATE RELAY routed_errors SCHEMA routed_error UNBRANCHED;
      CREATE VHOST edge json-arms-{{test_id}}.example.com;
      CREATE ENDPOINT routed_ingress ON edge PATH '/routed-documents' TYPE HTTP;
      CREATE INGESTOR routed_document_source
        FROM ENDPOINT routed_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING routed_document_batch_codec
        TO routed_documents
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION route_documents
        FROM routed_documents
        UNBRANCHED
        TO routed_numbers
          SET id = input.id,
              number = CASE
                WHEN input.kind = 'json' THEN JSON_VALUE(input.doc, '$.n' AS I64)
                WHEN input.kind = 'tolerant' THEN TRY_JSON_VALUE(input.doc, '$.n' AS I64)
                ELSE -1
              END,
              tolerant_number = TRY_JSON_VALUE(input.doc, '$.n' AS I64)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO routed_errors
          SET input_id = input.id,
              error_message = error.message;
      CREATE SUBSCRIPTION routed_numbers_subscription TO routed_numbers;
      CREATE SUBSCRIPTION routed_errors_subscription TO routed_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "json-arms-{{test_id}}.example.com" path "/routed-documents"
      """
      [{"id":"json-valid","kind":"json","doc":"{\"n\":5}"},{"id":"json-bad","kind":"json","doc":"{\"n\":"},{"id":"tolerant-bad","kind":"tolerant","doc":"{\"n\":\"x\"}"},{"id":"other","kind":"other","doc":"{\"n\":8}"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"json-valid" | "number":5 | "tolerant_number":5
      "input_id":"json-bad" | cast_failed: JSON_VALUE document is not valid JSON
      "id":"tolerant-bad"
      "id":"other" | "number":-1 | "tolerant_number":8
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: A JSON extraction keeps the type and sensitivity contract of its statement
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands fail with "<error>"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA raw_document (
        id STRING,
        doc STRING,
        secret_doc STRING SENSITIVE,
        number I64
      );
      CREATE SCHEMA parsed_document (
        id STRING,
        amount <amount_type>
      );
      CREATE RELAY raw_documents SCHEMA raw_document UNBRANCHED;
      CREATE RELAY parsed_documents SCHEMA parsed_document UNBRANCHED;
      CREATE JUNCTION parse_documents
        FROM raw_documents
        UNBRANCHED
        TO parsed_documents
          SET id = input.id,
              amount = <extraction>
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | amount_type       | extraction                                          | error                                                           |
      | 1            | I64               | JSON_VALUE(input.doc, '$.amount' AS I64)            | SET field 'amount' may be null but the output field is required |
      | 3            | I64               | JSON_VALUE(input.doc, '$.amount' AS I64)            | SET field 'amount' may be null but the output field is required |
      | 1            | I64 OPTIONAL      | JSON_VALUE(input.secret_doc, '$.amount' AS I64)     | would store sensitive data in a non-sensitive output field      |
      | 3            | I64 OPTIONAL      | TRY_JSON_VALUE(input.secret_doc, '$.amount' AS I64) | would store sensitive data in a non-sensitive output field      |
      | 1            | STRING OPTIONAL   | JSON_VALUE(input.doc, '$.amount' AS I64)            | has expression type Int64, expected declared output type Utf8   |
      | 3            | STRING OPTIONAL   | JSON_VALUE(input.doc, '$.amount' AS I64)            | has expression type Int64, expected declared output type Utf8   |
      | 1            | DATETIME OPTIONAL | JSON_VALUE(input.doc, '$.amount' AS DATETIME)       | JSON_VALUE cannot read DATETIME                                 |
      | 3            | BYTES OPTIONAL    | JSON_VALUE(input.doc, '$.amount' AS BYTES)          | JSON_VALUE cannot read BYTES                                    |
      | 1            | I64 OPTIONAL      | JSON_VALUE(input.number, '$.amount' AS I64)         | JSON_VALUE document must be STRING, found Int64                 |
      | 3            | BOOL              | JSON_EXISTS(input.number, '$.amount')               | JSON_EXISTS document must be STRING, found Int64                |
      | 1            | I64 OPTIONAL      | JSON_VALUE(input.doc, '$.items[-1]' AS I64)         | invalid JSON path '$.items[-1]'                                 |
      | 3            | I64 OPTIONAL      | JSON_VALUE(input.doc, 'amount' AS I64)              | invalid JSON path 'amount'                                      |
