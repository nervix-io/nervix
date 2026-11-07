Feature: Wire schema list fields
  An AVRO wire schema declares the item type of every array, so an array binds exactly the elements
  its items name and the Avro schema Nervix writes carries that item type. JSON and CBOR arrays are
  bare: an element takes the representation its internal type has, and a DATETIME or BYTES element
  that travels as text is declared on the codec by the list field's ENCODE rule.

  @wire_schema_lists
  Scenario Outline: AVRO arrays carry exactly the elements their item types declare on <cluster_size> nodes
    Given HTTP receiver "avro_lists" is running
    And HTTP receiver "avro_lists" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA measurement (
        id STRING,
        counts <counts>,
        totals <totals>,
        matrix <matrix>,
        stamps <stamps>
      );
      CREATE WIRE AVRO SCHEMA measurement_wire MODE STRICT (
        id string,
        counts <counts_wire>,
        totals <totals_wire>,
        matrix <matrix_wire>,
        stamps <stamps_wire>
      );
      CREATE WIRE AVRO CODEC measurement_codec FROM WIRE SCHEMA measurement_wire TO SCHEMA measurement
        ENCODE stamps AS RFC3339;
      CREATE RELAY measurements SCHEMA measurement UNBRANCHED;
      CREATE VHOST edge avro-lists-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/measurements' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR measurement_source
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING measurement_codec
        TO measurements INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT avro_lists CONFIG {
        'endpoint' = '{{http_receiver.avro_lists}}', 'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST EMITTER measurement_sink FROM measurements
        TO avro_lists METHOD 'POST' PATH '/measurements'
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          ENCODE USING measurement_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION measurement_subscription TO measurements;
      START;
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW CREATE WIRE AVRO SCHEMA measurement_wire;
      """
    Then the last command output contains
      """
      CREATE WIRE AVRO SCHEMA measurement_wire MODE STRICT (
        id STRING,
        counts <counts_canonical>,
        totals <totals_canonical>,
        matrix <matrix_canonical>,
        stamps <stamps_canonical>
      );
      """
    When http payload encoded as "AVRO" is posted to host "avro-lists-{{test_id}}.example.com" path "/measurements"
      """
      {"id":"m1","counts":[1,-2,2147483647],"totals":[9007199254740993,-9223372036854775808],"matrix":[[1.5,2.0,-0.25],[4.0,5.5,6.0]],"stamps":["2024-02-29T12:34:56Z","1999-12-31T23:59:59.123456789+02:00"]}
      """
    Then within "10s" the relay subscription receives a payload
      """
      {"counts":[1,-2,2147483647],"id":"m1","matrix":[[1.5,2.0,-0.25],[4.0,5.5,6.0]],"stamps":["2024-02-29T12:34:56+00:00","1999-12-31T21:59:59.123456789+00:00"],"totals":[9007199254740993,-9223372036854775808]}
      """
    And HTTP receiver "avro_lists" eventually receives at least 1 requests
    And HTTP receiver "avro_lists" request 1 carries this "AVRO" payload
      """
      {"id":"m1","counts":[1,-2,2147483647],"totals":[9007199254740993,-9223372036854775808],"matrix":[[1.5,2.0,-0.25],[4.0,5.5,6.0]],"stamps":["2024-02-29T12:34:56+00:00","1999-12-31T21:59:59.123456789+00:00"]}
      """

    Examples:
      | cluster_size | counts   | totals   | matrix           | stamps        | counts_wire | totals_wire | matrix_wire          | stamps_wire   | counts_canonical | totals_canonical | matrix_canonical     | stamps_canonical |
      | 1            | VEC<I32> | VEC<I64> | ARRAY<F64, 2, 3> | VEC<DATETIME> | array<int>  | array<long> | array<array<double>> | array<string> | ARRAY<INT>       | ARRAY<LONG>      | ARRAY<ARRAY<DOUBLE>> | ARRAY<STRING>    |
      | 3            | VEC<I32> | VEC<I64> | ARRAY<F64, 2, 3> | VEC<DATETIME> | array<int>  | array<long> | array<array<double>> | array<string> | ARRAY<INT>       | ARRAY<LONG>      | ARRAY<ARRAY<DOUBLE>> | ARRAY<STRING>    |

  @wire_schema_lists
  Scenario Outline: JSON list elements bind through the codec's ENCODE rules on <cluster_size> nodes
    Given HTTP receiver "json_lists" is running
    And HTTP receiver "json_lists" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA capture (chunks <chunks>, id STRING, stamps <stamps>);
      CREATE WIRE JSON SCHEMA capture_wire MODE STRICT (chunks array, id string, stamps array);
      CREATE WIRE JSON CODEC capture_codec FROM WIRE SCHEMA capture_wire TO SCHEMA capture
        ENCODE chunks AS BASE64, stamps AS RFC3339;
      CREATE RELAY captures SCHEMA capture UNBRANCHED;
      CREATE VHOST edge json-lists-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/captures' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR capture_source
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING capture_codec
        TO captures INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT json_lists CONFIG {
        'endpoint' = '{{http_receiver.json_lists}}', 'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST EMITTER capture_sink FROM captures
        TO json_lists METHOD 'POST' PATH '/captures'
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          ENCODE USING capture_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION capture_subscription TO captures;
      START;
      """
    And http payload encoded as "JSON" is posted to host "json-lists-{{test_id}}.example.com" path "/captures"
      """
      {"chunks":["AP8=","3q2+7w=="],"id":"j1","stamps":["2024-02-29T12:34:56Z","1999-12-31T23:59:59.123456789+02:00"]}
      """
    Then within "10s" the relay subscription receives a payload
      """
      {"chunks":["AP8=","3q2+7w=="],"id":"j1","stamps":["2024-02-29T12:34:56+00:00","1999-12-31T21:59:59.123456789+00:00"]}
      """
    And HTTP receiver "json_lists" eventually receives at least 1 requests
    And HTTP receiver "json_lists" request 1 carries this "JSON" payload
      """
      {"chunks":["AP8=","3q2+7w=="],"id":"j1","stamps":["2024-02-29T12:34:56+00:00","1999-12-31T21:59:59.123456789+00:00"]}
      """

    Examples:
      | cluster_size | chunks     | stamps        |
      | 1            | VEC<BYTES> | VEC<DATETIME> |
      | 3            | VEC<BYTES> | VEC<DATETIME> |

  @wire_schema_lists
  Scenario Outline: CBOR lists carry byte strings natively and datetimes as declared text on <cluster_size> nodes
    Given HTTP receiver "cbor_lists" is running
    And HTTP receiver "cbor_lists" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA capture (chunks <chunks>, id STRING, stamps <stamps>);
      CREATE WIRE CBOR SCHEMA capture_wire MODE STRICT (chunks array, id text, stamps array);
      CREATE WIRE CBOR CODEC capture_codec FROM WIRE SCHEMA capture_wire TO SCHEMA capture
        ENCODE stamps AS RFC3339;
      CREATE RELAY captures SCHEMA capture UNBRANCHED;
      CREATE VHOST edge cbor-lists-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/captures' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR capture_source
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING capture_codec
        TO captures INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT cbor_lists CONFIG {
        'endpoint' = '{{http_receiver.cbor_lists}}', 'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST EMITTER capture_sink FROM captures
        TO cbor_lists METHOD 'POST' PATH '/captures'
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          ENCODE USING capture_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION capture_subscription TO captures;
      START;
      """
    And http CBOR payload with byte string elements in field "chunks" is posted to host "cbor-lists-{{test_id}}.example.com" path "/captures"
      """
      {"chunks":["AP8=","3q2+7w=="],"id":"c1","stamps":["2024-02-29T12:34:56Z","1999-12-31T23:59:59.123456789+02:00"]}
      """
    Then within "10s" the relay subscription receives a payload
      """
      {"chunks":["AP8=","3q2+7w=="],"id":"c1","stamps":["2024-02-29T12:34:56+00:00","1999-12-31T21:59:59.123456789+00:00"]}
      """
    And HTTP receiver "cbor_lists" eventually receives at least 1 requests
    And HTTP receiver "cbor_lists" request 1 carries this CBOR payload with byte string elements in field "chunks"
      """
      {"chunks":["AP8=","3q2+7w=="],"id":"c1","stamps":["2024-02-29T12:34:56+00:00","1999-12-31T21:59:59.123456789+00:00"]}
      """

    Examples:
      | cluster_size | chunks     | stamps        |
      | 1            | VEC<BYTES> | VEC<DATETIME> |
      | 3            | VEC<BYTES> | VEC<DATETIME> |

  @wire_schema_lists
  Scenario Outline: A codec refuses a <format> list field its wire type cannot carry exactly on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA sample (id STRING, <field> <internal>);
      CREATE WIRE <format> SCHEMA sample_wire MODE STRICT (id <text>, <field> <wire>);
      """
    When these Plexus commands fail with "<reason>"
      """
      CREATE WIRE <format> CODEC sample_codec FROM WIRE SCHEMA sample_wire TO SCHEMA sample
        <rules>;
      """

    Examples:
      | nodes | format | text   | field  | internal         | wire          | rules                   | reason                                                                                                               |
      | 1     | AVRO   | string | counts | VEC<I32>         | array<long>   |                         | avro field 'counts' type mismatch: wire array<long>, internal VEC<I32>                                               |
      | 3     | AVRO   | string | counts | VEC<I32>         | array<long>   |                         | avro field 'counts' type mismatch: wire array<long>, internal VEC<I32>                                               |
      | 1     | AVRO   | string | matrix | ARRAY<I64, 2, 3> | array<long>   |                         | avro field 'matrix' array depth mismatch: wire array<long> has 1 array level, internal ARRAY<ARRAY<I64, 3>, 2> has 2 |
      | 3     | AVRO   | string | matrix | ARRAY<I64, 2, 3> | array<long>   |                         | avro field 'matrix' array depth mismatch: wire array<long> has 1 array level, internal ARRAY<ARRAY<I64, 3>, 2> has 2 |
      | 1     | AVRO   | string | stamps | VEC<DATETIME>    | array<string> |                         | avro field 'stamps' needs ENCODE stamps AS RFC3339 for its DATETIME elements                                         |
      | 3     | AVRO   | string | stamps | VEC<DATETIME>    | array<string> |                         | avro field 'stamps' needs ENCODE stamps AS RFC3339 for its DATETIME elements                                         |
      | 1     | AVRO   | string | chunks | VEC<BYTES>       | array<bytes>  | ENCODE chunks AS BASE64 | avro field 'chunks' carries BYTES natively, so ENCODE chunks AS BASE64 does not apply                                |
      | 3     | AVRO   | string | chunks | VEC<BYTES>       | array<bytes>  | ENCODE chunks AS BASE64 | avro field 'chunks' carries BYTES natively, so ENCODE chunks AS BASE64 does not apply                                |
      | 1     | JSON   | string | stamps | VEC<DATETIME>    | array         |                         | json field 'stamps' needs ENCODE stamps AS RFC3339 for its DATETIME elements                                         |
      | 3     | JSON   | string | stamps | VEC<DATETIME>    | array         |                         | json field 'stamps' needs ENCODE stamps AS RFC3339 for its DATETIME elements                                         |
      | 1     | JSON   | string | chunks | VEC<BYTES>       | array         |                         | json field 'chunks' needs ENCODE chunks AS BASE64 for its BYTES elements                                             |
      | 3     | JSON   | string | chunks | VEC<BYTES>       | array         |                         | json field 'chunks' needs ENCODE chunks AS BASE64 for its BYTES elements                                             |
      | 1     | CBOR   | text   | stamps | VEC<DATETIME>    | array         |                         | cbor field 'stamps' needs ENCODE stamps AS RFC3339 for its DATETIME elements                                         |
      | 3     | CBOR   | text   | stamps | VEC<DATETIME>    | array         |                         | cbor field 'stamps' needs ENCODE stamps AS RFC3339 for its DATETIME elements                                         |
      | 1     | CBOR   | text   | chunks | VEC<BYTES>       | array         | ENCODE chunks AS BASE64 | cbor field 'chunks' carries BYTES natively, so ENCODE chunks AS BASE64 does not apply                                |
      | 3     | CBOR   | text   | chunks | VEC<BYTES>       | array         | ENCODE chunks AS BASE64 | cbor field 'chunks' carries BYTES natively, so ENCODE chunks AS BASE64 does not apply                                |
