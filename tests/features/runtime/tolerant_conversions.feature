Feature: Tolerant conversions
  Scenario Outline: TRY_CAST yields a typed null where AS fails the message
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA raw_reading (
        id STRING,
        amount STRING OPTIONAL,
        level I64,
        observed STRING
      );
      CREATE SCHEMA tolerant_reading (
        id STRING,
        amount I64 OPTIONAL,
        amount_or_default I64,
        amount_state STRING,
        level U8 OPTIONAL,
        level_converted BOOL,
        observed_at DATETIME OPTIONAL,
        observed_converted BOOL
      );
      CREATE SCHEMA strict_reading (
        id STRING,
        route STRING,
        strict_amount I64 OPTIONAL
      );
      CREATE SCHEMA reading_error (
        input_id STRING,
        error_message STRING
      );
      CREATE CODEC raw_reading_batch_codec
        FROM JSON
        TO SCHEMA raw_reading
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY raw_readings SCHEMA raw_reading UNBRANCHED;
      CREATE RELAY tolerant_readings SCHEMA tolerant_reading UNBRANCHED;
      CREATE RELAY strict_readings SCHEMA strict_reading UNBRANCHED;
      CREATE RELAY reading_errors SCHEMA reading_error UNBRANCHED;
      CREATE VHOST edge tolerant-conversions-{{test_id}}.example.com;
      CREATE ENDPOINT reading_ingress ON edge PATH '/readings' TYPE HTTP;
      CREATE INGESTOR reading_source
        FROM ENDPOINT reading_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING raw_reading_batch_codec
        TO raw_readings
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION convert_readings
        FROM raw_readings
        UNBRANCHED
        TO tolerant_readings
          SET id = input.id,
              amount = TRY_CAST(input.amount AS I64),
              amount_or_default = coalesce(TRY_CAST(input.amount AS I64), -1),
              amount_state = CASE
                WHEN is_null(input.amount) THEN 'missing'
                WHEN is_null(TRY_CAST(input.amount AS I64)) THEN 'malformed'
                ELSE 'converted'
              END,
              level = TRY_CAST(input.level AS U8),
              level_converted = NOT is_null(TRY_CAST(input.level AS U8)),
              observed_at = TRY_CAST(input.observed AS DATETIME),
              observed_converted = NOT is_null(TRY_CAST(input.observed AS DATETIME))
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        TO strict_readings
          SET id = input.id,
              route = 'strict',
              strict_amount = input.amount AS I64
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO reading_errors
          SET input_id = input.id,
              error_message = error.message;
      CREATE SUBSCRIPTION tolerant_readings_subscription TO tolerant_readings;
      CREATE SUBSCRIPTION strict_readings_subscription TO strict_readings;
      CREATE SUBSCRIPTION reading_errors_subscription TO reading_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "tolerant-conversions-{{test_id}}.example.com" path "/readings"
      """
      [{"id":"converted","amount":"42","level":255,"observed":"2024-02-29T12:00:00Z"},{"id":"missing","level":256,"observed":"2262-04-12T00:00:00Z"},{"id":"malformed","amount":"4x2","level":-1,"observed":"yesterday"},{"id":"out-of-range","amount":"9223372036854775808","level":0,"observed":"1677-09-21T00:12:43.145224192Z"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"converted" | "amount":42 | "amount_or_default":42 | "amount_state":"converted" | "level":255 | "level_converted":true | "observed_at":"2024-02-29T12:00:00+00:00" | "observed_converted":true
      "id":"missing" | "amount_or_default":-1 | "amount_state":"missing" | "level_converted":false | "observed_converted":false
      "id":"malformed" | "amount_or_default":-1 | "amount_state":"malformed" | "level_converted":false | "observed_converted":false
      "id":"out-of-range" | "amount_or_default":-1 | "amount_state":"malformed" | "level":0 | "level_converted":true | "observed_at":"1677-09-21T00:12:43.145224192+00:00" | "observed_converted":true
      "id":"converted" | "route":"strict" | "strict_amount":42
      "id":"missing" | "route":"strict"
      "input_id":"malformed" | cast_failed: cannot cast value to Int64
      "input_id":"out-of-range" | cast_failed: cannot cast value to Int64
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: TRY_CAST suppresses only the failure of the conversion it performs
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE UDF add_one
        WITH ROTO_0_13
        ARGS (value I64)
        RETURNS I64
        CODE $roto$
          fn add_one(value: I64Column) -> I64Column {
              value.add_s(1)
          }
        $roto$;
      CREATE SCHEMA conversion_input (
        id STRING,
        raw STRING,
        divisor I64,
        value I64
      );
      CREATE SCHEMA conversion_output (
        id STRING,
        quotient STRING OPTIONAL,
        reparsed STRING OPTIONAL,
        narrowed U8 OPTIONAL,
        incremented STRING OPTIONAL
      );
      CREATE SCHEMA conversion_error (
        input_id STRING,
        error_message STRING
      );
      CREATE CODEC conversion_input_batch_codec
        FROM JSON
        TO SCHEMA conversion_input
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY conversion_inputs SCHEMA conversion_input UNBRANCHED;
      CREATE RELAY conversion_outputs SCHEMA conversion_output UNBRANCHED;
      CREATE RELAY conversion_errors SCHEMA conversion_error UNBRANCHED;
      CREATE VHOST edge owned-conversion-{{test_id}}.example.com;
      CREATE ENDPOINT conversion_ingress ON edge PATH '/conversions' TYPE HTTP;
      CREATE INGESTOR conversion_source
        FROM ENDPOINT conversion_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING conversion_input_batch_codec
        TO conversion_inputs
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION convert
        FROM conversion_inputs
        UNBRANCHED
        TO conversion_outputs
          SET id = input.id,
              quotient = TRY_CAST(100 / input.divisor AS STRING),
              reparsed = TRY_CAST(input.raw AS I64 AS STRING),
              narrowed = TRY_CAST(input.raw AS I64) AS U8,
              incremented = TRY_CAST(udf::add_one(input.value) AS STRING)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO conversion_errors
          SET input_id = input.id,
              error_message = error.message;
      CREATE SUBSCRIPTION conversion_outputs_subscription TO conversion_outputs;
      CREATE SUBSCRIPTION conversion_errors_subscription TO conversion_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "owned-conversion-{{test_id}}.example.com" path "/conversions"
      """
      [{"id":"clean","raw":"7","divisor":4,"value":1},{"id":"zero-divisor","raw":"7","divisor":0,"value":1},{"id":"unparsable","raw":"x","divisor":4,"value":1},{"id":"too-wide","raw":"300","divisor":4,"value":1},{"id":"udf-overflow","raw":"7","divisor":4,"value":9223372036854775807}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"clean" | "quotient":"25" | "reparsed":"7" | "narrowed":7 | "incremented":"2"
      "input_id":"zero-divisor" | division_by_zero: integer division by zero
      "input_id":"unparsable" | cast_failed: cannot cast value to Int64
      "input_id":"too-wide" | cast_failed: cannot cast value to UInt8
      "input_id":"udf-overflow" | overflow: UDF 'add_one'
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: A conditional arm converts only the messages that select it
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA routed_value (
        id STRING,
        kind STRING,
        raw STRING
      );
      CREATE SCHEMA routed_number (
        id STRING,
        number I64 OPTIONAL,
        converted BOOL
      );
      CREATE SCHEMA routed_error (
        input_id STRING,
        error_message STRING
      );
      CREATE CODEC routed_value_batch_codec
        FROM JSON
        TO SCHEMA routed_value
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY routed_values SCHEMA routed_value UNBRANCHED;
      CREATE RELAY routed_numbers SCHEMA routed_number UNBRANCHED;
      CREATE RELAY routed_errors SCHEMA routed_error UNBRANCHED;
      CREATE VHOST edge conversion-arms-{{test_id}}.example.com;
      CREATE ENDPOINT routed_ingress ON edge PATH '/routed-values' TYPE HTTP;
      CREATE INGESTOR routed_value_source
        FROM ENDPOINT routed_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING routed_value_batch_codec
        TO routed_values
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION route_values
        FROM routed_values
        UNBRANCHED
        TO routed_numbers
          SET id = input.id,
              number = CASE
                WHEN input.kind = 'tolerant' THEN TRY_CAST(input.raw AS I64)
                WHEN input.kind = 'strict' THEN input.raw AS I64
                ELSE -1
              END,
              converted = NOT is_null(output.number)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO routed_errors
          SET input_id = input.id,
              error_message = error.message;
      CREATE SUBSCRIPTION routed_numbers_subscription TO routed_numbers;
      CREATE SUBSCRIPTION routed_errors_subscription TO routed_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "conversion-arms-{{test_id}}.example.com" path "/routed-values"
      """
      [{"id":"tolerant-valid","kind":"tolerant","raw":"12"},{"id":"tolerant-invalid","kind":"tolerant","raw":"x"},{"id":"strict-valid","kind":"strict","raw":"13"},{"id":"strict-invalid","kind":"strict","raw":"y"},{"id":"unselected","kind":"other","raw":"z"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"tolerant-valid" | "number":12 | "converted":true
      "id":"tolerant-invalid" | "converted":false
      "id":"strict-valid" | "number":13 | "converted":true
      "input_id":"strict-invalid" | cast_failed: cannot cast value to Int64
      "id":"unselected" | "number":-1 | "converted":true
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: A tolerant conversion keeps the type contract of the field it initializes
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands fail with "<error>"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA raw_amount (
        id STRING,
        raw STRING SENSITIVE
      );
      CREATE SCHEMA parsed_amount (
        id STRING,
        amount <amount_type>
      );
      CREATE RELAY raw_amounts SCHEMA raw_amount UNBRANCHED;
      CREATE RELAY parsed_amounts SCHEMA parsed_amount UNBRANCHED;
      CREATE JUNCTION parse_amounts
        FROM raw_amounts
        UNBRANCHED
        TO parsed_amounts
          SET id = input.id,
              amount = TRY_CAST(input.raw AS I64)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | amount_type     | error                                                           |
      | 1            | I64             | SET field 'amount' may be null but the output field is required |
      | 3            | I64             | SET field 'amount' may be null but the output field is required |
      | 1            | I64 OPTIONAL    | would store sensitive data in a non-sensitive output field      |
      | 3            | I64 OPTIONAL    | would store sensitive data in a non-sensitive output field      |
      | 1            | STRING OPTIONAL | has expression type Int64, expected declared output type Utf8   |
      | 3            | STRING OPTIONAL | has expression type Int64, expected declared output type Utf8   |

  Scenario Outline: A tolerant conversion prepares an inferencer input tensor
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA raw_measurement (
        id STRING,
        raw STRING
      );
      CREATE SCHEMA inferred_measurement (
        result F32
      );
      CREATE CODEC raw_measurement_batch_codec
        FROM JSON
        TO SCHEMA raw_measurement
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY raw_measurements SCHEMA raw_measurement UNBRANCHED;
      CREATE RELAY inferred_measurements SCHEMA inferred_measurement UNBRANCHED;
      CREATE VHOST edge infer-tolerant-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/measurements' TYPE HTTP;
      CREATE INGESTOR raw_measurement_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING raw_measurement_batch_codec
        TO raw_measurements
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INFERENCER convert_measurements FROM raw_measurements
        USING RESOURCE inference VERSION 1
        FILE 'models/scalar_identity.onnx'
        INPUTS {
          "value" <tensor_type>[] = coalesce(TRY_CAST(input.raw AS F32), -1.0 AS F32)
        }
        OUTPUT SCHEMA { "result" <tensor_type>[] }
        UNBRANCHED
        TO inferred_measurements
          SET result = result
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION inferred_measurements_subscription TO inferred_measurements;
      START;
      """
    And http payload is posted to node "node-1" with host "infer-tolerant-{{test_id}}.example.com" path "/measurements"
      """
      [{"id":"decimal","raw":"3.5"},{"id":"malformed","raw":"3,5"},{"id":"exponent","raw":"2e1"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      {"result":3.5}
      {"result":-1.0}
      {"result":20.0}
      """

    Examples:
      | cluster_size | replica_count | tensor_type       |
      | 1            | 0             | DENSE TENSOR<F32> |
      | 3            | 0             | DENSE TENSOR<F32> |

  Scenario Outline: A materialized-state default converts only deterministic values
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA reading (
        id STRING,
        amount I64 OPTIONAL
      );
      CREATE RELAY readings SCHEMA reading UNBRANCHED;
      CREATE RELAY latest_readings SCHEMA reading UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY enriched_readings SCHEMA reading UNBRANCHED;
      CREATE JUNCTION constant_default
        FROM readings
        UNBRANCHED
        USING MATERIALIZED STATE latest_readings DEFAULT {
          id = 'none',
          amount = TRY_CAST('42' AS I64)
        }
        TO enriched_readings
          SET id = input.id,
              amount = relay_state.latest_readings.amount
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "materialized-state DEFAULT for 'latest_readings' must use deterministic side-effect-free expressions"
      """
      CREATE JUNCTION generated_default
        FROM readings
        UNBRANCHED
        USING MATERIALIZED STATE latest_readings DEFAULT {
          id = 'none',
          amount = TRY_CAST(uuid_v4() AS I64)
        }
        TO enriched_readings
          SET id = input.id,
              amount = relay_state.latest_readings.amount
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
