Feature: Numeric classification, math and bit functions
  Scenario Outline: Float classification, sign, truncation, precision rounding and trigonometry fail only the messages whose results are not finite
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA reading (
        id STRING,
        text STRING,
        value F64,
        digits I16,
        y F64,
        x F64,
        magnitude F64
      );
      CREATE SCHEMA derived_reading (
        id STRING,
        is_not_a_number BOOL,
        is_finite_number BOOL,
        is_infinite_number BOOL,
        literal_finite BOOL,
        signed F64,
        truncated F64,
        rounded F64,
        sine F64,
        angle F64,
        binary_log F64
      );
      CREATE SCHEMA reading_error (
        input_id STRING,
        error_message STRING
      );
      CREATE CODEC reading_batch_codec
        FROM JSON
        TO SCHEMA reading
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY readings SCHEMA reading UNBRANCHED;
      CREATE RELAY derived_readings SCHEMA derived_reading UNBRANCHED;
      CREATE RELAY reading_errors SCHEMA reading_error UNBRANCHED;
      CREATE VHOST edge float-functions-{{test_id}}.example.com;
      CREATE ENDPOINT reading_ingress ON edge PATH '/readings' TYPE HTTP;
      CREATE INGESTOR reading_source
        FROM ENDPOINT reading_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_batch_codec
        TO readings
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION derive_readings
        FROM readings
        UNBRANCHED
        TO derived_readings
          SET id = input.id,
              is_not_a_number = is_nan(input.text AS F64),
              is_finite_number = is_finite(input.text AS F64),
              is_infinite_number = is_infinite(input.text AS F64),
              literal_finite = is_finite(1.5),
              signed = sign(input.value),
              truncated = trunc(input.value),
              rounded = round(input.value, input.digits),
              sine = sin(radians(input.y * 90.0)),
              angle = degrees(atan2(input.y, input.x)),
              binary_log = log2(input.magnitude)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO reading_errors
          SET input_id = input.id,
              error_message = error.message;
      CREATE SUBSCRIPTION derived_readings_subscription TO derived_readings;
      CREATE SUBSCRIPTION reading_errors_subscription TO reading_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "float-functions-{{test_id}}.example.com" path "/readings"
      """
      [{"id":"exact-tie","text":"inf","value":0.125,"digits":2,"y":0.0,"x":-1.0,"magnitude":1024.0},{"id":"no-logarithm","text":"1","value":1.0,"digits":0,"y":0.0,"x":1.0,"magnitude":0.0},{"id":"stored-below-tie","text":"-0.0","value":-2.675,"digits":2,"y":1.0,"x":0.0,"magnitude":0.5},{"id":"overflowing-round","text":"1","value":1.7976931348623157e308,"digits":-308,"y":0.0,"x":1.0,"magnitude":1.0},{"id":"tens","text":"nan","value":1250.0,"digits":-2,"y":-1.0,"x":-1.0,"magnitude":1.0}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"exact-tie" | "is_not_a_number":false | "is_finite_number":false | "is_infinite_number":true | "literal_finite":true | "signed":1.0 | "truncated":0.0 | "rounded":0.13 | "sine":0.0 | "angle":180.0 | "binary_log":10.0
      "id":"stored-below-tie" | "is_not_a_number":false | "is_finite_number":true | "is_infinite_number":false | "signed":-1.0 | "truncated":-2.0 | "rounded":-2.67 | "sine":1.0 | "angle":90.0 | "binary_log":-1.0
      "id":"tens" | "is_not_a_number":true | "is_finite_number":false | "is_infinite_number":false | "signed":1.0 | "truncated":1250.0 | "rounded":1300.0 | "sine":-1.0 | "angle":-135.0 | "binary_log":0.0
      "input_id":"no-logarithm" | invalid_argument: log2 produced a non-finite result
      "input_id":"overflowing-round" | invalid_argument: round produced a non-finite result
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Integer bit operations, shifts and rounding to tens fail only the messages whose counts or results do not fit
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA register_sample (
        id STRING,
        flags U16,
        mask U16,
        value I32,
        left_count I64,
        right_count I64
      );
      CREATE SCHEMA decoded_register (
        id STRING,
        masked U16,
        merged U16,
        toggled U16,
        inverted U16,
        ones I64,
        literal_masked I64,
        doubled I32,
        halved I32,
        rounded I32
      );
      CREATE SCHEMA register_error (
        input_id STRING,
        error_message STRING
      );
      CREATE CODEC register_batch_codec
        FROM JSON
        TO SCHEMA register_sample
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY register_samples SCHEMA register_sample UNBRANCHED;
      CREATE RELAY decoded_registers SCHEMA decoded_register UNBRANCHED;
      CREATE RELAY register_errors SCHEMA register_error UNBRANCHED;
      CREATE VHOST edge bit-functions-{{test_id}}.example.com;
      CREATE ENDPOINT register_ingress ON edge PATH '/registers' TYPE HTTP;
      CREATE INGESTOR register_source
        FROM ENDPOINT register_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING register_batch_codec
        TO register_samples
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION decode_registers
        FROM register_samples
        UNBRANCHED
        TO decoded_registers
          SET id = input.id,
              masked = bitwise_and(input.flags, input.mask),
              merged = bitwise_or(input.flags, input.mask),
              toggled = bitwise_xor(input.flags, input.mask),
              inverted = bitwise_not(input.flags),
              ones = bit_count(input.flags),
              literal_masked = bitwise_and(61680, 65280),
              doubled = shift_left(input.value, input.left_count),
              halved = shift_right(input.value, input.right_count),
              rounded = round(input.value, -1)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO register_errors
          SET input_id = input.id,
              error_message = error.message;
      CREATE SUBSCRIPTION decoded_registers_subscription TO decoded_registers;
      CREATE SUBSCRIPTION register_errors_subscription TO register_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "bit-functions-{{test_id}}.example.com" path "/registers"
      """
      [{"id":"masks","flags":61680,"mask":65280,"value":-5,"left_count":1,"right_count":1},{"id":"negative-right-count","flags":1,"mask":1,"value":1,"left_count":0,"right_count":-1},{"id":"sign-fill","flags":0,"mask":65535,"value":-7,"left_count":0,"right_count":100},{"id":"overflowing-shift","flags":1,"mask":1,"value":1073741824,"left_count":2,"right_count":0},{"id":"shifted-out-zero","flags":65535,"mask":0,"value":0,"left_count":1000,"right_count":1000},{"id":"overflowing-round","flags":1,"mask":1,"value":2147483647,"left_count":0,"right_count":0}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"masks" | "masked":61440 | "merged":65520 | "toggled":4080 | "inverted":3855 | "ones":8 | "literal_masked":61440 | "doubled":-10 | "halved":-3 | "rounded":-10
      "id":"sign-fill" | "masked":0 | "merged":65535 | "toggled":65535 | "inverted":65535 | "ones":0 | "doubled":-7 | "halved":-1 | "rounded":-10
      "id":"shifted-out-zero" | "masked":0 | "merged":65535 | "inverted":0 | "ones":16 | "doubled":0 | "halved":0 | "rounded":0
      "input_id":"negative-right-count" | invalid_argument: integer right shift by a negative count
      "input_id":"overflowing-shift" | overflow: integer left shift overflowed
      "input_id":"overflowing-round" | overflow: integer rounding overflowed
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Numeric classification and bit functions reject operands outside their signatures when the statement is applied
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands fail with "<message>"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA register_sample (
        id STRING,
        flags U16,
        value I32,
        ratio F64
      );
      CREATE SCHEMA derived_sample (
        id STRING,
        derived <result_type>
      );
      CREATE RELAY register_samples SCHEMA register_sample UNBRANCHED;
      CREATE RELAY derived_samples SCHEMA derived_sample UNBRANCHED;
      CREATE JUNCTION derive_samples
        FROM register_samples
        UNBRANCHED
        TO derived_samples
          SET id = input.id,
              derived = <expression>
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | result_type | expression                            | message                                                |
      | 1            | BOOL        | is_nan(input.value)                   | function 'is_nan' requires floating-point input        |
      | 3            | BOOL        | is_nan(input.value)                   | function 'is_nan' requires floating-point input        |
      | 1            | U16         | bitwise_and(input.flags, input.value) | function 'bitwise_and' requires matching operand types |
      | 3            | U16         | bitwise_and(input.flags, input.value) | function 'bitwise_and' requires matching operand types |
      | 1            | I32         | shift_left(input.value, input.ratio)  | function 'shift_left' requires integer input           |
      | 3            | I32         | shift_left(input.value, input.ratio)  | function 'shift_left' requires integer input           |
      | 1            | F64         | round(input.ratio, input.ratio)       | function 'round' requires integer input                |
      | 3            | F64         | round(input.ratio, input.ratio)       | function 'round' requires integer input                |
