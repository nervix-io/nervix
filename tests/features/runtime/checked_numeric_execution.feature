Feature: Checked numeric execution
  Scenario Outline: Integer arithmetic fails only the messages whose operands fail in a batch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA operand (
        id STRING,
        left I64,
        right I64,
        divisor I64
      );
      CREATE SCHEMA computed (
        id STRING,
        sum I64,
        remainder I64,
        constant_quotient I64,
        constant_remainder I64
      );
      CREATE SCHEMA numeric_error (
        input_id STRING,
        error_message STRING
      );
      CREATE CODEC operand_batch_codec
        FROM JSON
        TO SCHEMA operand
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY operands SCHEMA operand UNBRANCHED;
      CREATE RELAY computed_results SCHEMA computed UNBRANCHED;
      CREATE RELAY numeric_errors SCHEMA numeric_error UNBRANCHED;
      CREATE VHOST edge checked-integers-{{test_id}}.example.com;
      CREATE ENDPOINT operand_ingress ON edge PATH '/operands' TYPE HTTP;
      CREATE INGESTOR operand_source
        FROM ENDPOINT operand_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING operand_batch_codec
        TO operands
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION compute
        FROM operands
        UNBRANCHED
        TO computed_results
          SET id = input.id,
              sum = input.left + input.right,
              remainder = input.left % input.divisor,
              constant_quotient = input.left / 7,
              constant_remainder = input.left % -7
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO numeric_errors
          SET input_id = input.id,
              error_message = error.message;
      CREATE SUBSCRIPTION computed_results_subscription TO computed_results;
      CREATE SUBSCRIPTION numeric_errors_subscription TO numeric_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "checked-integers-{{test_id}}.example.com" path "/operands"
      """
      [{"id":"ordinary","left":7,"right":5,"divisor":2},{"id":"minimum-remainder","left":-9223372036854775808,"right":0,"divisor":-1},{"id":"zero-divisor","left":7,"right":1,"divisor":0},{"id":"overflowing-sum","left":9223372036854775807,"right":1,"divisor":3}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"ordinary" | "sum":12 | "remainder":1 | "constant_quotient":1 | "constant_remainder":0
      "id":"minimum-remainder" | "sum":-9223372036854775808 | "remainder":0 | "constant_quotient":-1317624576693539401 | "constant_remainder":-1
      "input_id":"zero-divisor" | division_by_zero: integer remainder by zero
      "input_id":"overflowing-sum" | overflow: integer addition overflowed
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Narrow integer arithmetic fails only the messages whose exact results leave their width
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA narrow_operand (
        id STRING,
        tiny_left I8,
        tiny_right I8,
        byte_left U8,
        byte_right U8,
        short_left I16 OPTIONAL,
        short_right I16,
        word_left U16,
        word_right U16,
        int_left I32,
        int_right I32,
        uint_left U32,
        uint_right U32
      );
      CREATE SCHEMA narrow_result (
        id STRING,
        tiny_sum I8,
        tiny_difference I8,
        tiny_product I8,
        tiny_tripled I8,
        byte_sum U8,
        byte_difference U8,
        byte_product U8,
        byte_remaining U8,
        short_sum I16 OPTIONAL,
        short_difference I16 OPTIONAL,
        short_product I16 OPTIONAL,
        short_sum_missing BOOL,
        word_sum U16,
        word_difference U16,
        word_product U16,
        int_sum I32,
        int_difference I32,
        int_product I32,
        uint_sum U32,
        uint_difference U32,
        uint_product U32
      );
      CREATE SCHEMA numeric_error (
        input_id STRING,
        error_message STRING
      );
      CREATE CODEC narrow_operand_batch_codec
        FROM JSON
        TO SCHEMA narrow_operand
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY narrow_operands SCHEMA narrow_operand UNBRANCHED;
      CREATE RELAY narrow_results SCHEMA narrow_result UNBRANCHED;
      CREATE RELAY numeric_errors SCHEMA numeric_error UNBRANCHED;
      CREATE VHOST edge checked-narrow-{{test_id}}.example.com;
      CREATE ENDPOINT narrow_operand_ingress ON edge PATH '/operands' TYPE HTTP;
      CREATE INGESTOR narrow_operand_source
        FROM ENDPOINT narrow_operand_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING narrow_operand_batch_codec
        TO narrow_operands
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION compute_narrow
        FROM narrow_operands
        UNBRANCHED
        TO narrow_results
          SET id = input.id,
              tiny_sum = input.tiny_left + input.tiny_right,
              tiny_difference = input.tiny_left - input.tiny_right,
              tiny_product = input.tiny_left * input.tiny_right,
              tiny_tripled = input.tiny_left * 3 AS I8,
              byte_sum = input.byte_left + input.byte_right,
              byte_difference = input.byte_left - input.byte_right,
              byte_product = input.byte_left * input.byte_right,
              byte_remaining = 200 AS U8 - input.byte_left,
              short_sum = input.short_left + input.short_right,
              short_difference = input.short_left - input.short_right,
              short_product = input.short_left * input.short_right,
              short_sum_missing = is_null(input.short_left + input.short_right),
              word_sum = input.word_left + input.word_right,
              word_difference = input.word_left - input.word_right,
              word_product = input.word_left * input.word_right,
              int_sum = input.int_left + input.int_right,
              int_difference = input.int_left - input.int_right,
              int_product = input.int_left * input.int_right,
              uint_sum = input.uint_left + input.uint_right,
              uint_difference = input.uint_left - input.uint_right,
              uint_product = input.uint_left * input.uint_right
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO numeric_errors
          SET input_id = input.id,
              error_message = error.message;
      CREATE SUBSCRIPTION narrow_results_subscription TO narrow_results;
      CREATE SUBSCRIPTION numeric_errors_subscription TO numeric_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "checked-narrow-{{test_id}}.example.com" path "/operands"
      """
      [{"id":"ordinary","tiny_left":7,"tiny_right":-3,"byte_left":9,"byte_right":4,"short_left":150,"short_right":-200,"word_left":300,"word_right":200,"int_left":70000,"int_right":-30000,"uint_left":100000,"uint_right":40000},{"id":"tiny-sum","tiny_left":1,"tiny_right":127,"byte_left":1,"byte_right":1,"short_left":1,"short_right":1,"word_left":1,"word_right":1,"int_left":1,"int_right":1,"uint_left":1,"uint_right":1},{"id":"byte-difference","tiny_left":1,"tiny_right":1,"byte_left":3,"byte_right":5,"short_left":1,"short_right":1,"word_left":1,"word_right":1,"int_left":1,"int_right":1,"uint_left":1,"uint_right":1},{"id":"short-product","tiny_left":1,"tiny_right":1,"byte_left":1,"byte_right":1,"short_left":182,"short_right":182,"word_left":1,"word_right":1,"int_left":1,"int_right":1,"uint_left":1,"uint_right":1},{"id":"word-sum","tiny_left":1,"tiny_right":1,"byte_left":1,"byte_right":1,"short_left":1,"short_right":1,"word_left":65535,"word_right":1,"int_left":1,"int_right":1,"uint_left":1,"uint_right":1},{"id":"int-difference","tiny_left":1,"tiny_right":1,"byte_left":1,"byte_right":1,"short_left":1,"short_right":1,"word_left":1,"word_right":1,"int_left":-2147483648,"int_right":1,"uint_left":1,"uint_right":1},{"id":"uint-product","tiny_left":1,"tiny_right":1,"byte_left":1,"byte_right":1,"short_left":1,"short_right":1,"word_left":1,"word_right":1,"int_left":1,"int_right":1,"uint_left":65536,"uint_right":65536},{"id":"null-short","tiny_left":1,"tiny_right":1,"byte_left":1,"byte_right":1,"short_right":32767,"word_left":1,"word_right":1,"int_left":1,"int_right":1,"uint_left":1,"uint_right":1},{"id":"tiny-tripled","tiny_left":43,"tiny_right":1,"byte_left":1,"byte_right":1,"short_left":1,"short_right":1,"word_left":1,"word_right":1,"int_left":1,"int_right":1,"uint_left":1,"uint_right":1},{"id":"byte-remaining","tiny_left":1,"tiny_right":1,"byte_left":201,"byte_right":1,"short_left":1,"short_right":1,"word_left":1,"word_right":1,"int_left":1,"int_right":1,"uint_left":1,"uint_right":1}]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "id":"ordinary" | "tiny_sum":4 | "tiny_difference":10 | "tiny_product":-21 | "tiny_tripled":21 | "byte_sum":13 | "byte_difference":5 | "byte_product":36 | "byte_remaining":191 | "short_sum":-50 | "short_difference":350 | "short_product":-30000 | "short_sum_missing":false | "word_sum":500 | "word_difference":100 | "word_product":60000 | "int_sum":40000 | "int_difference":100000 | "int_product":-2100000000 | "uint_sum":140000 | "uint_difference":60000 | "uint_product":4000000000
      "id":"null-short" | "short_sum_missing":true | "tiny_sum":2 | "byte_remaining":199 | "word_product":1 | "int_difference":0 | "uint_product":1
      "input_id":"tiny-sum" | overflow: integer addition overflowed
      "input_id":"byte-difference" | overflow: integer subtraction overflowed
      "input_id":"short-product" | overflow: integer multiplication overflowed
      "input_id":"word-sum" | overflow: integer addition overflowed
      "input_id":"int-difference" | overflow: integer subtraction overflowed
      "input_id":"uint-product" | overflow: integer multiplication overflowed
      "input_id":"tiny-tripled" | overflow: integer multiplication overflowed
      "input_id":"byte-remaining" | overflow: integer subtraction overflowed
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Floating-point arithmetic and math functions fail only non-finite messages in a batch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA measurement (
        id STRING,
        value F64,
        divisor F64,
        radicand F64
      );
      CREATE SCHEMA derived (
        id STRING,
        quotient F64,
        root F64,
        magnitude F64,
        rounded F64
      );
      CREATE SCHEMA numeric_error (
        input_id STRING,
        error_message STRING
      );
      CREATE CODEC measurement_batch_codec
        FROM JSON
        TO SCHEMA measurement
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY measurements SCHEMA measurement UNBRANCHED;
      CREATE RELAY derived_results SCHEMA derived UNBRANCHED;
      CREATE RELAY numeric_errors SCHEMA numeric_error UNBRANCHED;
      CREATE VHOST edge checked-floats-{{test_id}}.example.com;
      CREATE ENDPOINT measurement_ingress ON edge PATH '/measurements' TYPE HTTP;
      CREATE INGESTOR measurement_source
        FROM ENDPOINT measurement_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING measurement_batch_codec
        TO measurements
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION derive
        FROM measurements
        UNBRANCHED
        TO derived_results
          SET id = input.id,
              quotient = input.value / input.divisor,
              root = sqrt(input.radicand),
              magnitude = abs(input.value),
              rounded = round(input.value)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO numeric_errors
          SET input_id = input.id,
              error_message = error.message;
      CREATE SUBSCRIPTION derived_results_subscription TO derived_results;
      CREATE SUBSCRIPTION numeric_errors_subscription TO numeric_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "checked-floats-{{test_id}}.example.com" path "/measurements"
      """
      [{"id":"ordinary","value":2.25,"divisor":0.5,"radicand":2.25},{"id":"negative-root","value":1.0,"divisor":2.0,"radicand":-4.0},{"id":"zero-divisor","value":1.5,"divisor":0.0,"radicand":1.0},{"id":"half-away","value":-2.5,"divisor":-0.5,"radicand":0.0}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"ordinary" | "quotient":4.5 | "root":1.5 | "magnitude":2.25 | "rounded":2.0
      "input_id":"negative-root" | invalid_argument: sqrt produced a non-finite result
      "input_id":"zero-divisor" | invalid_argument: floating-point operation produced a non-finite result
      "id":"half-away" | "quotient":5.0 | "root":0.0 | "magnitude":2.5 | "rounded":-3.0
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Reingestor and emitter routes send numeric failures to their error relays
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA operand (
        id STRING,
        left I64,
        divisor I64
      );
      CREATE SCHEMA numeric_error (
        input_id STRING,
        error_message STRING
      );
      CREATE WIRE JSON SCHEMA operand_wire MODE STRICT (
        id string,
        left integer,
        divisor integer
      );
      CREATE CODEC operand_codec
        FROM WIRE JSON SCHEMA operand_wire
        TO SCHEMA operand;
      CREATE CODEC operand_batch_codec
        FROM JSON
        TO SCHEMA operand
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA operand_id_branch (
        id STRING
      );
      CREATE BRANCH by_operand_id SCHEMA operand_id_branch TTL 5m;
      CREATE RELAY operands SCHEMA operand UNBRANCHED;
      CREATE RELAY partitioned_operands SCHEMA operand BRANCHED BY by_operand_id;
      CREATE RELAY numeric_errors SCHEMA numeric_error UNBRANCHED;
      CREATE VHOST edge checked-routes-{{test_id}}.example.com;
      CREATE ENDPOINT operand_ingress ON edge PATH '/operands' TYPE HTTP;
      CREATE INGESTOR operand_source
        FROM ENDPOINT operand_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING operand_batch_codec
        TO operands
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE REINGESTOR partition_operands
        FROM operands
        TO partitioned_operands
          INHERIT ALL
          SET left = input.left / input.divisor
          BRANCHED BY by_operand_id
          SET id = message.id
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR SEND TO numeric_errors
          SET input_id = input.id,
              error_message = error.message;
      CREATE CLIENT zeromq_main
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };
      CREATE EMITTER operand_sink
        FROM operands
        TO ZEROMQ zeromq_main MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING operand_codec
        INHERIT ALL
        SET left = input.left % input.divisor
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR SEND TO numeric_errors
        SET input_id = input.id,
            error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION partitioned_operands_subscription TO partitioned_operands;
      CREATE SUBSCRIPTION numeric_errors_subscription TO numeric_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "checked-routes-{{test_id}}.example.com" path "/operands"
      """
      [{"id":"ordinary","left":7,"divisor":2},{"id":"zero-divisor","left":7,"divisor":0}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"ordinary" | "left":3
      "input_id":"zero-divisor" | reingestor 'partition_operands' FILTER-MAP side error division_by_zero: integer division by zero
      "input_id":"zero-divisor" | emitter 'operand_sink' FILTER-MAP side error division_by_zero: integer remainder by zero
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
