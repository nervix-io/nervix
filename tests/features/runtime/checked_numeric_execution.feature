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
        remainder I64
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
              remainder = input.left % input.divisor
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
      "id":"ordinary" | "sum":12 | "remainder":1
      "id":"minimum-remainder" | "sum":-9223372036854775808 | "remainder":0
      "input_id":"zero-divisor" | division_by_zero: integer remainder by zero
      "input_id":"overflowing-sum" | overflow: integer addition overflowed
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
          BRANCHED BY by_operand_id
          SET left = input.left / input.divisor
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
