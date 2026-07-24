Feature: Conditional expressions
  Scenario Outline: CASE observes errors only from the selected result
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA calculation (
        id STRING,
        divisor I64,
        result I64 OPTIONAL
      );
      CREATE SCHEMA calculation_error (
        source_id STRING,
        error_code STRING
      );
      CREATE STRICT WIRE JSON SCHEMA calculation_wire (
        id string,
        divisor integer,
        result integer OPTIONAL
      );
      CREATE CODEC calculation_codec
        FROM WIRE JSON SCHEMA calculation_wire
        TO SCHEMA calculation;
      CREATE RELAY calculations SCHEMA calculation UNBRANCHED;
      CREATE RELAY calculated SCHEMA calculation UNBRANCHED;
      CREATE RELAY calculation_errors SCHEMA calculation_error UNBRANCHED;
      CREATE VHOST edge conditional-expression-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/calculations' TYPE HTTP;
      CREATE INGESTOR calculation_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING calculation_codec
        TO calculations
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION calculate
        FROM calculations
        UNBRANCHED
        TO calculated
          INHERIT ALL
          SET result = CASE
            WHEN input.divisor != 0 THEN 10 / input.divisor
            ELSE 0
          END
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO calculation_errors
            SET source_id = input.id,
                error_code = error.code;
      CREATE SUBSCRIPTION calculated_subscription TO calculated;
      CREATE SUBSCRIPTION calculation_errors_subscription TO calculation_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "conditional-expression-{{test_id}}.example.com" path "/calculations"
      """
      {"id":"zero","divisor":0}
      """
    And http payload is posted to node "node-1" with host "conditional-expression-{{test_id}}.example.com" path "/calculations"
      """
      {"id":"two","divisor":2}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      "divisor":0 | "id":"zero" | "result":0
      "divisor":2 | "id":"two" | "result":5
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: CASE composes with window aggregate expressions
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA conditional_sample (
        value I64
      );
      CREATE SCHEMA conditional_summary (
        result I64
      );
      CREATE STRICT WIRE JSON SCHEMA conditional_sample_wire (
        value integer
      );
      CREATE CODEC conditional_sample_codec
        FROM WIRE JSON SCHEMA conditional_sample_wire
        TO SCHEMA conditional_sample;
      CREATE RELAY conditional_samples SCHEMA conditional_sample UNBRANCHED;
      CREATE RELAY conditional_summaries SCHEMA conditional_summary UNBRANCHED;
      CREATE VHOST edge conditional-window-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/conditional-window' TYPE HTTP;
      CREATE INGESTOR conditional_sample_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING conditional_sample_codec
        TO conditional_samples
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR conditional_window
        FROM conditional_samples
        WIDTH 2 MESSAGES
        STEP 2 MESSAGES
        UNBRANCHED
        TO conditional_summaries
          SET result = CASE
            WHEN COUNT(input.value) > 1 THEN SUM(input.value)
            ELSE 0
          END
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION conditional_summaries_subscription TO conditional_summaries;
      START;
      """
    And http payload is posted to node "node-1" with host "conditional-window-{{test_id}}.example.com" path "/conditional-window"
      """
      {"value":4}
      """
    And http payload is posted to node "node-1" with host "conditional-window-{{test_id}}.example.com" path "/conditional-window"
      """
      {"value":6}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "result":10
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Conditional forms preserve order nulls and composition
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA conditional_input (
        id STRING,
        kind STRING OPTIONAL,
        active BOOL,
        score I64
      );
      CREATE SCHEMA conditional_output (
        id STRING,
        kind STRING OPTIONAL,
        active BOOL,
        score I64,
        simple_result I64,
        if_result I64,
        first_result I64,
        maybe_result I64 OPTIONAL
      );
      CREATE STRICT WIRE JSON SCHEMA conditional_input_wire (
        id string,
        kind string OPTIONAL,
        active boolean,
        score integer
      );
      CREATE CODEC conditional_input_codec
        FROM WIRE JSON SCHEMA conditional_input_wire
        TO SCHEMA conditional_input;
      CREATE RELAY conditional_inputs SCHEMA conditional_input UNBRANCHED;
      CREATE RELAY conditional_outputs SCHEMA conditional_output UNBRANCHED;
      CREATE VHOST edge conditional-forms-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/conditional-forms' TYPE HTTP;
      CREATE INGESTOR conditional_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING conditional_input_codec
        TO conditional_inputs
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION project_conditionals
        FROM conditional_inputs
        UNBRANCHED
        TO conditional_outputs
          INHERIT ALL
          SET simple_result = CASE input.kind
                WHEN "a" THEN 1
                WHEN "b" THEN 2
                ELSE 3
              END,
              if_result = IF input.score > 0 THEN input.score ELSE 0 END,
              first_result = CASE
                WHEN input.score >= 0 THEN 10
                WHEN input.score > 0 THEN 20
                ELSE 30
              END,
              maybe_result = CASE
                WHEN input.kind = "a" THEN 1
              END
          WHERE IF input.active THEN TRUE ELSE FALSE END
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION conditional_outputs_subscription TO conditional_outputs;
      START;
      """
    And http payload is posted to node "node-1" with host "conditional-forms-{{test_id}}.example.com" path "/conditional-forms"
      """
      {"id":"null-kind","active":true,"score":5}
      """
    And http payload is posted to node "node-1" with host "conditional-forms-{{test_id}}.example.com" path "/conditional-forms"
      """
      {"id":"filtered","kind":"b","active":false,"score":1}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      "first_result":10 | "id":"null-kind" | "if_result":5 | "simple_result":3
      """
    And the last relay subscription payload does not contain "maybe_result"
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
