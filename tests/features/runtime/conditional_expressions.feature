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
      CREATE WIRE JSON SCHEMA calculation_wire MODE STRICT (
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
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING calculation_codec
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
      CREATE WIRE JSON SCHEMA conditional_sample_wire MODE STRICT (
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
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING conditional_sample_codec
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
      CREATE WIRE JSON SCHEMA conditional_input_wire MODE STRICT (
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
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING conditional_input_codec
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

  Scenario Outline: Nested conditions evaluate conversions and patterns only for the messages that select them
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA routed_text (
        id STRING,
        kind STRING,
        raw STRING,
        pattern STRING
      );
      CREATE SCHEMA routed_result (
        id STRING,
        kind STRING,
        raw STRING,
        pattern STRING,
        result I64
      );
      CREATE SCHEMA routed_text_error (
        source_id STRING,
        error_message STRING
      );
      CREATE WIRE JSON SCHEMA routed_text_wire MODE STRICT (
        id string,
        kind string,
        raw string,
        pattern string
      );
      CREATE CODEC routed_text_codec
        FROM WIRE JSON SCHEMA routed_text_wire
        TO SCHEMA routed_text;
      CREATE RELAY routed_texts SCHEMA routed_text UNBRANCHED;
      CREATE RELAY routed_results SCHEMA routed_result UNBRANCHED;
      CREATE RELAY routed_text_errors SCHEMA routed_text_error UNBRANCHED;
      CREATE VHOST edge nested-arms-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/routed-texts' TYPE HTTP;
      CREATE INGESTOR routed_text_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING routed_text_codec
        TO routed_texts
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION route_texts
        FROM routed_texts
        UNBRANCHED
        TO routed_results
          INHERIT ALL
          SET result = CASE
            WHEN input.kind = 'number' THEN (input.raw AS I64) * 2
            WHEN input.kind = 'pattern' THEN IF regexp_like(input.raw, input.pattern) THEN 1 ELSE -1 END
            ELSE 0
          END
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO routed_text_errors
            SET source_id = input.id,
                error_message = error.message;
      CREATE SUBSCRIPTION routed_results_subscription TO routed_results;
      CREATE SUBSCRIPTION routed_text_errors_subscription TO routed_text_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "nested-arms-{{test_id}}.example.com" path "/routed-texts"
      """
      {"id":"doubled","kind":"number","raw":"21","pattern":"("}
      """
    And http payload is posted to node "node-1" with host "nested-arms-{{test_id}}.example.com" path "/routed-texts"
      """
      {"id":"matched","kind":"pattern","raw":"abc","pattern":"^a"}
      """
    And http payload is posted to node "node-1" with host "nested-arms-{{test_id}}.example.com" path "/routed-texts"
      """
      {"id":"unmatched","kind":"pattern","raw":"xyz","pattern":"^a"}
      """
    And http payload is posted to node "node-1" with host "nested-arms-{{test_id}}.example.com" path "/routed-texts"
      """
      {"id":"other","kind":"other","raw":"not-a-number","pattern":"("}
      """
    And http payload is posted to node "node-1" with host "nested-arms-{{test_id}}.example.com" path "/routed-texts"
      """
      {"id":"unparsable","kind":"number","raw":"x","pattern":"^a"}
      """
    And http payload is posted to node "node-1" with host "nested-arms-{{test_id}}.example.com" path "/routed-texts"
      """
      {"id":"broken","kind":"pattern","raw":"abc","pattern":"("}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"doubled" | "result":42
      "id":"matched" | "result":1
      "id":"unmatched" | "result":-1
      "id":"other" | "result":0
      "source_id":"unparsable" | cannot cast value to Int64
      "source_id":"broken" | invalid regular expression | unclosed group
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: A CASE arm invokes an eligible UDF only for the messages that select it
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE UDF supported
        WITH ROTO_0_13
        ARGS (text STRING)
        RETURNS BOOL
        CODE $roto$
          fn supported(text: StringColumn) -> BoolColumn {
              let out = ColumnBuilder.bool(text.len());
              let i = 0;
              while i < text.len() {
                  match text.get(i) {
                      Some(value) => push_verdict(out, value),
                      None => out.push_null(),
                  }
                  i += 1;
              }
              out.finish()
          }

          fn push_verdict(out: BoolColumnBuilder, value: String) {
              if value.bytes().len() > 8 {
                  out.push_null()
              } else {
                  out.push(true)
              }
          }
        $roto$;
      CREATE SCHEMA support_request (
        id STRING,
        kind STRING,
        text STRING
      );
      CREATE SCHEMA support_decision (
        id STRING,
        kind STRING,
        text STRING,
        supported BOOL
      );
      CREATE WIRE JSON SCHEMA support_request_wire MODE STRICT (
        id string,
        kind string,
        text string
      );
      CREATE CODEC support_request_codec
        FROM WIRE JSON SCHEMA support_request_wire
        TO SCHEMA support_request;
      CREATE RELAY support_requests SCHEMA support_request UNBRANCHED;
      CREATE RELAY support_decisions SCHEMA support_decision UNBRANCHED;
      CREATE VHOST edge guarded-udf-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/support-requests' TYPE HTTP;
      CREATE INGESTOR support_request_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING support_request_codec
        TO support_requests
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION decide_support
        FROM support_requests
        UNBRANCHED
        TO support_decisions
          INHERIT ALL
          SET supported = CASE
            WHEN input.kind = 'check' THEN udf::supported(input.text)
            ELSE false
          END
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION support_decisions_subscription TO support_decisions;
      START;
      """
    And http payload is posted to node "node-1" with host "guarded-udf-{{test_id}}.example.com" path "/support-requests"
      """
      {"id":"checked","kind":"check","text":"short"}
      """
    And http payload is posted to node "node-1" with host "guarded-udf-{{test_id}}.example.com" path "/support-requests"
      """
      {"id":"skipped","kind":"skip","text":"far too long for the function"}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"checked" | "supported":true
      "id":"skipped" | "supported":false
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: A CASE arm reads ingestion headers only for the messages that select it and a read outside it answers every message
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA tagged_note (
        id STRING,
        kind STRING
      );
      CREATE SCHEMA routed_note (
        id STRING,
        kind STRING,
        route STRING,
        echoed STRING
      );
      CREATE WIRE JSON SCHEMA tagged_note_wire MODE STRICT (
        id string,
        kind string
      );
      CREATE CODEC tagged_note_codec
        FROM WIRE JSON SCHEMA tagged_note_wire
        TO SCHEMA tagged_note;
      CREATE RELAY routed_notes SCHEMA routed_note UNBRANCHED;
      CREATE VHOST edge header-arms-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/notes' TYPE HTTP;
      CREATE INGESTOR note_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING tagged_note_codec
        TO routed_notes
          SET id = input.id,
              kind = input.kind,
              route = CASE
                WHEN input.kind = 'routed' THEN coalesce(read_header('route'), 'absent')
                ELSE 'direct'
              END,
              echoed = coalesce(
                CASE WHEN input.kind = 'routed' THEN read_header('route') END,
                read_header('route'),
                'absent'
              )
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION routed_notes_subscription TO routed_notes;
      START;
      """
    And http payload is posted to node "node-1" with host "header-arms-{{test_id}}.example.com" path "/notes" and header "route" value "alpha"
      """
      {"id":"first","kind":"routed"}
      """
    And http payload is posted to node "node-1" with host "header-arms-{{test_id}}.example.com" path "/notes" and header "route" value "beta"
      """
      {"id":"second","kind":"plain"}
      """
    And http payload is posted to node "node-1" with host "header-arms-{{test_id}}.example.com" path "/notes" and header "route" value "gamma"
      """
      {"id":"third","kind":"routed"}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"first" | "route":"alpha" | "echoed":"alpha"
      "id":"second" | "route":"direct" | "echoed":"beta"
      "id":"third" | "route":"gamma" | "echoed":"gamma"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
