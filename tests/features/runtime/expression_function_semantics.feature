Feature: Expression function semantics
  Scenario Outline: Case conversion agrees across literal, column and repeated expressions
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA greeting (
        id STRING,
        raw STRING
      );
      CREATE SCHEMA normalized_greeting (
        id STRING,
        raw STRING,
        uppered STRING,
        uppered_again STRING,
        lowered STRING,
        literal_uppered STRING,
        agrees BOOL,
        aliases_agree BOOL
      );
      CREATE WIRE JSON SCHEMA greeting_wire MODE STRICT (
        id string,
        raw string
      );
      CREATE CODEC greeting_codec
        FROM WIRE JSON SCHEMA greeting_wire
        TO SCHEMA greeting;
      CREATE RELAY greetings SCHEMA greeting UNBRANCHED;
      CREATE RELAY normalized_greetings SCHEMA normalized_greeting UNBRANCHED;
      CREATE VHOST edge case-mapping-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/greetings' TYPE HTTP;
      CREATE INGESTOR greeting_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING greeting_codec
        TO greetings
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION normalize_greetings
        FROM greetings
        UNBRANCHED
        TO normalized_greetings
          INHERIT ALL
          SET uppered = upper(input.raw),
              uppered_again = upper(output.uppered),
              lowered = lower(input.raw),
              literal_uppered = upper('Grüßen'),
              agrees = output.uppered_again = upper('Grüßen'),
              aliases_agree = substr(input.raw, 1, 3) = substring(input.raw, 1, 3)
                AND length(input.raw) = char_length(input.raw)
          WHERE output.agrees AND output.aliases_agree
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION normalized_greetings_subscription TO normalized_greetings;
      START;
      """
    And http payload is posted to node "node-1" with host "case-mapping-{{test_id}}.example.com" path "/greetings"
      """
      {"id":"expansion","raw":"Grüßen"}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"expansion" | "uppered":"GRÜSSEN" | "uppered_again":"GRÜSSEN" | "lowered":"grüßen" | "literal_uppered":"GRÜSSEN" | "agrees":true | "aliases_agree":true
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: NULLIF uses the same equality as the comparison operator
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
        left_text STRING,
        right_text STRING
      );
      CREATE SCHEMA compared_measurement (
        id STRING,
        left_text STRING,
        right_text STRING,
        equal BOOL,
        nullified BOOL
      );
      CREATE WIRE JSON SCHEMA measurement_wire MODE STRICT (
        id string,
        left_text string,
        right_text string
      );
      CREATE CODEC measurement_codec
        FROM WIRE JSON SCHEMA measurement_wire
        TO SCHEMA measurement;
      CREATE RELAY measurements SCHEMA measurement UNBRANCHED;
      CREATE RELAY compared_measurements SCHEMA compared_measurement UNBRANCHED;
      CREATE VHOST edge nullif-equality-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/measurements' TYPE HTTP;
      CREATE INGESTOR measurement_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING measurement_codec
        TO measurements
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION compare_measurements
        FROM measurements
        UNBRANCHED
        TO compared_measurements
          INHERIT ALL
          SET equal = (input.left_text AS F64) = (input.right_text AS F64),
              nullified = is_null(nullif((input.left_text AS F64), (input.right_text AS F64)))
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION compared_measurements_subscription TO compared_measurements;
      START;
      """
    And http payload is posted to node "node-1" with host "nullif-equality-{{test_id}}.example.com" path "/measurements"
      """
      {"id":"not-a-number","left_text":"nan","right_text":"nan"}
      """
    And http payload is posted to node "node-1" with host "nullif-equality-{{test_id}}.example.com" path "/measurements"
      """
      {"id":"signed-zero","left_text":"0.0","right_text":"-0.0"}
      """
    And http payload is posted to node "node-1" with host "nullif-equality-{{test_id}}.example.com" path "/measurements"
      """
      {"id":"identical","left_text":"1.5","right_text":"1.5"}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"not-a-number" | "equal":false | "nullified":false
      "id":"signed-zero" | "equal":true | "nullified":true
      "id":"identical" | "equal":true | "nullified":true
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Regular expressions prepare constant patterns and read per-message patterns
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA note (
        id STRING,
        raw STRING,
        pattern STRING
      );
      CREATE SCHEMA matched_note (
        id STRING,
        raw STRING,
        pattern STRING,
        literal_match BOOL,
        folded_match BOOL,
        message_match BOOL,
        message_piece STRING OPTIONAL,
        rewritten STRING,
        guarded BOOL
      );
      CREATE SCHEMA note_error (
        source_id STRING,
        error_code STRING,
        error_message STRING
      );
      CREATE WIRE JSON SCHEMA note_wire MODE STRICT (
        id string,
        raw string,
        pattern string
      );
      CREATE CODEC note_codec
        FROM WIRE JSON SCHEMA note_wire
        TO SCHEMA note;
      CREATE RELAY notes SCHEMA note UNBRANCHED;
      CREATE RELAY matched_notes SCHEMA matched_note UNBRANCHED;
      CREATE RELAY note_errors SCHEMA note_error UNBRANCHED;
      CREATE VHOST edge regexp-patterns-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/notes' TYPE HTTP;
      CREATE INGESTOR note_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING note_codec
        TO notes
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION match_notes
        FROM notes
        UNBRANCHED
        TO matched_notes
          INHERIT ALL
          SET literal_match = regexp_like(input.raw, 'h[a-z]+'),
              folded_match = regexp_like(input.raw, lower('H[A-Z]+')),
              message_match = regexp_like(input.raw, input.pattern),
              message_piece = regexp_substr(input.raw, input.pattern),
              rewritten = regexp_replace(input.raw, '([a-z])([a-z]*)', '${1}_$2'),
              guarded = CASE
                WHEN input.id = 'guarded' THEN regexp_like(input.raw, '(')
                ELSE false
              END
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO note_errors
            SET source_id = input.id,
                error_code = error.code,
                error_message = error.message;
      CREATE SUBSCRIPTION matched_notes_subscription TO matched_notes;
      CREATE SUBSCRIPTION note_errors_subscription TO note_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "regexp-patterns-{{test_id}}.example.com" path "/notes"
      """
      {"id":"hello","raw":"hello world","pattern":"h[a-z]+"}
      """
    And http payload is posted to node "node-1" with host "regexp-patterns-{{test_id}}.example.com" path "/notes"
      """
      {"id":"digits","raw":"a1b22","pattern":"[0-9]+"}
      """
    And http payload is posted to node "node-1" with host "regexp-patterns-{{test_id}}.example.com" path "/notes"
      """
      {"id":"again","raw":"hi there","pattern":"h[a-z]+"}
      """
    And http payload is posted to node "node-1" with host "regexp-patterns-{{test_id}}.example.com" path "/notes"
      """
      {"id":"guarded","raw":"hello","pattern":"h[a-z]+"}
      """
    And http payload is posted to node "node-1" with host "regexp-patterns-{{test_id}}.example.com" path "/notes"
      """
      {"id":"unclosed","raw":"hello","pattern":"("}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"hello" | "literal_match":true | "folded_match":true | "message_match":true | "message_piece":"hello" | "rewritten":"h_ello w_orld" | "guarded":false
      "id":"digits" | "literal_match":false | "folded_match":false | "message_match":true | "message_piece":"1" | "rewritten":"a_1b_22" | "guarded":false
      "id":"again" | "literal_match":true | "folded_match":true | "message_match":true | "message_piece":"hi" | "rewritten":"h_i t_here" | "guarded":false
      "source_id":"guarded" | "error_code":"evaluation" | invalid regular expression | unclosed group
      "source_id":"unclosed" | "error_code":"evaluation" | invalid regular expression | unclosed group
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: CASE ignores float function errors from unselected arms
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA growth (
        id STRING,
        exponent F64
      );
      CREATE SCHEMA grown_value (
        id STRING,
        exponent F64,
        result F64
      );
      CREATE SCHEMA growth_error (
        source_id STRING,
        error_message STRING
      );
      CREATE WIRE JSON SCHEMA growth_wire MODE STRICT (
        id string,
        exponent number
      );
      CREATE CODEC growth_codec
        FROM WIRE JSON SCHEMA growth_wire
        TO SCHEMA growth;
      CREATE RELAY growths SCHEMA growth UNBRANCHED;
      CREATE RELAY grown_values SCHEMA grown_value UNBRANCHED;
      CREATE RELAY growth_errors SCHEMA growth_error UNBRANCHED;
      CREATE VHOST edge unselected-arm-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/growths' TYPE HTTP;
      CREATE INGESTOR growth_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING growth_codec
        TO growths
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION grow
        FROM growths
        UNBRANCHED
        TO grown_values
          INHERIT ALL
          SET result = CASE
            WHEN input.exponent < 700.0 THEN exp(input.exponent)
            ELSE 0.0
          END
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO growth_errors
            SET source_id = input.id,
                error_message = error.message;
      CREATE SUBSCRIPTION grown_values_subscription TO grown_values;
      CREATE SUBSCRIPTION growth_errors_subscription TO growth_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "unselected-arm-{{test_id}}.example.com" path "/growths"
      """
      {"id":"bounded","exponent":0.0}
      """
    And http payload is posted to node "node-1" with host "unselected-arm-{{test_id}}.example.com" path "/growths"
      """
      {"id":"unbounded","exponent":1000.0}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"bounded" | "result":1.0
      "id":"unbounded" | "result":0.0
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: List sum reports integer overflow as a message error
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
        samples <samples_type>
      );
      CREATE SCHEMA reading_total (
        id STRING,
        samples <samples_type>,
        total I64 OPTIONAL
      );
      CREATE SCHEMA reading_error (
        source_id STRING,
        error_message STRING
      );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT (
        id string,
        samples array
      );
      CREATE CODEC reading_codec
        FROM WIRE JSON SCHEMA reading_wire
        TO SCHEMA reading;
      CREATE RELAY readings SCHEMA reading UNBRANCHED;
      CREATE RELAY reading_totals SCHEMA reading_total UNBRANCHED;
      CREATE RELAY reading_errors SCHEMA reading_error UNBRANCHED;
      CREATE VHOST edge list-sum-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/readings' TYPE HTTP;
      CREATE INGESTOR reading_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_codec
        TO readings
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION total_readings
        FROM readings
        UNBRANCHED
        TO reading_totals
          INHERIT ALL
          SET total = sum(input.samples)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO reading_errors
            SET source_id = input.id,
                error_message = error.message;
      CREATE SUBSCRIPTION reading_totals_subscription TO reading_totals;
      CREATE SUBSCRIPTION reading_errors_subscription TO reading_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "list-sum-{{test_id}}.example.com" path "/readings"
      """
      {"id":"small","samples":[1,2,3]}
      """
    And http payload is posted to node "node-1" with host "list-sum-{{test_id}}.example.com" path "/readings"
      """
      {"id":"overflowing","samples":[9000000000000000000,9000000000000000000]}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"small" | "total":6
      "source_id":"overflowing" | "error_message":"junction 'total_readings' FILTER-MAP side error overflow
      """

    Examples:
      | cluster_size | replica_count | samples_type |
      | 1            | 0             | VEC<I64>     |
      | 3            | 0             | VEC<I64>     |

  Scenario Outline: Count and position functions read unsigned counts in full and refuse text a STRING cannot hold
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA sizing (
        id STRING,
        text STRING,
        position U64,
        times U64 OPTIONAL,
        width U64 OPTIONAL
      );
      CREATE SCHEMA sized_text (
        id STRING,
        lefted STRING,
        righted STRING,
        tail STRING,
        part STRING,
        guarded STRING,
        repeated STRING OPTIONAL,
        left_padded STRING OPTIONAL,
        right_padded STRING OPTIONAL
      );
      CREATE SCHEMA sizing_error (
        source_id STRING,
        error_message STRING
      );
      CREATE CODEC sizing_batch_codec
        FROM JSON
        TO SCHEMA sizing
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY sizings SCHEMA sizing UNBRANCHED;
      CREATE RELAY sized_texts SCHEMA sized_text UNBRANCHED;
      CREATE RELAY sizing_errors SCHEMA sizing_error UNBRANCHED;
      CREATE VHOST edge text-sizing-{{test_id}}.example.com;
      CREATE ENDPOINT sizing_ingress ON edge PATH '/sizings' TYPE HTTP;
      CREATE INGESTOR sizing_source
        FROM ENDPOINT sizing_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING sizing_batch_codec
        TO sizings
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION size_texts
        FROM sizings
        UNBRANCHED
        TO sized_texts
          SET id = input.id,
              lefted = left(input.text, input.position),
              righted = right(input.text, input.position),
              tail = substr(input.text, input.position),
              part = split_part(input.text, '.', input.position),
              guarded = CASE
                WHEN input.position < (10 AS U64) THEN repeat(input.text, input.position)
                ELSE 'skipped'
              END,
              repeated = repeat(input.text, input.times),
              left_padded = lpad(input.text, input.width, '*'),
              right_padded = rpad(input.text, input.width, '*')
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO sizing_errors
            SET source_id = input.id,
                error_message = error.message;
      CREATE SUBSCRIPTION sized_texts_subscription TO sized_texts;
      CREATE SUBSCRIPTION sizing_errors_subscription TO sizing_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "text-sizing-{{test_id}}.example.com" path "/sizings"
      """
      [{"id":"small","text":"a.b","position":2,"times":2,"width":5},{"id":"wide","text":"a.b","position":18446744073709551615,"times":1,"width":1},{"id":"repeat-oversized","text":"a.b","position":1,"times":18446744073709551615,"width":1},{"id":"pad-oversized","text":"a.b","position":1,"times":1,"width":18446744073709551615},{"id":"absent","text":"a.b","position":1,"times":null,"width":null}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      {"guarded":"a.ba.b","id":"small","left_padded":"**a.b","lefted":"a.","part":"b","repeated":"a.ba.b","right_padded":"a.b**","righted":".b","tail":".b"}
      {"guarded":"skipped","id":"wide","left_padded":"a","lefted":"a.b","part":"","repeated":"a.b","right_padded":"a","righted":"a.b","tail":""}
      {"guarded":"a.b","id":"absent","lefted":"a","part":"a","righted":"b","tail":"a.b"}
      "source_id":"repeat-oversized" | "error_message":"junction 'size_texts' FILTER-MAP side error overflow: repeat result exceeds the text one STRING column holds
      "source_id":"pad-oversized" | "error_message":"junction 'size_texts' FILTER-MAP side error overflow: lpad result exceeds the text one STRING column holds
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: List item functions reject nested elements when the statement is applied
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands fail with "function 'first' requires ARRAY or VEC elements of a scalar type"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA detection_batch (
        id STRING,
        detections <detections_type>
      );
      CREATE SCHEMA first_detection (
        id STRING,
        detection <detection_type> OPTIONAL
      );
      CREATE RELAY detection_batches SCHEMA detection_batch UNBRANCHED;
      CREATE RELAY first_detections SCHEMA first_detection UNBRANCHED;
      CREATE JUNCTION select_first_detection
        FROM detection_batches
        UNBRANCHED
        TO first_detections
          INHERIT ALL EXCEPT detections
          SET detection = first(input.detections)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | detections_type    | detection_type |
      | 1            | VEC<ARRAY<F32, 6>> | ARRAY<F32, 6>  |
      | 3            | VEC<ARRAY<F32, 6>> | ARRAY<F32, 6>  |
