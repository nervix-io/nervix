Feature: Roto user-defined functions
  Scenario Outline: Roto UDFs compose with NSPL expressions over Arrow batches
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE UDF add_one
        WITH ROTO_0_11
        ARGS (value I64)
        RETURNS I64
        CODE $roto$
          fn add_one(value: I64Column) -> I64Column {
              value.add_s(1)
          }
        $roto$;
      CREATE SCHEMA udf_input (
        id STRING,
        value I64
      );
      CREATE SCHEMA udf_output (
        id STRING,
        result I64
      );
      CREATE STRICT WIRE JSON SCHEMA udf_input_wire (
        id string,
        value integer
      );
      CREATE CODEC udf_input_codec
        FROM WIRE JSON SCHEMA udf_input_wire
        TO SCHEMA udf_input;
      CREATE RELAY udf_inputs SCHEMA udf_input UNBRANCHED;
      CREATE RELAY udf_outputs SCHEMA udf_output UNBRANCHED;
      CREATE VHOST edge udf-{{test_id}}.example.com;
      CREATE ENDPOINT udf_ingress ON edge PATH '/udf' TYPE HTTP;
      CREATE INGESTOR udf_source
        FROM ENDPOINT udf_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING udf_input_codec
        TO udf_inputs
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION apply_udf
        FROM udf_inputs
        UNBRANCHED
        TO udf_outputs
          SET id = LOWER(input.id),
              result = udf::add_one(ABS(input.value))
          WHERE udf::add_one(input.value) > 0
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION udf_outputs_subscription TO udf_outputs;
      START;
      """
    And http payload is posted to node "node-1" with host "udf-{{test_id}}.example.com" path "/udf"
      """
      {"id":"FIRST","value":-2}
      """
    And http payload is posted to node "node-1" with host "udf-{{test_id}}.example.com" path "/udf"
      """
      {"id":"SECOND","value":4}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      "id":"second" | "result":5
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: A Roto UDF uses the per-element escape hatch for Luhn validation
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE UDF luhn_valid
        WITH ROTO_0_11
        ARGS (pan STRING)
        RETURNS BOOL
        CODE $roto$
          fn luhn_valid(pan: StringColumn) -> BoolColumn {
              let out = ColumnBuilder.bool(pan.len());
              let i = 0;
              while i < pan.len() {
                  match pan.get(i) {
                      Some(value) => out.push(check(value)),
                      None => out.push_null(),
                  }
                  i += 1;
              }
              out.finish()
          }

          fn check(value: String) -> bool {
              let bytes = value.bytes();
              let len = bytes.len();
              if len < 12 || len > 19 {
                  return false;
              }

              let sum = 0;
              let i = 0;
              for byte in bytes.list() {
                  if byte < 48 || byte > 57 {
                      return false;
                  }
                  let digit = byte - 48;
                  let doubled = if (len - 1 - i) % 2 == 1 {
                      digit * 2
                  } else {
                      digit
                  };
                  sum += if doubled > 9 { doubled - 9 } else { doubled };
                  i += 1;
              }
              sum % 10 == 0
          }

          test known_cards {
              if check("4111111111111111") && !check("4111111111111112") {
                  accept
              } else {
                  reject
              }
          }
        $roto$;
      CREATE SCHEMA card_event (
        id STRING,
        pan STRING
      );
      CREATE STRICT WIRE JSON SCHEMA card_event_wire (
        id string,
        pan string
      );
      CREATE CODEC card_event_codec
        FROM WIRE JSON SCHEMA card_event_wire
        TO SCHEMA card_event;
      CREATE RELAY card_events SCHEMA card_event UNBRANCHED;
      CREATE RELAY valid_cards SCHEMA card_event UNBRANCHED;
      CREATE VHOST card_edge luhn-{{test_id}}.example.com;
      CREATE ENDPOINT card_ingress ON card_edge PATH '/cards' TYPE HTTP;
      CREATE INGESTOR card_source
        FROM ENDPOINT card_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING card_event_codec
        TO card_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION validate_cards
        FROM card_events
        UNBRANCHED
        TO valid_cards
          INHERIT ALL
          WHERE udf::luhn_valid(input.pan)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION valid_cards_subscription TO valid_cards;
      START;
      """
    And http payload is posted to node "node-1" with host "luhn-{{test_id}}.example.com" path "/cards"
      """
      {"id":"invalid","pan":"4111111111111112"}
      """
    And http payload is posted to node "node-1" with host "luhn-{{test_id}}.example.com" path "/cards"
      """
      {"id":"valid","pan":"4111111111111111"}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      "id":"valid" | "pan":"4111111111111111"
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Non-volatile UDFs cannot access non-deterministic functions
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these NSPL commands fail with "now() requires VOLATILE"
      """
      CREATE UDF invalid_clock
        WITH ROTO_0_11
        ARGS (value DATETIME)
        RETURNS BOOL
        CODE $roto$
          fn invalid_clock(value: DatetimeColumn) -> BoolColumn {
              value.lt_s(now())
          }
        $roto$;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A failing in-source Roto test rejects UDF creation
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these NSPL commands fail with "Roto test block failed"
      """
      CREATE UDF broken_increment
        WITH ROTO_0_11
        ARGS (value I64)
        RETURNS I64
        CODE $roto$
          fn broken_increment(value: I64Column) -> I64Column {
              value
          }

          fn increment_scalar(value: i64) -> i64 {
              value
          }

          test increments_by_one {
              if increment_scalar(41) == 42 {
                  accept
              } else {
                  reject
              }
          }
        $roto$;
      """
    And these NSPL commands are executed on the leader node
      """
      SHOW UDFS;
      """
    Then the last command output does not contain
      """
      broken_increment
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
