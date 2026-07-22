Feature: Route-local value construction
  Scenario Outline: Transforming routes explicitly inherit and construct output in order
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA source_event (
        id STRING,
        amount I64,
        raw STRING,
        note STRING OPTIONAL
      );
      CREATE SCHEMA projected_event (
        id STRING,
        amount I64,
        normalized STRING,
        note STRING OPTIONAL
      );
      CREATE STRICT WIRE JSON SCHEMA source_event_wire (
        id string,
        amount integer,
        raw string,
        note string OPTIONAL
      );
      CREATE CODEC source_event_codec
        FROM WIRE JSON SCHEMA source_event_wire
        TO SCHEMA source_event;
      CREATE RELAY source_events SCHEMA source_event UNBRANCHED;
      CREATE RELAY projected_events SCHEMA projected_event UNBRANCHED;
      CREATE VHOST edge value-construction-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        DECODE USING source_event_codec
        TO source_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION project_events
        FROM source_events
        UNBRANCHED
        TO projected_events
          INHERIT ALL EXCEPT raw
          SET amount = amount + 1,
              amount = amount * 2,
              normalized = lower(trim(input.raw))
          WHERE output.amount = 20
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION projected_events_subscription TO projected_events;
      START;
      """
    When http payload is posted to node "node-1" with host "value-construction-{{test_id}}.example.com" path "/events"
      """
      {"id":"selected","amount":9,"raw":"  READY  "}
      """
    And http payload is posted to node "node-1" with host "value-construction-{{test_id}}.example.com" path "/events"
      """
      {"id":"filtered","amount":2,"raw":"  DROP  ","note":"not emitted"}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "amount":20,"id":"selected","normalized":"ready"
      """
    And the last relay subscription payload does not contain "note\""
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Header-capable ingestor error routes can read the source envelope
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA header_calculation (
        id STRING,
        numerator I64,
        denominator I64
      );
      CREATE SCHEMA header_calculation_result (
        id STRING,
        result I64
      );
      CREATE SCHEMA header_calculation_error (
        input_id STRING,
        source_route STRING OPTIONAL,
        operation STRING
      );
      CREATE STRICT WIRE JSON SCHEMA header_calculation_wire (
        id string,
        numerator integer,
        denominator integer
      );
      CREATE CODEC header_calculation_codec
        FROM WIRE JSON SCHEMA header_calculation_wire
        TO SCHEMA header_calculation;
      CREATE RELAY header_calculation_results SCHEMA header_calculation_result UNBRANCHED;
      CREATE RELAY header_calculation_errors SCHEMA header_calculation_error UNBRANCHED;
      CREATE VHOST edge header-error-{{test_id}}.example.com;
      CREATE ENDPOINT header_calculation_ingress ON edge PATH '/calculations' TYPE HTTP;
      CREATE INGESTOR header_calculation_source
        FROM ENDPOINT header_calculation_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING header_calculation_codec
        TO header_calculation_results
          SET id = input.id,
              result = input.numerator / input.denominator
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO header_calculation_errors
          SET input_id = input.id,
              source_route = read_header('route'),
              operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION header_calculation_errors_subscription TO header_calculation_errors;
      START;
      """
    When http payload is posted to node "node-1" with host "header-error-{{test_id}}.example.com" path "/calculations" and header "route" value "error-route-header"
      """
      {"id":"header-division-by-zero","numerator":10,"denominator":0}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "input_id":"header-division-by-zero","operation":"set","source_route":"error-route-header"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Error routes use standard scalar functions and captured construction state
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
        numerator I64,
        denominator I64
      );
      CREATE SCHEMA calculation_result (
        id STRING,
        result I64
      );
      CREATE SCHEMA calculation_error (
        input_id STRING,
        error_code STRING,
        operation STRING,
        operation_index U32 OPTIONAL,
        message_digest STRING,
        attempted_result I64 OPTIONAL
      );
      CREATE STRICT WIRE JSON SCHEMA calculation_wire (
        id string,
        numerator integer,
        denominator integer
      );
      CREATE CODEC calculation_codec
        FROM WIRE JSON SCHEMA calculation_wire
        TO SCHEMA calculation;
      CREATE RELAY calculations SCHEMA calculation UNBRANCHED;
      CREATE RELAY calculation_results SCHEMA calculation_result UNBRANCHED;
      CREATE RELAY calculation_errors SCHEMA calculation_error UNBRANCHED;
      CREATE VHOST edge error-construction-{{test_id}}.example.com;
      CREATE ENDPOINT calculation_ingress ON edge PATH '/calculations' TYPE HTTP;
      CREATE INGESTOR calculation_source
        FROM ENDPOINT calculation_ingress MODE NO_ACK SEQUENTIAL
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
        TO calculation_results
          SET id = input.id,
              result = input.numerator,
              result = result / input.denominator
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO calculation_errors
          SET input_id = input.id,
              error_code = upper(error.code),
              operation = error.operation,
              operation_index = error.operation_index,
              message_digest = md5(error.message),
              attempted_result = partial_output.result;
      CREATE SUBSCRIPTION calculation_errors_subscription TO calculation_errors;
      START;
      """
    When http payload is posted to node "node-1" with host "error-construction-{{test_id}}.example.com" path "/calculations"
      """
      {"id":"division-by-zero","numerator":10,"denominator":0}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "attempted_result":10,"error_code":"EVALUATION","input_id":"division-by-zero"
      """
    And the last relay subscription payload contains
      """
      "message_digest":"
      """
    And the last relay subscription payload contains
      """
      "operation":"set","operation_index":2
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
