Feature: Domain clock contract regressions
  Scenario Outline: External source fixture records request timing and count
    Given the HTTP mock server is running
    And clock source recorder "{{test_id}}" is reset
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA clock_source_record (
        user_id I64
      );
      CREATE WIRE JSON SCHEMA clock_source_wire MODE STRICT (
        user_id integer
      );
      CREATE CODEC clock_source_codec
        FROM WIRE JSON SCHEMA clock_source_wire
        TO SCHEMA clock_source_record;
      CREATE RELAY clock_source_records SCHEMA clock_source_record UNBRANCHED;
      CREATE CLIENT clock_source
        TYPE HTTP
        CONFIG {
          'endpoint' = '{{mock_http_addr}}/clock-source/{{test_id}}?fixture=domain-clock&delay_ms=25',
          'method' = 'GET',
          'timeout_ms' = 5000
        };
      CREATE INGESTOR clock_source_reader
        FROM HTTP clock_source EVERY 1h
        ON QUIESCE SUSPEND DECODE USING clock_source_codec
        TIMESTAMP NOW
        TO clock_source_records
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION clock_source_subscription TO clock_source_records;
      START;
      """
    Then within "5s" clock source recorder "{{test_id}}" records 1 requests
    And within "5s" the relay subscription receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @clock_contract_expected_failure @delayed_clock_progress
  Scenario Outline: Delayed clock progress cannot move observed logical time backwards
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 10s SKEW 2s;
      """
    And domain clock progress for domain "{{domain}}" is paused before delivery
    When these NSPL commands are executed
      """
      CREATE SCHEMA clock_request (
        sequence I64
      );
      CREATE SCHEMA clock_observation (
        sequence I64,
        observed_at DATETIME
      );
      CREATE WIRE JSON SCHEMA clock_request_wire MODE STRICT (
        sequence integer
      );
      CREATE CODEC clock_request_codec
        FROM WIRE JSON SCHEMA clock_request_wire
        TO SCHEMA clock_request;
      CREATE RELAY clock_observations SCHEMA clock_observation UNBRANCHED;
      CREATE VHOST edge clock-contract-{{test_id}}.example.com;
      CREATE ENDPOINT clock_request_endpoint
        ON edge
        PATH '/clock'
        TYPE HTTP;
      CREATE INGESTOR clock_request_source
        FROM ENDPOINT clock_request_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING clock_request_codec
        TIMESTAMP NOW
        TO clock_observations
          SET sequence = message.sequence,
              observed_at = now()
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION clock_observations_subscription TO clock_observations;
      """
    When these NSPL commands are executed on the leader node
      """
      START;
      """
    Then within "10s" domain clock progress for domain "{{domain}}" reaches the delivery pause
    When physical time passes for "1s"
    And http payload is posted to host "clock-contract-{{test_id}}.example.com" path "/clock"
      """
      {"sequence":1}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "sequence":1
      """
    And the last relay subscription payload field "observed_at" is saved as timestamp placeholder "before_progress"
    When domain clock progress for domain "{{domain}}" resumes
    And http payload is posted to host "clock-contract-{{test_id}}.example.com" path "/clock"
      """
      {"sequence":2}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "sequence":2
      """
    And the last relay subscription payload field "observed_at" is saved as timestamp placeholder "after_progress"
    And timestamp placeholder "after_progress" is not before timestamp placeholder "before_progress"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  @clock_contract_expected_failure @logical_origin_admission
  Scenario Outline: A paced domain admits an event at its historical logical origin
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 1s SKEW 100ms;
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA historical_event (
        sequence I64,
        occurred_at DATETIME
      );
      CREATE WIRE JSON SCHEMA historical_event_wire MODE STRICT (
        sequence integer,
        occurred_at string
      );
      CREATE CODEC historical_event_codec
        FROM WIRE JSON SCHEMA historical_event_wire
        TO SCHEMA historical_event
        ENCODE occurred_at AS RFC3339;
      CREATE RELAY historical_events SCHEMA historical_event UNBRANCHED;
      CREATE VHOST edge historical-clock-{{test_id}}.example.com;
      CREATE ENDPOINT historical_event_endpoint
        ON edge
        PATH '/events'
        TYPE HTTP;
      CREATE INGESTOR historical_event_source
        FROM ENDPOINT historical_event_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING historical_event_codec
        TIMESTAMP AT occurred_at
        TO historical_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION historical_events_subscription TO historical_events;
      """
    When these NSPL commands are executed on the leader node
      """
      START AT '2000-01-01T00:00:00Z' TIME RATE 1.0;
      """
    And http payload is posted to host "historical-clock-{{test_id}}.example.com" path "/events"
      """
      {"sequence":1,"occurred_at":"2000-01-01T00:00:00Z"}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "occurred_at":"2000-01-01T00:00:00+00:00","sequence":1
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
