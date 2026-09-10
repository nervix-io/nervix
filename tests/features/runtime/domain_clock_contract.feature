Feature: Domain clock contract regressions
  @domain_clock_authority
  Scenario: One fenced authority emits coalesced progress for each clock generation
    Given the production sticky scheduler is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 2 node nervix cluster is started
    And the active domain is "authority_domain"
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      CREATE SCHEMA authority_clock_request (
        sequence I64
      );
      CREATE SCHEMA authority_clock_observation (
        sequence I64,
        observed_at DATETIME
      );
      CREATE WIRE JSON SCHEMA authority_clock_request_wire MODE STRICT (
        sequence integer
      );
      CREATE CODEC authority_clock_request_codec
        FROM WIRE JSON SCHEMA authority_clock_request_wire
        TO SCHEMA authority_clock_request;
      CREATE RELAY authority_clock_observations
        SCHEMA authority_clock_observation UNBRANCHED;
      CREATE VHOST edge authority-clock-{{test_id}}.example.com;
      CREATE ENDPOINT authority_clock_endpoint
        ON edge
        PATH '/clock'
        TYPE HTTP;
      CREATE INGESTOR authority_clock_source
        FROM ENDPOINT authority_clock_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING authority_clock_request_codec
        TIMESTAMP NOW
        TO authority_clock_observations
          SET sequence = message.sequence,
              observed_at = now()
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION authority_clock_subscription TO authority_clock_observations;
      """
    And domain clock progress for domain "{{domain}}" on node "node-2" is paused before delivery
    When these NSPL commands are executed
      """
      START AT '2000-01-01T00:00:00Z' TIME RATE 1.0;
      """
    And node "node-3" is added to the cluster
    Then node "node-1" eventually reports raft voters "node-1,node-2,node-3"
    Then within "20s" domain clock progress for domain "{{domain}}" on node "node-2" reaches the delivery pause
    When these NSPL commands are executed
      """
      STOP;
      START AT '2010-01-01T00:00:00Z' TIME RATE 1.0;
      """
    And domain clock progress for domain "{{domain}}" on node "node-2" resumes
    Given domain clock progress for domain "{{domain}}" on node "node-2" is paused before delivery
    Then within "20s" domain clock progress for domain "{{domain}}" on node "node-2" reaches the delivery pause
    And node "node-2" eventually accepts http traffic for host "authority-clock-{{test_id}}.example.com" path "/clock"
      """
      {"sequence":1}
      """
    And within "5s" the relay subscription receives a payload
      """
      "sequence":1
      """
    And the last relay subscription payload field "observed_at" is saved as timestamp placeholder "restarted_clock_time"
    And timestamp placeholder "restarted_clock_time" is not before "2010-01-01T00:00:00Z"
    And timestamp placeholder "restarted_clock_time" is before "2011-01-01T00:00:00Z"
    When domain clock progress for domain "{{domain}}" on node "node-2" resumes

  @domain_clock_authority
  Scenario: Nonleader clock-owner loss preserves the committed mapping
    Given the production sticky scheduler is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    And the active domain is "authority_domain"
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 10ms;
      """
    And domain clock progress for domain "{{domain}}" on node "node-2" is paused before delivery
    When these NSPL commands are executed
      """
      START AT '2000-01-01T00:00:00Z' TIME RATE 1.0;
      """
    Then within "20s" domain clock progress for domain "{{domain}}" on node "node-2" reaches the delivery pause
    When domain clock progress for domain "{{domain}}" on node "node-2" resumes
    And leadership is transferred from node "node-1" to node "node-3"
    Then node "node-3" eventually reports leader "node-3"
    Given domain clock progress for domain "{{domain}}" on node "node-2" is paused before delivery
    Then within "20s" domain clock progress for domain "{{domain}}" on node "node-2" reaches the delivery pause
    When domain clock progress for domain "{{domain}}" on node "node-2" resumes
    Given domain clock progress for domain "{{domain}}" on node "node-2" is paused before delivery
    When node "node-1" is restarted 1 times with a new interconnect address
    Then within "20s" domain clock progress for domain "{{domain}}" on node "node-2" reaches the delivery pause
    When domain clock progress for domain "{{domain}}" on node "node-2" resumes
    Given domain clock progress for domain "{{domain}}" on node "node-2" is paused before delivery
    When node "node-2" is restarted 1 times with a new interconnect address
    Then within "20s" domain clock progress for domain "{{domain}}" on node "node-2" reaches the delivery pause
    When domain clock progress for domain "{{domain}}" on node "node-2" resumes
    Given domain clock progress for domain "{{domain}}" on node "node-1" is paused before delivery
    When node "node-2" is stopped
    Then node "node-3" eventually reports leader "node-3"
    And within "20s" domain clock progress for domain "{{domain}}" on node "node-1" reaches the delivery pause

  @domain_bound_clock
  Scenario: A joining or restarted node installs the current clock generation before execution
    Given the production sticky scheduler is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 1h SKEW 1m;
      """
    When these NSPL commands are executed
      """
      START AT '2000-01-01T00:00:00Z' TIME RATE 1.0;
      """
    And node "node-2" is added to the cluster
    And the cluster is restarted
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA joined_clock_request (
        sequence I64
      );
      CREATE SCHEMA joined_clock_observation (
        sequence I64,
        observed_at DATETIME
      );
      CREATE WIRE JSON SCHEMA joined_clock_request_wire MODE STRICT (
        sequence integer
      );
      CREATE CODEC joined_clock_request_codec
        FROM WIRE JSON SCHEMA joined_clock_request_wire
        TO SCHEMA joined_clock_request;
      CREATE RELAY joined_clock_observations
        SCHEMA joined_clock_observation UNBRANCHED;
      CREATE VHOST edge joined-clock-{{test_id}}.example.com;
      CREATE ENDPOINT joined_clock_endpoint
        ON edge
        PATH '/clock'
        TYPE HTTP;
      CREATE INGESTOR joined_clock_source
        FROM ENDPOINT joined_clock_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING joined_clock_request_codec
        TIMESTAMP NOW
        TO joined_clock_observations
          SET sequence = message.sequence,
              observed_at = now()
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION joined_clock_subscription TO joined_clock_observations;
      """
    Then node "node-2" eventually accepts http traffic for host "joined-clock-{{test_id}}.example.com" path "/clock"
      """
      {"sequence":1}
      """
    And within "5s" the relay subscription receives a payload
      """
      "sequence":1
      """
    And the last relay subscription payload field "observed_at" is saved as timestamp placeholder "joined_clock_time"
    And timestamp placeholder "joined_clock_time" is before "2001-01-01T00:00:00Z"

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

  @domain_cadence
  Scenario Outline: HTTP polling follows paced domain cadence over multiple periods
    Given the HTTP mock server is running
    And clock source recorder "{{test_id}}" is reset
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 1s;
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA cadence_source_record (
        user_id I64
      );
      CREATE WIRE JSON SCHEMA cadence_source_wire MODE STRICT (
        user_id integer
      );
      CREATE CODEC cadence_source_codec
        FROM WIRE JSON SCHEMA cadence_source_wire
        TO SCHEMA cadence_source_record;
      CREATE RELAY cadence_source_records SCHEMA cadence_source_record UNBRANCHED;
      CREATE CLIENT cadence_source
        TYPE HTTP
        CONFIG {
          'endpoint' = '{{mock_http_addr}}/clock-source/{{test_id}}?fixture=http-cadence&delay_ms=25',
          'method' = 'GET',
          'timeout_ms' = 5000
        };
      CREATE INGESTOR cadence_source_reader
        FROM HTTP cadence_source EVERY 1s
        ON QUIESCE SUSPEND DECODE USING cadence_source_codec
        TIMESTAMP NOW
        TO cadence_source_records
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 4.0;
      """
    Then within "650ms" clock source recorder "{{test_id}}" records 3 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @domain_cadence
  Scenario Outline: Slow external polling coalesces missed cadence with fresh due timestamps
    Given the HTTP mock server is running
    And clock source recorder "{{test_id}}" is reset
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 1s;
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA cadence_sample (
        source STRING,
        value F64,
        timestamp STRING,
        due STRING
      );
      CREATE WIRE JSON SCHEMA cadence_sample_wire MODE STRICT (
        source string,
        value number,
        timestamp string,
        due string
      );
      CREATE CODEC cadence_sample_codec
        FROM WIRE JSON SCHEMA cadence_sample_wire
        TO SCHEMA cadence_sample;
      CREATE SCHEMA cadence_observation (
        due STRING,
        executed_at DATETIME
      );
      CREATE RELAY cadence_observations SCHEMA cadence_observation UNBRANCHED;
      CREATE CLIENT cadence_prometheus
        TYPE PROMETHEUS
        CONFIG {
          'addr' = '{{mock_http_addr}}/prometheus-clock-source/{{test_id}}/350',
          'timeout_ms' = 5000
        };
      CREATE INGESTOR cadence_reader
        FROM PROMETHEUS cadence_prometheus QUERY 'vector(42.5)' EVERY 1s
        ON QUIESCE SUSPEND DECODE USING cadence_sample_codec
        TIMESTAMP NOW
        TO cadence_observations
          SET due = message.due,
              executed_at = now()
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION cadence_subscription TO cadence_observations;
      START AT '2000-01-01T00:00:00Z' TIME RATE 10.0;
      """
    Then within "5s" clock source recorder "{{test_id}}" and relay subscription observe 3 fresh executions on "1s" due cadence separated by at least "3s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @domain_cadence
  Scenario Outline: Polling cadence rebinds across slow and fast clock generations
    Given the HTTP mock server is running
    And clock source recorder "{{test_id}}" is reset
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 1s;
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA rebound_sample (
        source STRING,
        value F64,
        timestamp STRING,
        due STRING
      );
      CREATE WIRE JSON SCHEMA rebound_sample_wire MODE STRICT (
        source string,
        value number,
        timestamp string,
        due string
      );
      CREATE CODEC rebound_sample_codec
        FROM WIRE JSON SCHEMA rebound_sample_wire
        TO SCHEMA rebound_sample;
      CREATE SCHEMA rebound_observation (
        due STRING,
        executed_at DATETIME
      );
      CREATE RELAY rebound_observations SCHEMA rebound_observation UNBRANCHED;
      CREATE CLIENT rebound_prometheus
        TYPE PROMETHEUS
        CONFIG {
          'addr' = '{{mock_http_addr}}/prometheus-clock-source/{{test_id}}/10',
          'timeout_ms' = 5000
        };
      CREATE INGESTOR rebound_reader
        FROM PROMETHEUS rebound_prometheus QUERY 'vector(42.5)' EVERY 1s
        ON QUIESCE SUSPEND DECODE USING rebound_sample_codec
        TIMESTAMP NOW
        TO rebound_observations
          SET due = message.due,
              executed_at = now()
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rebound_subscription TO rebound_observations;
      START AT '2000-01-01T00:00:00Z' TIME RATE 0.0001;
      """
    And physical time passes for "300ms"
    Then within "50ms" clock source recorder "{{test_id}}" records 0 requests
    When these NSPL commands are executed
      """
      STOP;
      START AT '2010-01-01T00:00:00Z' TIME RATE 20.0;
      """
    Then within "5s" clock source recorder "{{test_id}}" and relay subscription observe 3 fresh executions on "1s" due cadence separated by at least "1s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @delayed_clock_progress
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

  Scenario Outline: Out-of-range paced starts and projections return typed timestamp diagnostics
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 1s SKEW 100ms;
      """
    When these NSPL commands fail with "outside the signed Unix-nanosecond range"
      """
      START AT '<outside_timestamp>' TIME RATE 1.0;
      """
    And these NSPL commands are executed
      """
      CREATE SCHEMA clock_diagnostic (
        sequence I64
      );
      CREATE RELAY clock_diagnostics SCHEMA clock_diagnostic UNBRANCHED;
      CREATE SUBSCRIPTION clock_diagnostics_subscription TO clock_diagnostics;
      """
    And these NSPL commands are executed on the leader node
      """
      START AT '2262-04-11T23:47:16.854775807Z' TIME RATE 1.0;
      """
    Then within "10s" the active session observes a server error
    And the last server error contains
      """
      domain clock projection
      """
    And the last server error contains
      """
      leaves the signed Unix-nanosecond range
      """

    Examples:
      | cluster_size | outside_timestamp              |
      | 1            | 1677-09-21T00:12:43.145224191Z |
      | 1            | 2262-04-11T23:47:16.854775808Z |
      | 3            | 1677-09-21T00:12:43.145224191Z |
      | 3            | 2262-04-11T23:47:16.854775808Z |

  @logical_origin_admission
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
