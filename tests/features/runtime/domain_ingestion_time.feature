Feature: Logical ingestion time and admission
  @logical_ingestion_time
  Scenario Outline: Admission uses inclusive logical skew and excludes unreached future centers
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 1s SKEW 100ms;
      CREATE SCHEMA event ( sequence I64, occurred_at DATETIME );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( sequence integer, occurred_at string );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event
        ENCODE occurred_at AS RFC3339;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge ingestion-clock-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TIMESTAMP AT occurred_at
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      """
    And domain clock progress for domain "{{domain}}" is paused before delivery
    When these NSPL commands are executed on the leader node
      """
      START AT '2000-01-01T00:00:00Z' TIME RATE 1e-300;
      """
    Then within "5s" domain clock progress for domain "{{domain}}" reaches the delivery pause
    When http payload is posted to host "ingestion-clock-{{test_id}}.example.com" path "/events"
      """
      {"sequence":1,"occurred_at":"1999-12-31T23:59:59.900000000Z"}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "occurred_at":"1999-12-31T23:59:59.900+00:00","sequence":1
      """
    When http payload is posted to host "ingestion-clock-{{test_id}}.example.com" path "/events"
      """
      {"sequence":2,"occurred_at":"2000-01-01T00:00:00.100000000Z"}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "occurred_at":"2000-01-01T00:00:00.100+00:00","sequence":2
      """
    When http payload is posted to host "ingestion-clock-{{test_id}}.example.com" path "/events"
      """
      {"sequence":3,"occurred_at":"1999-12-31T23:59:59.899999999Z"}
      """
    And http payload is posted to host "ingestion-clock-{{test_id}}.example.com" path "/events"
      """
      {"sequence":4,"occurred_at":"2000-01-01T00:00:00.100000001Z"}
      """
    And http payload is posted to host "ingestion-clock-{{test_id}}.example.com" path "/events"
      """
      {"sequence":5,"occurred_at":"2000-01-01T00:00:01Z"}
      """
    Then the relay subscription does not receive a payload within "200ms"
    When domain clock progress for domain "{{domain}}" resumes

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @logical_ingestion_time
  Scenario Outline: Logical ingestion timestamps feed isolated duration windows at different rates
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 1s SKEW 1s;
      CREATE SCHEMA event ( tenant STRING, value I64, occurred_at DATETIME );
      CREATE SCHEMA summary ( tenant STRING, total I64, samples I64, source_at DATETIME );
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH tenants SCHEMA tenant_key TTL 1h;
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, value integer, occurred_at string );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event
        ENCODE occurred_at AS RFC3339;
      CREATE RELAY events SCHEMA event BRANCHED BY tenants;
      CREATE RELAY summaries SCHEMA summary BRANCHED BY tenants;
      CREATE VHOST edge ingestion-window-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TIMESTAMP <timestamp_source>
        TO events INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR totals FROM events WIDTH <width> DURATION STEP <width> DURATION
        BRANCHED BY tenants
        TO summaries SET tenant = FIRST(input.tenant), total = SUM(input.value),
          samples = COUNT(input.value), source_at = FIRST(input.occurred_at)
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION observations TO summaries;
      START AT '2000-01-01T00:00:00Z' TIME RATE <rate>;
      """
    When http payload is posted to host "ingestion-window-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","value":10,"occurred_at":"2000-01-01T00:00:00Z"}
      """
    And http payload is posted to host "ingestion-window-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","value":100,"occurred_at":"2000-01-01T00:00:00Z"}
      """
    And http payload is posted to host "ingestion-window-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","value":20,"occurred_at":"2000-01-01T00:00:00Z"}
      """
    And http payload is posted to host "ingestion-window-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","value":200,"occurred_at":"2000-01-01T00:00:00Z"}
      """
    Then within "10s" the relay subscription receives payloads
      """
      "samples":2,"source_at":"2000-01-01T00:00:00+00:00","tenant":"acme","total":30
      "samples":2,"source_at":"2000-01-01T00:00:00+00:00","tenant":"beta","total":300
      """

    Examples:
      | cluster_size | timestamp_source | rate | width |
      | 1            | NOW              | 0.01 | 30ms  |
      | 3            | NOW              | 0.01 | 30ms  |
      | 1            | NOW              | 4.0  | 12s   |
      | 3            | NOW              | 4.0  | 12s   |
      | 1            | AT occurred_at   | 0.01 | 30ms  |
      | 3            | AT occurred_at   | 0.01 | 30ms  |
      | 1            | AT occurred_at   | 4.0  | 12s   |
      | 3            | AT occurred_at   | 4.0  | 12s   |

  @logical_ingestion_time
  Scenario Outline: Admission retains exactly 256 reached logical tick windows with inclusive SKEW
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD <period> SKEW <period>;
      CREATE SCHEMA request ( sequence I64, occurred_at DATETIME );
      CREATE SCHEMA observation ( sequence I64, occurred_at DATETIME, observed_at DATETIME );
      CREATE WIRE JSON SCHEMA request_wire MODE STRICT ( sequence integer, occurred_at string );
      CREATE CODEC request_codec FROM WIRE JSON SCHEMA request_wire TO SCHEMA request
        ENCODE occurred_at AS RFC3339;
      CREATE RELAY observations SCHEMA observation UNBRANCHED;
      CREATE VHOST edge retained-clock-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE ENDPOINT clock_ingress ON edge PATH '/clock' TYPE HTTP;
      CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING request_codec
        TIMESTAMP AT occurred_at
        TO observations INHERIT ALL SET observed_at = now()
        UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INGESTOR clock_source FROM ENDPOINT clock_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING request_codec
        TIMESTAMP NOW
        TO observations INHERIT ALL SET observed_at = now()
        UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION clock_observations TO observations;
      """
    And domain clock progress for domain "{{domain}}" is paused before delivery
    When these NSPL commands are executed on the leader node
      """
      START AT '2000-01-01T00:00:00Z' TIME RATE <rate>;
      """
    Then within "5s" domain clock progress for domain "{{domain}}" reaches the delivery pause
    And within "45s" admission at host "retained-clock-{{test_id}}.example.com" retains 256 positions from "2000-01-01T00:00:00Z" with period "<period>"
    When domain clock progress for domain "{{domain}}" resumes

    Examples:
      | cluster_size | rate | period |
      | 1            | 0.01 | 1ms    |
      | 3            | 0.01 | 1ms    |
      | 1            | 4.0  | 400ms  |
      | 3            | 4.0  | 400ms  |

  @logical_ingestion_time
  Scenario Outline: Buffered TIMESTAMP NOW is selected when quiesced intake is delivered
    Given a <cluster_size> node nervix cluster is started
    And the entity gate for domain "{{domain}}" pauses after engagement
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 1s SKEW 1s;
      CREATE SCHEMA request ( sequence I64, occurred_at DATETIME );
      CREATE SCHEMA observation (
        sequence I64, occurred_at DATETIME, observed_at DATETIME, samples I64
      );
      CREATE WIRE JSON SCHEMA request_wire MODE STRICT ( sequence integer, occurred_at string );
      CREATE CODEC request_codec FROM WIRE JSON SCHEMA request_wire TO SCHEMA request
        ENCODE occurred_at AS RFC3339;
      CREATE CODEC delivery_codec FROM WIRE JSON SCHEMA request_wire TO SCHEMA request
        ENCODE occurred_at AS RFC3339;
      CREATE RELAY buffered_events SCHEMA request UNBRANCHED;
      CREATE RELAY observations SCHEMA observation UNBRANCHED;
      CREATE VHOST edge quiesced-clock-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE ENDPOINT clock_ingress ON edge PATH '/clock' TYPE HTTP;
      CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING request_codec
        TIMESTAMP NOW
        TO buffered_events INHERIT ALL
        UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INGESTOR clock_source FROM ENDPOINT clock_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING request_codec
        TIMESTAMP NOW
        TO observations INHERIT ALL SET observed_at = now(), samples = 1
        UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR delivered FROM buffered_events
        WIDTH 2s DURATION STEP 2s DURATION UNBRANCHED
        TO observations SET sequence = FIRST(input.sequence),
          occurred_at = FIRST(input.occurred_at),
          observed_at = FIRST(input.occurred_at), samples = COUNT(input.sequence)
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION clock_observations TO observations;
      START AT '2000-01-01T00:00:00Z' TIME RATE 4.0;
      """
    When these NSPL commands begin executing in the background
      """
      ALTER INGESTOR source SET DECODE USING delivery_codec;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    And within "5s" node "node-1" eventually reports describe ingestor "source" as "status: quiesced"
    When http payload is posted to host "quiesced-clock-{{test_id}}.example.com" path "/events"
      """
      {"sequence":1,"occurred_at":"1999-01-01T00:00:00Z"}
      """
    Then within "5s" node "node-1" eventually reports describe ingestor "source" as "nervix_ingestor_quiesce_buffered_records: 1"
    And within "5s" the clock at host "quiesced-clock-{{test_id}}.example.com" advances by "4s" and is saved as "release_time"
    When the entity gate pause for domain "{{domain}}" is released
    Then the background NSPL execution succeeds
    When http payload is posted to host "quiesced-clock-{{test_id}}.example.com" path "/events"
      """
      {"sequence":2,"occurred_at":"1999-01-01T00:00:00Z"}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "samples":2,"sequence":1
      """
    And the last relay subscription payload contains '"occurred_at":"1999-01-01T00:00:00+00:00"'

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
