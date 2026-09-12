@physical_infrastructure_deadlines
Feature: Physical infrastructure deadlines under domain pacing

  @physical_poll_cancellation
  Scenario Outline: Cancelling an external poll stays physical across extreme clock generations
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
      CREATE SCHEMA physical_poll_record (
        user_id I64
      );
      CREATE WIRE JSON SCHEMA physical_poll_wire MODE STRICT (
        user_id integer
      );
      CREATE CODEC physical_poll_codec
        FROM WIRE JSON SCHEMA physical_poll_wire
        TO SCHEMA physical_poll_record;
      CREATE RELAY physical_poll_records SCHEMA physical_poll_record UNBRANCHED;
      CREATE CLIENT physical_poll_source
        TYPE HTTP
        CONFIG {
          'endpoint' = '{{mock_http_addr}}/clock-source/{{test_id}}?delay_ms=10000',
          'method' = 'GET',
          'timeout_ms' = 30000
        };
      CREATE INGESTOR physical_poll_reader
        FROM HTTP physical_poll_source EVERY 1h
        ON QUIESCE SUSPEND DECODE USING physical_poll_codec
        TIMESTAMP NOW
        TO physical_poll_records
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 0.0001;
      """
    Then within "2s" clock source recorder "{{test_id}}" records 1 requests
    And within "1500ms" these NSPL commands complete on the leader node
      """
      STOP;
      """
    When these NSPL commands are executed on the leader node
      """
      START AT '2010-01-01T00:00:00Z' TIME RATE 100.0;
      """
    Then within "2s" clock source recorder "{{test_id}}" records 2 requests
    And within "1500ms" these NSPL commands complete on the leader node
      """
      STOP;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @physical_poll_cancellation
  Scenario Outline: Cancelling an external Prometheus query stays physical in a fast domain
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
      CREATE SCHEMA physical_prometheus_sample (
        source STRING,
        value F64,
        timestamp STRING,
        due STRING
      );
      CREATE WIRE JSON SCHEMA physical_prometheus_wire MODE STRICT (
        source string,
        value number,
        timestamp string,
        due string
      );
      CREATE CODEC physical_prometheus_codec
        FROM WIRE JSON SCHEMA physical_prometheus_wire
        TO SCHEMA physical_prometheus_sample;
      CREATE RELAY physical_prometheus_samples
        SCHEMA physical_prometheus_sample UNBRANCHED;
      CREATE CLIENT physical_prometheus_source
        TYPE PROMETHEUS
        CONFIG {
          'addr' = '{{mock_http_addr}}/prometheus-clock-source/{{test_id}}/10000',
          'timeout_ms' = 30000
        };
      CREATE INGESTOR physical_prometheus_reader
        FROM PROMETHEUS physical_prometheus_source QUERY 'vector(42.5)' EVERY 1s
        ON QUIESCE SUSPEND DECODE USING physical_prometheus_codec
        TIMESTAMP NOW
        TO physical_prometheus_samples
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 100.0;
      """
    Then within "2s" clock source recorder "{{test_id}}" records 1 requests
    And within "1500ms" these NSPL commands complete on the leader node
      """
      STOP;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @physical_request_timeout
  Scenario Outline: HTTP request timeout stays physical while logical polling cadence scales
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
      CREATE SCHEMA physical_timeout_record (
        user_id I64
      );
      CREATE WIRE JSON SCHEMA physical_timeout_wire MODE STRICT (
        user_id integer
      );
      CREATE CODEC physical_timeout_codec
        FROM WIRE JSON SCHEMA physical_timeout_wire
        TO SCHEMA physical_timeout_record;
      CREATE RELAY physical_timeout_records SCHEMA physical_timeout_record UNBRANCHED;
      CREATE CLIENT physical_timeout_source
        TYPE HTTP
        CONFIG {
          'endpoint' = '{{mock_http_addr}}/clock-source/{{test_id}}?delay_ms=10000',
          'method' = 'GET',
          'timeout_ms' = 250
        };
      CREATE INGESTOR physical_timeout_reader
        FROM HTTP physical_timeout_source EVERY 1s
        ON QUIESCE SUSPEND DECODE USING physical_timeout_codec
        TIMESTAMP NOW
        TO physical_timeout_records
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 0.0001;
      """
    Then within "2s" clock source recorder "{{test_id}}" records 1 requests
    When physical time passes for "700ms"
    Then within "50ms" clock source recorder "{{test_id}}" records 1 requests
    And within "1500ms" these NSPL commands complete on the leader node
      """
      STOP;
      """
    Given clock source recorder "{{test_id}}" is reset
    When these NSPL commands are executed on the leader node
      """
      START AT '2010-01-01T00:00:00Z' TIME RATE 100.0;
      """
    Then within "1500ms" clock source recorder "{{test_id}}" records at least 3 requests
    And the first 3 requests recorded by clock source recorder "{{test_id}}" are separated by at least "200ms"
    And within "1500ms" these NSPL commands complete on the leader node
      """
      STOP;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @physical_required_wait
  Scenario Outline: Required materialized state has no physical lifetime and preserves each branch
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 1s;
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA physical_wait_notification (
        tenant STRING,
        id STRING,
        source STRING
      );
      CREATE SCHEMA physical_wait_preference (
        tenant STRING,
        theme STRING
      );
      CREATE WIRE JSON SCHEMA physical_wait_notification_wire MODE STRICT (
        tenant string,
        id string,
        source string
      );
      CREATE WIRE JSON SCHEMA physical_wait_preference_wire MODE STRICT (
        tenant string,
        theme string
      );
      CREATE CODEC physical_wait_notification_codec
        FROM WIRE JSON SCHEMA physical_wait_notification_wire
        TO SCHEMA physical_wait_notification;
      CREATE CODEC physical_wait_preference_codec
        FROM WIRE JSON SCHEMA physical_wait_preference_wire
        TO SCHEMA physical_wait_preference;
      CREATE SCHEMA physical_wait_tenant (
        tenant STRING
      );
      CREATE BRANCH physical_wait_by_tenant SCHEMA physical_wait_tenant TTL 1h;
      CREATE RELAY physical_wait_preferences
        SCHEMA physical_wait_preference BRANCHED BY physical_wait_by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY physical_wait_pending
        SCHEMA physical_wait_notification BRANCHED BY physical_wait_by_tenant;
      CREATE RELAY physical_wait_output
        SCHEMA physical_wait_notification BRANCHED BY physical_wait_by_tenant;
      CREATE VHOST edge physical-wait-{{test_id}}.example.com;
      CREATE ENDPOINT physical_wait_state_endpoint
        ON edge
        PATH '/state'
        TYPE HTTP;
      CREATE ENDPOINT physical_wait_input_endpoint
        ON edge
        PATH '/input'
        TYPE HTTP;
      CREATE INGESTOR physical_wait_state_source
        FROM ENDPOINT physical_wait_state_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING physical_wait_preference_codec
        TIMESTAMP NOW
        TO physical_wait_preferences
          INHERIT ALL
          BRANCHED BY physical_wait_by_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR physical_wait_input_source
        FROM ENDPOINT physical_wait_input_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING physical_wait_notification_codec
        TIMESTAMP NOW
        TO physical_wait_pending
          INHERIT ALL
          BRANCHED BY physical_wait_by_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR physical_wait_processor FROM physical_wait_pending
        DEDUPLICATE ON input.id
        MAX TIME 1h
        BRANCHED BY physical_wait_by_tenant
        USING MATERIALIZED STATE physical_wait_preferences REQUIRED WAIT
        TO physical_wait_output
          INHERIT ALL
          SET source = relay_state.physical_wait_preferences.theme
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION physical_wait_subscription TO physical_wait_output;
      START AT '1990-01-01T00:00:00Z' TIME RATE <rate>;
      """
    When http payload is posted to node "node-1" with host "physical-wait-{{test_id}}.example.com" path "/input"
      """
      {"tenant":"acme","id":"a-1","source":"input"}
      """
    And http payload is posted to node "node-1" with host "physical-wait-{{test_id}}.example.com" path "/input"
      """
      {"tenant":"globex","id":"g-1","source":"input"}
      """
    Then the relay subscription does not receive a payload within "1200ms"
    When http payload is posted to node "node-1" with host "physical-wait-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"globex","theme":"globex-theme"}
      """
    And http payload is posted to node "node-1" with host "physical-wait-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"acme","theme":"acme-theme"}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "id":"a-1" | "source":"acme-theme" | "tenant":"acme"
      key={"tenant":"globex"} | "id":"g-1" | "source":"globex-theme" | "tenant":"globex"
      """
    When http payload is posted to node "node-1" with host "physical-wait-{{test_id}}.example.com" path "/input"
      """
      {"tenant":"initech","id":"i-1","source":"input"}
      """
    Then the relay subscription does not receive a payload within "250ms"
    And within "1500ms" these NSPL commands complete on the leader node
      """
      STOP;
      """

    Examples:
      | cluster_size | rate   |
      | 1            | 0.0001 |
      | 1            | 100.0  |
      | 3            | 0.0001 |
      | 3            | 100.0  |

  @physical_ack_keepalive
  Scenario Outline: Delayed acknowledgements stay alive on physical time at extreme domain rates
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 1s;
      """
    And Kafka topic "physical_ack_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA physical_ack_record (
        user_id I64
      );
      CREATE WIRE JSON SCHEMA physical_ack_wire MODE STRICT (
        user_id integer
      );
      CREATE CODEC physical_ack_codec
        FROM WIRE JSON SCHEMA physical_ack_wire
        TO SCHEMA physical_ack_record;
      CREATE RELAY physical_ack_records SCHEMA physical_ack_record UNBRANCHED;
      CREATE CLIENT physical_ack_kafka
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
      CREATE INGESTOR physical_ack_source
        FROM KAFKA physical_ack_kafka TOPIC physical_ack_in_{{test_id}} OFFSET BY CONSUMER GROUP physical_ack_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 500ms RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING physical_ack_codec
        TIMESTAMP NOW
        TO physical_ack_records
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER physical_ack_sink
        FROM physical_ack_records
        TO KAFKA physical_ack_kafka TOPIC physical_ack_out_{{test_id}}
        MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING physical_ack_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START AT '1980-01-01T00:00:00Z' TIME RATE <rate>;
      """
    And emitter "physical_ack_sink" enters stall mode
    And Kafka message is published to topic "physical_ack_in_{{test_id}}"
      """
      {"user_id":77}
      """
    Then the observed broker does not receive a payload within "1200ms"
    When emitter "physical_ack_sink" leaves stall mode
    Then within "4s" the observed broker receives payloads
      """
      {"user_id":77}
      """
    And the observed broker does not receive a payload within "1200ms"

    Examples:
      | cluster_size | rate   |
      | 1            | 0.0001 |
      | 1            | 100.0  |
      | 3            | 0.0001 |
      | 3            | 100.0  |

  @physical_silent_peer
  Scenario: Silent peers obey the same shutdown bound without and after a domain clock
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 1s;
      """
    When a silent peer starts an interconnect handshake with node "node-3"
    And node "node-3" is stopped while timing shutdown
    Then the last cluster operation completes within "12s"
    And node "node-1" eventually observes a stable leader
    When node "node-3" is started
    Then node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    When these NSPL commands are executed on the leader node
      """
      START AT '1970-01-01T00:00:00Z' TIME RATE 0.0001;
      STOP;
      START AT '2030-01-01T00:00:00Z' TIME RATE 100.0;
      """
    And a silent peer starts an interconnect handshake with node "node-3"
    And node "node-3" is stopped while timing shutdown
    Then the last cluster operation completes within "12s"
    And node "node-1" eventually observes a stable leader
