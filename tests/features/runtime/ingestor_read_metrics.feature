Feature: Ingestor read interactions

  Scenario Outline: Broker read metrics count raw payloads before filtering on <cluster_size> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "read_metrics_{{test_id}}" exists with 1 partitions
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR measured
        FROM broker TOPIC 'read_metrics_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'read_metrics_{{test_id}}' START AT EARLIEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING event_codec
        FILTER WHERE input.user_id > 1
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION events_subscription TO events;
      START;
      """
    And these Kafka messages are rapidly published to topic "read_metrics_{{test_id}}"
      """
      {"user_id":1}
      {"user_id":2}
      {"user_id":3}
      """
    Then within "30s" the relay subscription receives payloads
      """
      {"user_id":2}
      {"user_id":3}
      """
    And within "30s" DESCRIBE INGESTOR "measured" on the leader node contains
      """
      source:
      """
    When these Plexus commands are executed
      """
      DESCRIBE INGESTOR measured;
      """
    Then the last command output owner is saved as placeholder "read_owner"
    And node "{{read_owner}}" observability metric "nervix_ingestor_read_payloads_total" with labels eventually equals 3
      """
      domain="{{domain}}"
      ingestor="measured"
      connector="KAFKA"
      entity="TOPIC"
      """
    When these Plexus commands are executed
      """
      DESCRIBE RELAY events;
      """
    Then the last command output owner is saved as placeholder "relay_owner"
    And node "{{relay_owner}}" observability metric "nervix_messages_total" with labels eventually equals 2
      """
      domain="{{domain}}"
      target_kind="RELAY"
      target="events"
      direction="received"
      relay="events"
      """
    And node "{{read_owner}}" observability metric "nervix_ingestor_read_bytes_total" with labels eventually equals 39
      """
      ingestor="measured"
      connector="KAFKA"
      entity="TOPIC"
      """
    And node "{{read_owner}}" observability metric "nervix_ingestor_reads_total" with labels eventually reaches at least 1
      """
      ingestor="measured"
      outcome="delivered"
      connector="KAFKA"
      entity="TOPIC"
      """
    And within "30s" DESCRIBE INGESTOR "measured" on the leader node contains
      """
      messages_total sent relay=events
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Paced HTTP polls record empty and failed outcomes and duration on <cluster_size> nodes
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE HTTP CLIENT receiver_api CONFIG {
        'endpoint' = '{{http_receiver.api}}/poll', 'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST INGESTOR polled FROM receiver_api METHOD 'GET' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then HTTP receiver "api" eventually receives at least 1 requests
    And within "30s" DESCRIBE INGESTOR "polled" on the leader node contains
      """
      source:
      """
    When these Plexus commands are executed
      """
      DESCRIBE INGESTOR polled;
      """
    Then the last command output owner is saved as placeholder "poll_owner"
    And node "{{poll_owner}}" observability metric "nervix_ingestor_reads_total" with labels eventually reaches at least 1
      """
      ingestor="polled"
      connector="HTTP"
      entity="REQUEST"
      outcome="empty"
      """
    And node "{{poll_owner}}" observability metric "nervix_ingestor_poll_duration_seconds_count" with labels eventually reaches at least 1
      """
      ingestor="polled"
      connector="HTTP"
      entity="REQUEST"
      """
    And node "{{poll_owner}}" observability metric "nervix_ingestor_read_payloads_total" with labels eventually equals 0
      """
      ingestor="polled"
      connector="HTTP"
      entity="REQUEST"
      """

    Given HTTP receiver "api" answers unscripted requests with "respond 500"
    Then node "{{poll_owner}}" observability metric "nervix_ingestor_reads_total" with labels eventually reaches at least 1
      """
      ingestor="polled"
      connector="HTTP"
      entity="REQUEST"
      outcome="failed"
      """
    Given HTTP receiver "api" answers unscripted requests with "respond 204"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Acknowledged unfolding refusals are counted before replay on <cluster_size> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "read_refusal_{{test_id}}" exists with 1 partitions
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( user_id I64 );
      CREATE JAQ JSON CODEC event_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR measured
        FROM broker TOPIC 'read_refusal_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'read_refusal_{{test_id}}' START AT EARLIEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION events_subscription TO events;
      """
    And extension execution is saturated on every node
    And Kafka message is published to topic "read_refusal_{{test_id}}"
      """
      {"user_id":42}
      """
    And these Plexus commands are executed
      """
      START;
      """
    Then within "30s" the active session observes a server error containing
      """
      the node's bounded execution did not unfold the payload
      """
    When these Plexus commands are executed
      """
      DESCRIBE INGESTOR measured;
      """
    Then the last command output owner is saved as placeholder "read_owner"
    And node "{{read_owner}}" observability metric "nervix_ingestor_unfolding_refused_total" with labels eventually reaches at least 1
      """
      ingestor="measured"
      connector="KAFKA"
      entity="TOPIC"
      """
    When extension execution is released on every node
    Then within "30s" the relay subscription receives a payload
      """
      "user_id":42
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Endpoint unfolding refusals count only the request refused on <cluster_size> nodes
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( user_id I64 );
      CREATE JAQ JSON CODEC event_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge refusal-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR measured FROM intake MODE NO ACK
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION event_subscription TO events;
      START;
      """
    When extension execution is saturated on every node
    And endpoint request "refused" is posted on node "node-1" host "refusal-{{test_id}}.example.com" path "/events"
      """
      {"user_id":42}
      """
    Then node "node-1" observability metric "nervix_ingestor_unfolding_refused_total" with labels eventually equals 1
      """
      domain="{{domain}}"
      ingestor="measured"
      connector="HTTP"
      entity="ENDPOINT"
      """
    When extension execution is released on every node
    And endpoint request "accepted" is posted on node "node-1" host "refusal-{{test_id}}.example.com" path "/events"
      """
      {"user_id":42}
      """
    Then within "30s" the relay subscription receives a payload
      """
      "user_id":42
      """
    And node "node-1" observability metric "nervix_ingestor_unfolding_refused_total" with labels eventually equals 1
      """
      ingestor="measured"
      connector="HTTP"
      entity="ENDPOINT"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
