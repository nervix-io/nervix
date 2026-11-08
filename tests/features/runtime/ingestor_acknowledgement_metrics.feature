Feature: Ingestor acknowledgement and lifecycle interactions

  An ingestor counts how each acknowledgement tree it waited on ended, times every acknowledgement
  and rejection its transport answered, counts every suspension of its connector by its cause and
  every resume by its outcome, counts the positions it stopped before its transport confirmed them,
  and shows whether its current task has a transient failure.

  Scenario Outline: A Kafka ingestor counts acknowledged trees and timed transport acknowledgements on <cluster_size> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "ack_outcomes_{{test_id}}" exists with 1 partitions
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR measured
        FROM broker TOPIC 'ack_outcomes_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'ack_outcomes_{{test_id}}' START AT EARLIEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION events_subscription TO events;
      START;
      """
    And these Kafka messages are rapidly published to topic "ack_outcomes_{{test_id}}"
      """
      {"user_id":1}
      {"user_id":2}
      {"user_id":3}
      """
    Then within "30s" the relay subscription receives payloads
      """
      {"user_id":1}
      {"user_id":2}
      {"user_id":3}
      """
    When these Plexus commands are executed
      """
      DESCRIBE INGESTOR measured;
      """
    Then the last command output owner is saved as placeholder "ingestor_owner"
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually equals 3
      """
      domain="{{domain}}"
      ingestor="measured"
      connector="KAFKA"
      entity="TOPIC"
      outcome="acknowledged"
      """
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_transport_acknowledgements_total" with labels eventually equals 3
      """
      ingestor="measured"
      connector="KAFKA"
      entity="TOPIC"
      operation="acknowledge"
      outcome="succeeded"
      """
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_transport_acknowledgement_duration_seconds_count" with labels eventually equals 3
      """
      ingestor="measured"
      connector="KAFKA"
      entity="TOPIC"
      operation="acknowledge"
      """
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_resumes_total" with labels eventually equals 1
      """
      ingestor="measured"
      connector="KAFKA"
      entity="TOPIC"
      outcome="ready"
      """
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_transient_failure" with labels eventually equals 0
      """
      ingestor="measured"
      connector="KAFKA"
      entity="TOPIC"
      """
    And within "30s" DESCRIBE INGESTOR "measured" on the leader node contains
      """
      entity="TOPIC",outcome="acknowledged"} total=3
      """
    And within "30s" DESCRIBE INGESTOR "measured" on the leader node contains
      """
      entity="TOPIC",operation="acknowledge",outcome="succeeded"} total=3
      """
    And within "30s" DESCRIBE INGESTOR "measured" on the leader node contains
      """
      entity="TOPIC",operation="acknowledge"} count=3
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # A window keeps a row it retains until the window fills, and its acknowledgement share parks
  # while it waits, so the tree reports nothing until the ACK TIMEOUT expires. The placement keeps
  # the window on the ingestor's node, where no remote owner reports progress on the parked share.
  Scenario Outline: An expired ACK TIMEOUT is counted as a timeout and not as a failed tree on <cluster_size> nodes
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "ack_timeout_{{test_id}}" exists with 1 partitions
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( user_id I64 );
      CREATE SCHEMA pair ( first_user I64, samples I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RELAY pairs SCHEMA pair UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR measured
        FROM broker TOPIC 'ack_timeout_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'ack_timeout_{{test_id}}' START AT EARLIEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 2s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR pairing FROM events
        WIDTH 2 MESSAGES STEP 2 MESSAGES UNBRANCHED
        TO pairs SET first_user = FIRST(input.user_id), samples = COUNT(input.user_id)
        ON MESSAGE ERROR LOG;
      CREATE PLACEMENT keep_window_local FROM measured TO pairing REQUIRE COLOCATION;
      CREATE SUBSCRIPTION pairs_subscription TO pairs;
      START;
      """
    And Kafka message is published to topic "ack_timeout_{{test_id}}"
      """
      {"user_id":7}
      """
    # The rejected record is read again and fills the window, which acknowledges both of its rows.
    Then within "30s" the relay subscription receives a payload
      """
      "samples":2
      """
    And within "30s" the active session observes a server error containing
      """
      ingestor ACK chain for ingestor 'measured' in domain '{{domain}}' timed out: no acknowledgement progress within ACK TIMEOUT 2s
      """
    When these Plexus commands are executed
      """
      DESCRIBE INGESTOR measured;
      """
    Then the last command output owner is saved as placeholder "ingestor_owner"
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually equals 1
      """
      ingestor="measured"
      connector="KAFKA"
      entity="TOPIC"
      outcome="timed_out"
      """
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_transport_acknowledgements_total" with labels eventually equals 1
      """
      ingestor="measured"
      connector="KAFKA"
      entity="TOPIC"
      operation="reject"
      outcome="succeeded"
      """
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually equals 1
      """
      ingestor="measured"
      connector="KAFKA"
      entity="TOPIC"
      outcome="acknowledged"
      """
    And within "30s" DESCRIBE INGESTOR "measured" on the leader node contains
      """
      entity="TOPIC",outcome="timed_out"} total=1
      """
    And within "30s" DESCRIBE INGESTOR "measured" on the leader node contains
      """
      entity="TOPIC",outcome="failed"} total=0
      """
    And within "30s" DESCRIBE INGESTOR "measured" on the leader node contains
      """
      entity="TOPIC",operation="reject"} count=1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A quiesce suspends an ingestor's connector and its release resumes it on <cluster_size> nodes
    Given Kafka is running
    And entity gate deadline is configured as "30s"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "quiesce_suspend_{{test_id}}" exists with 1 partitions
    And the entity gate for domain "{{domain}}" pauses after engagement
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE WIRE JSON CODEC event_codec_v2 FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR measured
        FROM broker TOPIC 'quiesce_suspend_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'quiesce_suspend_{{test_id}}' START AT EARLIEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION events_subscription TO events;
      START;
      """
    And Kafka message is published to topic "quiesce_suspend_{{test_id}}"
      """
      {"user_id":1}
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"user_id":1}
      """
    When these Plexus commands are executed
      """
      DESCRIBE INGESTOR measured;
      """
    Then the last command output owner is saved as placeholder "ingestor_owner"
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_suspends_total" with labels eventually equals 0
      """
      ingestor="measured"
      connector="KAFKA"
      entity="TOPIC"
      cause="quiesce"
      """
    When these Plexus commands begin executing in the background
      """
      ALTER KAFKA TOPIC INGESTOR measured SET DECODE USING event_codec_v2;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    And within "30s" DESCRIBE INGESTOR "measured" on the leader node contains
      """
      quiesce state: entity hold
      """
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_suspends_total" with labels eventually equals 1
      """
      ingestor="measured"
      connector="KAFKA"
      entity="TOPIC"
      cause="quiesce"
      """
    When the entity gate pause for domain "{{domain}}" is released
    Then the background Plexus execution succeeds
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_resumes_total" with labels eventually reaches at least 2
      """
      ingestor="measured"
      connector="KAFKA"
      entity="TOPIC"
      outcome="ready"
      """
    And within "30s" DESCRIBE INGESTOR "measured" on the leader node contains
      """
      entity="TOPIC",cause="quiesce"} total=1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # The stallable endpoint drops the consumer's connection and holds every new one unanswered, so
  # the read fails, the connector needs a resume and every resume fails until the endpoint resumes.
  Scenario Outline: A RabbitMQ ingestor that loses its broker counts failed resumes and shows its transient failure on <cluster_size> nodes
    Given RabbitMQ is running
    And a stallable RabbitMQ endpoint is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "lost_broker_{{test_id}}" exists
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RABBITMQ CLIENT rabbit_main CONFIG {
        'addr' = '{{rabbitmq_stallable_addr}}',
        'connect_timeout_ms' = '1000'
      };
      CREATE RABBITMQ QUEUE INGESTOR rabbit_in
        FROM rabbit_main QUEUE 'lost_broker_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION events_subscription TO events;
      START;
      """
    Then RabbitMQ queue "lost_broker_{{test_id}}" eventually has 1 consumers
    When RabbitMQ message is published to queue "lost_broker_{{test_id}}"
      """
      {"user_id":1}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":1}
      """
    When these Plexus commands are executed
      """
      DESCRIBE INGESTOR rabbit_in;
      """
    Then the last command output owner is saved as placeholder "ingestor_owner"
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_transient_failure" with labels eventually equals 0
      """
      ingestor="rabbit_in"
      connector="RABBITMQ"
      entity="QUEUE"
      """
    When the stallable endpoint "rabbitmq" is paused
    And the stallable endpoint "rabbitmq" resets its connections
    Then node "{{ingestor_owner}}" observability metric "nervix_ingestor_suspends_total" with labels eventually equals 1
      """
      ingestor="rabbit_in"
      connector="RABBITMQ"
      entity="QUEUE"
      cause="read_failure"
      """
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_resumes_total" with labels eventually reaches at least 1
      """
      ingestor="rabbit_in"
      connector="RABBITMQ"
      entity="QUEUE"
      outcome="failed"
      """
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_transient_failure" with labels eventually equals 1
      """
      ingestor="rabbit_in"
      connector="RABBITMQ"
      entity="QUEUE"
      """
    When the stallable endpoint "rabbitmq" is resumed
    Then RabbitMQ queue "lost_broker_{{test_id}}" eventually has 1 consumers
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_resumes_total" with labels eventually equals 2
      """
      ingestor="rabbit_in"
      connector="RABBITMQ"
      entity="QUEUE"
      outcome="ready"
      """
    # A broker ingestor's transient status clears once its transport confirms a position again.
    When RabbitMQ message is published to queue "lost_broker_{{test_id}}"
      """
      {"user_id":2}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":2}
      """
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_transient_failure" with labels eventually equals 0
      """
      ingestor="rabbit_in"
      connector="RABBITMQ"
      entity="QUEUE"
      """
    And within "30s" DESCRIBE INGESTOR "rabbit_in" on the leader node contains
      """
      entity="QUEUE",cause="read_failure"} total=1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # The emitter's fault mode fails every tree the ingestor waits on, so each delivery is rejected and
  # requeued until the fault clears. Its stall mode then keeps a later delivery's tree alive,
  # unconfirmed, until the domain stops the ingestor.
  Scenario Outline: A RabbitMQ ingestor counts failed trees and the positions it stopped before confirming on <cluster_size> nodes
    Given RabbitMQ is running
    And HTTP receiver "api" is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "failed_trees_{{test_id}}" exists
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( user_id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RABBITMQ CLIENT rabbit_main CONFIG { 'addr' = '{{rabbitmq_addr}}' };
      CREATE HTTP CLIENT receiver_api CONFIG { 'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000 };
      CREATE RABBITMQ QUEUE INGESTOR rabbit_in
        FROM rabbit_main QUEUE 'failed_trees_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 60s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP REQUEST EMITTER forwarded FROM events
        TO receiver_api METHOD 'POST' PATH '/events'
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION events_subscription TO events;
      START;
      """
    Then RabbitMQ queue "failed_trees_{{test_id}}" eventually has 1 consumers
    When these Plexus commands are executed
      """
      DESCRIBE INGESTOR rabbit_in;
      """
    Then the last command output owner is saved as placeholder "ingestor_owner"
    When emitter "forwarded" enters fault mode
    And RabbitMQ message is published to queue "failed_trees_{{test_id}}"
      """
      {"user_id":1}
      """
    Then node "{{ingestor_owner}}" observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually reaches at least 1
      """
      ingestor="rabbit_in"
      connector="RABBITMQ"
      entity="QUEUE"
      outcome="failed"
      """
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_transport_acknowledgements_total" with labels eventually reaches at least 1
      """
      ingestor="rabbit_in"
      connector="RABBITMQ"
      entity="QUEUE"
      operation="reject"
      outcome="succeeded"
      """
    When emitter "forwarded" leaves fault mode
    Then HTTP receiver "api" eventually receives at least 1 request
    And RabbitMQ queue "failed_trees_{{test_id}}" eventually has 0 unacknowledged deliveries
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually equals 1
      """
      ingestor="rabbit_in"
      connector="RABBITMQ"
      entity="QUEUE"
      outcome="acknowledged"
      """
    When emitter "forwarded" enters stall mode
    And RabbitMQ message is published to queue "failed_trees_{{test_id}}"
      """
      {"user_id":2}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":2}
      """
    And RabbitMQ queue "failed_trees_{{test_id}}" eventually has 1 unacknowledged deliveries
    When these Plexus commands are executed
      """
      STOP;
      """
    Then RabbitMQ queue "failed_trees_{{test_id}}" eventually has 0 consumers
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_unconfirmed_stop_positions_total" with labels eventually equals 1
      """
      ingestor="rabbit_in"
      connector="RABBITMQ"
      entity="QUEUE"
      """
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually equals 1
      """
      ingestor="rabbit_in"
      connector="RABBITMQ"
      entity="QUEUE"
      outcome="shutdown"
      """
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually equals 0
      """
      ingestor="rabbit_in"
      connector="RABBITMQ"
      entity="QUEUE"
      outcome="timed_out"
      """
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_transient_failure" with labels eventually equals 0
      """
      ingestor="rabbit_in"
      connector="RABBITMQ"
      entity="QUEUE"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
