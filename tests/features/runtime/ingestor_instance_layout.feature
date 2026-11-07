@ingestor_instances @ingestor_instance_layout
Feature: Kafka domain offsets survive intake unit layout changes
  Changing a source count preserves committed domain positions and publishes the new units.

  @ingestor_instance_lifecycle
  Scenario Outline: Newly placed instances retain a canonical future start through STOP and START on <nodes> nodes
    Given Kafka is running
    And runtime replication is configured with replica count <replicas> and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <nodes> node nervix cluster is started
    And Kafka topic "future_layout_{{test_id}}" exists with 3 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( partition I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( partition integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR spread_ingestor
        FROM broker TOPIC 'future_layout_{{test_id}}' OFFSET BY DOMAIN INSTANCES 1
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observing TO events;
      START AT '2100-01-01T00:00:00Z';
      """
    Then within "60s" every instance of ingestor "spread_ingestor" reports running and ready on its scheduled owner
    When these Plexus commands are executed on the leader node
      """
      STOP;
      """
    When Kafka message is published to topic "future_layout_{{test_id}}" partition 0
      """
      {"partition":0}
      """
    When Kafka message is published to topic "future_layout_{{test_id}}" partition 1
      """
      {"partition":1}
      """
    When Kafka message is published to topic "future_layout_{{test_id}}" partition 2
      """
      {"partition":2}
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER KAFKA TOPIC INGESTOR spread_ingestor
        SET FROM broker TOPIC 'future_layout_{{test_id}}' OFFSET BY DOMAIN INSTANCES 3
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND;
      START;
      """
    Then within "60s" every instance of ingestor "spread_ingestor" reports running and ready on its scheduled owner
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "partition":0
      "partition":1
      "partition":2
      """
    And the relay subscription does not receive a payload within "2s"

    Examples:
      | nodes | replicas |
      | 1     | 0        |
      | 3     | 1        |

  @ingestor_instance_lifecycle
  Scenario Outline: A stopped domain-offset source changes its unit count on <nodes> nodes
    Given the production sticky scheduler is configured
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE KAFKA TOPIC INGESTOR spread_ingestor
        FROM broker TOPIC 'events' OFFSET BY DOMAIN INSTANCES 3
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER KAFKA TOPIC INGESTOR spread_ingestor
        SET FROM broker TOPIC 'events' OFFSET BY DOMAIN INSTANCES 4
        MODE NO ACK ON QUIESCE SUSPEND;
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 4 instances of ingestor "spread_ingestor" on <owners> distinct owners

    Examples:
      | nodes | owners |
      | 1     | 1      |
      | 3     | 3      |

  @ingestor_instance_lifecycle
  Scenario Outline: A domain-offset source retains acknowledged positions through live and stopped layout changes on <nodes> nodes
    Given Kafka is running
    And runtime replication is configured with replica count <replicas> and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <nodes> node nervix cluster is started
    And Kafka topic "layout_input_{{test_id}}" exists with 6 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( phase I64, partition I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( phase integer, partition integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR spread_ingestor
        FROM broker TOPIC 'layout_input_{{test_id}}' OFFSET BY DOMAIN INSTANCES 3
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observing TO events;
      START AT '2000-01-01T00:00:00Z';
      """
    Then within "60s" every instance of ingestor "spread_ingestor" reports running and ready on its scheduled owner
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 0
      """
      {"phase":0,"partition":0}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 1
      """
      {"phase":0,"partition":1}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 2
      """
      {"phase":0,"partition":2}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 3
      """
      {"phase":0,"partition":3}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 4
      """
      {"phase":0,"partition":4}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 5
      """
      {"phase":0,"partition":5}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "phase":0 | "partition":0
      "phase":0 | "partition":1
      "phase":0 | "partition":2
      "phase":0 | "partition":3
      "phase":0 | "partition":4
      "phase":0 | "partition":5
      """
    And within "5s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "events" across physical nodes totals 6
    When these Plexus commands are executed on the leader node
      """
      ALTER KAFKA TOPIC INGESTOR spread_ingestor
        SET FROM broker TOPIC 'layout_input_{{test_id}}' OFFSET BY DOMAIN INSTANCES 4
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 4 instances of ingestor "spread_ingestor" on <owners> distinct owners
    And within "60s" every instance of ingestor "spread_ingestor" reports running and ready on its scheduled owner
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 0
      """
      {"phase":1,"partition":0}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 1
      """
      {"phase":1,"partition":1}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 2
      """
      {"phase":1,"partition":2}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 3
      """
      {"phase":1,"partition":3}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 4
      """
      {"phase":1,"partition":4}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 5
      """
      {"phase":1,"partition":5}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "phase":1 | "partition":0
      "phase":1 | "partition":1
      "phase":1 | "partition":2
      "phase":1 | "partition":3
      "phase":1 | "partition":4
      "phase":1 | "partition":5
      """
    And within "5s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "events" across physical nodes totals 12
    When these Plexus commands are executed on the leader node
      """
      STOP;
      ALTER KAFKA TOPIC INGESTOR spread_ingestor
        SET FROM broker TOPIC 'layout_input_{{test_id}}' OFFSET BY DOMAIN INSTANCES 2
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 2 instances of ingestor "spread_ingestor" on <shrink_owners> distinct owners
    And within "60s" every instance of ingestor "spread_ingestor" reports running and ready on its scheduled owner
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 0
      """
      {"phase":2,"partition":0}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 1
      """
      {"phase":2,"partition":1}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 2
      """
      {"phase":2,"partition":2}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 3
      """
      {"phase":2,"partition":3}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 4
      """
      {"phase":2,"partition":4}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 5
      """
      {"phase":2,"partition":5}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "phase":2 | "partition":0
      "phase":2 | "partition":1
      "phase":2 | "partition":2
      "phase":2 | "partition":3
      "phase":2 | "partition":4
      "phase":2 | "partition":5
      """
    And within "5s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "events" across physical nodes totals 18
    When these Plexus commands are executed on the leader node
      """
      ALTER KAFKA TOPIC INGESTOR spread_ingestor
        SET FROM broker TOPIC 'layout_input_{{test_id}}' OFFSET BY DOMAIN INSTANCES 4
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
        ON QUIESCE SUSPEND;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 4 instances of ingestor "spread_ingestor" on <owners> distinct owners
    And within "60s" every instance of ingestor "spread_ingestor" reports running and ready on its scheduled owner
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 0
      """
      {"phase":3,"partition":0}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 1
      """
      {"phase":3,"partition":1}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 2
      """
      {"phase":3,"partition":2}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 3
      """
      {"phase":3,"partition":3}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 4
      """
      {"phase":3,"partition":4}
      """
    When Kafka message is published to topic "layout_input_{{test_id}}" partition 5
      """
      {"phase":3,"partition":5}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "phase":3 | "partition":0
      "phase":3 | "partition":1
      "phase":3 | "partition":2
      "phase":3 | "partition":3
      "phase":3 | "partition":4
      "phase":3 | "partition":5
      """
    And within "5s" DESCRIBE DOMAIN section "input_output" metric "messages_total" "sent" relay "events" across physical nodes totals 24
    Then the relay subscription does not receive a payload within "2s"

    Examples:
      | nodes | replicas | owners | shrink_owners |
      | 1     | 0        | 1      | 1             |
      | 3     | 1        | 3      | 2             |
