Feature: Kafka ingestion
  Background:
    Given Kafka is running

  Scenario Outline: Kafka ingestor delivers JSON payloads to a subscribed relay
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
        CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_main TOPIC 'notifications_{{test_id}}' OFFSET BY CONSUMER GROUP 'nervix_cucumber_{{test_id}}' INSTANCES <instances> MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_kafka_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    When Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then Kafka consumer group "nervix_cucumber_{{test_id}}" eventually has <instances> consumers
    And the relay subscription receives a payload
      """
      "user_id":42
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | instances | replica_count |
      | 1            | 1         | 0             |
      | 1            | 2         | 0             |
      | 3            | 1         | 0             |
      | 3            | 2         | 0             |
      | 3            | 1         | 1             |
      | 3            | 2         | 1             |

  Scenario Outline: Kafka emitter forwards a multi-chunk unbranched batch with exact count parity
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "forwarding_input_{{test_id}}" exists with 1 partitions
    And Kafka topic "forwarding_output_{{test_id}}" exists with 1 partitions
    And Kafka topic "forwarding_output_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA forwarding_event (
        user_id I64
      );
      CREATE WIRE JSON SCHEMA forwarding_event_wire MODE STRICT (
        user_id integer
      );
      CREATE CODEC forwarding_event_codec
        FROM WIRE JSON SCHEMA forwarding_event_wire
        TO SCHEMA forwarding_event;
      CREATE RELAY forwarding_events SCHEMA forwarding_event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
      CREATE KAFKA TOPIC INGESTOR kafka_forwarding_source
        FROM kafka_main TOPIC 'forwarding_input_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'nervix_cucumber_forwarding_{{test_id}}'
        MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING forwarding_event_codec
        TO forwarding_events
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ATTACHED EMITTER kafka_forward
        FROM forwarding_events
        TO KAFKA kafka_main TOPIC forwarding_output_{{test_id}}
        MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING forwarding_event_codec
        INHERIT ALL
        FLUSH EACH 500ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And 4096 JSON messages with user id 42 are rapidly published to "KAFKA" input "forwarding_input_{{test_id}}"
    Then within "30s" the observed broker receives exactly 4096 messages

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @ingestor_group_programs
  Scenario Outline: Kafka NO_ACK executes ingestor programs once for a collected ingest group
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "grouped_program_input_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed
      """
      CREATE SCHEMA grouped_event (
        user_id I64
      );
      CREATE SCHEMA routed_grouped_event (
        user_id I64,
        group_clock DATETIME
      );
      CREATE WIRE JSON SCHEMA grouped_event_wire MODE STRICT (
        user_id integer
      );
      CREATE CODEC grouped_event_codec
        FROM WIRE JSON SCHEMA grouped_event_wire
        TO SCHEMA grouped_event;
      CREATE RELAY grouped_events SCHEMA routed_grouped_event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
      CREATE KAFKA TOPIC INGESTOR grouped_event_source
        FROM kafka_main TOPIC 'grouped_program_input_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'nervix_cucumber_grouped_program_{{test_id}}'
        MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING grouped_event_codec
        FILTER WHERE now() = now()
        TO grouped_events
        INHERIT ALL
        SET group_clock = now()
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION grouped_events_subscription TO grouped_events;
      """
    And 16 JSON messages with user id 42 are rapidly published to "KAFKA" input "grouped_program_input_{{test_id}}"
    And these NSPL commands are executed
      """
      START;
      """
    Then within "20s" 16 relay subscription payloads share field "group_clock"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Kafka NO_ACK keeps the rest of a collected ingest group when one message fails to decode
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "mixed_group_input_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed
      """
      CREATE SCHEMA mixed_event (
        tenant STRING,
        user_id I64
      );
      CREATE WIRE JSON SCHEMA mixed_event_wire MODE STRICT (
        tenant string,
        user_id integer
      );
      CREATE CODEC mixed_event_codec
        FROM WIRE JSON SCHEMA mixed_event_wire
        TO SCHEMA mixed_event;
      CREATE RELAY mixed_events SCHEMA mixed_event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
      CREATE KAFKA TOPIC INGESTOR mixed_event_source
        FROM kafka_main TOPIC 'mixed_group_input_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'nervix_cucumber_mixed_group_{{test_id}}'
        MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING mixed_event_codec
        TO mixed_events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION mixed_events_subscription TO mixed_events;
      """
    And these Kafka messages are rapidly published to topic "mixed_group_input_{{test_id}}"
      """
      {"tenant":"acme","user_id":1}
      {"tenant":"acme","user_id":"two"}
      {"tenant":"beta","user_id":3}
      {"tenant":"beta","user_id":4}
      """
    And these NSPL commands are executed
      """
      START;
      """
    Then within "20s" the relay subscription receives payloads
      """
      "tenant":"acme","user_id":1
      "tenant":"beta","user_id":3
      "tenant":"beta","user_id":4
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Kafka NO_ACK keeps the nested lists of a collected ingest group exact when a <wire_format> message is refused inside one
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "nested_group_input_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed
      """
      CREATE SCHEMA nested_event (
        tenant STRING,
        matrix <matrix_type>
      );
      CREATE WIRE <wire_format> SCHEMA nested_event_wire MODE STRICT (
        tenant string,
        matrix array
      );
      CREATE CODEC nested_event_codec
        FROM WIRE <wire_format> SCHEMA nested_event_wire
        TO SCHEMA nested_event;
      CREATE SCHEMA nested_tenant ( tenant STRING );
      CREATE BRANCH by_nested_tenant SCHEMA nested_tenant TTL 5m;
      CREATE RELAY nested_events SCHEMA nested_event BRANCHED BY by_nested_tenant;
      CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
      CREATE KAFKA TOPIC INGESTOR nested_event_source
        FROM kafka_main TOPIC 'nested_group_input_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'nervix_cucumber_nested_group_{{test_id}}'
        MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING nested_event_codec
        TO nested_events
        INHERIT ALL
        BRANCHED BY by_nested_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION nested_events_subscription TO nested_events;
      """
    And these Kafka messages encoded as "<wire_format>" are rapidly published to topic "nested_group_input_{{test_id}}"
      """
      {"tenant":"acme","matrix":[[1,2],[3]]}
      {"tenant":"beta","matrix":[[4],[5,300]]}
      {"tenant":"acme","matrix":[[6],[7,8]]}
      {"tenant":"beta","matrix":[[9],[10,11]]}
      """
    And these NSPL commands are executed
      """
      START;
      """
    Then within "20s" the relay subscription receives payloads
      """
      payload={"matrix":[[1,2],[3]],"tenant":"acme"}
      payload={"matrix":[[6],[7,8]],"tenant":"acme"}
      payload={"matrix":[[9],[10,11]],"tenant":"beta"}
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | wire_format | matrix_type  |
      | 1            | JSON        | VEC<VEC<U8>> |
      | 3            | JSON        | VEC<VEC<U8>> |
      | 1            | CBOR        | VEC<VEC<U8>> |
      | 3            | CBOR        | VEC<VEC<U8>> |

  @decode_rejection
  Scenario Outline: A Kafka <mode_name> ingestor consumes a payload its codec refuses, reports its record and delivers the next one
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "poison_input_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed
      """
      CREATE SCHEMA poison_event (
        tenant STRING,
        user_id I64
      );
      CREATE WIRE JSON SCHEMA poison_event_wire MODE STRICT (
        tenant string,
        user_id integer
      );
      CREATE CODEC poison_event_codec
        FROM WIRE JSON SCHEMA poison_event_wire
        TO SCHEMA poison_event;
      CREATE RELAY poison_events SCHEMA poison_event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
      CREATE KAFKA TOPIC INGESTOR poison_source
        FROM kafka_main TOPIC 'poison_input_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'nervix_cucumber_poison_{{test_id}}'
        MODE <mode>
        ON QUIESCE SUSPEND DECODE USING poison_event_codec
        TO poison_events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION poison_events_subscription TO poison_events;
      """
    And these Kafka messages are rapidly published to topic "poison_input_{{test_id}}"
      """
      {"tenant":"acme","user_id":"one"}
      {"tenant":"acme","user_id":2}
      """
    And these NSPL commands are executed
      """
      START;
      """
    Then within "30s" the relay subscription receives a payload
      """
      "tenant":"acme","user_id":2
      """
    And within "30s" the active session observes a server error containing
      """
      ingestor 'poison_source' general error in domain '{{domain}}': decode rejection
      """
    And the last server error contains
      """
      codec 'poison_event_codec' refused the payload from Kafka topic 'poison_input_{{test_id}}' partition 0 offset 0: codec 'poison_event_codec' failed to parse field 'user_id'
      """
    And within "30s" DESCRIBE INGESTOR "poison_source" on the leader node contains
      """
      nervix_ingestor_decode_rejections_total: 1
      """
    And within "30s" DESCRIBE INGESTOR "poison_source" on the leader node contains
      """
      codec 'poison_event_codec' refused the payload from Kafka topic 'poison_input_{{test_id}}' partition 0 offset 0
      """
    And within "30s" Kafka consumer group "nervix_cucumber_poison_{{test_id}}" next offset for topic "poison_input_{{test_id}}" partition 0 is "at least 2"

    Examples:
      | cluster_size | mode_name      | mode                                                                                     |
      | 1            | ACK SEQUENTIAL | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s                         |
      | 3            | ACK SEQUENTIAL | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s                         |
      | 1            | ACK PARALLEL   | ACK PARALLEL MAX 8 BATCH TIMEOUT 500ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s |
      | 3            | ACK PARALLEL   | ACK PARALLEL MAX 8 BATCH TIMEOUT 500ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s |

  @decode_rejection
  Scenario Outline: A Kafka ingestor that ignores general errors consumes a payload its codec refuses without reporting it, and counts it
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "ignored_poison_input_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed
      """
      CREATE SCHEMA poison_event (
        tenant STRING,
        user_id I64
      );
      CREATE WIRE JSON SCHEMA poison_event_wire MODE STRICT (
        tenant string,
        user_id integer
      );
      CREATE CODEC poison_event_codec
        FROM WIRE JSON SCHEMA poison_event_wire
        TO SCHEMA poison_event;
      CREATE RELAY poison_events SCHEMA poison_event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
      CREATE KAFKA TOPIC INGESTOR ignored_poison_source
        FROM kafka_main TOPIC 'ignored_poison_input_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'nervix_cucumber_ignored_poison_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING poison_event_codec
        TO poison_events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR IGNORE;
      CREATE SUBSCRIPTION poison_events_subscription TO poison_events;
      """
    And these Kafka messages are rapidly published to topic "ignored_poison_input_{{test_id}}"
      """
      {"tenant":"acme","user_id":"one"}
      {"tenant":"acme","user_id":2}
      """
    And these NSPL commands are executed
      """
      START;
      """
    Then within "30s" the relay subscription receives a payload
      """
      "tenant":"acme","user_id":2
      """
    And within "30s" DESCRIBE INGESTOR "ignored_poison_source" on the leader node contains
      """
      nervix_ingestor_decode_rejections_total: 1
      """
    And within "30s" DESCRIBE INGESTOR "ignored_poison_source" on the leader node contains
      """
      codec 'poison_event_codec' refused the payload from Kafka topic 'ignored_poison_input_{{test_id}}' partition 0 offset 0
      """
    And within "30s" Kafka consumer group "nervix_cucumber_ignored_poison_{{test_id}}" next offset for topic "ignored_poison_input_{{test_id}}" partition 0 is "at least 2"
    And the active session observes no server error containing "decode rejection" within "2s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @decode_rejection
  Scenario Outline: A Kafka ACK SEQUENTIAL ingestor presents again a payload the node could not unfold, and counts no decode rejection
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "unfolded_input_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed
      """
      CREATE SCHEMA unfolded_event ( user_id I64 );
      CREATE CODEC unfolded_codec FROM JSON TO SCHEMA unfolded_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY unfolded_events SCHEMA unfolded_event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
      CREATE KAFKA TOPIC INGESTOR unfolded_source
        FROM kafka_main TOPIC 'unfolded_input_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'nervix_cucumber_unfolded_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING unfolded_codec
        TO unfolded_events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION unfolded_events_subscription TO unfolded_events;
      """
    And extension execution is saturated on every node
    And these Kafka messages are rapidly published to topic "unfolded_input_{{test_id}}"
      """
      {"user_id":42}
      """
    And these NSPL commands are executed
      """
      START;
      """
    Then within "30s" the active session observes a server error containing
      """
      the node's bounded execution did not unfold the payload
      """
    When extension execution is released on every node
    Then within "30s" the relay subscription receives a payload
      """
      "user_id":42
      """
    And within "30s" DESCRIBE INGESTOR "unfolded_source" on the leader node contains
      """
      nervix_ingestor_decode_rejections_total: 0
      """
    And within "30s" DESCRIBE INGESTOR "unfolded_source" on the leader node contains
      """
      last decode rejection: -
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Kafka ingestor reports transient source failures and recovers
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When ingestor "kafka_notifications" enters fault mode
    And these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
        CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_main TOPIC 'notifications_reconnect_{{test_id}}' OFFSET BY CONSUMER GROUP 'nervix_cucumber_reconnect_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_kafka_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "5s" DESCRIBE INGESTOR "kafka_notifications" on the leader node contains
      """
      transient error: ingestor fault injector failed source
      """
    When ingestor "kafka_notifications" leaves fault mode
    And Kafka message is published to topic "notifications_reconnect_{{test_id}}"
      """
      {"user_id":43}
      """
    Then the relay subscription receives a payload
      """
      "user_id":43
      """
    And within "5s" DESCRIBE INGESTOR "kafka_notifications" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @kafka_no_ack_reconnect
  Scenario Outline: A NO ACK Kafka ingestor whose broker is down reconnects on the doubling reconnect cadence on <cluster_size> nodes
    Given a mock Kafka broker serves topic "no_ack_down_{{test_id}}"
    And the mock Kafka broker goes down
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    # The broker is down when the ingestor starts, so every attempt to take up the topic's
    # partitions fails within the client's half-second socket timeout.
    When these Plexus statements are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA down_event ( seq I64 );
      CREATE WIRE JSON SCHEMA down_wire MODE STRICT ( seq integer );
      CREATE CODEC down_codec FROM WIRE JSON SCHEMA down_wire TO SCHEMA down_event;
      CREATE RELAY down_events SCHEMA down_event UNBRANCHED;
      CREATE KAFKA CLIENT down_kafka CONFIG {
        'bootstrap.servers' = '{{mock_kafka_addr}}',
        'auto.offset.reset' = 'earliest',
        'socket.timeout.ms' = '500'
      };
      CREATE KAFKA TOPIC INGESTOR down_source
        FROM down_kafka TOPIC 'no_ack_down_{{test_id}}'
        OFFSET BY DOMAIN MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING down_codec
        TO down_events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION down_subscription TO down_events;
      START;
      """
    # Without a declared retry policy the ingestor waits 250 ms after its first failure and doubles
    # the wait after each one, so the wait reaches two seconds after four failures.
    Then within "60s" DESCRIBE INGESTOR "down_source" on the leader node reports a reconnect backoff of at least "2s"
    When the mock Kafka broker comes back up
    And mock Kafka message is published to topic "no_ack_down_{{test_id}}"
      """
      {"seq":1}
      """
    Then within "60s" the relay subscription receives a payload
      """
      "seq":1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @kafka_refused_commit
  Scenario Outline: A Kafka ingestor on <cluster_size> nodes reports a commit its broker refuses as a transient status and reports progress only once the broker commits
    Given a mock Kafka broker serves topic "refused_commit_{{test_id}}"
    And the mock Kafka broker refuses every offset commit
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA refused_commit_event ( event_id I64 );
      CREATE WIRE JSON SCHEMA refused_commit_wire MODE STRICT ( event_id integer );
      CREATE CODEC refused_commit_codec
        FROM WIRE JSON SCHEMA refused_commit_wire TO SCHEMA refused_commit_event;
      CREATE RELAY refused_commit_records SCHEMA refused_commit_event UNBRANCHED;
      CREATE KAFKA CLIENT refused_commit_kafka CONFIG {
        'bootstrap.servers' = '{{mock_kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE KAFKA TOPIC INGESTOR refused_commit_source
        FROM refused_commit_kafka TOPIC 'refused_commit_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'refused_commit_group_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
          RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING refused_commit_codec
        TO refused_commit_records INHERIT ALL UNBRANCHED
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION refused_commit_subscription TO refused_commit_records;
      START;
      """
    And mock Kafka message is published to topic "refused_commit_{{test_id}}"
      """
      {"event_id":1}
      """
    Then within "30s" the relay subscription receives a payload
      """
      "event_id":1
      """
    And within "30s" DESCRIBE INGESTOR "refused_commit_source" on the leader node contains
      """
      transient error: GroupAuthorizationFailed (Broker: Group authorization failed)
      """
    And within "2s" mock Kafka consumer group "refused_commit_group_{{test_id}}" next offset for topic "refused_commit_{{test_id}}" partition 0 is "below 1"
    When the mock Kafka broker accepts offset commits again
    Then within "30s" mock Kafka consumer group "refused_commit_group_{{test_id}}" next offset for topic "refused_commit_{{test_id}}" partition 0 is "at least 1"
    And within "30s" DESCRIBE INGESTOR "refused_commit_source" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Kafka ACK PARALLEL applies FILTER WHERE and route WHERE across a whole batch of interleaved branches
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA event (
        user_id I64,
        tenant I64,
        level I64
      );
        CREATE SCHEMA routed_event (
        user_id I64,
        tenant I64,
        level I64,
        source_offset I64 OPTIONAL
      );
        CREATE WIRE JSON SCHEMA event_wire MODE STRICT (
        user_id integer,
        tenant integer,
        level integer
      );
        CREATE CODEC event_codec
        FROM WIRE JSON SCHEMA event_wire
        TO SCHEMA event;
        CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant I64 );
        CREATE IF NOT EXISTS BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
        CREATE RELAY events SCHEMA routed_event BRANCHED BY by_tenant;
        CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
        CREATE KAFKA TOPIC INGESTOR event_source
        FROM kafka_main TOPIC 'events_{{test_id}}' OFFSET BY CONSUMER GROUP 'nervix_cucumber_{{test_id}}' MODE ACK PARALLEL MAX 8 BATCH TIMEOUT 500ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        FILTER WHERE input.level > 0
        TO events
        INHERIT ALL
        SET source_offset = metadata.offset
        WHERE message.user_id > 10
        BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION events_subscription TO events;
      """
    And Kafka message is published to topic "events_{{test_id}}" partition 0
      """
      {"user_id":11,"tenant":1,"level":1}
      """
    And Kafka message is published to topic "events_{{test_id}}" partition 0
      """
      {"user_id":12,"tenant":2,"level":1}
      """
    And Kafka message is published to topic "events_{{test_id}}" partition 0
      """
      {"user_id":13,"tenant":1,"level":0}
      """
    And Kafka message is published to topic "events_{{test_id}}" partition 0
      """
      {"user_id":5,"tenant":2,"level":1}
      """
    And Kafka message is published to topic "events_{{test_id}}" partition 0
      """
      {"user_id":14,"tenant":2,"level":1}
      """
    And Kafka message is published to topic "events_{{test_id}}" partition 0
      """
      {"user_id":15,"tenant":1,"level":1}
      """
    # The whole corpus is already on the topic when the consumer joins, so the first
    # ACK PARALLEL poll fills one batch of six rather than six batches of one. That is
    # what puts records from both branches, both filter stages and both surviving and
    # dropped rows into a single dispatched group.
    And these NSPL commands are executed
      """
      START;
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      "user_id":11 | "tenant":1 | "level":1 | "source_offset":0
      "user_id":12 | "tenant":2 | "level":1 | "source_offset":1
      "user_id":14 | "tenant":2 | "level":1 | "source_offset":4
      "user_id":15 | "tenant":1 | "level":1 | "source_offset":5
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Kafka ACK PARALLEL reads source metadata and headers for every message of a batch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "sourced_events_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed
      """
      CREATE SCHEMA sourced_event (
        user_id I64
      );
      CREATE SCHEMA routed_sourced_event (
        user_id I64,
        source_topic STRING OPTIONAL,
        source_partition I32 OPTIONAL,
        source_offset I64 OPTIONAL,
        first_route STRING,
        route_count I64
      );
      CREATE WIRE JSON SCHEMA sourced_event_wire MODE STRICT (
        user_id integer
      );
      CREATE CODEC sourced_event_codec
        FROM WIRE JSON SCHEMA sourced_event_wire
        TO SCHEMA sourced_event;
      CREATE RELAY sourced_events SCHEMA routed_sourced_event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
      CREATE KAFKA TOPIC INGESTOR sourced_event_source
        FROM kafka_main TOPIC 'sourced_events_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'nervix_cucumber_sourced_{{test_id}}'
        MODE ACK PARALLEL MAX 8 BATCH TIMEOUT 500ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING sourced_event_codec
        TO sourced_events
        INHERIT ALL
        SET source_topic = metadata.topic,
            source_partition = metadata.partition,
            source_offset = metadata.offset,
            first_route = coalesce(read_header('route'), 'absent'),
            route_count = count(read_headers('route'))
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION sourced_events_subscription TO sourced_events;
      """
    And Kafka message with headers "route=primary,route=secondary,tenant=acme" is published to topic "sourced_events_{{test_id}}" partition 0
      """
      {"user_id":11}
      """
    And Kafka message with headers "" is published to topic "sourced_events_{{test_id}}" partition 0
      """
      {"user_id":12}
      """
    And Kafka message with headers "route=backup" is published to topic "sourced_events_{{test_id}}" partition 0
      """
      {"user_id":13}
      """
    # The whole corpus is already on the topic when the consumer joins, so one ACK PARALLEL
    # poll holds all three messages at once and every metadata row is appended from the
    # batch of source messages the group still owns.
    And these NSPL commands are executed
      """
      START;
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      "user_id":11 | "source_topic":"sourced_events_{{test_id}}" | "source_partition":0 | "source_offset":0 | "first_route":"primary" | "route_count":2
      "user_id":12 | "source_topic":"sourced_events_{{test_id}}" | "source_partition":0 | "source_offset":1 | "first_route":"absent" | "route_count":0
      "user_id":13 | "source_topic":"sourced_events_{{test_id}}" | "source_partition":0 | "source_offset":2 | "first_route":"backup" | "route_count":1
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Kafka ACK PARALLEL blocks downstream publish after immediate NoAck
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
        CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_main TOPIC 'notifications_{{test_id}}' OFFSET BY CONSUMER GROUP 'nervix_cucumber_{{test_id}}' MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 100ms ACK TIMEOUT 2s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_kafka_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE EMITTER kafka_forward FROM notifications TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And emitter "kafka_forward" enters fault mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":41}
      """
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker does not receive a payload within "500ms"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
  Scenario Outline: Kafka ACK PARALLEL blocks downstream publish after ack timeout
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
        CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_main TOPIC 'notifications_{{test_id}}' OFFSET BY CONSUMER GROUP 'nervix_cucumber_{{test_id}}' MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 100ms ACK TIMEOUT 500ms RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_kafka_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE EMITTER kafka_forward FROM notifications TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And emitter "kafka_forward" enters stall mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":51}
      """
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":52}
      """
    Then the observed broker does not receive a payload within "700ms"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Kafka ACK ingestor waits while an attached emitter is stalled
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "notifications_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
        CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_main TOPIC 'notifications_{{test_id}}' OFFSET BY CONSUMER GROUP 'nervix_cucumber_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 500ms RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_kafka_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE EMITTER kafka_forward FROM notifications TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And emitter "kafka_forward" enters stall mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":77}
      """
    Then the observed broker does not receive a payload within "1200ms"
    When emitter "kafka_forward" leaves fault mode
    Then the observed broker receives a payload
      """
      "user_id":77
      """
    And the observed broker does not receive a payload within "1200ms"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: Kafka ACK PARALLEL waits for batch timeout before dispatching a partial batch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
        CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_main TOPIC 'notifications_{{test_id}}' OFFSET BY CONSUMER GROUP 'nervix_cucumber_{{test_id}}' MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 3s ACK TIMEOUT 10s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_kafka_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":61}
      """
    Then the relay subscription receives a payload no sooner than "3s" after it was published
      """
      "user_id":61
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
  Scenario Outline: Kafka ACK SEQUENTIAL ignores detached emitter failures
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
        CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_main TOPIC 'notifications_{{test_id}}' OFFSET BY CONSUMER GROUP 'nervix_cucumber_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_kafka_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE DETACHED EMITTER kafka_forward FROM notifications TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And emitter "kafka_forward" enters fault mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
  Scenario Outline: Kafka ACK SEQUENTIAL replays on attached emitter failure
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
        CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_main TOPIC 'notifications_{{test_id}}' OFFSET BY CONSUMER GROUP 'nervix_cucumber_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_kafka_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE EMITTER kafka_forward FROM notifications TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And emitter "kafka_forward" enters fault mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":43}
      """
    Then the relay subscription receives a payload
      """
      "user_id":43
      """
    And within "2s" the relay subscription receives payloads
      """
      "user_id":43
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
  Scenario Outline: Kafka ACK SEQUENTIAL ignores detached deduplicator branch failures
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE RELAY forwarded_notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
        CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_main TOPIC 'notifications_{{test_id}}' OFFSET BY CONSUMER GROUP 'nervix_cucumber_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_kafka_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE DETACHED DEDUPLICATOR passthrough FROM notifications
        DEDUPLICATE ON input.user_id
        MAX TIME 10m
        BRANCHED BY by_kafka_notifications
        TO forwarded_notifications
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        CREATE EMITTER kafka_forward FROM forwarded_notifications TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":51}
      """
    And emitter "kafka_forward" enters fault mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":52}
      """
    Then the relay subscription receives a payload
      """
      "user_id":52
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
  Scenario Outline: Kafka ACK SEQUENTIAL replays on attached deduplicator branch failure
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_kafka_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE RELAY forwarded_notifications SCHEMA notification BRANCHED BY by_kafka_notifications;
        CREATE KAFKA CLIENT kafka_main
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };
        CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_main TOPIC 'notifications_{{test_id}}' OFFSET BY CONSUMER GROUP 'nervix_cucumber_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_kafka_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE DEDUPLICATOR passthrough FROM notifications
        DEDUPLICATE ON input.user_id
        MAX TIME 10m
        BRANCHED BY by_kafka_notifications
        TO forwarded_notifications
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        CREATE EMITTER kafka_forward FROM forwarded_notifications TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION forwarded_notifications_subscription TO forwarded_notifications;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":51}
      """
    Then the relay subscription receives a payload
      """
      {"user_id":51}
      """
    When emitter "kafka_forward" enters fault mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":53}
      """
    Then the relay subscription receives a payload
      """
      "user_id":53
      """
    And within "2s" the relay subscription receives payloads
      """
      "user_id":53
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
