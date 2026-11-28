@ingestor_rate_limit
Feature: Ingestor rate limits
  An unkeyed RATE LIMIT caps how fast an ingestor admits payloads, counted one each or measured by
  an expression over what the transport knows before decoding. Every clause is one allowance that
  holds at most its BURST and refills at its count per period, starting full when the ingestor's
  execution starts. An ingestor whose entity and mode can hold its source back stops taking
  payloads while an allowance is spent and loses none of them; an on-demand source is asked for no
  more than the allowance admits now. A payload whose cost exceeds a clause's BURST, or whose
  measure fails, is consumed as a rejection with operation rate_limit through ON GENERAL ERROR.

  Every timing check records an instant before the ingestor could admit its first payload and
  requires the n-th arrival no sooner than the allowance permits after it, which a loaded host can
  only make later.

  Scenario Outline: A Kafka <mode_name> ingestor admits its payloads no faster than its allowance and loses none
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "rate_count_{{test_id}}" exists with 1 partitions
    When these Kafka messages are rapidly published to topic "rate_count_{{test_id}}"
      """
      {"seq":1}
      {"seq":2}
      {"seq":3}
      {"seq":4}
      {"seq":5}
      {"seq":6}
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA event (seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR limited
        FROM kafka_main TOPIC 'rate_count_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'rate_count_{{test_id}}' START AT EARLIEST
        MODE <mode>
        RATE LIMIT 2 PER 1s BURST 2
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      """
    And the rate limit start is recorded
    And these Plexus commands are executed
      """
      START;
      """
    Then within "60s" the relay subscription receives one payload for each fragment set and records their arrivals
      """
      "seq":1
      "seq":2
      "seq":3
      "seq":4
      "seq":5
      "seq":6
      """
    And arrival 3 came no sooner than "500ms" after the recorded rate limit start
    And arrival 6 came no sooner than "2s" after the recorded rate limit start
    And within "30s" Kafka consumer group "rate_count_{{test_id}}" next offset for topic "rate_count_{{test_id}}" partition 0 is "at least 6"
    When these Plexus commands are executed
      """
      DESCRIBE INGESTOR limited;
      """
    Then the last command output contains
      """
      rate limit: 2 PER 1s BURST 2
      """
    And the last command output owner is saved as placeholder "ingestor_owner"
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_rate_limit_held_total" with labels eventually reaches at least 1
      """
      domain="{{domain}}"
      ingestor="limited"
      connector="KAFKA"
      entity="TOPIC"
      """

    Examples:
      | cluster_size | mode_name    | mode                                                                                     |
      | 1            | NO ACK       | NO ACK                                                                                   |
      | 3            | NO ACK       | NO ACK                                                                                   |
      | 1            | ACK PARALLEL | ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s |
      | 3            | ACK PARALLEL | ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s |

  Scenario Outline: A rate-limited Kafka ingestor keeps its group membership past the poll deadline
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "rate_group_{{test_id}}" exists with 1 partitions
    When these Plexus commands are executed
      """
      CREATE SCHEMA event (seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'session.timeout.ms' = '6000',
        'max.poll.interval.ms' = '6000',
        'heartbeat.interval.ms' = '500'
      };
      CREATE KAFKA TOPIC INGESTOR limited
        FROM kafka_main TOPIC 'rate_group_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'rate_group_{{test_id}}' START AT EARLIEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        RATE LIMIT <allowance>
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      START;
      """
    Then within "30s" Kafka consumer group "rate_group_{{test_id}}" eventually has 1 consumers
    When the rate limit start is recorded
    And these Kafka messages are rapidly published to topic "rate_group_{{test_id}}"
      """
      {"seq":1}
      {"seq":2}
      """
    Then Kafka consumer group "rate_group_{{test_id}}" keeps its single stable member for "9s"
    And within "45s" the relay subscription receives one payload for each fragment set and records their arrivals
      """
      "seq":1
      "seq":2
      """
    And arrival 2 came no sooner than "<second_arrival>" after the recorded rate limit start
    And within "30s" Kafka consumer group "rate_group_{{test_id}}" next offset for topic "rate_group_{{test_id}}" partition 0 is "at least 2"
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | allowance                           | second_arrival |
      | 1            | 1 PER 15s BURST 1                   | 15s            |
      | 3            | 1 PER 15s BURST 1                   | 15s            |
      | 1            | 9 OF payload.bytes PER 15s BURST 10 | 13333ms        |
      | 3            | 9 OF payload.bytes PER 15s BURST 10 | 13333ms        |

  # A held consumer's group service is due half of max.poll.interval.ms after its last poll, the
  # cadence on which the Kafka driver wakes a consumer that waits for records. The 7.5-second hold
  # before the second record ends before the 10 seconds half of the 20-second interval leaves, so
  # the consumer runs no control job: it never pauses fetching, polls the group or resumes. In MODE
  # NO ACK the driver commits on its own cadence, so nothing else runs a control job meanwhile.
  Scenario Outline: A rate-limited Kafka ingestor whose hold ends within half its poll interval never pauses fetching
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "rate_short_{{test_id}}" exists with 1 partitions
    When these Plexus commands are executed
      """
      CREATE SCHEMA event (seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'session.timeout.ms' = '6000',
        'max.poll.interval.ms' = '20000',
        'heartbeat.interval.ms' = '500'
      };
      CREATE KAFKA TOPIC INGESTOR limited
        FROM kafka_main TOPIC 'rate_short_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'rate_short_{{test_id}}' START AT EARLIEST
        MODE NO ACK
        RATE LIMIT 2 PER 15s BURST 1
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      START;
      """
    Then within "30s" Kafka consumer group "rate_short_{{test_id}}" eventually has 1 consumers
    When these Plexus commands are executed
      """
      DESCRIBE INGESTOR limited;
      """
    Then the last command output owner is saved as placeholder "ingestor_owner"
    And node "{{ingestor_owner}}" observability metric "nervix_execution_jobs_total" with labels is saved as placeholder "control_jobs"
      """
      class="connector_storage"
      """
    When the rate limit start is recorded
    And these Kafka messages are rapidly published to topic "rate_short_{{test_id}}"
      """
      {"seq":1}
      {"seq":2}
      """
    Then within "45s" the relay subscription receives one payload for each fragment set and records their arrivals
      """
      "seq":1
      "seq":2
      """
    And arrival 2 came no sooner than "7500ms" after the recorded rate limit start
    And node "{{ingestor_owner}}" observability metric "nervix_execution_jobs_total" with labels equals saved placeholder "control_jobs"
      """
      class="connector_storage"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @kafka_allowance_rebalance @deloxide_stress
  Scenario Outline: A rate-limited Kafka ingestor pauses a partition that leaves and returns during a hold
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "rate_rebalance_{{test_id}}" exists with 1 partitions
    When these Plexus commands are executed
      """
      CREATE SCHEMA event (seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'session.timeout.ms' = '6000',
        'max.poll.interval.ms' = '6000',
        'heartbeat.interval.ms' = '500'
      };
      CREATE KAFKA TOPIC INGESTOR limited
        FROM kafka_main TOPIC 'rate_rebalance_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'rate_rebalance_{{test_id}}' START AT EARLIEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        RATE LIMIT 1 PER 30s BURST 1
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      START;
      """
    Then within "30s" Kafka consumer group "rate_rebalance_{{test_id}}" eventually has 1 consumers
    When the rate limit start is recorded
    And these Kafka messages are rapidly published to topic "rate_rebalance_{{test_id}}"
      """
      {"seq":1}
      {"seq":2}
      """
    Then within "5s" Kafka consumer group "rate_rebalance_{{test_id}}" next offset for topic "rate_rebalance_{{test_id}}" partition 0 is "at least 1"
    When an external member joins Kafka consumer group "rate_rebalance_{{test_id}}" on topic "rate_rebalance_{{test_id}}"
    Then within "10s" the external member of Kafka consumer group "rate_rebalance_{{test_id}}" holds topic "rate_rebalance_{{test_id}}" partition 0
    And within "5s" Kafka consumer group "rate_rebalance_{{test_id}}" eventually has 2 consumers
    When the external member leaves Kafka consumer group "rate_rebalance_{{test_id}}"
    Then within "5s" Kafka consumer group "rate_rebalance_{{test_id}}" eventually has 1 consumers
    And Kafka consumer group "rate_rebalance_{{test_id}}" keeps its single stable member for "9s"
    And within "45s" the relay subscription receives one payload for each fragment set and records their arrivals
      """
      "seq":1
      "seq":2
      """
    And arrival 2 came no sooner than "30s" after the recorded rate limit start
    And within "30s" Kafka consumer group "rate_rebalance_{{test_id}}" next offset for topic "rate_rebalance_{{test_id}}" partition 0 is "at least 2"
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Layered per-second and per-minute clauses each hold a Kafka ingestor to their own allowance
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "rate_layered_{{test_id}}" exists with 1 partitions
    When these Kafka messages are rapidly published to topic "rate_layered_{{test_id}}"
      """
      {"seq":1}
      {"seq":2}
      {"seq":3}
      {"seq":4}
      {"seq":5}
      {"seq":6}
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA event (seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR layered
        FROM kafka_main TOPIC 'rate_layered_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'rate_layered_{{test_id}}' START AT EARLIEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        RATE LIMIT 4 PER 1s BURST 1
        RATE LIMIT 60 PER 1m BURST 3
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      """
    And the rate limit start is recorded
    And these Plexus commands are executed
      """
      START;
      """
    Then within "60s" the relay subscription receives one payload for each fragment set and records their arrivals
      """
      "seq":1
      "seq":2
      "seq":3
      "seq":4
      "seq":5
      "seq":6
      """
    # The per-second clause admits one payload every 250ms, so the second waits for it.
    And arrival 2 came no sooner than "250ms" after the recorded rate limit start
    # The per-minute clause admits three at once and then one a second, so the sixth waits for it.
    And arrival 6 came no sooner than "3s" after the recorded rate limit start
    When these Plexus commands are executed
      """
      SHOW CREATE INGESTOR layered;
      """
    Then the last command output contains
      """
      RATE LIMIT 4 PER 1s BURST 1
      """
    And the last command output contains
      """
      RATE LIMIT 60 PER 1m BURST 3
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A RabbitMQ ACK SEQUENTIAL ingestor limited in bytes holds each payload longer than its ACK TIMEOUT without timing it out
    Given RabbitMQ is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "rate_bytes_{{test_id}}" exists
    When RabbitMQ messages are published to queue "rate_bytes_{{test_id}}"
      """
      [{"seq":1},{"seq":2},{"seq":3}]
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA event (seq I64);
      CREATE SCHEMA measured (seq I64, bytes U64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA measured UNBRANCHED;
      CREATE RABBITMQ CLIENT rabbit_main CONFIG { 'addr' = '{{rabbitmq_addr}}' };
      CREATE RABBITMQ QUEUE INGESTOR byte_limited
        FROM rabbit_main QUEUE 'rate_bytes_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 3s RETRY POLICY BACKOFF 200ms MAX 1s
        RATE LIMIT 9 OF payload.bytes PER 5s BURST 9
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL SET bytes = payload.bytes UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      """
    And the rate limit start is recorded
    And these Plexus commands are executed
      """
      START;
      """
    # Each nine-byte payload spends the whole nine-byte burst, which refills in 5s: longer than the
    # 3s ACK TIMEOUT, which starts only once a payload is admitted.
    Then within "90s" the relay subscription receives one payload for each fragment set and records their arrivals
      """
      "bytes":9 | "seq":1
      "bytes":9 | "seq":2
      "bytes":9 | "seq":3
      """
    And arrival 2 came no sooner than "5s" after the recorded rate limit start
    And arrival 3 came no sooner than "10s" after the recorded rate limit start
    When these Plexus commands are executed
      """
      DESCRIBE INGESTOR byte_limited;
      """
    Then the last command output contains
      """
      rate limit: 9 OF payload.bytes PER 5s BURST 9
      """
    And the last command output owner is saved as placeholder "ingestor_owner"
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually equals 3
      """
      domain="{{domain}}"
      ingestor="byte_limited"
      connector="RABBITMQ"
      entity="QUEUE"
      outcome="acknowledged"
      """
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_ack_outcomes_total" with labels eventually equals 0
      """
      domain="{{domain}}"
      ingestor="byte_limited"
      connector="RABBITMQ"
      entity="QUEUE"
      outcome="timed_out"
      """
    And RabbitMQ queue "rate_bytes_{{test_id}}" eventually has 0 unacknowledged deliveries

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An SQS ingestor whose allowance is spent leaves the rest of its queue unreceived
    Given SQS is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "rate_budget_{{test_id}}" exists with a visibility timeout of "600s"
    When SQS messages are published to queue "rate_budget_{{test_id}}"
      """
      [{"seq":1},{"seq":2},{"seq":3},{"seq":4},{"seq":5},{"seq":6}]
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA event (seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = '{{sqs_endpoint}}',
        'region' = 'us-east-1',
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret'
      };
      CREATE SQS QUEUE INGESTOR budgeted
        FROM sqs_main QUEUE 'rate_budget_{{test_id}}'
        MODE ACK PARALLEL MAX 10 BATCH TIMEOUT 100ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        RATE LIMIT 2 PER 1h BURST 2
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      START;
      """
    # The burst admits two payloads, and the allowance returns only after half an hour. A receive
    # that took more than two would keep the rest in flight for the queue's ten-minute visibility
    # timeout; asked only for what the allowance admits, it leaves them in the queue.
    Then within "60s" the relay subscription receives one payload for each fragment set and records their arrivals
      """
      "seq":
      "seq":
      """
    And SQS queue "rate_budget_{{test_id}}" eventually has 0 messages in flight
    When these Plexus commands are executed
      """
      DESCRIBE INGESTOR budgeted;
      """
    Then the last command output owner is saved as placeholder "ingestor_owner"
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_rate_limit_held_total" with labels eventually reaches at least 1
      """
      domain="{{domain}}"
      ingestor="budgeted"
      connector="SQS"
      entity="QUEUE"
      """
    And within "30s" DESCRIBE INGESTOR "budgeted" on the leader node contains
      """
      status: running
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A client producer's batches wait in its queue for a row allowance while its admission stays open
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_in (region STRING, order_id STRING, amount I64);
      CREATE RELAY events SCHEMA order_in UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR row_limited FROM SCHEMA order_in
        MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        RATE LIMIT 2 OF payload.rows PER 1s BURST 4
        ON QUIESCE SUSPEND TIMESTAMP NOW
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      START;
      """
    And client "app" is connected to the leader node
    When client "app" opens producer "orders" on ingestor "row_limited" expecting fields "region STRING, order_id STRING, amount I64"
    And the rate limit start is recorded
    And producer "orders" submits batch "first" with rows
      | region | order_id | amount |
      | eu     | o-1      | 1      |
      | eu     | o-2      | 2      |
      | eu     | o-3      | 3      |
      | eu     | o-4      | 4      |
    And producer "orders" submits batch "second" with rows
      | region | order_id | amount |
      | us     | o-5      | 5      |
      | us     | o-6      | 6      |
      | us     | o-7      | 7      |
      | us     | o-8      | 8      |
    And producer "orders" submits batch "third" with rows
      | region | order_id | amount |
      | ap     | o-9      | 9      |
      | ap     | o-10     | 10     |
      | ap     | o-11     | 11     |
      | ap     | o-12     | 12     |
    Then producer "orders" eventually reports admission "open"
    # The client delivers each batch from a task of its own, so the three may arrive in any order.
    # Whichever arrives first spends the burst of four rows, and the allowance returns two rows a
    # second, so the next four-row batch completes no sooner than 2s and the last no sooner than 4s.
    And batches "first, second, third" complete no sooner than "0s, 2s, 4s" after the recorded rate limit start, in completion order
    And within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "order_id":"o-1"
      "order_id":"o-2"
      "order_id":"o-3"
      "order_id":"o-4"
      "order_id":"o-5"
      "order_id":"o-6"
      "order_id":"o-7"
      "order_id":"o-8"
      "order_id":"o-9"
      "order_id":"o-10"
      "order_id":"o-11"
      "order_id":"o-12"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A client producer batch whose rows exceed its clause's burst fails processing as rejected and the next batch is delivered
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_in (region STRING, order_id STRING, amount I64);
      CREATE RELAY events SCHEMA order_in UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR burst_limited FROM SCHEMA order_in
        MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        RATE LIMIT 2 OF payload.rows PER 1s BURST 2
        ON QUIESCE SUSPEND TIMESTAMP NOW
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      START;
      """
    And client "app" is connected to the leader node
    When client "app" opens producer "orders" on ingestor "burst_limited" expecting fields "region STRING, order_id STRING, amount I64"
    # Three rows cost more than the burst of two can ever hold, so no wait admits the batch.
    And producer "orders" submits batch "oversized" with rows
      | region | order_id | amount |
      | eu     | o-1      | 1      |
      | eu     | o-2      | 2      |
      | eu     | o-3      | 3      |
    Then batch "oversized" fails processing because "rejected"
    # The refused batch spent nothing, so the full allowance admits the next one at once.
    When producer "orders" submits batch "fitting" with rows
      | region | order_id | amount |
      | us     | o-4      | 4      |
    Then batch "fitting" completes
    And within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "order_id":"o-4"
      """
    And within "30s" DESCRIBE INGESTOR "burst_limited" on the leader node contains
      """
      nervix_ingestor_rate_limit_rejected_total: 1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A payload whose cost exceeds its clause's burst is consumed as a rate_limit rejection and the next one is delivered
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "rate_oversized_{{test_id}}" exists with 1 partitions
    When these Plexus commands are executed
      """
      CREATE SCHEMA event (seq I64, note STRING OPTIONAL);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer, note string OPTIONAL);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR oversized_in
        FROM kafka_main TOPIC 'rate_oversized_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'rate_oversized_{{test_id}}' START AT EARLIEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        RATE LIMIT 1KiB OF payload.bytes PER 1s BURST 16
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      """
    And these Kafka messages are rapidly published to topic "rate_oversized_{{test_id}}"
      """
      {"seq":1}
      {"seq":2,"note":"thirty-two bytes long"}
      {"seq":3}
      """
    And these Plexus commands are executed
      """
      START;
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      "seq":1
      "seq":3
      """
    And within "30s" the active session observes a server error containing
      """
      ingestor 'oversized_in' general error in domain '{{domain}}': rate limit rejection
      """
    And the last server error contains
      """
      a payload costing 40 exceeds the burst 16 of RATE LIMIT clause 1 from Kafka topic 'rate_oversized_{{test_id}}' partition 0 offset 1
      """
    And within "30s" DESCRIBE INGESTOR "oversized_in" on the leader node contains
      """
      nervix_ingestor_rate_limit_rejected_total: 1
      """
    And within "30s" Kafka consumer group "rate_oversized_{{test_id}}" next offset for topic "rate_oversized_{{test_id}}" partition 0 is "at least 3"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A payload whose measure fails is consumed as a rate_limit rejection without quoting a value
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "rate_measure_{{test_id}}" exists with 1 partitions
    When these Plexus commands are executed
      """
      CREATE SCHEMA event (seq I64, note STRING OPTIONAL);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer, note string OPTIONAL);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR measured_in
        FROM kafka_main TOPIC 'rate_measure_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'rate_measure_{{test_id}}' START AT EARLIEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        RATE LIMIT 1KiB OF payload.bytes - (10 AS U64) PER 1s BURST 1KiB
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      """
    And these Kafka messages are rapidly published to topic "rate_measure_{{test_id}}"
      """
      {"seq":1,"note":"long enough"}
      {"seq":2}
      {"seq":3,"note":"long enough"}
      """
    And these Plexus commands are executed
      """
      START;
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      "seq":1
      "seq":3
      """
    And within "30s" the active session observes a server error containing
      """
      ingestor 'measured_in' general error in domain '{{domain}}': rate limit rejection
      """
    And the last server error contains
      """
      the measure of RATE LIMIT clause 1 failed for the payload from Kafka topic 'rate_measure_{{test_id}}' partition 0 offset 1
      """
    And within "30s" DESCRIBE INGESTOR "measured_in" on the leader node contains
      """
      nervix_ingestor_rate_limit_program_rejections_total: 1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: SET RATE LIMIT, DROP RATE LIMIT and SET FROM change a running Kafka ingestor's allowance
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "rate_altered_{{test_id}}" exists with 1 partitions
    When these Plexus commands are executed
      """
      CREATE SCHEMA event (seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR altered_in
        FROM kafka_main TOPIC 'rate_altered_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'rate_altered_{{test_id}}' START AT EARLIEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      START;
      """
    And these Kafka messages are rapidly published to topic "rate_altered_{{test_id}}"
      """
      {"seq":1}
      """
    Then within "60s" the relay subscription receives a payload
      """
      "seq":1
      """
    When these Plexus commands are executed
      """
      ALTER KAFKA TOPIC INGESTOR altered_in SET RATE LIMIT 1 PER 1h BURST 1;
      SHOW CREATE INGESTOR altered_in;
      """
    Then the last command output contains
      """
      RATE LIMIT 1 PER 1h BURST 1
      """
    # One payload spends the burst, and the next one waits an hour for it.
    When these Kafka messages are rapidly published to topic "rate_altered_{{test_id}}"
      """
      {"seq":2}
      {"seq":3}
      """
    Then within "60s" the relay subscription receives a payload
      """
      "seq":2
      """
    When these Plexus commands are executed
      """
      DESCRIBE INGESTOR altered_in;
      """
    Then the last command output contains
      """
      rate limit: 1 PER 1h BURST 1
      """
    And the last command output owner is saved as placeholder "ingestor_owner"
    And node "{{ingestor_owner}}" observability metric "nervix_ingestor_rate_limit_held_total" with labels eventually reaches at least 1
      """
      domain="{{domain}}"
      ingestor="altered_in"
      """
    # The held payload was never admitted, so it is read again once the limit is gone.
    When these Plexus commands are executed
      """
      ALTER KAFKA TOPIC INGESTOR altered_in DROP RATE LIMIT;
      """
    Then within "60s" the relay subscription receives a payload
      """
      "seq":3
      """
    When these Plexus commands are executed
      """
      ALTER KAFKA TOPIC INGESTOR altered_in SET FROM kafka_main TOPIC 'rate_altered_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'rate_altered_{{test_id}}' START AT EARLIEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        RATE LIMIT 1 PER 1h BURST 1
        ON QUIESCE SUSPEND;
      DESCRIBE INGESTOR altered_in;
      """
    Then the last command output contains
      """
      rate limit: 1 PER 1h BURST 1
      """
    When these Kafka messages are rapidly published to topic "rate_altered_{{test_id}}"
      """
      {"seq":4}
      {"seq":5}
      """
    Then within "60s" the relay subscription receives a payload
      """
      "seq":4
      """
    When these Plexus commands are executed
      """
      ALTER KAFKA TOPIC INGESTOR altered_in SET FROM kafka_main TOPIC 'rate_altered_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'rate_altered_{{test_id}}' START AT EARLIEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND;
      """
    Then within "60s" the relay subscription receives a payload
      """
      "seq":5
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An entity or mode that cannot hold its source back refuses RATE LIMIT where it is written
    Given a 1 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-rate-refusal.example.com;
      CREATE HTTP ENDPOINT http_ingress ON edge PATH '/events';
      CREATE WEBSOCKETS ENDPOINT ws_ingress ON edge PATH '/stream';
      CREATE HTTP CLIENT http_main CONFIG { 'endpoint' = 'http://127.0.0.1:9', 'timeout_ms' = 5000 };
      CREATE RABBITMQ CLIENT rabbit_main CONFIG { 'addr' = 'amqp://127.0.0.1:5672' };
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 1 MAX 4 CONFIG { 'addr' = 'redis://127.0.0.1:6379', 'subscription_buffer_messages' = '1024', 'idle_timeout_ms' = '30000' };
      CREATE MQTT CLIENT mqtt_main CONFIG { 'addr' = 'mqtt://127.0.0.1:1883' };
      CREATE NATS CLIENT nats_main CONFIG { 'addr' = 'nats://127.0.0.1:4222' };
      CREATE PROMETHEUS CLIENT prom_main CONFIG { 'addr' = 'http://127.0.0.1:9090', 'timeout_ms' = 5000 };
      CREATE WEBSOCKETS CLIENT ws_main CONFIG { 'endpoint' = 'ws://127.0.0.1:9/stream' };
      """
    When these Plexus commands fail with "<refusal>"
      """
      CREATE <entity> INGESTOR refused FROM <body> RATE LIMIT 10 PER 1s BURST 10 ON QUIESCE <quiesce>
        DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | entity                | body                                                                       | quiesce                                | refusal                                                                                                  |
      | NATS SUBJECT          | nats_main SUBJECT 'orders.*' QUEUE GROUP 'readers' INSTANCES 1 MODE NO ACK | DROP                                   | NATS SUBJECT ingestors in MODE NO ACK cannot hold their source back, so they take no RATE LIMIT          |
      | REDIS PUBSUB          | redis_main CHANNEL 'orders' MODE NO ACK                                    | DROP                                   | REDIS PUBSUB ingestors in MODE NO ACK cannot hold their source back, so they take no RATE LIMIT          |
      | WEBSOCKETS CONNECTION | ws_main MODE NO ACK                                                        | DROP                                   | WEBSOCKETS CONNECTION ingestors in MODE NO ACK cannot hold their source back, so they take no RATE LIMIT |
      | HTTP ENDPOINT         | http_ingress MODE NO ACK                                                   | BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE | HTTP ENDPOINT ingestors in MODE NO ACK cannot hold their source back, so they take no RATE LIMIT         |
      | HTTP ENDPOINT         | http_ingress MODE ACK MAX 8 ACK TIMEOUT 5s                                 | REJECT RETRY AFTER 5s                  | HTTP ENDPOINT ingestors in MODE ACK cannot hold their source back, so they take no RATE LIMIT            |
      | WEBSOCKETS ENDPOINT   | ws_ingress MODE NO ACK                                                     | REJECT RETRY AFTER 5s                  | WEBSOCKETS ENDPOINT ingestors in MODE NO ACK cannot hold their source back, so they take no RATE LIMIT   |
      | RABBITMQ QUEUE        | rabbit_main QUEUE 'orders' MODE NO ACK                                     | SUSPEND                                | RABBITMQ QUEUE ingestors in MODE NO ACK cannot hold their source back, so they take no RATE LIMIT        |
      | MQTT TOPIC            | mqtt_main TOPIC 'devices/+/orders' SESSION PERSISTENT QOS 1 MODE NO ACK    | DROP                                   | MQTT TOPIC ingestors in MODE NO ACK cannot hold their source back, so they take no RATE LIMIT            |
      | HTTP REQUEST          | http_main METHOD 'GET' EVERY 1s MODE NO ACK                                | SUSPEND                                | HTTP REQUEST ingestors read on their EVERY cadence, so they take no RATE LIMIT                           |
      | PROMETHEUS QUERY      | prom_main QUERY 'up' EVERY 15s MODE NO ACK                                 | SUSPEND                                | PROMETHEUS QUERY ingestors read on their EVERY cadence, so they take no RATE LIMIT                       |

  Scenario: An alteration that leaves a RATE LIMIT under a mode that cannot hold its source back is refused
    Given a 1 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RABBITMQ CLIENT rabbit_main CONFIG { 'addr' = 'amqp://127.0.0.1:5672' };
      CREATE RABBITMQ QUEUE INGESTOR held_queue FROM rabbit_main QUEUE 'orders'
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 200ms MAX 1s
        RATE LIMIT 10 PER 1s BURST 10
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "RABBITMQ QUEUE ingestors in MODE NO ACK cannot hold their source back, so they take no RATE LIMIT"
      """
      ALTER RABBITMQ QUEUE INGESTOR held_queue SET MODE NO ACK;
      """
    And these Plexus commands are executed
      """
      ALTER RABBITMQ QUEUE INGESTOR held_queue DROP RATE LIMIT, SET MODE NO ACK;
      SHOW CREATE INGESTOR held_queue;
      """
    Then the last command output contains
      """
      MODE NO ACK
      """
