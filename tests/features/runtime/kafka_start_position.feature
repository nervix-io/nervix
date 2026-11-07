Feature: Kafka consumer group start position
  A Kafka ingestor that reads through a consumer group states where the group starts a partition
  it has no offset for. What the group committed keeps its place whichever position the statement
  declares. An ingestor whose domain owns its offsets declares no position: the domain's start
  point places it.

  Background:
    Given Kafka is running

  @kafka_start_position
  Scenario Outline: A new consumer group declared START AT LATEST skips the retained backlog and receives the next record in <mode_name> on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And Kafka topic "start_latest_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When Kafka message is published to topic "start_latest_{{test_id}}"
      """
      {"user_id":11}
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE WIRE JSON CODEC notification_codec FROM WIRE SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_main TOPIC 'start_latest_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'start_latest_{{test_id}}' START AT LATEST
        MODE <mode>
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then Kafka consumer group "start_latest_{{test_id}}" eventually has 1 consumers
    # The step fails on any other first payload: the backlog precedes whatever it publishes.
    When Kafka message is published to topic "start_latest_{{test_id}}" until the relay subscription receives '"user_id":22'
      """
      {"user_id":22}
      """
    Then the relay subscription does not receive a payload containing fragments within "2s"
      """
      "user_id":11
      """

    Examples:
      | cluster_size | mode_name      | mode                                                           |
      | 1            | ACK SEQUENTIAL | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 1s MAX 30s |
      | 3            | ACK SEQUENTIAL | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 1s MAX 30s |
      | 1            | NO ACK         | NO ACK                                                         |
      | 3            | NO ACK         | NO ACK                                                         |

  @kafka_start_position
  Scenario Outline: A new consumer group declared START AT EARLIEST replays the retained backlog on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And Kafka topic "start_earliest_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When Kafka message is published to topic "start_earliest_{{test_id}}"
      """
      {"user_id":11}
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE WIRE JSON CODEC notification_codec FROM WIRE SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_main TOPIC 'start_earliest_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'start_earliest_{{test_id}}' START AT EARLIEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 1s MAX 30s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      "user_id":11
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @kafka_start_position
  Scenario Outline: A consumer group keeps the offsets it committed whichever START AT it declares on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And Kafka topic "start_kept_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When Kafka message is published to topic "start_kept_{{test_id}}"
      """
      {"user_id":11}
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE WIRE JSON CODEC notification_codec FROM WIRE SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_main TOPIC 'start_kept_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'start_kept_{{test_id}}' START AT EARLIEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 1s MAX 30s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      "user_id":11
      """
    And within "60s" Kafka consumer group "start_kept_{{test_id}}" next offset for topic "start_kept_{{test_id}}" partition 0 is "at least 1"
    When these Plexus commands are executed on the active session
      """
      STOP;
      """
    And Kafka message is published to topic "start_kept_{{test_id}}"
      """
      {"user_id":22}
      """
    And these Plexus commands are executed on the active session
      """
      ALTER KAFKA TOPIC INGESTOR kafka_notifications SET START AT LATEST;
      SHOW CREATE INGESTOR kafka_notifications;
      """
    Then the last command output contains
      """
      OFFSET BY CONSUMER GROUP 'start_kept_{{test_id}}' START AT LATEST
      """
    # LATEST would skip this record if it placed a partition the group holds an offset for.
    When these Plexus commands are executed on the active session
      """
      START;
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      "user_id":22
      """
    And within "60s" Kafka consumer group "start_kept_{{test_id}}" next offset for topic "start_kept_{{test_id}}" partition 0 is "at least 2"
    When these Plexus commands are executed on the active session
      """
      STOP;
      ALTER KAFKA TOPIC INGESTOR kafka_notifications SET START AT EARLIEST;
      START;
      """
    # EARLIEST would replay both records if it placed a partition the group holds an offset for,
    # and they would precede this one.
    And Kafka message is published to topic "start_kept_{{test_id}}" until the relay subscription receives '"user_id":33'
      """
      {"user_id":33}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @kafka_start_position
  Scenario Outline: ALTER SET START AT places a running ingestor's group that holds no offset on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And Kafka topic "start_altered_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When Kafka message is published to topic "start_altered_{{test_id}}"
      """
      {"user_id":11}
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE WIRE JSON CODEC notification_codec FROM WIRE SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_main TOPIC 'start_altered_{{test_id}}'
        OFFSET BY CONSUMER GROUP 'start_altered_{{test_id}}' START AT LATEST
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 1s MAX 30s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then Kafka consumer group "start_altered_{{test_id}}" eventually has 1 consumers
    # The group read nothing and committed nothing, so the altered position decides where it starts.
    When these Plexus commands are executed on the active session
      """
      ALTER KAFKA TOPIC INGESTOR kafka_notifications SET START AT EARLIEST;
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      "user_id":11
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @kafka_start_position
  Scenario Outline: A consumer group's start position reads back through DESCRIBE and SHOW CREATE on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '127.0.0.1:1' };
      CREATE KAFKA TOPIC INGESTOR intake
        FROM kafka_main TOPIC 'orders' OFFSET BY CONSUMER GROUP 'readers' START AT <position> INSTANCES 2
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE INGESTOR intake;
      """
    Then the last command output contains
      """
      OFFSET BY CONSUMER GROUP 'readers' START AT <position> INSTANCES 2 MODE NO ACK
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE INGESTOR intake;
      """
    Then the last command output contains
      """
      OFFSET BY CONSUMER GROUP 'readers' START AT <position>
      """

    Examples:
      | cluster_size | position |
      | 1            | EARLIEST |
      | 3            | EARLIEST |
      | 1            | LATEST   |
      | 3            | LATEST   |

  @kafka_start_position
  Scenario Outline: A Kafka ingestor refuses a start position that does not fit its offset ownership on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE KAFKA TOPIC INGESTOR refused
        FROM kafka_main TOPIC 'orders' <offsets>
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | offsets                            | expected                                |
      | 1            | OFFSET BY CONSUMER GROUP 'readers' | expected START AT, found MODE           |
      | 3            | OFFSET BY CONSUMER GROUP 'readers' | expected START AT, found MODE           |
      | 1            | OFFSET BY DOMAIN START AT EARLIEST | expected INSTANCES \| MODE, found START |
      | 3            | OFFSET BY DOMAIN START AT EARLIEST | expected INSTANCES \| MODE, found START |

  @kafka_start_position
  Scenario Outline: A domain-offset Kafka ingestor refuses START AT without changing its definition on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '127.0.0.1:1' };
      CREATE KAFKA TOPIC INGESTOR intake
        FROM kafka_main TOPIC 'orders' OFFSET BY DOMAIN
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail
      """
      ALTER KAFKA TOPIC INGESTOR intake SET START AT LATEST;
      """
    Then the last command error contains
      """
      cannot set START AT on a KAFKA ingestor whose domain owns its offsets
      """
    When these Plexus commands are executed
      """
      SHOW CREATE INGESTOR intake;
      """
    Then the last command output contains
      """
      TOPIC 'orders' OFFSET BY DOMAIN MODE NO ACK
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @kafka_start_position
  Scenario Outline: A Kafka client refuses auto.offset.reset and names the clause that sets it on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail
      """
      CREATE KAFKA CLIENT kafka_main CONFIG {
        'bootstrap.servers' = '127.0.0.1:1',
        'auto.offset.reset' = '<reset>'
      };
      """
    Then the last command error contains
      """
      client config key 'auto.offset.reset' is set by a statement; write START AT on the consumer group of the KAFKA TOPIC ingestor
      """

    Examples:
      | cluster_size | reset    |
      | 1            | earliest |
      | 3            | earliest |
      | 1            | error    |
      | 3            | error    |

  @kafka_start_position
  Scenario Outline: A domain-offset ingestor in an unpaced domain reads what a partition retains on a plain START on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And Kafka topic "domain_retained_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When Kafka message is published to topic "domain_retained_{{test_id}}"
      """
      {"user_id":11}
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE WIRE JSON CODEC notification_codec FROM WIRE SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_main TOPIC 'domain_retained_{{test_id}}'
        OFFSET BY DOMAIN
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 1s MAX 30s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      "user_id":11
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @kafka_start_position
  Scenario Outline: START AT NOW places a domain-offset ingestor in an unpaced domain past what a partition retains on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And Kafka topic "domain_now_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When Kafka message is published to topic "domain_now_{{test_id}}"
      """
      {"user_id":11}
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE WIRE JSON CODEC notification_codec FROM WIRE SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_main TOPIC 'domain_now_{{test_id}}'
        OFFSET BY DOMAIN
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 1s MAX 30s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START AT NOW;
      """
    # The step fails on any other first payload: the retained record precedes whatever it publishes.
    And Kafka message is published to topic "domain_now_{{test_id}}" until the relay subscription receives '"user_id":22'
      """
      {"user_id":22}
      """
    Then the relay subscription does not receive a payload containing fragments within "2s"
      """
      "user_id":11
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
