Feature: Pulsar subscription types
  A PULSAR TOPIC ingestor declares the type it reads its subscription as. The broker decides what
  that type means: SHARED and KEY_SHARED deliver to every consumer, FAILOVER to one active consumer
  with the others standing by, and EXCLUSIVE to the one consumer it admits.

  @pulsar_subscription_type
  Scenario Outline: A Pulsar ingestor reads its subscription as the declared TYPE <type> under MODE <delivery>
    Given Pulsar is running
    And a <cluster_size> node nervix cluster is started
    And a Pulsar domain graph reads through "{{pulsar_addr}}"
    When these Plexus statements are executed
      """
      CREATE SUBSCRIPTION observed TO output;
      CREATE PULSAR TOPIC INGESTOR events
        FROM broker TOPIC 'typed_{{test_id}}'
        SUBSCRIPTION 'typed_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST TYPE <type>
        MODE <delivery>
        ON QUIESCE SUSPEND DECODE USING item_codec
        TO output INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then within "60s" Pulsar subscription "typed_{{test_id}}" of topic "typed_{{test_id}}" has type "<reported>"
    And within "30s" DESCRIBE INGESTOR "events" on the leader node contains
      """
      options: SUBSCRIPTION 'typed_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST TYPE <type> INSTANCES 1
      """
    When Pulsar message is published to topic "typed_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And within "60s" Pulsar subscription "typed_{{test_id}}" of topic "typed_{{test_id}}" holds 0 unacknowledged messages

    Examples:
      | cluster_size | type       | reported   | delivery                                                                                |
      | 1            | SHARED     | Shared     | NO ACK                                                                                  |
      | 3            | SHARED     | Shared     | NO ACK                                                                                  |
      | 1            | SHARED     | Shared     | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s                        |
      | 3            | SHARED     | Shared     | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s                        |
      | 1            | SHARED     | Shared     | ACK PARALLEL MAX 4 BATCH TIMEOUT 50ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s |
      | 3            | SHARED     | Shared     | ACK PARALLEL MAX 4 BATCH TIMEOUT 50ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s |
      | 1            | KEY_SHARED | Key_Shared | NO ACK                                                                                  |
      | 3            | KEY_SHARED | Key_Shared | NO ACK                                                                                  |
      | 1            | KEY_SHARED | Key_Shared | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s                        |
      | 3            | KEY_SHARED | Key_Shared | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s                        |
      | 1            | KEY_SHARED | Key_Shared | ACK PARALLEL MAX 4 BATCH TIMEOUT 50ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s |
      | 3            | KEY_SHARED | Key_Shared | ACK PARALLEL MAX 4 BATCH TIMEOUT 50ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s |
      | 1            | FAILOVER   | Failover   | NO ACK                                                                                  |
      | 3            | FAILOVER   | Failover   | NO ACK                                                                                  |
      | 1            | FAILOVER   | Failover   | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s                        |
      | 3            | FAILOVER   | Failover   | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s                        |
      | 1            | FAILOVER   | Failover   | ACK PARALLEL MAX 4 BATCH TIMEOUT 50ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s |
      | 3            | FAILOVER   | Failover   | ACK PARALLEL MAX 4 BATCH TIMEOUT 50ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s |
      | 1            | EXCLUSIVE  | Exclusive  | NO ACK                                                                                  |
      | 3            | EXCLUSIVE  | Exclusive  | NO ACK                                                                                  |
      | 1            | EXCLUSIVE  | Exclusive  | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s                        |
      | 3            | EXCLUSIVE  | Exclusive  | ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s                        |
      | 1            | EXCLUSIVE  | Exclusive  | ACK PARALLEL MAX 4 BATCH TIMEOUT 50ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s |
      | 3            | EXCLUSIVE  | Exclusive  | ACK PARALLEL MAX 4 BATCH TIMEOUT 50ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s |

  # The emitter's fault makes the ingestor reject the batch, and only the broker's redelivery brings
  # the message back to the emitter once the fault ends. Under FAILOVER and EXCLUSIVE the broker
  # answers a redelivery request by rewinding the consumer to what it has not acknowledged.
  @pulsar_subscription_type
  Scenario Outline: A message a failed emission rejects returns through a <type> subscription
    Given Pulsar is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And a Pulsar domain graph reads through "{{pulsar_addr}}"
    And Pulsar topic "rejected_out_{{test_id}}" is observed
    When these Plexus statements are executed
      """
      CREATE PULSAR TOPIC INGESTOR events
        FROM broker TOPIC 'rejected_{{test_id}}'
        SUBSCRIPTION 'rejected_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST TYPE <type>
        MODE ACK SEQUENTIAL ACK TIMEOUT 120s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING item_codec
        TO output INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE PULSAR TOPIC EMITTER forwarded FROM output
        TO broker TOPIC 'rejected_out_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 120s RETRY POLICY BACKOFF 100ms MAX 1s
          ENCODE USING item_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then within "60s" Pulsar subscription "rejected_{{test_id}}" of topic "rejected_{{test_id}}" has type "<reported>"
    When emitter "forwarded" enters fault mode
    And Pulsar message is published to topic "rejected_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "60s" DESCRIBE EMITTER "forwarded" on the leader node contains
      """
      transient error: fault injector failed emitter 'forwarded'
      """
    And Pulsar subscription "rejected_{{test_id}}" of topic "rejected_{{test_id}}" holds 1 unacknowledged message
    When emitter "forwarded" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """
    And within "120s" Pulsar subscription "rejected_{{test_id}}" of topic "rejected_{{test_id}}" holds 0 unacknowledged messages

    Examples:
      | cluster_size | type       | reported   |
      | 1            | SHARED     | Shared     |
      | 3            | SHARED     | Shared     |
      | 1            | KEY_SHARED | Key_Shared |
      | 3            | KEY_SHARED | Key_Shared |
      | 1            | FAILOVER   | Failover   |
      | 3            | FAILOVER   | Failover   |
      | 1            | EXCLUSIVE  | Exclusive  |
      | 3            | EXCLUSIVE  | Exclusive  |

  @pulsar_subscription_type
  Scenario Outline: A subscription another consumer holds is the ingestor's transient status until it leaves
    Given Pulsar is running
    And a <cluster_size> node nervix cluster is started
    And Pulsar consumer "other_{{test_id}}" joins subscription "held_{{test_id}}" of topic "held_{{test_id}}" as "<held_as>"
    And a Pulsar domain graph reads through "{{pulsar_addr}}"
    When these Plexus statements are executed
      """
      CREATE SUBSCRIPTION observed TO output;
      """
    And Pulsar ingestor "events" reads subscription "held_{{test_id}}" of topic "held_{{test_id}}" declared "<ownership> TYPE <type>"
    Then within "60s" DESCRIBE INGESTOR "events" on the leader node contains
      """
      transient error: the PULSAR broker refused a consumer of subscription 'held_{{test_id}}' of topic 'persistent://public/default/held_{{test_id}}' with ConsumerBusy: <reason>
      """
    When Pulsar consumer "other_{{test_id}}" leaves
    And Pulsar message is published to topic "held_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And within "60s" DESCRIBE INGESTOR "events" on the leader node contains
      """
      transient error: -
      """

    # An owned subscription joins at open, so a refused join is the one start that reaches the host's
    # resume path where it would otherwise fail the ingestor's start.
    @pulsar_subscription_conflict @deloxide_stress
    Examples:
      | cluster_size | held_as   | ownership                           | type      | reason                                  |
      | 1            | Exclusive | OWNED BY INGESTOR START AT EARLIEST | EXCLUSIVE | Exclusive consumer is already connected |
      | 3            | Exclusive | OWNED BY INGESTOR START AT EARLIEST | EXCLUSIVE | Exclusive consumer is already connected |

    Examples:
      | cluster_size | held_as   | ownership                           | type      | reason                                  |
      | 1            | Exclusive | PROVISIONED                         | EXCLUSIVE | Exclusive consumer is already connected |
      | 3            | Exclusive | PROVISIONED                         | EXCLUSIVE | Exclusive consumer is already connected |
      | 1            | Shared    | OWNED BY INGESTOR START AT EARLIEST | EXCLUSIVE | Subscription is of different type       |
      | 3            | Shared    | OWNED BY INGESTOR START AT EARLIEST | EXCLUSIVE | Subscription is of different type       |
      | 1            | Exclusive | OWNED BY INGESTOR START AT EARLIEST | SHARED    | Subscription is of different type       |
      | 3            | Exclusive | OWNED BY INGESTOR START AT EARLIEST | SHARED    | Subscription is of different type       |

  # Removal joins the subscription as its declared type, so the broker has to admit that join once
  # the ingestor's own consumer has left, EXCLUSIVE included.
  @pulsar_subscription_type @pulsar_subscription_removal
  Scenario Outline: Dropping a running <type> ingestor removes the subscription it owns
    Given Pulsar is running
    And a <cluster_size> node nervix cluster is started
    And a Pulsar domain graph reads through "{{pulsar_addr}}"
    And Pulsar ingestor "events" reads subscription "dropped_{{test_id}}" of topic "dropped_{{test_id}}" declared "OWNED BY INGESTOR START AT EARLIEST TYPE <type>"
    Then within "60s" Pulsar subscription "dropped_{{test_id}}" of topic "dropped_{{test_id}}" has type "<reported>"
    When these Plexus statements are executed
      """
      DROP INGESTOR events;
      """
    Then within "60s" Pulsar topic "dropped_{{test_id}}" has no durable subscription "dropped_{{test_id}}"

    Examples:
      | cluster_size | type       | reported   |
      | 1            | SHARED     | Shared     |
      | 3            | SHARED     | Shared     |
      | 1            | KEY_SHARED | Key_Shared |
      | 3            | KEY_SHARED | Key_Shared |
      | 1            | FAILOVER   | Failover   |
      | 3            | FAILOVER   | Failover   |
      | 1            | EXCLUSIVE  | Exclusive  |
      | 3            | EXCLUSIVE  | Exclusive  |

  # A standby of a lower priority level gives way to the ingestor's consumer, so which consumer the
  # broker makes active does not depend on how it hashes their names.
  @pulsar_subscription_type
  Scenario Outline: A FAILOVER subscription hands over to its standby when the active ingestor stops
    Given Pulsar is running
    And a <cluster_size> node nervix cluster is started
    And Pulsar consumer "standby_{{test_id}}" joins subscription "failover_{{test_id}}" of topic "failover_{{test_id}}" as a "Failover" standby
    And a Pulsar domain graph reads through "{{pulsar_addr}}"
    When these Plexus statements are executed
      """
      CREATE SUBSCRIPTION observed TO output;
      """
    And Pulsar ingestor "events" reads subscription "failover_{{test_id}}" of topic "failover_{{test_id}}" declared "PROVISIONED TYPE FAILOVER"
    Then within "60s" Pulsar subscription "failover_{{test_id}}" of topic "failover_{{test_id}}" has 2 consumers
    And within "60s" Pulsar subscription "failover_{{test_id}}" of topic "failover_{{test_id}}" has active consumer "{{domain}}~events-0"
    When Pulsar message is published to topic "failover_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    When these Plexus statements are executed
      """
      STOP;
      """
    Then within "60s" Pulsar subscription "failover_{{test_id}}" of topic "failover_{{test_id}}" has active consumer "standby_{{test_id}}"
    When Pulsar message is published to topic "failover_{{test_id}}"
      """
      {"user_id":43}
      """
    Then within "60s" Pulsar consumer "standby_{{test_id}}" receives a payload
      """
      "user_id":43
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # Every key publishes a round of its sequence before the next key's next message, so a subscription
  # that delivered to the instances in turn would break each key's order.
  @pulsar_subscription_type
  Scenario Outline: KEY_SHARED instances keep each key's messages in publication order
    Given Pulsar is running
    And a <cluster_size> node nervix cluster is started
    And a Pulsar domain graph reads through "{{pulsar_addr}}"
    When these Plexus statements are executed
      """
      CREATE SCHEMA keyed_item (key_id I64, sequence I64);
      CREATE WIRE JSON SCHEMA keyed_item_wire MODE STRICT (key_id integer, sequence integer);
      CREATE CODEC keyed_item_codec FROM WIRE JSON SCHEMA keyed_item_wire TO SCHEMA keyed_item;
      CREATE RELAY keyed_output SCHEMA keyed_item UNBRANCHED;
      CREATE SUBSCRIPTION observed TO keyed_output;
      CREATE PULSAR TOPIC INGESTOR events
        FROM broker TOPIC 'keyed_{{test_id}}'
        SUBSCRIPTION 'keyed_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST TYPE KEY_SHARED INSTANCES 2
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s
        ON QUIESCE SUSPEND DECODE USING keyed_item_codec
        TO keyed_output INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then within "60s" Pulsar subscription "keyed_{{test_id}}" of topic "keyed_{{test_id}}" has 2 consumers
    And within "60s" Pulsar subscription "keyed_{{test_id}}" of topic "keyed_{{test_id}}" has type "Key_Shared"
    When 32 keys each publish 8 sequenced messages to Pulsar topic "keyed_{{test_id}}"
    Then within "120s" the relay subscription receives 8 messages of each of 32 keys in publication order
    And within "60s" every consumer of Pulsar subscription "keyed_{{test_id}}" of topic "keyed_{{test_id}}" was delivered messages

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @pulsar_subscription_type
  Scenario Outline: SET TYPE changes what the next join reads as and keeps the subscription's cursor
    Given Pulsar is running
    And a <cluster_size> node nervix cluster is started
    And a Pulsar domain graph reads through "{{pulsar_addr}}"
    And Pulsar ingestor "events" reads subscription "altered_{{test_id}}" of topic "altered_{{test_id}}" declared "<ownership> TYPE SHARED"
    Then within "60s" Pulsar subscription "altered_{{test_id}}" of topic "altered_{{test_id}}" has type "Shared"
    When these Plexus statements are executed
      """
      STOP;
      """
    Then within "60s" Pulsar subscription "altered_{{test_id}}" of topic "altered_{{test_id}}" has no consumers
    When Pulsar message is published to topic "altered_{{test_id}}"
      """
      {"user_id":22}
      """
    Then within "30s" Pulsar subscription "altered_{{test_id}}" of topic "altered_{{test_id}}" holds 1 unacknowledged message
    When these Plexus statements are executed
      """
      ALTER PULSAR TOPIC INGESTOR events SET TYPE EXCLUSIVE;
      SHOW CREATE INGESTOR events;
      """
    Then the last command output contains
      """
      <ownership> TYPE EXCLUSIVE
      """
    And within "30s" Pulsar subscription "altered_{{test_id}}" of topic "altered_{{test_id}}" holds 1 unacknowledged message
    When these Plexus statements are executed
      """
      CREATE SUBSCRIPTION observed TO output;
      START;
      """
    Then within "60s" Pulsar subscription "altered_{{test_id}}" of topic "altered_{{test_id}}" has type "Exclusive"
    And the relay subscription receives a payload
      """
      "user_id":22
      """

    Examples:
      | cluster_size | ownership                           |
      | 1            | OWNED BY INGESTOR START AT EARLIEST |
      | 3            | OWNED BY INGESTOR START AT EARLIEST |

  @pulsar_subscription_type
  Scenario Outline: SET TYPE on a running ingestor rejoins its subscription as the new type
    Given Pulsar is running
    And a <cluster_size> node nervix cluster is started
    And a Pulsar domain graph reads through "{{pulsar_addr}}"
    When these Plexus statements are executed
      """
      CREATE SUBSCRIPTION observed TO output;
      """
    And Pulsar ingestor "events" reads subscription "rejoined_{{test_id}}" of topic "rejoined_{{test_id}}" declared "OWNED BY INGESTOR START AT EARLIEST TYPE KEY_SHARED"
    Then within "60s" Pulsar subscription "rejoined_{{test_id}}" of topic "rejoined_{{test_id}}" has type "Key_Shared"
    When these Plexus statements are executed
      """
      ALTER PULSAR TOPIC INGESTOR events SET TYPE FAILOVER;
      """
    Then within "60s" Pulsar subscription "rejoined_{{test_id}}" of topic "rejoined_{{test_id}}" has type "Failover"
    When Pulsar message is published to topic "rejoined_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @pulsar_subscription_type
  Scenario Outline: A subscription TYPE that reads through one consumer refuses more instances
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA item (user_id I64);
      CREATE WIRE JSON SCHEMA item_wire MODE STRICT (user_id integer);
      CREATE CODEC item_codec FROM WIRE JSON SCHEMA item_wire TO SCHEMA item;
      CREATE RELAY output SCHEMA item UNBRANCHED;
      CREATE PULSAR CLIENT broker CONFIG {'addr' = 'pulsar://127.0.0.1:1'};
      CREATE PULSAR TOPIC INGESTOR events FROM broker TOPIC 'events'
        SUBSCRIPTION 'readers' PROVISIONED TYPE SHARED INSTANCES 2 MODE NO ACK ON QUIESCE SUSPEND
        DECODE USING item_codec TO output INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail
      """
      CREATE PULSAR TOPIC INGESTOR refused FROM broker TOPIC 'events'
        SUBSCRIPTION 'others' PROVISIONED TYPE <type> INSTANCES 2 MODE NO ACK ON QUIESCE SUSPEND
        DECODE USING item_codec TO output INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      PULSAR subscription TYPE <type> reads through one consumer, so it refuses INSTANCES 2
      """
    When these Plexus commands fail
      """
      ALTER PULSAR TOPIC INGESTOR events SET TYPE <type>;
      """
    Then the last command error contains
      """
      PULSAR subscription TYPE <type> reads through one consumer, so it refuses INSTANCES 2
      """
    When these Plexus commands fail
      """
      ALTER PULSAR TOPIC INGESTOR events
        SET FROM broker TOPIC 'events' SUBSCRIPTION 'readers' PROVISIONED TYPE <type> INSTANCES 3
        MODE NO ACK ON QUIESCE SUSPEND;
      """
    Then the last command error contains
      """
      PULSAR subscription TYPE <type> reads through one consumer, so it refuses INSTANCES 3
      """
    When these Plexus commands are executed
      """
      SHOW CREATE INGESTOR events;
      """
    Then the last command output contains
      """
      SUBSCRIPTION 'readers' PROVISIONED TYPE SHARED INSTANCES 2
      """

    Examples:
      | cluster_size | type      |
      | 1            | FAILOVER  |
      | 3            | FAILOVER  |
      | 1            | EXCLUSIVE |
      | 3            | EXCLUSIVE |
