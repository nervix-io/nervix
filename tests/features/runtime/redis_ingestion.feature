Feature: Redis ingestion
  @redis_subscription_backpressure @deloxide_stress
  Scenario Outline: Redis subscription intake stays bounded under <handling>
    Given Redis is running
    And entity gate deadline is configured as "30s"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the entity gate for domain "{{domain}}" pauses after engagement
    When these Plexus statements are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE CODEC notification_after_hold FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 1 MAX 1 CONFIG {
        'addr' = '{{redis_addr}}',
        'subscription_buffer_messages' = '2',
        'idle_timeout_ms' = '30000'
      };
      CREATE REDIS PUBSUB INGESTOR redis_notifications
        FROM redis_main CHANNEL 'bounded_{{test_id}}' MODE NO ACK
        ON QUIESCE <handling> DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then Redis channel "bounded_{{test_id}}" eventually has 1 subscribers
    When these Plexus statements begin executing in the background
      """
      ALTER REDIS PUBSUB INGESTOR redis_notifications SET DECODE USING notification_after_hold;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    And within "20s" DESCRIBE INGESTOR "redis_notifications" on the leader node contains
      """
      quiesce state: entity hold
      """
    When 256 Redis messages are published to channel "bounded_{{test_id}}"
      """
      {"user_id":8}
      """
    Then within "20s" DESCRIBE INGESTOR "redis_notifications" on the leader node contains
      """
      nervix_ingestor_quiesce_buffered_records: <retained>
      """
    And within "20s" DESCRIBE INGESTOR "redis_notifications" on the leader node contains
      """
      nervix_ingestor_quiesce_buffered_bytes: <bytes>
      """
    And within "20s" DESCRIBE INGESTOR "redis_notifications" on the leader node contains
      """
      nervix_ingestor_quiesce_dropped_total: <dropped>
      """
    When the entity gate pause for domain "{{domain}}" is released
    Then the background Plexus execution succeeds
    When Redis message is published to channel "bounded_{{test_id}}"
      """
      {"user_id":73}
      """
    Then the relay subscription receives a payload
      """
      "user_id":73
      """
    And within "20s" DESCRIBE INGESTOR "redis_notifications" on the leader node contains
      """
      nervix_ingestor_quiesce_buffered_records: 0
      """

    Examples:
      | cluster_size | handling                                    | retained | bytes | dropped |
      | 1            | BUFFER MAX SIZE 30B ON OVERFLOW DROP NEWEST | 2        | 30    | 254     |
      | 3            | BUFFER MAX SIZE 30B ON OVERFLOW DROP NEWEST | 2        | 30    | 254     |
      | 1            | DROP                                        | 0        | 0     | 256     |
      | 3            | DROP                                        | 0        | 0     | 256     |

  @redis_idle_subscription @deloxide_stress
  Scenario Outline: An idle Redis subscription detects a silent connection and resubscribes
    Given Redis is running
    And a stallable endpoint "redis_idle_peer" stands in front of "redis_addr"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 1 MAX 1 CONFIG {
        'addr' = '{{redis_idle_peer}}',
        'idle_timeout_ms' = '1000',
        'subscription_buffer_messages' = '2'
      };
      CREATE REDIS PUBSUB INGESTOR redis_notifications
        FROM redis_main CHANNEL 'idle_{{test_id}}' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then Redis channel "idle_{{test_id}}" eventually has 1 subscribers
    When the stallable endpoint "redis_idle_peer" is paused
    Then within "20s" DESCRIBE INGESTOR "redis_notifications" on the leader node contains
      """
      idle_timeout_ms 1s
      """
    When the stallable endpoint "redis_idle_peer" resets its connections
    Then Redis channel "idle_{{test_id}}" eventually has 0 subscribers
    When the stallable endpoint "redis_idle_peer" is resumed
    Then Redis channel "idle_{{test_id}}" eventually has 1 subscribers
    When Redis message is published to channel "idle_{{test_id}}"
      """
      {"user_id":73}
      """
    Then the relay subscription receives a payload
      """
      "user_id":73
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Redis ingestor delivers JSON payloads to a subscribed relay
    Given Redis is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
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
        CREATE IF NOT EXISTS BRANCH by_redis_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_redis_notifications;
        CREATE REDIS CLIENT redis_main
        POOL SIZE MIN 1 MAX 4
        CONFIG {
          'addr' = '{{redis_addr}}',
          'subscription_buffer_messages' = '1024',
          'idle_timeout_ms' = '30000'
        };
        CREATE REDIS PUBSUB INGESTOR redis_notifications
        FROM redis_main CHANNEL '<channel>_{{test_id}}' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_redis_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    When Redis message is published to channel "<channel>_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'
    # The broker's channel subscriber count identifies the exact CHANNEL used for delivery.
    And Redis channel "<channel>_{{test_id}}" eventually has 1 subscribers

    Examples:
      | cluster_size | replica_count | channel       |
      | 1            | 0             | notifications |
      | 3            | 0             | notifications |
      | 3            | 1             | notifications |
      | 1            | 0             | Orders:EU/*   |
      | 3            | 0             | Orders:EU/*   |

  @redis_pattern_subscription
  Scenario Outline: A Redis pattern subscription delivers every channel it matches with the delivering channel and pattern as metadata
    Given Redis is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE SCHEMA delivered_notification (
        user_id I64,
        channel STRING OPTIONAL,
        pattern STRING OPTIONAL
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA delivered_notification UNBRANCHED;
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 1 MAX 1 CONFIG {
        'addr' = '{{redis_addr}}',
        'subscription_buffer_messages' = '16',
        'idle_timeout_ms' = '30000'
      };
      CREATE REDIS PUBSUB INGESTOR redis_notifications
        FROM redis_main PATTERN 'orders_{{test_id}}.*' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications INHERIT ALL
        SET channel = metadata.channel, pattern = metadata.pattern
        WHERE channel != 'orders_{{test_id}}.skip'
        UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    # Redis counts no subscribers per pattern, so the first delivery shows the subscription is live.
    Then within "20s" repeatedly publishing Redis message to channel "orders_{{test_id}}.ready" yields a relay subscription payload
      """
      {"user_id":0}
      """
    When Redis message is published to channel "orders_{{test_id}}.eu"
      """
      {"user_id":1}
      """
    And Redis message is published to channel "orders_{{test_id}}.us"
      """
      {"user_id":2}
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      "user_id":1 | "channel":"orders_{{test_id}}.eu" | "pattern":"orders_{{test_id}}.*"
      "user_id":2 | "channel":"orders_{{test_id}}.us" | "pattern":"orders_{{test_id}}.*"
      """
    # One subscription delivers in publication order, so a message from a channel the pattern does
    # not match, or one the route drops by its channel, would arrive before the fence after it.
    When Redis message is published to channel "returns_{{test_id}}.eu" that no subscription receives
      """
      {"user_id":3}
      """
    And Redis message is published to channel "orders_{{test_id}}.skip"
      """
      {"user_id":4}
      """
    And Redis message is published to channel "orders_{{test_id}}.apac"
      """
      {"user_id":5}
      """
    Then within "20s" the relay subscription receives payloads in order
      """
      "user_id":5
      """
    When these Plexus statements are executed
      """
      ALTER REDIS PUBSUB INGESTOR redis_notifications SET FROM redis_main
        CHANNEL 'audit_{{test_id}}' MODE NO ACK ON QUIESCE DROP;
      """
    Then Redis channel "audit_{{test_id}}" eventually has 1 subscribers
    When Redis message is published to channel "audit_{{test_id}}"
      """
      {"user_id":6}
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      "user_id":6 | "channel":"audit_{{test_id}}"
      """
    # A subscription payload leaves a null field out, so a channel subscription's null pattern is
    # the absent key.
    And the last relay subscription payload does not contain '"pattern":'
    # The replaced pattern subscription has ended, so Redis delivers its channels to no one.
    When Redis message is published to channel "orders_{{test_id}}.eu" that no subscription receives
      """
      {"user_id":7}
      """
    And Redis message is published to channel "audit_{{test_id}}"
      """
      {"user_id":8}
      """
    Then within "20s" the relay subscription receives payloads in order
      """
      "user_id":8
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @retained_task_handles
  Scenario Outline: Redis ingestor reports transient source failures and recovers
    Given Redis is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When ingestor "redis_notifications" enters fault mode
    And these Plexus commands are executed
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
        CREATE IF NOT EXISTS BRANCH by_redis_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_redis_notifications;
        CREATE REDIS CLIENT redis_main
        POOL SIZE MIN 1 MAX 4
        CONFIG {
          'addr' = '{{redis_addr}}',
          'subscription_buffer_messages' = '1024',
          'idle_timeout_ms' = '30000'
        };
        CREATE REDIS PUBSUB INGESTOR redis_notifications
        FROM redis_main CHANNEL 'notifications_reconnect_{{test_id}}' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_redis_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "5s" DESCRIBE INGESTOR "redis_notifications" on the leader node contains
      """
      transient error: ingestor fault injector failed source
      """
    When ingestor "redis_notifications" leaves fault mode
    And Redis message is published to channel "notifications_reconnect_{{test_id}}"
      """
      {"user_id":43}
      """
    Then the relay subscription receives a payload
      """
      "user_id":43
      """
    And within "5s" DESCRIBE INGESTOR "redis_notifications" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |
