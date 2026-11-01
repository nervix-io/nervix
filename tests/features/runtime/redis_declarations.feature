Feature: Redis Pub/Sub declarations
  An ingestor subscribes to one literal channel or to a channel pattern, and an emitter publishes
  to one literal channel. Channels and patterns are case-preserving literals. The delivery, quiesce
  and header rules apply at CREATE without opening Redis.

  Scenario Outline: Redis Pub/Sub declarations preserve channel literals and optional batching
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'redis://127.0.0.1:1/2?protocol=resp3',
        'connect_timeout_ms' = '1000',
        'idle_timeout_ms' = '30000',
        'subscription_buffer_messages' = '16'
      };
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE REDIS PUBSUB INGESTOR intake
        FROM redis_main CHANNEL 'Orders:EU/*' MODE NO ACK
        ON QUIESCE <quiesce> DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      SHOW CREATE INGESTOR intake;
      """
    Then the last command output contains
      """
      CHANNEL 'Orders:EU/*' MODE NO ACK
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE REDIS PUBSUB EMITTER publishing FROM events
        TO redis_main CHANNEL 'Orders:EU/*'
        MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING event_codec INHERIT ALL
        <batch>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER publishing;
      """
    Then the last command output contains
      """
      CHANNEL 'Orders:EU/*'
      """
    And the last command output contains
      """
      MODE NO ACK
      """

    Examples:
      | cluster_size | quiesce                                      | batch                               |
      | 1            | DROP                                         |                                     |
      | 3            | DROP                                         |                                     |
      | 1            | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | BATCH MAX MESSAGES 8 MAX SIZE 64KiB |
      | 3            | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | BATCH MAX MESSAGES 8 MAX SIZE 64KiB |

  @redis_pattern_declaration
  Scenario Outline: A Redis Pub/Sub ingestor keeps the channel pattern it subscribes to literal through SHOW CREATE, DESCRIBE and ALTER
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'redis://127.0.0.1:1',
        'idle_timeout_ms' = '30000',
        'subscription_buffer_messages' = '16'
      };
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE REDIS PUBSUB INGESTOR intake
        FROM redis_main PATTERN 'Orders:[EU]?/*' MODE NO ACK
        ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      SHOW CREATE INGESTOR intake;
      """
    Then the last command output contains
      """
      redis_main PATTERN 'Orders:[EU]?/*' MODE NO ACK
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE INGESTOR intake;
      """
    Then the last command output contains
      """
      from: REDIS PUBSUB redis_main PATTERN 'Orders:[EU]?/*'
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER REDIS PUBSUB INGESTOR intake SET FROM redis_main CHANNEL 'Orders:EU' MODE NO ACK ON QUIESCE DROP;
      SHOW CREATE INGESTOR intake;
      """
    Then the last command output contains
      """
      redis_main CHANNEL 'Orders:EU' MODE NO ACK
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER REDIS PUBSUB INGESTOR intake SET FROM redis_main PATTERN 'Orders:*' MODE NO ACK ON QUIESCE DROP;
      DESCRIBE INGESTOR intake;
      """
    Then the last command output contains
      """
      from: REDIS PUBSUB redis_main PATTERN 'Orders:*'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @redis_pattern_declaration
  Scenario Outline: A Redis Pub/Sub ingestor refuses an empty channel pattern
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'redis://127.0.0.1:1',
        'idle_timeout_ms' = '30000',
        'subscription_buffer_messages' = '16'
      };
      """
    When these Plexus commands fail
      """
      CREATE REDIS PUBSUB INGESTOR refused
        FROM redis_main PATTERN '' MODE NO ACK
        ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      invalid redis_channel_pattern '': must not be empty
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Redis Pub/Sub ingestors refuse unsupported delivery and quiesce choices
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'redis://127.0.0.1:1',
        'idle_timeout_ms' = '30000',
        'subscription_buffer_messages' = '16'
      };
      """
    When these Plexus commands fail
      """
      CREATE REDIS PUBSUB INGESTOR refused
        FROM redis_main CHANNEL 'Orders' MODE <mode>
        ON QUIESCE <quiesce> DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | mode                                                              | quiesce | expected |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s     | DROP    | NO ACK   |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s     | DROP    | NO ACK   |
      | 1            | ACK PARALLEL MAX 2 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | DROP    | NO ACK   |
      | 3            | ACK PARALLEL MAX 2 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | DROP    | NO ACK   |
      | 1            | NO ACK                                                            | SUSPEND | BUFFER   |
      | 3            | NO ACK                                                            | SUSPEND | BUFFER   |

  Scenario Outline: Redis Pub/Sub refuses transport header operations in both directions
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'redis://127.0.0.1:1',
        'idle_timeout_ms' = '30000',
        'subscription_buffer_messages' = '16'
      };
      """
    When these Plexus commands fail
      """
      CREATE REDIS PUBSUB INGESTOR refused_read
        FROM redis_main CHANNEL 'Orders' MODE NO ACK
        ON QUIESCE DROP DECODE USING event_codec
        TO events SET value = read_header('tenant') UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      REDIS PUBSUB ingestors do not support read_header or read_headers
      """
    When these Plexus commands fail
      """
      CREATE REDIS PUBSUB EMITTER refused_write FROM events
        TO redis_main CHANNEL 'Orders'
        MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING event_codec INHERIT ALL
        INVOKE write_header('tenant', 'north')
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      REDIS PUBSUB emitters do not support write_header
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Redis Pub/Sub emitters refuse acknowledged publishing modes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE REDIS CLIENT redis_main POOL SIZE MIN 0 MAX 1 CONFIG {
        'addr' = 'redis://127.0.0.1:1',
        'idle_timeout_ms' = '30000',
        'subscription_buffer_messages' = '16'
      };
      """
    When these Plexus commands fail
      """
      CREATE REDIS PUBSUB EMITTER refused FROM events TO redis_main CHANNEL 'Orders'
        MODE <mode> ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      NO ACK
      """

    Examples:
      | cluster_size | mode                                                          |
      | 1            | ACK RETRY POLICY BACKOFF 1s MAX 30s                           |
      | 3            | ACK RETRY POLICY BACKOFF 1s MAX 30s                           |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
