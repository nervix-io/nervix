Feature: Kafka topic declarations
  KAFKA TOPIC uses one literal, case-preserving topic rule in both directions. Its offset
  ownership, delivery, quiesce and publishing modes, the configuration a Kafka client needs to
  reach a broker, and the one name a Kafka client accepts for each librdkafka property, apply at
  CREATE without contacting a broker.

  Scenario Outline: Kafka topic declarations preserve topic literals and offset ownership in both directions on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (tenant STRING, value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '127.0.0.1:1' };
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE KAFKA TOPIC INGESTOR intake
        FROM kafka_main TOPIC '<topic>' <offsets> INSTANCES 2
        MODE <ingest_mode>
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL
        SET tenant = coalesce(read_header('tenant'), 'absent')
        UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE INGESTOR intake;
      """
    Then the last command output contains
      """
      TOPIC '<topic>' <offsets> INSTANCES 2 MODE <ingest_mode> ON QUIESCE SUSPEND
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE KAFKA TOPIC EMITTER publishing FROM events
        TO kafka_main TOPIC '<topic>'
        MODE <emit_mode>
        ENCODE USING event_codec INHERIT ALL
        INVOKE write_header('tenant', input.tenant)
        <batch>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER publishing;
      """
    Then the last command output contains
      """
      TOPIC '<topic>'
      """
    And the last command output contains
      """
      MODE <emit_mode>
      """
    And the last command output contains
      """
      <shown>
      """

    Examples:
      | cluster_size | topic          | offsets                               | ingest_mode                                                                           | emit_mode                                                         | batch                                 | shown                 |
      | 1            | Orders.EU_v2-1 | OFFSET BY DOMAIN                      | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s                         | NO ACK RETRY POLICY BACKOFF 250ms MAX 30s                         |                                       | write_header          |
      | 3            | Orders.EU_v2-1 | OFFSET BY DOMAIN                      | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s                         | NO ACK RETRY POLICY BACKOFF 250ms MAX 30s                         |                                       | write_header          |
      | 1            | orders         | OFFSET BY CONSUMER GROUP 'Readers EU' | ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | BATCH MAX MESSAGES 10 MAX SIZE 512KiB | BATCH MAX MESSAGES 10 |
      | 3            | orders         | OFFSET BY CONSUMER GROUP 'Readers EU' | ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | BATCH MAX MESSAGES 10 MAX SIZE 512KiB | BATCH MAX MESSAGES 10 |
      | 1            | ..orders       | OFFSET BY CONSUMER GROUP 'readers'    | NO ACK                                                                                | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s     |                                       | write_header          |
      | 3            | ..orders       | OFFSET BY CONSUMER GROUP 'readers'    | NO ACK                                                                                | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s     |                                       | write_header          |

  Scenario Outline: Kafka topic ingestors refuse offset ownership and quiesce modes the topic does not declare on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (tenant STRING, value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE KAFKA TOPIC INGESTOR refused
        FROM kafka_main TOPIC 'orders' <offsets>
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s
        ON QUIESCE <quiesce> DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | offsets                     | quiesce                                      | expected                                           |
      | 1            |                             | SUSPEND                                      | expected OFFSET, found MODE                        |
      | 3            |                             | SUSPEND                                      | expected OFFSET, found MODE                        |
      | 1            | OFFSET BY CONSUMER GROUP '' | SUSPEND                                      | invalid kafka_consumer_group '': must not be empty |
      | 3            | OFFSET BY CONSUMER GROUP '' | SUSPEND                                      | invalid kafka_consumer_group '': must not be empty |
      | 1            | OFFSET BY DOMAIN            | DROP                                         | expected SUSPEND, found DROP                       |
      | 3            | OFFSET BY DOMAIN            | DROP                                         | expected SUSPEND, found DROP                       |
      | 1            | OFFSET BY DOMAIN            | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | expected SUSPEND, found BUFFER                     |
      | 3            | OFFSET BY DOMAIN            | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | expected SUSPEND, found BUFFER                     |

  Scenario Outline: Kafka topic emitters refuse publishing modes the topic does not declare on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (tenant STRING, value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE KAFKA TOPIC EMITTER refused FROM events TO kafka_main TOPIC 'orders'
        MODE <mode> ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | mode                                      | expected                                     |
      | 1            | SINGLE RETRY POLICY BACKOFF 250ms MAX 30s | expected NO ACK \| ACK, found SINGLE         |
      | 3            | SINGLE RETRY POLICY BACKOFF 250ms MAX 30s | expected NO ACK \| ACK, found SINGLE         |
      | 1            | ACK RETRY POLICY BACKOFF 1s MAX 30s       | expected SEQUENTIAL \| PARALLEL, found RETRY |
      | 3            | ACK RETRY POLICY BACKOFF 1s MAX 30s       | expected SEQUENTIAL \| PARALLEL, found RETRY |

  Scenario Outline: Kafka topic names outside the Kafka topic rule are refused in both directions on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (tenant STRING, value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE KAFKA TOPIC INGESTOR refused_intake
        FROM kafka_main TOPIC '<topic>' OFFSET BY CONSUMER GROUP 'readers'
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      invalid kafka_topic '<topic>': <reason>
      """
    When these Plexus commands fail
      """
      CREATE KAFKA TOPIC EMITTER refused_publishing FROM events
        TO kafka_main TOPIC '<topic>'
        MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      invalid kafka_topic '<topic>': <reason>
      """

    Examples:
      | cluster_size | topic     | reason                           |
      | 1            | orders/eu | holds '/', which it may not hold |
      | 3            | orders/eu | holds '/', which it may not hold |
      | 1            | Orders EU | holds ' ', which it may not hold |
      | 3            | Orders EU | holds ' ', which it may not hold |
      | 1            | ..        | may not be '..'                  |
      | 3            | ..        | may not be '..'                  |

  Scenario Outline: Kafka clients refuse a client that names no broker and keys Nervix sets or librdkafka does not know on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail
      """
      CREATE KAFKA CLIENT refused_client CONFIG {
        <config>
      };
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | config                                                            | expected                                               |
      | 1            | 'client.id' = 'nervix'                                            | missing required client config key 'bootstrap.servers' |
      | 3            | 'client.id' = 'nervix'                                            | missing required client config key 'bootstrap.servers' |
      | 1            | 'bootstrap.servers' = '127.0.0.1:1', 'group.id' = 'readers'       | client config key 'group.id' is reserved for Nervix    |
      | 3            | 'bootstrap.servers' = '127.0.0.1:1', 'group.id' = 'readers'       | client config key 'group.id' is reserved for Nervix    |
      | 1            | 'bootstrap.servers' = '127.0.0.1:1', 'nervix.unknown' = 'value'   | invalid client config key 'nervix.unknown'             |
      | 3            | 'bootstrap.servers' = '127.0.0.1:1', 'nervix.unknown' = 'value'   | invalid client config key 'nervix.unknown'             |
      | 1            | 'bootstrap.servers' = '127.0.0.1:1', 'fetch.wait.max.ms' = 'soon' | invalid client config key 'fetch.wait.max.ms'          |
      | 3            | 'bootstrap.servers' = '127.0.0.1:1', 'fetch.wait.max.ms' = 'soon' | invalid client config key 'fetch.wait.max.ms'          |

  @kafka_client_property_names
  Scenario Outline: Kafka clients refuse every other name librdkafka reads for a property on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail
      """
      CREATE KAFKA CLIENT refused_client CONFIG {
        'bootstrap.servers' = '127.0.0.1:1', <config>
      };
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | config                                                              | expected                                                                                                                |
      | 1            | 'metadata.broker.list' = '127.0.0.2:1'                              | client config key 'metadata.broker.list' is the driver's other name for 'bootstrap.servers'; write 'bootstrap.servers'  |
      | 3            | 'metadata.broker.list' = '127.0.0.2:1'                              | client config key 'metadata.broker.list' is the driver's other name for 'bootstrap.servers'; write 'bootstrap.servers'  |
      | 1            | 'acks' = 'all', 'request.required.acks' = '1'                       | client config key 'request.required.acks' is the driver's other name for 'acks'; write 'acks'                           |
      | 3            | 'acks' = 'all', 'request.required.acks' = '1'                       | client config key 'request.required.acks' is the driver's other name for 'acks'; write 'acks'                           |
      | 1            | 'sasl.mechanisms' = 'PLAIN'                                         | client config key 'sasl.mechanisms' is the driver's other name for 'sasl.mechanism'; write 'sasl.mechanism'             |
      | 3            | 'sasl.mechanisms' = 'PLAIN'                                         | client config key 'sasl.mechanisms' is the driver's other name for 'sasl.mechanism'; write 'sasl.mechanism'             |
      | 1            | 'delivery.timeout.ms' = '5000', 'topic.message.timeout.ms' = '9000' | client config key 'topic.message.timeout.ms' is written under the driver's 'topic.' prefix; write it without the prefix |
      | 3            | 'delivery.timeout.ms' = '5000', 'topic.message.timeout.ms' = '9000' | client config key 'topic.message.timeout.ms' is written under the driver's 'topic.' prefix; write it without the prefix |

  @kafka_client_property_names
  Scenario Outline: Kafka clients accept each librdkafka property under the Java client's name on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE KAFKA CLIENT accepted_client CONFIG {
        'bootstrap.servers' = '127.0.0.1:1',
        'delivery.timeout.ms' = '30000',
        'linger.ms' = '20',
        'acks' = 'all',
        'topic.metadata.refresh.interval.ms' = '1000'
      };
      SHOW CREATE CLIENT accepted_client;
      """
    Then the last command output contains
      """
      'delivery.timeout.ms' = '30000'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
