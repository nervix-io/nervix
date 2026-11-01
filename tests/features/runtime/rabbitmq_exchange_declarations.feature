Feature: RabbitMQ exchange declarations
  A RABBITMQ EXCHANGE emitter names its exchange with a string the AMQP short-string byte limit holds
  and computes every record's routing key with an exact, non-null, non-sensitive STRING expression.
  Both RabbitMQ emitter entities may write MANDATORY, and no other envelope clause. These checks
  inspect stopped graph definitions without connecting to a broker.

  Scenario Outline: RabbitMQ exchange emission preserves its exchange, routing key and publishing policy
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (seq I64, category STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer, category string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RABBITMQ CLIENT broker CONFIG {'addr' = 'amqp://127.0.0.1:1'};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE RABBITMQ EXCHANGE EMITTER outgoing FROM events
        TO broker EXCHANGE 'Events.Éurope-01' ROUTING KEY concat('orders.', input.category)
        MODE <mode> RETRY POLICY BACKOFF 200ms MAX 3s
        ENCODE USING event_codec
        <envelope>
        INHERIT ALL
        <batch>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER outgoing;
      """
    Then the last command output contains
      """
      TO broker EXCHANGE 'Events.Éurope-01' ROUTING KEY concat('orders.', input.category)
      """
    And the last command output contains
      """
      MODE <mode> RETRY POLICY BACKOFF 200ms MAX 3s
      """
    And the last command output contains
      """
      ENCODE USING event_codec
      """
    And the last command output contains
      """
      <envelope_evidence>
      """
    And the last command output contains
      """
      <batch_evidence>
      """

    # MANDATORY appears nowhere else in the statement, so finding it proves the envelope kept it.
    Examples:
      | cluster_size | mode                              | envelope  | envelope_evidence | batch                                | batch_evidence                       |
      | 1            | NO ACK                            |           | FLUSH IMMEDIATE   |                                      | FLUSH IMMEDIATE                      |
      | 3            | NO ACK                            | MANDATORY | MANDATORY         |                                      | FLUSH IMMEDIATE                      |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 7s     | MANDATORY | MANDATORY         |                                      | FLUSH IMMEDIATE                      |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 7s     |           | FLUSH IMMEDIATE   |                                      | FLUSH IMMEDIATE                      |
      | 1            | ACK PARALLEL MAX 2 ACK TIMEOUT 7s |           | FLUSH IMMEDIATE   | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | BATCH MAX MESSAGES 10 MAX SIZE 64KiB |
      | 3            | ACK PARALLEL MAX 2 ACK TIMEOUT 7s | MANDATORY | MANDATORY         | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | BATCH MAX MESSAGES 10 MAX SIZE 64KiB |

  Scenario Outline: RabbitMQ queue emission preserves MANDATORY
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (seq I64, category STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer, category string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RABBITMQ CLIENT broker CONFIG {'addr' = 'amqp://127.0.0.1:1'};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE RABBITMQ QUEUE EMITTER outgoing FROM events
        TO broker QUEUE 'Orders.Éurope-01'
        MODE <mode> RETRY POLICY BACKOFF 200ms MAX 3s
        ENCODE USING event_codec
        MANDATORY
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER outgoing;
      """
    Then the last command output contains
      """
      MANDATORY
      """
    When these Plexus statements are executed on the leader node
      """
      DESCRIBE EMITTER outgoing;
      """
    Then the last command output contains
      """
      envelope: MANDATORY
      """

    Examples:
      | cluster_size | mode                              |
      | 1            | NO ACK                            |
      | 3            | ACK PARALLEL MAX 2 ACK TIMEOUT 7s |

  Scenario Outline: RabbitMQ exchange emission refuses a routing key that is not an exact, non-null, non-sensitive STRING
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (seq I64, category STRING, note STRING OPTIONAL, token STRING SENSITIVE);
      CREATE SCHEMA published (seq I64, category STRING);
      CREATE WIRE JSON SCHEMA published_wire MODE STRICT (seq integer, category string);
      CREATE CODEC published_codec FROM WIRE JSON SCHEMA published_wire TO SCHEMA published;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RABBITMQ CLIENT broker CONFIG {'addr' = 'amqp://127.0.0.1:1'};
      """
    When these Plexus statements fail with "ROUTING KEY requires an exact, non-null, non-sensitive STRING value"
      """
      CREATE RABBITMQ EXCHANGE EMITTER outgoing FROM events
        TO broker EXCHANGE 'events' ROUTING KEY <routing_key>
        MODE NO ACK RETRY POLICY BACKOFF 200ms MAX 3s ENCODE USING published_codec
        INHERIT ALL EXCEPT note, token
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size | routing_key |
      | 1            | input.seq   |
      | 3            | input.seq   |
      | 1            | input.note  |
      | 3            | input.note  |
      | 1            | input.token |
      | 3            | input.token |

  Scenario Outline: RabbitMQ exchange emission refuses an exchange outside the protocol byte contract
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "<diagnostic>"
      """
      CREATE RABBITMQ EXCHANGE EMITTER outgoing FROM events TO broker EXCHANGE '<exchange>'
        ROUTING KEY input.category
        MODE NO ACK RETRY POLICY BACKOFF 200ms MAX 3s ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size | exchange                                                                                                                                                                                                                                                         | diagnostic        |
      | 1            |                                                                                                                                                                                                                                                                  | must not be empty |
      | 3            |                                                                                                                                                                                                                                                                  | must not be empty |
      | 1            | XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX | is 256 bytes long |
      | 3            | éééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééé                                                                                                                                 | is 256 bytes long |

  Scenario Outline: RabbitMQ emitters refuse the envelope clauses they do not declare
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "found PERSISTENT"
      """
      CREATE RABBITMQ EXCHANGE EMITTER outgoing FROM events TO broker EXCHANGE 'events'
        ROUTING KEY input.category
        MODE NO ACK RETRY POLICY BACKOFF 200ms MAX 3s ENCODE USING event_codec
        PERSISTENT
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus statements fail with "found TIMESTAMP"
      """
      CREATE RABBITMQ QUEUE EMITTER outgoing FROM events TO broker QUEUE 'orders'
        MODE NO ACK RETRY POLICY BACKOFF 200ms MAX 3s ENCODE USING event_codec
        MANDATORY TIMESTAMP occurred_at
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
