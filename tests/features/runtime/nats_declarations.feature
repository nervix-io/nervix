Feature: NATS declarations
  The Core NATS subject contract keeps subjects and queue groups exactly as written in both
  directions, with wildcards only where a queue subscription reads. The stream emitter names a
  stream and a literal subject and can write a message ID. Declaration rules apply at CREATE
  without connecting to NATS.

  @nats_stream_declaration
  Scenario Outline: NATS STREAM emitter preserves its destination, acknowledgement mode, and message ID
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG { 'addr' = 'nats://127.0.0.1:1' };
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE NATS STREAM EMITTER publishing FROM events
        TO nats_main STREAM 'orders' SUBJECT 'orders.created'
        MODE ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING event_codec MESSAGE ID input.value
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER publishing;
      """
    Then the last command output contains
      """
      STREAM 'orders' SUBJECT 'orders.created'
      """
    And the last command output contains
      """
      MODE ACK PARALLEL MAX 8 ACK TIMEOUT 5s
      """
    And the last command output contains
      """
      MESSAGE ID input.value
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: NATS SUBJECT declarations preserve subject literals in both directions
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG {
        'addr' = 'nats://127.0.0.1:1',
        'timeout_ms' = '1000'
      };
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE NATS SUBJECT INGESTOR intake
        FROM nats_main SUBJECT '<subscription>' QUEUE GROUP 'Readers-EU' INSTANCES 2
        MODE NO ACK ON QUIESCE <quiesce> DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      SHOW CREATE INGESTOR intake;
      """
    Then the last command output contains
      """
      SUBJECT '<subscription>' QUEUE GROUP 'Readers-EU' INSTANCES 2 MODE NO ACK
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE NATS SUBJECT EMITTER publishing FROM events
        TO nats_main SUBJECT 'Orders.Created.EU'
        MODE <mode>
        ENCODE USING event_codec INHERIT ALL
        <batch>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER publishing;
      """
    Then the last command output contains
      """
      SUBJECT 'Orders.Created.EU'
      """
    And the last command output contains
      """
      MODE <mode>
      """

    Examples:
      | cluster_size | subscription | quiesce                                      | mode                                      | batch                               |
      | 1            | Orders.*.EU  | DROP                                         | NO ACK RETRY POLICY BACKOFF 250ms MAX 30s |                                     |
      | 3            | Orders.*.EU  | DROP                                         | NO ACK RETRY POLICY BACKOFF 250ms MAX 30s |                                     |
      | 1            | orders.>     | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | NO ACK RETRY POLICY BACKOFF 250ms MAX 30s | BATCH MAX MESSAGES 8 MAX SIZE 64KiB |
      | 3            | orders.>     | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | NO ACK RETRY POLICY BACKOFF 250ms MAX 30s | BATCH MAX MESSAGES 8 MAX SIZE 64KiB |

  Scenario Outline: NATS SUBJECT ingestors refuse delivery and quiesce modes Core NATS cannot honor
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG { 'addr' = 'nats://127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE NATS SUBJECT INGESTOR refused
        FROM nats_main SUBJECT 'orders.*' QUEUE GROUP 'Readers' INSTANCES 1
        MODE <mode> ON QUIESCE <quiesce> DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | mode                                                              | quiesce | expected                               |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s     | DROP    | expected NO ACK, found ACK             |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s     | DROP    | expected NO ACK, found ACK             |
      | 1            | ACK PARALLEL MAX 2 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | DROP    | expected NO ACK, found ACK             |
      | 3            | ACK PARALLEL MAX 2 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | DROP    | expected NO ACK, found ACK             |
      | 1            | NO ACK                                                            | SUSPEND | expected BUFFER \| DROP, found SUSPEND |
      | 3            | NO ACK                                                            | SUSPEND | expected BUFFER \| DROP, found SUSPEND |

  Scenario Outline: NATS SUBJECT ingestors require their queue group and instance count
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG { 'addr' = 'nats://127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE NATS SUBJECT INGESTOR refused
        FROM nats_main SUBJECT 'orders.*' <options>
        MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | options               | expected                              |
      | 1            | INSTANCES 1           | expected QUEUE GROUP, found INSTANCES |
      | 3            | INSTANCES 1           | expected QUEUE GROUP, found INSTANCES |
      | 1            | QUEUE GROUP 'Readers' | expected INSTANCES, found MODE        |
      | 3            | QUEUE GROUP 'Readers' | expected INSTANCES, found MODE        |

  Scenario Outline: NATS SUBJECT ingestors refuse a subscription subject the subject rule excludes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG { 'addr' = 'nats://127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE NATS SUBJECT INGESTOR refused
        FROM nats_main SUBJECT '<subject>' QUEUE GROUP 'Readers' INSTANCES 1
        MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      invalid nats_subject '<subject>': <reason>
      """

    Examples:
      | cluster_size | subject          | reason                                                              |
      | 1            | orders..created  | holds an empty level separated by '.'                               |
      | 3            | orders..created  | holds an empty level separated by '.'                               |
      | 1            | orders.cre*      | holds the wildcard '*' inside a level instead of as the whole level |
      | 3            | orders.cre*      | holds the wildcard '*' inside a level instead of as the whole level |
      | 1            | orders.>.created | holds the wildcard '>' before its last level                        |
      | 3            | orders.>.created | holds the wildcard '>' before its last level                        |

  Scenario Outline: NATS SUBJECT emitters refuse a publication subject with a wildcard or an empty token
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG { 'addr' = 'nats://127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE NATS SUBJECT EMITTER refused FROM events
        TO nats_main SUBJECT '<subject>'
        MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      invalid nats_publish_subject '<subject>': <reason>
      """

    Examples:
      | cluster_size | subject         | reason                                |
      | 1            | orders.*        | holds '*', which it may not hold      |
      | 3            | orders.*        | holds '*', which it may not hold      |
      | 1            | orders.>        | holds '>', which it may not hold      |
      | 3            | orders.>        | holds '>', which it may not hold      |
      | 1            | orders..created | holds an empty level separated by '.' |
      | 3            | orders..created | holds an empty level separated by '.' |

  Scenario Outline: NATS SUBJECT emitters refuse publishing modes other than Core and JetStream
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG { 'addr' = 'nats://127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE NATS SUBJECT EMITTER refused FROM events TO nats_main SUBJECT 'Orders.Created'
        MODE <mode> ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      expected NO ACK, found ACK
      """

    Examples:
      | cluster_size | mode                                                          |
      | 1            | ACK RETRY POLICY BACKOFF 1s MAX 30s                           |
      | 3            | ACK RETRY POLICY BACKOFF 1s MAX 30s                           |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |

  Scenario Outline: NATS client declarations refuse invalid configuration at CREATE
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail
      """
      CREATE NATS CLIENT refused_client CONFIG {
        <config>
      };
      """
    Then the last command error contains
      """
      <error>
      """

    Examples:
      | cluster_size | config                                                             | error                                                                                     |
      | 1            | 'timeout_ms' = '1000'                                              | missing required client config key 'addr'                                                 |
      | 3            | 'timeout_ms' = '1000'                                              | missing required client config key 'addr'                                                 |
      | 1            | 'addr' = 'nats://127.0.0.1:4222', 'adrr' = 'nats://127.0.0.1:4222' | unknown client config key 'adrr'                                                          |
      | 3            | 'addr' = 'nats://127.0.0.1:4222', 'adrr' = 'nats://127.0.0.1:4222' | unknown client config key 'adrr'                                                          |
      | 1            | 'addr' = 'http://127.0.0.1:4222'                                   | invalid client config key 'addr'                                                          |
      | 3            | 'addr' = 'http://127.0.0.1:4222'                                   | invalid client config key 'addr'                                                          |
      | 1            | 'addr' = 'nats://127.0.0.1:4222', 'timeout_ms' = '0'               | invalid client config key 'timeout_ms': expected positive, schedulable whole milliseconds |
      | 3            | 'addr' = 'nats://127.0.0.1:4222', 'timeout_ms' = '0'               | invalid client config key 'timeout_ms': expected positive, schedulable whole milliseconds |
      | 1            | 'addr' = 'tls://127.0.0.1:4222', 'tls_cert_file' = 'client.pem'    | client config key 'tls_cert_file' requires 'tls_key_file'                                 |
      | 3            | 'addr' = 'tls://127.0.0.1:4222', 'tls_cert_file' = 'client.pem'    | client config key 'tls_cert_file' requires 'tls_key_file'                                 |

  Scenario Outline: NATS client inspection redacts the credentials an address carries
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE NATS CLIENT checked CONFIG {
        'addr' = 'nats://reader:private-nats-password@127.0.0.1:4222'
      };
      SHOW CREATE CLIENT checked;
      """
    Then the last command output contains
      """
      'addr' = '********'
      """
    And the last command output does not contain
      """
      private-nats-password
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
