Feature: ClickHouse declarations
  ClickHouse clients and table emitters reject declarations that their transport cannot use.

  Scenario Outline: ClickHouse table declaration keeps a valid one- or two-part locator
    Given a 1 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (id I64);
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE CLICKHOUSE CLIENT checked CONFIG {
        'addr' = 'http://127.0.0.1:8123',
        'timeout_ms' = '5000'
      };
      """
    When these Plexus commands are executed
      """
      CREATE CLICKHOUSE TABLE EMITTER outgoing FROM events
        TO checked TABLE '<table>'
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        VALUES { 'id' = input.id }
        BATCH MAX MESSAGES 10 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | table        |
      | Orders       |
      | sales.Orders |

  Scenario Outline: ClickHouse clients refuse unusable request and TLS settings at CREATE
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "<diagnostic>"
      """
      CREATE CLICKHOUSE CLIENT checked CONFIG { <config> };
      """

    Examples:
      | cluster_size | config                                                                                                    | diagnostic                                      |
      | 1            | 'addr' = 'http://127.0.0.1:8123'                                                                          | missing required client config key 'timeout_ms' |
      | 3            | 'addr' = 'http://127.0.0.1:8123'                                                                          | missing required client config key 'timeout_ms' |
      | 1            | 'addr' = 'http://127.0.0.1:8123', 'timeout_ms' = '0'                                                      | invalid client config key 'timeout_ms'          |
      | 3            | 'addr' = 'http://127.0.0.1:8123', 'timeout_ms' = '0'                                                      | invalid client config key 'timeout_ms'          |
      | 1            | 'addr' = 'https://127.0.0.1:8124', 'timeout_ms' = '5000'                                                  | client config key 'addr' requires 'tls_ca_file' |
      | 3            | 'addr' = 'https://127.0.0.1:8124', 'timeout_ms' = '5000'                                                  | client config key 'addr' requires 'tls_ca_file' |
      | 1            | 'addr' = 'http://127.0.0.1:8123', 'timeout_ms' = '5000', 'tls_ca_file' = '/tmp/missing-clickhouse-ca.pem' | invalid client config key 'tls_ca_file'         |
      | 3            | 'addr' = 'http://127.0.0.1:8123', 'timeout_ms' = '5000', 'tls_ca_file' = '/tmp/missing-clickhouse-ca.pem' | invalid client config key 'tls_ca_file'         |

  Scenario Outline: ClickHouse table locators refuse a three-part path
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (id I64);
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE CLICKHOUSE CLIENT checked CONFIG {
        'addr' = 'http://127.0.0.1:8123',
        'timeout_ms' = '5000'
      };
      """
    When these Plexus statements fail
      """
      CREATE CLICKHOUSE TABLE EMITTER outgoing FROM events
        TO checked TABLE 'catalog.database.Events'
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        VALUES { 'id' = input.id }
        BATCH MAX MESSAGES 10 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: ClickHouse table emitter refuses undeclared publishing and row clauses
    Given a 1 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (id I64);
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE CLICKHOUSE CLIENT checked CONFIG {
        'addr' = 'http://127.0.0.1:8123',
        'timeout_ms' = '5000'
      };
      """
    When these Plexus statements fail
      """
      CREATE CLICKHOUSE TABLE EMITTER outgoing FROM events
        TO checked TABLE 'Events'
        MODE <mode>
        <body>
        <batch>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | mode                                     | body                                                     | batch                               |
      | NO ACK RETRY POLICY BACKOFF 100ms MAX 1s | VALUES { 'id' = input.id }                               | BATCH MAX MESSAGES 10 MAX SIZE 1MiB |
      | ACK RETRY POLICY BACKOFF 100ms MAX 1s    | VALUES { 'id' = input.id } INVOKE write_header('x', 'y') | BATCH MAX MESSAGES 10 MAX SIZE 1MiB |
      | ACK RETRY POLICY BACKOFF 100ms MAX 1s    | VALUES { 'id' = input.id } ON CONFLICT DO NOTHING        | BATCH MAX MESSAGES 10 MAX SIZE 1MiB |
      | ACK RETRY POLICY BACKOFF 100ms MAX 1s    | VALUES { 'id' = input.id, 'id' = input.id }              | BATCH MAX MESSAGES 10 MAX SIZE 1MiB |
      | ACK RETRY POLICY BACKOFF 100ms MAX 1s    | VALUES { 'id' = NULL }                                   | BATCH MAX MESSAGES 10 MAX SIZE 1MiB |
      | ACK RETRY POLICY BACKOFF 100ms MAX 1s    | VALUES { 'id' = input.id }                               |                                     |
