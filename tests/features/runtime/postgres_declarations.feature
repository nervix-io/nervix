Feature: Postgres declarations describe the table and client Nervix can execute

  Scenario Outline: Postgres table declarations refuse malformed paths on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE POSTGRES CLIENT db POOL SIZE MIN 0 MAX 2
        CONFIG { 'addr' = 'postgres://127.0.0.1:5432/nervix?sslmode=disable' };
      """
    When these Plexus statements fail with "<reason>"
      """
      CREATE POSTGRES TABLE EMITTER outgoing FROM events TO db TABLE '<table>'
        MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s VALUES { 'seq' = input.seq }
        BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes | table               | reason      |
      | 1     | .Orders             | empty level |
      | 3     | .Orders             | empty level |
      | 1     | sales.              | empty level |
      | 3     | sales.              | empty level |
      | 1     | sales..Orders       | empty level |
      | 3     | sales..Orders       | empty level |
      | 1     | region.sales.Orders | at most 2   |
      | 3     | region.sales.Orders | at most 2   |

  Scenario Outline: Postgres declarations refuse what an insert cannot carry on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE POSTGRES CLIENT db POOL SIZE MIN 0 MAX 2
        CONFIG { 'addr' = 'postgres://127.0.0.1:5432/nervix?sslmode=disable' };
      """
    When these Plexus statements fail with "<reason>"
      """
      CREATE POSTGRES TABLE EMITTER outgoing FROM events TO db TABLE 'sales.Orders'
        MODE <mode> RETRY POLICY BACKOFF 10ms MAX 1s VALUES { <values> } <conflict>
        BATCH MAX MESSAGES 2 MAX SIZE <size>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes | mode   | values                                 | conflict                      | size | reason                                      |
      | 1     | NO ACK | 'seq' = input.seq                      |                               | 1MiB | ACK                                         |
      | 3     | NO ACK | 'seq' = input.seq                      |                               | 1MiB | ACK                                         |
      | 1     | ACK    | 'seq' = write_header('trace', 'test')  |                               | 1MiB | write_header                                |
      | 3     | ACK    | 'seq' = write_header('trace', 'test')  |                               | 1MiB | write_header                                |
      | 1     | ACK    | 'seq' = input.seq, 'seq' = input.seq   |                               | 1MiB | duplicate VALUES key 'seq'                  |
      | 3     | ACK    | 'seq' = input.seq, 'seq' = input.seq   |                               | 1MiB | duplicate VALUES key 'seq'                  |
      | 1     | ACK    | 'seq' = input.seq, 'note' = input.note | ON CONFLICT DO UPDATE         | 1MiB | requires a conflict target                  |
      | 3     | ACK    | 'seq' = input.seq, 'note' = input.note | ON CONFLICT DO UPDATE         | 1MiB | requires a conflict target                  |
      | 1     | ACK    | 'seq' = input.seq                      | ON CONFLICT ('seq') DO UPDATE | 1MiB | a VALUES column outside its conflict target |
      | 3     | ACK    | 'seq' = input.seq                      | ON CONFLICT ('seq') DO UPDATE | 1MiB | a VALUES column outside its conflict target |
      | 1     | ACK    | 'seq' = input.seq                      | ON CONFLICT ('') DO NOTHING   | 1MiB | empty column name                           |
      | 3     | ACK    | 'seq' = input.seq                      | ON CONFLICT ('') DO NOTHING   | 1MiB | empty column name                           |
      | 1     | ACK    | 'seq' = input.seq                      |                               | 1GiB | accept BATCH MAX SIZE up to 1073741822B     |
      | 3     | ACK    | 'seq' = input.seq                      |                               | 1GiB | accept BATCH MAX SIZE up to 1073741822B     |

  Scenario Outline: Postgres client declarations hold the address to its declared TLS policy on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "<reason>"
      """
      CREATE POSTGRES CLIENT db POOL SIZE MIN 0 MAX 2
        CONFIG { 'addr' = '<scheme>://user:private-pg-credential@127.0.0.1:5432/nervix<query>'<tls> };
      """
    Then the last command error does not contain
      """
      private-pg-credential
      """

    Examples:
      | nodes | scheme     | query                                | tls                                     | reason                                                   |
      | 1     | postgres   |                                      |                                         | 'addr' must set the sslmode query option once            |
      | 3     | postgres   |                                      |                                         | 'addr' must set the sslmode query option once            |
      | 1     | postgresql | ?sslmode=require                     |                                         | 'addr' must set the sslmode query option once            |
      | 3     | postgresql | ?sslmode=require                     |                                         | 'addr' must set the sslmode query option once            |
      | 1     | postgres   | ?sslmode=disable&sslmode=verify-full |                                         | 'addr' must set the sslmode query option once            |
      | 3     | postgres   | ?sslmode=disable&sslmode=verify-full |                                         | 'addr' must set the sslmode query option once            |
      | 1     | http       | ?sslmode=disable                     |                                         | 'addr' must be a postgres or postgresql URL              |
      | 3     | http       | ?sslmode=disable                     |                                         | 'addr' must be a postgres or postgresql URL              |
      | 1     | postgres   | ?sslmode=disable                     | , 'tls_ca_file' = '/nonexistent/ca.pem' | 'tls_ca_file' requires 'addr' to set sslmode=verify-full |
      | 3     | postgres   | ?sslmode=disable                     | , 'tls_ca_file' = '/nonexistent/ca.pem' | 'tls_ca_file' requires 'addr' to set sslmode=verify-full |
      | 1     | postgres   | ?sslmode=disable&port=private-port   |                                         | invalid client config key 'addr'                         |
      | 3     | postgres   | ?sslmode=disable&port=private-port   |                                         | invalid client config key 'addr'                         |

  Scenario Outline: Postgres client declarations refuse an invalid connection pool key on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "<reason>"
      """
      CREATE POSTGRES CLIENT db POOL SIZE MIN 0 MAX 2 CONFIG {
        'addr' = 'postgres://127.0.0.1:5432/nervix?sslmode=disable', <config>
      };
      """

    Examples:
      | nodes | config                             | reason                                                                                                 |
      | 1     | 'pool_acquire_timeout_ms' = '0'    | invalid client config key 'pool_acquire_timeout_ms': expected positive, schedulable whole milliseconds |
      | 3     | 'pool_acquire_timeout_ms' = '0'    | invalid client config key 'pool_acquire_timeout_ms': expected positive, schedulable whole milliseconds |
      | 1     | 'pool_idle_timeout_ms' = '1.5s'    | invalid client config key 'pool_idle_timeout_ms': expected positive, schedulable whole milliseconds    |
      | 3     | 'pool_idle_timeout_ms' = '1.5s'    | invalid client config key 'pool_idle_timeout_ms': expected positive, schedulable whole milliseconds    |
      | 1     | 'pool_max_lifetime_ms' = '-1'      | invalid client config key 'pool_max_lifetime_ms': expected positive, schedulable whole milliseconds    |
      | 3     | 'pool_max_lifetime_ms' = '-1'      | invalid client config key 'pool_max_lifetime_ms': expected positive, schedulable whole milliseconds    |
      | 1     | 'pool_test_before_acquire' = 'yes' | invalid client config key 'pool_test_before_acquire': expected boolean                                 |
      | 3     | 'pool_test_before_acquire' = 'yes' | invalid client config key 'pool_test_before_acquire': expected boolean                                 |
      | 1     | 'pool_max_lifetime' = '1000'       | unknown client config key 'pool_max_lifetime'                                                          |
      | 3     | 'pool_max_lifetime' = '1000'       | unknown client config key 'pool_max_lifetime'                                                          |

  Scenario Outline: Postgres client inspection reports its default connection pool keys on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE POSTGRES CLIENT db POOL SIZE MIN 0 MAX 2 CONFIG {
        'addr' = 'postgres://127.0.0.1:5432/nervix?sslmode=disable'
      };
      """
    When these Plexus statements are executed on the leader node
      """
      DESCRIBE CLIENT db;
      """
    Then the last command output contains
      """
      pool_acquire_timeout_ms: 30000 (default)
      pool_idle_timeout_ms: 600000 (default)
      pool_max_lifetime_ms: 1800000 (default)
      pool_test_before_acquire: true (default)
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |

  Scenario Outline: Postgres client inspection reports its declared connection pool keys on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE POSTGRES CLIENT db POOL SIZE MIN 0 MAX 2 CONFIG {
        'addr' = 'postgres://127.0.0.1:5432/nervix?sslmode=disable',
        'pool_acquire_timeout_ms' = '5000',
        'pool_idle_timeout_ms' = '45000',
        'pool_max_lifetime_ms' = '90000',
        'pool_test_before_acquire' = 'false'
      };
      DESCRIBE CLIENT db;
      """
    Then the last command output contains
      """
      pool_acquire_timeout_ms: 5000
      pool_idle_timeout_ms: 45000
      pool_max_lifetime_ms: 90000
      pool_test_before_acquire: false
      """
    And the last command output does not contain
      """
      (default)
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |

  Scenario Outline: Postgres declarations preserve qualified names and conflict choices on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64, note STRING );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE POSTGRES CLIENT db POOL SIZE MIN 0 MAX 2
        CONFIG { 'addr' = 'postgres://127.0.0.1:5432/nervix?sslmode=disable&application_name=nervix' };
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE POSTGRES TABLE EMITTER outgoing FROM events TO db TABLE 'sales.Orders'
        MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        VALUES { 'seq' = input.seq, 'note' = input.note } ON CONFLICT <conflict>
        BATCH MAX MESSAGES 2 MAX SIZE 1073741822B
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER outgoing;
      """
    Then the last command output contains
      """
      TABLE 'sales.Orders'
      """
    And the last command output contains
      """
      ON CONFLICT <conflict>
      """

    Examples:
      | nodes | conflict                  |
      | 1     | DO NOTHING                |
      | 3     | DO NOTHING                |
      | 1     | ('seq') DO NOTHING        |
      | 3     | ('seq') DO NOTHING        |
      | 1     | ('seq', 'Note') DO UPDATE |
      | 3     | ('seq', 'Note') DO UPDATE |
