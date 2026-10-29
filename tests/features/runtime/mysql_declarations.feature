Feature: MySQL declarations describe the table and client Nervix can execute

  Scenario Outline: MySQL clients declare operation deadlines and paired TLS identity on <nodes> nodes
    Given MySQL is running
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has dev TLS resource directory "dev_tls"
    When these Plexus statements are executed on the leader node
      """
      CREATE MYSQL CLIENT db POOL SIZE MIN 0 MAX 2
        CONFIG {
          'addr' = 'mysql://127.0.0.1:3306/nervix?require_ssl=true',
          'timeout_ms' = '1500',
          'tls_cert_file' = '{{dev_tls}}/node.pem',
          'tls_key_file' = '{{dev_tls}}/node-key.pem'
        };
      SHOW CREATE CLIENT db;
      """
    Then the last command output contains
      """
      'timeout_ms' = '1500'
      """
    And the last command output contains
      """
      'tls_cert_file' = '{{dev_tls}}/node.pem'
      """
    And the last command output contains
      """
      'tls_key_file' = '{{dev_tls}}/node-key.pem'
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |

  Scenario Outline: MySQL table declarations refuse malformed paths on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE MYSQL CLIENT db POOL SIZE MIN 0 MAX 2
        CONFIG { 'addr' = 'mysql://127.0.0.1:3306/nervix' };
      """
    When these Plexus statements fail with "<reason>"
      """
      CREATE MYSQL TABLE EMITTER outgoing FROM events TO db TABLE '<table>'
        MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s VALUES { 'seq' = input.seq }
        BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes | table         | reason      |
      | 1     | .Orders       | empty level |
      | 3     | .Orders       | empty level |
      | 1     | shop.         | empty level |
      | 3     | shop.         | empty level |
      | 1     | shop..Orders  | empty level |
      | 3     | shop..Orders  | empty level |
      | 1     | a.shop.Orders | at most 2   |
      | 3     | a.shop.Orders | at most 2   |

  Scenario Outline: MySQL refuses duplicate mapped columns at CREATE and ALTER on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE MYSQL CLIENT db POOL SIZE MIN 0 MAX 2
        CONFIG { 'addr' = 'mysql://127.0.0.1:3306/nervix' };
      """
    When these Plexus statements fail with "duplicate"
      """
      CREATE MYSQL TABLE EMITTER outgoing FROM events TO db TABLE 'Orders'
        MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        VALUES { 'seq' = input.seq, 'seq' = input.seq }
        BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus statements are executed on the leader node
      """
      CREATE MYSQL TABLE EMITTER outgoing FROM events TO db TABLE 'Orders'
        MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s VALUES { 'seq' = input.seq }
        BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus statements fail with "duplicate"
      """
      ALTER MYSQL TABLE EMITTER outgoing SET TO db TABLE 'Orders'
        MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        VALUES { 'seq' = input.seq, 'seq' = input.seq };
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |

  Scenario Outline: MySQL refuses invalid operation deadlines and unpaired identity files on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "<reason>"
      """
      CREATE MYSQL CLIENT db POOL SIZE MIN 0 MAX 2
        CONFIG { 'addr' = 'mysql://127.0.0.1:3306/nervix', '<key>' = '<value>' };
      """

    Examples:
      | nodes | key           | value             | reason        |
      | 1     | timeout_ms    | 0                 | timeout_ms    |
      | 3     | timeout_ms    | 0                 | timeout_ms    |
      | 1     | timeout_ms    | 1.5               | timeout_ms    |
      | 3     | timeout_ms    | 1.5               | timeout_ms    |
      | 1     | tls_cert_file | /mounted/cert.pem | tls_key_file  |
      | 3     | tls_cert_file | /mounted/cert.pem | tls_key_file  |
      | 1     | tls_key_file  | /mounted/key.pem  | tls_cert_file |
      | 3     | tls_key_file  | /mounted/key.pem  | tls_cert_file |

  Scenario Outline: MySQL client declarations reserve URL pool sizing for POOL SIZE on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "POOL SIZE"
      """
      CREATE MYSQL CLIENT db POOL SIZE MIN 0 MAX 2
        CONFIG { 'addr' = 'mysql://user:private-pool-credential@127.0.0.1:3306/nervix?<key>=1' };
      """
    Then the last command error does not contain
      """
      private-pool-credential
      """

    Examples:
      | nodes | key      |
      | 1     | pool_min |
      | 3     | pool_min |
      | 1     | pool_max |
      | 3     | pool_max |

  Scenario Outline: MySQL declarations refuse unsupported publishing choices on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE MYSQL CLIENT db POOL SIZE MIN 0 MAX 2
        CONFIG { 'addr' = 'mysql://127.0.0.1:3306/nervix' };
      """
    When these Plexus statements fail with "<reason>"
      """
      CREATE MYSQL TABLE EMITTER outgoing FROM events TO db TABLE 'Orders'
        MODE <mode> RETRY POLICY BACKOFF 10ms MAX 1s VALUES { 'seq' = <mapping> }
        <clause> BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes | mode   | mapping                       | clause                        | reason       |
      | 1     | NO ACK | input.seq                     |                               | ACK          |
      | 3     | NO ACK | input.seq                     |                               | ACK          |
      | 1     | ACK    | write_header('trace', 'test') |                               | write_header |
      | 3     | ACK    | write_header('trace', 'test') |                               | write_header |
      | 1     | ACK    | input.seq                     | ON CONFLICT ('seq') DO UPDATE | DO           |
      | 3     | ACK    | input.seq                     | ON CONFLICT ('seq') DO UPDATE | DO           |

  Scenario Outline: MySQL declarations preserve qualified names and conflict choices on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64, data BYTES );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE MYSQL CLIENT db POOL SIZE MIN 0 MAX 2
        CONFIG { 'addr' = 'mysql://127.0.0.1:3306/nervix?require_ssl=true' };
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE MYSQL TABLE EMITTER outgoing FROM events TO db TABLE 'shop.Orders'
        MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        VALUES { 'seq' = input.seq, 'data' = input.data } ON CONFLICT <action>
        BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER outgoing;
      """
    Then the last command output contains
      """
      TABLE 'shop.Orders'
      """
    And the last command output contains
      """
      ON CONFLICT <action>
      """

    Examples:
      | nodes | action     |
      | 1     | DO NOTHING |
      | 3     | DO NOTHING |
      | 1     | DO UPDATE  |
      | 3     | DO UPDATE  |
