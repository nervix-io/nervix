Feature: MySQL declarations describe the table and client Nervix can execute

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
      | 1     | a.shop.Orders | 2 levels    |
      | 3     | a.shop.Orders | 2 levels    |

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
