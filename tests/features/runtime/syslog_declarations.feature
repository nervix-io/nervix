Feature: Syslog listener and collector declarations
  SYSLOG LISTENER runs on every node and SYSLOG COLLECTOR sends through one client. Both sides
  deliver without acknowledgements and carry no transport headers, and what each declares applies
  at CREATE without opening a socket.

  Scenario Outline: Syslog declarations render the listener and the collector they declare
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (host STRING, message STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (host string, message string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_main CONFIG { 'protocol' = 'udp', 'addr' = '127.0.0.1:5514' };
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SYSLOG LISTENER INGESTOR intake
        FROM syslog_main MODE NO ACK ON QUIESCE <quiesce> DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      SHOW CREATE INGESTOR intake;
      """
    Then the last command output contains
      """
      FROM syslog_main MODE NO ACK ON QUIESCE <quiesce>
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SYSLOG COLLECTOR EMITTER publishing FROM events
        TO syslog_main MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING event_codec INHERIT ALL
        <batch>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER publishing;
      """
    Then the last command output contains
      """
      MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
      """
    And the last command output contains
      """
      <shown>
      """

    Examples:
      | cluster_size | quiesce                                      | batch                                | shown                                |
      | 1            | SUSPEND                                      |                                      | ENCODE USING event_codec             |
      | 3            | SUSPEND                                      |                                      | ENCODE USING event_codec             |
      | 1            | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | BATCH MAX MESSAGES 10 MAX SIZE 64KiB |
      | 3            | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | BATCH MAX MESSAGES 10 MAX SIZE 64KiB |
      | 1            | DROP                                         |                                      | ENCODE USING event_codec             |
      | 3            | DROP                                         |                                      | ENCODE USING event_codec             |

  Scenario Outline: Syslog listeners refuse delivery, instancing and quiesce they do not declare
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (host STRING, message STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (host string, message string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_main CONFIG { 'protocol' = 'udp', 'addr' = '127.0.0.1:5514' };
      """
    When these Plexus commands fail
      """
      CREATE SYSLOG LISTENER INGESTOR refused
        FROM syslog_main <options>MODE <mode> ON QUIESCE <quiesce> DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | options     | mode                                                          | quiesce               | expected                       |
      | 1            |             | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | SUSPEND               | expected NO ACK, found ACK     |
      | 3            |             | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | SUSPEND               | expected NO ACK, found ACK     |
      | 1            | INSTANCES 2 | NO ACK                                                        | SUSPEND               | expected MODE, found INSTANCES |
      | 3            | INSTANCES 2 | NO ACK                                                        | SUSPEND               | expected MODE, found INSTANCES |
      | 1            |             | NO ACK                                                        | REJECT RETRY AFTER 5s | found REJECT                   |
      | 3            |             | NO ACK                                                        | REJECT RETRY AFTER 5s | found REJECT                   |

  Scenario Outline: Syslog collectors refuse publishing modes they do not declare
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (host STRING, message STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (host string, message string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_main CONFIG { 'protocol' = 'udp', 'addr' = '127.0.0.1:5514' };
      """
    When these Plexus commands fail
      """
      CREATE SYSLOG COLLECTOR EMITTER refused FROM events TO syslog_main
        MODE <mode> ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      found <found>
      """

    Examples:
      | cluster_size | mode                                   | found  |
      | 1            | ACK RETRY POLICY BACKOFF 1s MAX 30s    | ACK    |
      | 3            | ACK RETRY POLICY BACKOFF 1s MAX 30s    | ACK    |
      | 1            | SINGLE RETRY POLICY BACKOFF 1s MAX 30s | SINGLE |
      | 3            | SINGLE RETRY POLICY BACKOFF 1s MAX 30s | SINGLE |

  Scenario Outline: Syslog entities refuse transport headers on both sides
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (host STRING, message STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (host string, message string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_main CONFIG { 'protocol' = 'udp', 'addr' = '127.0.0.1:5514' };
      """
    When these Plexus commands fail
      """
      CREATE SYSLOG LISTENER INGESTOR reading
        FROM syslog_main MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL SET host = coalesce(read_header('host'), 'absent')
        UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      read_header
      """
    When these Plexus commands fail
      """
      CREATE SYSLOG COLLECTOR EMITTER writing FROM events TO syslog_main
        MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING event_codec INHERIT ALL
        INVOKE write_header('host', input.host)
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      write_header
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
