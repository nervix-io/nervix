Feature: Nervix refuses connector declarations that cannot execute

  Scenario Outline: OTEL resource mappings require literal values on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( event_ts DATETIME, body STRING );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE OTEL CLIENT destination CONFIG { 'endpoint' = 'http://127.0.0.1:4317', 'protocol' = 'grpc' };
      """
    When these Plexus statements fail with "RESOURCE"
      """
      CREATE OTEL LOGS EMITTER outgoing FROM events TO destination
        MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        VALUES { 'time' = input.event_ts, 'body' = input.body }
        RESOURCE { 'service.name' = input.body }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |

  Scenario Outline: Declared exact values and resource literals remain usable on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( event_ts DATETIME, body STRING, severity I32 );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE OTEL CLIENT destination CONFIG { 'endpoint' = 'http://127.0.0.1:4317', 'protocol' = 'grpc' };
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE OTEL LOGS EMITTER outgoing FROM events TO destination
        MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        VALUES { 'time' = input.event_ts, 'body' = input.body, 'severity_number' = input.severity }
        ATTRIBUTES { 'codes' = [1, 2], 'service' = input.body }
        RESOURCE { 'service.name' = 'example', 'attempts' = [1, 2] }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER outgoing;
      """
    Then the last command output contains
      """
      'severity_number' = input.severity
      """
    When these Plexus statements fail with "VALUES key 'severity_number' requires I32, found Int64"
      """
      ALTER OTEL LOGS EMITTER outgoing SET TO destination
        MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        VALUES { 'time' = input.event_ts, 'body' = input.body, 'severity_number' = 9 };
      """
    And these Plexus statements are executed on the leader node
      """
      SHOW CREATE EMITTER outgoing;
      """
    Then the last command output contains
      """
      'severity_number' = input.severity
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |

  Scenario Outline: Transport ingestor durations are refused at CREATE on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      """
    When these Plexus statements fail with "<clause>"
      """
      CREATE KAFKA TOPIC INGESTOR incoming
        FROM broker TOPIC 'Orders' OFFSET BY CONSUMER GROUP 'Readers'
        MODE ACK PARALLEL MAX 2 BATCH TIMEOUT <batch_timeout> ACK TIMEOUT <ack_timeout>
        RETRY POLICY BACKOFF <backoff> MAX <ceiling>
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes | batch_timeout        | ack_timeout | backoff | ceiling | clause           |
      | 1     | 10ms                 | nonsense    | 10ms    | 1s      | ACK TIMEOUT      |
      | 3     | 10ms                 | nonsense    | 10ms    | 1s      | ACK TIMEOUT      |
      | 1     | 1000000000000000000s | 1s          | 10ms    | 1s      | BATCH TIMEOUT    |
      | 3     | 1000000000000000000s | 1s          | 10ms    | 1s      | BATCH TIMEOUT    |
      | 1     | 10ms                 | 1s          | 2s      | 1s      | RETRY POLICY MAX |
      | 3     | 10ms                 | 1s          | 2s      | 1s      | RETRY POLICY MAX |

  Scenario Outline: Duplicate database mappings are refused at CREATE on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE <connector> CLIENT destination <pool> CONFIG { 'addr' = '<address>' <config> };
      """
    When these Plexus statements fail with "duplicate VALUES key 'seq'"
      """
      CREATE <connector> <entity> EMITTER outgoing FROM events
        TO destination <entity> 'events'
        MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        VALUES { 'seq' = input.seq, 'seq' = input.seq }
        BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes | connector  | entity     | pool                  | address                                     | config                  |
      | 1     | CLICKHOUSE | TABLE      |                       | http://127.0.0.1:8123                       |                         |
      | 3     | CLICKHOUSE | TABLE      |                       | http://127.0.0.1:8123                       |                         |
      | 1     | POSTGRES   | TABLE      | POOL SIZE MIN 1 MAX 2 | postgres://127.0.0.1/events?sslmode=disable |                         |
      | 3     | POSTGRES   | TABLE      | POOL SIZE MIN 1 MAX 2 | postgres://127.0.0.1/events?sslmode=disable |                         |
      | 1     | MYSQL      | TABLE      | POOL SIZE MIN 1 MAX 2 | mysql://127.0.0.1/events                    |                         |
      | 3     | MYSQL      | TABLE      | POOL SIZE MIN 1 MAX 2 | mysql://127.0.0.1/events                    |                         |
      | 1     | MONGODB    | COLLECTION | POOL SIZE MIN 1 MAX 2 | mongodb://127.0.0.1:27017                   | , 'database' = 'events' |
      | 3     | MONGODB    | COLLECTION | POOL SIZE MIN 1 MAX 2 | mongodb://127.0.0.1:27017                   | , 'database' = 'events' |

  Scenario Outline: OTEL mapped values require the declared exact type on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( event_ts DATETIME, body STRING );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE OTEL CLIENT destination CONFIG { 'endpoint' = 'http://127.0.0.1:4317', 'protocol' = 'grpc' };
      """
    When these Plexus statements fail with "VALUES key 'severity_number' requires I32, found Int64"
      """
      CREATE OTEL LOGS EMITTER outgoing FROM events TO destination
        MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
        VALUES { 'time' = input.event_ts, 'body' = input.body, 'severity_number' = 9 }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |

  Scenario Outline: An invalid delivery ALTER keeps the accepted ingestor on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE KAFKA TOPIC INGESTOR incoming
        FROM broker TOPIC 'Orders' OFFSET BY CONSUMER GROUP 'Readers'
        MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 10ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "BATCH TIMEOUT"
      """
      ALTER KAFKA TOPIC INGESTOR incoming
        SET FROM broker TOPIC 'Orders' OFFSET BY CONSUMER GROUP 'Readers'
        MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 1000000000000000000s
        ACK TIMEOUT 1s RETRY POLICY BACKOFF 10ms MAX 1s ON QUIESCE SUSPEND;
      """
    And these Plexus statements are executed on the leader node
      """
      SHOW CREATE INGESTOR incoming;
      """
    Then the last command output contains
      """
      MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 10ms MAX 1s
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |

  Scenario Outline: Batching follows the destination declaration on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE HTTP CLIENT api CONFIG { 'endpoint' = 'http://127.0.0.1:8080', 'timeout_ms' = 5000 };
      CREATE HTTP REQUEST EMITTER accepted_request FROM events TO api
        METHOD 'GET' PATH '/' MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE KAFKA TOPIC EMITTER outgoing FROM events TO broker TOPIC 'Orders'
        MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec
        INHERIT ALL BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "found BATCH"
      """
      CREATE HTTP REQUEST EMITTER request FROM events TO api
        METHOD 'GET' PATH '/' MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s WITHOUT BODY
        BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus statements are executed on the leader node
      """
      SHOW CREATE EMITTER outgoing;
      """
    Then the last command output contains
      """
      BATCH MAX MESSAGES 2 MAX SIZE 1MiB
      """
    When these Plexus statements fail with "ACK"
      """
      ALTER HTTP REQUEST EMITTER accepted_request
        SET MODE NO ACK RETRY POLICY BACKOFF 10ms MAX 1s;
      """
    And these Plexus statements are executed on the leader node
      """
      SHOW CREATE EMITTER accepted_request;
      """
    Then the last command output contains
      """
      MODE ACK RETRY POLICY BACKOFF 10ms MAX 1s
      """

    Examples:
      | nodes |
      | 1     |
      | 3     |
