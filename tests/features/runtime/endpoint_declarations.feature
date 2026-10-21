Feature: Declared endpoint surfaces

  Scenario Outline: Placement completion follows each ingestor's declared execution scope
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY candidate_events SCHEMA event UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR candidate_producer
        FROM SCHEMA event MODE ACK SEQUENTIAL ACK TIMEOUT 5s
        RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE SUSPEND
        TO candidate_events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE VHOST edge endpoints-{{test_id}}.example.com;
      CREATE ENDPOINT http_intake ON edge PATH '/http' TYPE HTTP;
      CREATE ENDPOINT websocket_intake ON edge PATH '/ws' TYPE WEBSOCKETS;
      CREATE HTTP ENDPOINT INGESTOR candidate_http
        FROM http_intake MODE NO ACK ON QUIESCE REJECT RETRY AFTER 1s
        DECODE USING event_codec
        TO candidate_events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WEBSOCKETS ENDPOINT INGESTOR candidate_websocket
        FROM websocket_intake MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB
        DECODE USING event_codec
        TO candidate_events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT syslog_listener TYPE SYSLOG
        CONFIG { 'protocol' = 'udp', 'addr' = '127.0.0.1:5514' };
      CREATE SYSLOG LISTENER INGESTOR candidate_syslog
        FROM syslog_listener MODE NO ACK ON QUIESCE SUSPEND
        DECODE USING event_codec
        TO candidate_events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When the active session collects completion pages for "CREATE PLACEMENT p FROM candidate_" at byte 34 with page size 2
    Then the collected completion candidates are
      """
      candidate_events
      candidate_producer
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Endpoint ingestors refuse unsupported window and transport clauses
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge refusals-{{test_id}}.example.com;
      CREATE ENDPOINT intake ON edge PATH '/events' TYPE <protocol>;
      CREATE <protocol> ENDPOINT INGESTOR rejected
        FROM intake MODE NO ACK ON QUIESCE REJECT RETRY AFTER 1s
        DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail
      """
      <operation> <protocol> ENDPOINT INGESTOR rejected
        FROM intake <clauses>
        DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      parse error
      """

    Examples:
      | cluster_size | operation | protocol   | clauses                                                                                                 |
      | 1            | CREATE    | HTTP       | MODE ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE REJECT RETRY AFTER 1s |
      | 3            | CREATE    | HTTP       | MODE ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE REJECT RETRY AFTER 1s |
      | 1            | CREATE    | WEBSOCKETS | MODE ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE REJECT RETRY AFTER 1s |
      | 3            | CREATE    | WEBSOCKETS | MODE ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE REJECT RETRY AFTER 1s |
      | 1            | CREATE    | HTTP       | MODE ACK MAX 8 ACK TIMEOUT 5s ON QUIESCE BUFFER MAX SIZE 1MiB                                           |
      | 3            | CREATE    | HTTP       | MODE ACK MAX 8 ACK TIMEOUT 5s ON QUIESCE BUFFER MAX SIZE 1MiB                                           |
      | 1            | CREATE    | WEBSOCKETS | MODE ACK MAX 8 ACK TIMEOUT 5s ON QUIESCE BUFFER MAX SIZE 1MiB                                           |
      | 3            | CREATE    | WEBSOCKETS | MODE ACK MAX 8 ACK TIMEOUT 5s ON QUIESCE BUFFER MAX SIZE 1MiB                                           |
      | 1            | CREATE    | HTTP       | MODE ACK MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE REJECT RETRY AFTER 1s          |
      | 3            | CREATE    | HTTP       | MODE ACK MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE REJECT RETRY AFTER 1s          |
      | 1            | CREATE    | WEBSOCKETS | MODE ACK MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE REJECT RETRY AFTER 1s          |
      | 3            | CREATE    | WEBSOCKETS | MODE ACK MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE REJECT RETRY AFTER 1s          |
      | 1            | ALTER     | HTTP       | MODE ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE REJECT RETRY AFTER 1s |
      | 3            | ALTER     | HTTP       | MODE ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE REJECT RETRY AFTER 1s |
      | 1            | ALTER     | WEBSOCKETS | MODE ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE REJECT RETRY AFTER 1s |
      | 3            | ALTER     | WEBSOCKETS | MODE ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE REJECT RETRY AFTER 1s |
      | 1            | ALTER     | HTTP       | MODE ACK MAX 8 ACK TIMEOUT 5s ON QUIESCE BUFFER MAX SIZE 1MiB                                           |
      | 3            | ALTER     | HTTP       | MODE ACK MAX 8 ACK TIMEOUT 5s ON QUIESCE BUFFER MAX SIZE 1MiB                                           |
      | 1            | ALTER     | WEBSOCKETS | MODE ACK MAX 8 ACK TIMEOUT 5s ON QUIESCE BUFFER MAX SIZE 1MiB                                           |
      | 3            | ALTER     | WEBSOCKETS | MODE ACK MAX 8 ACK TIMEOUT 5s ON QUIESCE BUFFER MAX SIZE 1MiB                                           |
      | 1            | ALTER     | HTTP       | MODE ACK MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE REJECT RETRY AFTER 1s          |
      | 3            | ALTER     | HTTP       | MODE ACK MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE REJECT RETRY AFTER 1s          |
      | 1            | ALTER     | WEBSOCKETS | MODE ACK MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE REJECT RETRY AFTER 1s          |
      | 3            | ALTER     | WEBSOCKETS | MODE ACK MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE REJECT RETRY AFTER 1s          |
