Feature: Process termination signals

  Scenario Outline: The first termination signal starts graceful shutdown of a server process
    Given a nervix-server process is started
    When the server process receives <signal>
    Then the server process exits with status 0
    And the server process log contains "termination signal received; requesting graceful shutdown signal=<signal>"
    And the server process log contains "shutdown terminal-teardown phase finished"

    Examples:
      | signal  |
      | SIGINT  |
      | SIGTERM |

  Scenario Outline: A repeated termination signal forces termination while graceful shutdown is stalled
    Given a nervix-server process is started with drain timeout "10m"
    And the server process is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the server process is configured with these NSPL commands
      """
      CREATE SCHEMA held_event ( sequence I64 );
      CREATE WIRE JSON SCHEMA held_event_wire MODE STRICT ( sequence integer );
      CREATE CODEC held_event_codec
        FROM WIRE JSON SCHEMA held_event_wire
        TO SCHEMA held_event;
      CREATE RELAY held_events SCHEMA held_event UNBRANCHED;
      CREATE VHOST edge held-{{test_id}}.example.com;
      CREATE ENDPOINT held_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR held_source
        FROM ENDPOINT held_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING held_event_codec
        TO held_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT unreachable_sink TYPE SYSLOG CONFIG {
        'protocol' = 'tcp',
        'addr' = '{{syslog_emit_addr}}'
      };
      CREATE EMITTER held_output FROM held_events
        TO SYSLOG unreachable_sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING held_event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to the server process with host "held-{{test_id}}.example.com" path "/events"
      """
      {"sequence":1}
      """
    And the server process receives <first>
    Then the server process log eventually contains "termination signal received; requesting graceful shutdown signal=<first>"
    When the server process receives <second>
    Then the server process exits with status <status> within "10s" of the last signal
    And the server process log contains "repeated termination signal received; abandoning graceful shutdown signal=<second>"
    And the server process log does not contain "shutdown terminal-teardown phase finished"

    Examples:
      | first   | second  | status |
      | SIGINT  | SIGINT  | 130    |
      | SIGTERM | SIGTERM | 143    |
      | SIGINT  | SIGTERM | 143    |
      | SIGTERM | SIGINT  | 130    |

  Scenario: A server process that cannot register termination signal handlers refuses to start
    Given a nervix-server process is started with an open file limit of 4
    Then the server process exits with status 1
    And the server process log contains "failed to register termination signal handlers"
