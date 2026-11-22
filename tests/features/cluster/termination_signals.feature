@shutdown_qualification
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
    # The emitter's destination accepts its connection and never answers, so its publish stays on
    # the wire for the client's ten-minute timeout, and the save waits for it.
    Given a silent endpoint "stalled_api" is listening
    And a nervix-server process is started
    And the server process is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the server process is configured with these Plexus commands
      """
      CREATE SCHEMA held_event ( sequence I64 );
      CREATE WIRE JSON SCHEMA held_event_wire MODE STRICT ( sequence integer );
      CREATE WIRE JSON CODEC held_event_codec
        FROM WIRE SCHEMA held_event_wire
        TO SCHEMA held_event;
      CREATE RELAY held_events SCHEMA held_event UNBRANCHED;
      CREATE VHOST edge held-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT held_ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR held_ingestor
        FROM held_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING held_event_codec
        TO held_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT stalled_api CONFIG {
        'endpoint' = 'http://{{stalled_api}}', 'timeout_ms' = 600000
      };
      CREATE HTTP REQUEST EMITTER held_output FROM held_events
        TO stalled_api METHOD 'POST' PATH '/events'
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to the server process with host "held-{{test_id}}.example.com" path "/events"
      """
      {"sequence":1}
      """
    Then the silent endpoint "stalled_api" eventually accepts a connection
    When the server process receives <first>
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
