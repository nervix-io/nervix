@shutdown_qualification
Feature: Process shutdown deadline

  Scenario Outline: Graceful shutdown cancels an accepted resource upload <progress>
    Given a nervix-server process is started
    And the server process is configured with these NSPL commands
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE held_archive;
      """
    And an authenticated upload of resource "held_archive" <progress> is held open on the server process
    When the server process receives SIGTERM
    Then the server process exits with status 0 within "60s" of the last signal
    And the server process log contains "shutdown admission phase finished outcome=Completed"
    And the server process log contains "shutdown terminal-teardown phase finished outcome=Completed"

    Examples:
      | progress                      |
      | waiting for its first message |
      | waiting for its next chunk    |
      | sending chunks slowly         |

  Scenario Outline: The shutdown deadline ends a process whose drain cannot finish in a <pace> domain
    Given a nervix-server process is started with drain timeout "10m" and shutdown timeout "5s"
    And the server process is configured with these NSPL commands
      """
      <domain>
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
        TIMESTAMP NOW
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
      <start>
      """
    When http payload is posted to the server process with host "held-{{test_id}}.example.com" path "/events"
      """
      {"sequence":1}
      """
    And the server process receives SIGTERM
    Then the server process exits with status 1 no sooner than "5s" and within "60s" of the last signal
    And the server process log contains "shutdown deadline expired"

    Examples:
      | pace                                   | domain                                                       | start                             |
      | unpaced                                | CREATE UNPACED DOMAIN {{domain}};                            | START;                            |
      | paced a million times slower than real | CREATE PACED DOMAIN {{domain}} WITH PERIOD 1000h SKEW 1000h; | START AT NOW TIME RATE 0.000001;  |
      | paced a million times faster than real | CREATE PACED DOMAIN {{domain}} WITH PERIOD 1000h SKEW 1000h; | START AT NOW TIME RATE 1000000.0; |
