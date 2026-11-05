@nats_reply_validation
Feature: NATS reply subject validation
  Scenario Outline: A literal NATS reply subject is validated before the emitter runs
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE NATS CLIENT nats_main CONFIG { 'addr' = 'nats://127.0.0.1:9' };
      """
    When these Plexus statements fail with "NATS REPLY TO requires a valid publication subject"
      """
      CREATE NATS SUBJECT EMITTER request_out FROM events
        TO nats_main SUBJECT 'requests'
        MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec
        REPLY TO '<reply>' INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size | reply       |
      | 1            | replies.*   |
      | 3            | replies.*   |
      | 1            | replies..x  |
      | 3            | replies..x  |
      | 1            | reply space |
      | 3            | reply space |
