@deadlock_diagnostics
Feature: Diagnostic nodes track their blocking locks for active deadlocks
  A diagnostic build selects the `deloxide` mode: every thread-blocking lock of a process is tracked
  in one wait-for graph, and the process installs its deadlock detector once, before any tracked
  lock or runtime worker exists. The first active deadlock the detector reports is described on
  standard error, recorded as evidence, and ends the process. These scenarios run only in a scenario
  binary built for that mode, through `just test-deloxide`; the ordinary suite leaves them out.
  Deadlocks are provoked only in the disposable processes of nervix-deadlock's probes: no node can be
  made to deadlock on request.

  Scenario Outline: In-process diagnostic nodes run a workload with every blocking lock tracked on a <nodes> node cluster
    Given the scenario process tracks its blocking locks for deadlocks
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY incoming SCHEMA event UNBRANCHED;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-deadlock-diagnostics.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT event_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO incoming INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION route_events
        FROM incoming
        UNBRANCHED
        TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION outgoing_subscription TO outgoing;
      START;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-deadlock-diagnostics.example.com" path "/events"
      """
      {"seq":1}
      """
    Then the relay subscription receives a payload
      """
      "seq":1
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER JUNCTION route_events SET FILTER WHERE input.seq >= 10;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-deadlock-diagnostics.example.com" path "/events"
      """
      {"seq":10}
      """
    Then the relay subscription receives a payload
      """
      "seq":10
      """
    And the scenario process has recorded no deadlock findings

    Examples:
      | nodes |
      | 1     |
      | 3     |

  Scenario Outline: Diagnostic server processes record a running detector and stop gracefully on a <nodes> node cluster
    Given a <nodes> node diagnostic nervix-server process cluster is started with deadlock evidence
    And the server process cluster is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( seq I64 );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      START;
      """
    Then the server process cluster has schema "event"
    When every server process of the cluster receives SIGTERM
    Then every server process of the cluster exits with status 0
    And no server process log of the cluster contains the deadlock detector's start-up output
    And every server process of the cluster recorded a running deadlock detector and no findings

    Examples:
      | nodes |
      | 1     |
      | 3     |
