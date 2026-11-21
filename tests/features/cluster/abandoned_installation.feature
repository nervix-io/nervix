@abandoned_installation
Feature: An installation a node began finishes when the request that began it ends
  A node brings the work it runs up to date with the newest committed change in one installation.
  Several parts of Nervix start that installation: the node's own background reconciliation, the
  commands it runs, and the handlers of requests other nodes send it. A requester whose deadline
  passes resets its request, and the node drops the handler that served it. An installation that
  began still runs to its end; the caller that started it only stops waiting for the result. A
  stopped domain's teardown that such a handler began therefore ends the domain's tasks, writes
  its final snapshot and reports what the stop cut, and the domain's STOP answers.

  The scenario is topology-specific: the installation under test is begun by the drain-status
  request one node sends another, which a one-node cluster never sends.

  @slots_3
  Scenario: A stopped domain's teardown that a drain-status request began still completes after its requester gives up, and STOP answers
    Given HTTP receiver "stopping" is running
    And HTTP receiver "stopping" answers unscripted requests with "hold response until released"
    And HTTP receiver "pausing" is running
    And HTTP receiver "pausing" answers unscripted requests with "hold response until released"
    And runtime replication is configured with replica count 2 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( tenant STRING, value I64, source STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, value integer, source string );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_key TTL 30m;
      CREATE RELAY state SCHEMA event BRANCHED BY by_tenant WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST edge abandoned-installation-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/state';
      CREATE HTTP ENDPOINT INGESTOR ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO state INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT stopping_api CONFIG {
        'endpoint' = '{{http_receiver.stopping}}', 'timeout_ms' = 120000
      };
      CREATE HTTP REQUEST EMITTER published FROM state
        ATTACHED
        TO stopping_api METHOD 'POST' PATH concat('/events/', input.tenant)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "abandoned-installation-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"alpha","value":1,"source":"alpha-state"}
      """
    Then within "30s" node "node-1" eventually reports materialized state for relay "state" containing
      """
      {"branch":{"tenant":"alpha"},"fields":{"source":"alpha-state","tenant":"alpha","value":1}}
      """
    And HTTP receiver "stopping" eventually receives at least 1 request
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "follower"
    Given the active domain is saved as placeholder "stopped_domain"
    And the active domain is "{{stopped_domain}}_paused"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( id STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( id string );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SCHEMA marker ( id STRING );
      CREATE RELAY markers SCHEMA marker UNBRANCHED;
      CREATE VHOST edge abandoned-installation-paused-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT pausing_api CONFIG {
        'endpoint' = '{{http_receiver.pausing}}', 'timeout_ms' = 120000
      };
      CREATE HTTP REQUEST EMITTER published FROM events
        ATTACHED
        TO pausing_api METHOD 'POST' PATH concat('/events/', input.id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "abandoned-installation-paused-{{test_id}}.example.com" path "/events"
      """
      {"id":"1"}
      """
    Then HTTP receiver "pausing" eventually receives at least 1 request
    # Every node keeps the relay's records, and each writes them last for 10 seconds as the domain
    # stops: far longer than the 2-second deadline of a drain-status request.
    Given the active domain is "{{stopped_domain}}"
    And materialized relay "state" takes "10s" to write its state as its domain stops
    And the background reconciliation of node "{{follower}}" holds once domain "{{stopped_domain}}" is stopped
    # The stop waits for the publish its destination holds on the wire.
    When these Plexus commands begin executing in the background as "stop"
      """
      STOP;
      """
    # The schema change pauses the other domain. Its drain waits for that domain's own held publish,
    # so the leader keeps asking every node for its drain status, and each node applies the
    # committed state before it answers.
    Given the active domain is "{{stopped_domain}}_paused"
    When these Plexus commands begin executing in the background as "alter"
      """
      ALTER SCHEMA marker ADD FIELD note STRING OPTIONAL;
      """
    Then background Plexus execution "alter" is still running after "2s"
    # The stop's publish ends, so the stop ends. The follower's own reconciliation does not tear the
    # domain down; the follower's next drain-status answer does, and the leader's request deadline
    # passes while that teardown writes the relay's records: the leader resets the request and the
    # follower drops its handler.
    When HTTP receiver "stopping" releases its held responses with "respond 204"
    Then the background reconciliation of node "{{follower}}" reaches its hold
    And background Plexus execution "alter" is still running after "5s"
    When HTTP receiver "pausing" releases its held responses with "respond 204"
    And the background reconciliation of node "{{follower}}" is released
    Then within "60s" background Plexus execution "alter" reaches a successful outcome
    And within "60s" background Plexus execution "stop" reaches a successful outcome
    And the last command output contains
      """
      stopped domain '{{stopped_domain}}'
      """
