Feature: Interconnect relay admission
  Scenario: A waiting relay admission leaves another domain runnable
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN blocked_{{test_id}};
      CREATE UNPACED DOMAIN runnable_{{test_id}};
      """
    Given the active domain is "blocked_{{test_id}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY input SCHEMA event UNBRANCHED;
      CREATE CLIENT sink TYPE ZEROMQ CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge blocked-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER sink_output FROM input
        TO ZEROMQ sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Given the active domain is "runnable_{{test_id}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY input SCHEMA event UNBRANCHED;
      CREATE CLIENT sink TYPE ZEROMQ CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge runnable-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER sink_output FROM input
        TO ZEROMQ sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    Then node "node-3" eventually forwards http traffic for host "runnable-{{test_id}}.example.com" path "/events" to the observed broker
      """
      {"seq":0}
      """
    Given remote relay admission for domain "blocked_{{test_id}}" is paused
    And remote relay admission for domain "runnable_{{test_id}}" is paused
    When http payload begins posting in the background to node "node-3" with host "blocked-{{test_id}}.example.com" path "/events"
      """
      {"seq":1}
      """
    Then the remote relay admission pause for domain "blocked_{{test_id}}" is reached
    When http payload is posted to node "node-3" with host "runnable-{{test_id}}.example.com" path "/events"
      """
      {"seq":2}
      """
    Then the remote relay admission pause for domain "runnable_{{test_id}}" is reached
    When the remote relay admission pause for domain "runnable_{{test_id}}" is released
    And the remote relay admission pause for domain "blocked_{{test_id}}" is released
    Then the background http publish succeeds

  Scenario: A waiting relay admission leaves another branch runnable
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN branches_{{test_id}};
      """
    Given the active domain is "branches_{{test_id}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event (
        tenant STRING,
        seq I64
      );
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant
        SCHEMA tenant_branch TTL 5m MAX INSTANCES 8 EVICT LRU;
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (
        tenant string,
        seq integer
      );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY input SCHEMA event BRANCHED BY by_tenant;
      CREATE CLIENT sink TYPE ZEROMQ CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge branches-{{test_id}}.example.com;
      CREATE ENDPOINT blocked_ingress ON edge PATH '/blocked' TYPE HTTP;
      CREATE ENDPOINT runnable_ingress ON edge PATH '/runnable' TYPE HTTP;
      CREATE INGESTOR blocked_source
        FROM ENDPOINT blocked_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO input INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INGESTOR runnable_source
        FROM ENDPOINT runnable_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO input INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER sink_output FROM input
        TO ZEROMQ sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    Then node "node-3" eventually forwards http traffic for host "branches-{{test_id}}.example.com" path "/runnable" to the observed broker
      """
      {"tenant":"warmup","seq":0}
      """
    Given remote relay admission for branch '{"tenant":"blocked"}' in domain "branches_{{test_id}}" is paused
    And remote relay admission for branch '{"tenant":"runnable"}' in domain "branches_{{test_id}}" is paused
    When http payload begins posting in the background to node "node-3" with host "branches-{{test_id}}.example.com" path "/blocked"
      """
      {"tenant":"blocked","seq":1}
      """
    Then the remote relay admission pause for branch '{"tenant":"blocked"}' in domain "branches_{{test_id}}" is reached
    When http payload is posted to node "node-3" with host "branches-{{test_id}}.example.com" path "/runnable"
      """
      {"tenant":"runnable","seq":2}
      """
    Then the remote relay admission pause for branch '{"tenant":"runnable"}' in domain "branches_{{test_id}}" is reached
    When the remote relay admission pause for branch '{"tenant":"runnable"}' in domain "branches_{{test_id}}" is released
    And the remote relay admission pause for branch '{"tenant":"blocked"}' in domain "branches_{{test_id}}" is released
    Then the background http publish succeeds

  Scenario: Evicting a branch cancels its waiting remote admission
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN cancellation_{{test_id}};
      """
    Given the active domain is "cancellation_{{test_id}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event (
        tenant STRING,
        seq I64
      );
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant
        SCHEMA tenant_branch TTL 5m MAX INSTANCES 1 EVICT LRU;
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (
        tenant string,
        seq integer
      );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY input SCHEMA event BRANCHED BY by_tenant;
      CREATE CLIENT sink TYPE ZEROMQ CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge cancellation-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO input INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER sink_output FROM input
        TO ZEROMQ sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    Then node "node-3" eventually forwards http traffic for host "cancellation-{{test_id}}.example.com" path "/events" to the observed broker
      """
      {"tenant":"warmup","seq":0}
      """
    Given remote relay admission for branch '{"tenant":"cancelled"}' in domain "cancellation_{{test_id}}" is paused
    When http payload begins posting in the background to node "node-3" with host "cancellation-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"cancelled","seq":1}
      """
    Then the remote relay admission pause for branch '{"tenant":"cancelled"}' in domain "cancellation_{{test_id}}" is reached
    When http payload is posted to node "node-3" with host "cancellation-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"replacement","seq":2}
      """
    Then the observed broker receives a payload
      """
      {"tenant":"replacement","seq":2}
      """
    And the background http publish succeeds
    When the remote relay admission pause for branch '{"tenant":"cancelled"}' in domain "cancellation_{{test_id}}" is released
    Then the observed broker does not receive a payload within "500ms"
