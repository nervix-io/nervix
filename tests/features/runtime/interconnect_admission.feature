Feature: Interconnect relay admission
  @domain_clock_http2
  Scenario: A waiting historical slow-domain relay admission leaves a fast domain runnable
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE PACED DOMAIN blocked_{{test_id}} WITH PERIOD 100us SKEW 100ms;
      CREATE PACED DOMAIN runnable_{{test_id}} WITH PERIOD 100ms SKEW 100ms;
      """
    Given the active domain is "blocked_{{test_id}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY input SCHEMA event UNBRANCHED;
      CREATE ZEROMQ CLIENT sink CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge blocked-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR source
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TIMESTAMP NOW
        TO input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER sink_output FROM input
        TO ZEROMQ sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 0.0001;
      """
    Given the active domain is "runnable_{{test_id}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY input SCHEMA event UNBRANCHED;
      CREATE ZEROMQ CLIENT sink CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge runnable-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR source
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TIMESTAMP NOW
        TO input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER sink_output FROM input
        TO ZEROMQ sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 100.0;
      """
    When these Plexus commands are executed through the client on node "node-1"
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
    When these Plexus commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN branches_{{test_id}};
      """
    Given the active domain is "branches_{{test_id}}"
    When these Plexus commands are executed on the leader node
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
      CREATE ZEROMQ CLIENT sink CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge branches-{{test_id}}.example.com;
      CREATE ENDPOINT blocked_ingress ON edge PATH '/blocked' TYPE HTTP;
      CREATE ENDPOINT runnable_ingress ON edge PATH '/runnable' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR blocked_source
        FROM blocked_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO input INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR runnable_source
        FROM runnable_ingress MODE NO ACK
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
    When these Plexus commands are executed through the client on node "node-1"
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
    When these Plexus commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN cancellation_{{test_id}};
      """
    Given the active domain is "cancellation_{{test_id}}"
    When these Plexus commands are executed on the leader node
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
      CREATE ZEROMQ CLIENT sink CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge cancellation-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR source
        FROM ingress MODE NO ACK
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
    When these Plexus commands are executed through the client on node "node-1"
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

  # Both relays share the branch key, under branches with different TTLs. On node-3 the endpoint
  # ingestors hold both branches, and both relays have their owner on node-1. The expiring branch
  # belongs to the idle relay alone, so the admission the active relay's branch is still waiting
  # for must survive it.
  Scenario: An idle branch that expires leaves another relay's waiting admission for the same key
    Given branched relay expiration scan interval is configured as "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN expiry_{{test_id}};
      """
    Given the active domain is "expiry_{{test_id}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (
        tenant STRING,
        seq I64
      );
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_active_tenant SCHEMA tenant_branch TTL 1h;
      CREATE BRANCH by_idle_tenant SCHEMA tenant_branch TTL 500ms;
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (
        tenant string,
        seq integer
      );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY active_events SCHEMA event BRANCHED BY by_active_tenant;
      CREATE RELAY idle_events SCHEMA event BRANCHED BY by_idle_tenant;
      CREATE ZEROMQ CLIENT sink CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge expiry-{{test_id}}.example.com;
      CREATE ENDPOINT active_ingress ON edge PATH '/active' TYPE HTTP;
      CREATE ENDPOINT idle_ingress ON edge PATH '/idle' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR active_source
        FROM active_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO active_events INHERIT ALL BRANCHED BY by_active_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR idle_source
        FROM idle_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO idle_events INHERIT ALL BRANCHED BY by_idle_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER active_output FROM active_events
        TO ZEROMQ sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER idle_output FROM idle_events
        TO ZEROMQ sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When these Plexus commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    Then node "node-3" eventually forwards http traffic for host "expiry-{{test_id}}.example.com" path "/active" to the observed broker
      """
      {"tenant":"warmup","seq":0}
      """
    Given remote relay admission for branch '{"tenant":"shared"}' in domain "expiry_{{test_id}}" is paused
    When http payload begins posting in the background to node "node-3" with host "expiry-{{test_id}}.example.com" path "/active"
      """
      {"tenant":"shared","seq":1}
      """
    Then the remote relay admission pause for branch '{"tenant":"shared"}' in domain "expiry_{{test_id}}" is reached
    When http payload is posted to node "node-3" with host "expiry-{{test_id}}.example.com" path "/idle"
      """
      {"tenant":"shared","seq":2}
      """
    Then node "node-3" observability metric "nervix_branch_evictions_total" with labels eventually equals 1
      """
      domain="expiry_{{test_id}}"
      branch="by_idle_tenant"
      physical_node_id="node-3"
      reason="ttl"
      """
    When the remote relay admission pause for branch '{"tenant":"shared"}' in domain "expiry_{{test_id}}" is released
    Then within "30s" the observed broker receives payloads
      """
      {"tenant":"shared","seq":1}
      """
    And the background http publish succeeds

  # Every node-3 branch opens its own relay channel to the relay's owner on node-1. The first 600
  # tenants' branches expire, and then the same tenants return with new branches, so 1,201 channels
  # from node-3 reach node-1 within the ten minutes the receiver retains a channel's watermark,
  # more than the incoming queue capacity of 1,024 that bounds the work a peer has in flight.
  # Each acknowledged request completes before the next tenant posts, so channel history grows
  # independently of the active work the receiver admits.
  @relay_channel_turnover @long_running
  Scenario: A node keeps delivering to another node's relay owner after more than 1,024 of its branch channels turn over
    Given branched relay expiration scan interval is configured as "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus statements are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN turnover_{{test_id}};
      """
    Given the active domain is "turnover_{{test_id}}"
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA event (
        tenant STRING,
        seq I64
      );
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 30s;
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (
        tenant string,
        seq integer
      );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE ZEROMQ CLIENT sink CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge turnover-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR source
        FROM ingress MODE ACK MAX 1 ACK TIMEOUT 30s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER output FROM events
        TO ZEROMQ sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When these Plexus statements are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    When endpoint request "warmup" is posted on node "node-3" host "turnover-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"warmup","seq":0}
      """
    Then endpoint request "warmup" answers HTTP 200 with "completed" cause "none"
    And within "15s" the observed broker receives payloads
      """
      {"tenant":"warmup","seq":0}
      """
    When 600 http payloads with distinct tenants and sequence 1 are acknowledged by node "node-3" with host "turnover-{{test_id}}.example.com" path "/events"
    Then within "120s" the observed broker receives every generated payload exactly once
    And within "120s" node "node-3" observability metric "nervix_branch_evictions_total" with labels eventually equals 601
      """
      domain="turnover_{{test_id}}"
      branch="by_tenant"
      physical_node_id="node-3"
      reason="ttl"
      """
    When 600 http payloads with distinct tenants and sequence 2 are acknowledged by node "node-3" with host "turnover-{{test_id}}.example.com" path "/events"
    Then within "120s" the observed broker receives every generated payload exactly once
