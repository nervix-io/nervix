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
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY input SCHEMA event UNBRANCHED;
      CREATE ZEROMQ CLIENT emitter CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge blocked-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TIMESTAMP NOW
        TO input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ PUSH EMITTER emitter_output FROM input
        TO emitter MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 0.0001;
      """
    Given the active domain is "runnable_{{test_id}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY input SCHEMA event UNBRANCHED;
      CREATE ZEROMQ CLIENT emitter CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge runnable-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TIMESTAMP NOW
        TO input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ PUSH EMITTER emitter_output FROM input
        TO emitter MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 5s
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
      CREATE PARAMETRIZER by_tenant
        SCHEMA tenant_branch TTL 5m MAX INSTANCES 8 EVICT LRU;
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (
        tenant string,
        seq integer
      );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY input SCHEMA event BRANCHED BY by_tenant;
      CREATE ZEROMQ CLIENT emitter CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge branches-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT blocked_ingress ON edge PATH '/blocked';
      CREATE HTTP ENDPOINT runnable_ingress ON edge PATH '/runnable';
      CREATE HTTP ENDPOINT INGESTOR blocked_ingestor
        FROM blocked_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO input INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR runnable_ingestor
        FROM runnable_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO input INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ PUSH EMITTER emitter_output FROM input
        TO emitter MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 5s
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
      CREATE PARAMETRIZER by_tenant
        SCHEMA tenant_branch TTL 5m MAX INSTANCES 1 EVICT LRU;
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (
        tenant string,
        seq integer
      );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY input SCHEMA event BRANCHED BY by_tenant;
      CREATE ZEROMQ CLIENT emitter CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge cancellation-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO input INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ PUSH EMITTER emitter_output FROM input
        TO emitter MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 5s
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
      CREATE PARAMETRIZER by_active_tenant SCHEMA tenant_branch TTL 1h;
      CREATE PARAMETRIZER by_idle_tenant SCHEMA tenant_branch TTL 500ms;
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (
        tenant string,
        seq integer
      );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY active_events SCHEMA event BRANCHED BY by_active_tenant;
      CREATE RELAY idle_events SCHEMA event BRANCHED BY by_idle_tenant;
      CREATE ZEROMQ CLIENT emitter CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge expiry-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT active_ingress ON edge PATH '/active';
      CREATE HTTP ENDPOINT idle_ingress ON edge PATH '/idle';
      CREATE HTTP ENDPOINT INGESTOR active_ingestor
        FROM active_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO active_events INHERIT ALL BRANCHED BY by_active_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR idle_ingestor
        FROM idle_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO idle_events INHERIT ALL BRANCHED BY by_idle_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ PUSH EMITTER active_output FROM active_events
        TO emitter MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ PUSH EMITTER idle_output FROM idle_events
        TO emitter MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 5s
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
      parametrizer="by_idle_tenant"
      physical_node_id="node-3"
      reason="ttl"
      """
    When the remote relay admission pause for branch '{"tenant":"shared"}' in domain "expiry_{{test_id}}" is released
    Then within "30s" the observed broker receives payloads
      """
      {"tenant":"shared","seq":1}
      """
    And the background http publish succeeds

  Scenario: An idle producer expires while another producer delivers the same relay branch
    Given branched relay expiration scan interval is configured as "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN shared_expiry_{{test_id}};
      """
    Given the active domain is "shared_expiry_{{test_id}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (
        tenant STRING,
        seq I64
      );
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_branch TTL 10s;
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (
        tenant string,
        seq integer
      );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE ZEROMQ CLIENT emitter CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge shared-expiry-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT active_ingress ON edge PATH '/active';
      CREATE HTTP ENDPOINT idle_ingress ON edge PATH '/idle';
      CREATE HTTP ENDPOINT INGESTOR active_ingestor
        FROM active_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR idle_ingestor
        FROM idle_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ PUSH EMITTER emitter_output FROM events
        TO emitter MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec INHERIT ALL FLUSH EACH 8s MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When these Plexus commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    Then node "node-3" eventually accepts http traffic for host "shared-expiry-{{test_id}}.example.com" path "/idle"
      """
      {"tenant":"shared","seq":1}
      """
    # Waiting for the emitter's flush ages the idle producer's branch. The active producer
    # binds the same key on node-3 just before the idle producer's TTL expires.
    Then within "20s" the observed broker receives payloads
      """
      {"tenant":"shared","seq":1}
      """
    Given remote relay admission for branch '{"tenant":"shared"}' in domain "shared_expiry_{{test_id}}" is paused
    When http payload begins posting in the background to node "node-3" with host "shared-expiry-{{test_id}}.example.com" path "/active"
      """
      {"tenant":"shared","seq":2}
      """
    Then the remote relay admission pause for branch '{"tenant":"shared"}' in domain "shared_expiry_{{test_id}}" is reached
    And node "node-3" observability metric "nervix_branch_evictions_total" with labels eventually equals 1
      """
      domain="shared_expiry_{{test_id}}"
      parametrizer="by_tenant"
      physical_node_id="node-3"
      reason="ttl"
      """
    And node "node-3" observability metric "nervix_branch_instances" with labels eventually equals 1
      """
      domain="shared_expiry_{{test_id}}"
      parametrizer="by_tenant"
      physical_node_id="node-3"
      """
    When the remote relay admission pause for branch '{"tenant":"shared"}' in domain "shared_expiry_{{test_id}}" is released
    Then within "30s" the observed broker receives payloads
      """
      {"tenant":"shared","seq":2}
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
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_branch TTL 30s;
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (
        tenant string,
        seq integer
      );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE ZEROMQ CLIENT emitter CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge turnover-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR ingestor
        FROM ingress MODE ACK MAX 1 ACK TIMEOUT 30s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ PUSH EMITTER output FROM events
        TO emitter MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 5s
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
      parametrizer="by_tenant"
      physical_node_id="node-3"
      reason="ttl"
      """
    When 600 http payloads with distinct tenants and sequence 2 are acknowledged by node "node-3" with host "turnover-{{test_id}}.example.com" path "/events"
    Then within "120s" the observed broker receives every generated payload exactly once

  # Every node-3 branch opens its own relay channel to the relay's owner on node-1, and node-1 keeps
  # a watermark for each of those channels: the channel's position and the outcome of its last
  # batch. Node-1 starts with an incoming queue capacity of 4, so it takes few deliveries at once,
  # while the warm-up tenant and 48 more keep their channels in use together: every tenant posts
  # once, and then every tenant posts again on the same branch and channel. A watermark is part of
  # the memory a channel's deliveries need, so node-1 keeps one for every channel in use and the
  # second round arrives as completely as the first. A one-node cluster delivers nothing to another
  # node, so this behavior has no one-node example.
  @relay_channel_watermarks
  Scenario: A node keeps delivering to another node's relay owner on many more branch channels than the owner takes deliveries at once
    Given node-1 starts with interconnect-incoming-queue-capacity = 4
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus statements are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN watermarks_{{test_id}};
      """
    Given the active domain is "watermarks_{{test_id}}"
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA event (
        tenant STRING,
        seq I64
      );
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_branch TTL 10m;
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (
        tenant string,
        seq integer
      );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE ZEROMQ CLIENT emitter CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge watermarks-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR ingestor
        FROM ingress MODE ACK MAX 1 ACK TIMEOUT 30s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ PUSH EMITTER output FROM events
        TO emitter MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When these Plexus statements are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    When endpoint request "warmup" is posted on node "node-3" host "watermarks-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"warmup","seq":0}
      """
    Then endpoint request "warmup" answers HTTP 200 with "completed" cause "none"
    And within "15s" the observed broker receives payloads
      """
      {"tenant":"warmup","seq":0}
      """
    When 48 http payloads with distinct tenants and sequence 1 are acknowledged by node "node-3" with host "watermarks-{{test_id}}.example.com" path "/events"
    Then within "120s" the observed broker receives every generated payload exactly once
    When 48 http payloads with distinct tenants and sequence 2 are acknowledged by node "node-3" with host "watermarks-{{test_id}}.example.com" path "/events"
    Then within "120s" the observed broker receives every generated payload exactly once

  # Node-1 keeps the watermark of every node-3 branch channel that delivered to it for twice the
  # longest total relay timeout that channel's deliveries captured. The warm-up tenant's channel
  # delivers under the default five-minute timeout, so node-1 keeps its watermark for ten minutes.
  # Lowering REMOTE_RELAY_TOTAL_TIMEOUT to 20s governs the deliveries that start afterwards: node-1
  # keeps the watermarks of the channels the ten tenants then open for 40s, and forgets them while
  # the warm-up channel's watermark remains. A one-node cluster delivers nothing to another node,
  # so this behavior has no one-node example.
  @relay_channel_watermarks @long_running
  Scenario: A receiver forgets the channels of deliveries that started under a lowered total relay timeout within twice that timeout
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus statements are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN retention_{{test_id}};
      """
    Given the active domain is "retention_{{test_id}}"
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA event (
        tenant STRING,
        seq I64
      );
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_branch TTL 10m;
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (
        tenant string,
        seq integer
      );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE ZEROMQ CLIENT emitter CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge retention-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR ingestor
        FROM ingress MODE ACK MAX 1 ACK TIMEOUT 30s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ PUSH EMITTER output FROM events
        TO emitter MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When these Plexus statements are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    When endpoint request "warmup" is posted on node "node-3" host "retention-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"warmup","seq":0}
      """
    Then endpoint request "warmup" answers HTTP 200 with "completed" cause "none"
    And within "15s" the observed broker receives payloads
      """
      {"tenant":"warmup","seq":0}
      """
    And node "node-1" observability metric "nervix_interconnect_relay_channel_watermarks" with labels eventually equals 1
      """
      nervix_interconnect_relay_channel_watermarks
      """
    When these Plexus statements are executed on the leader node
      """
      ALTER CLUSTER SET REMOTE_RELAY_TOTAL_TIMEOUT = 20s;
      """
    Then node "node-3" eventually reports status containing "node=node-3 remote-relay-total-timeout running=20s"
    When 10 http payloads with distinct tenants and sequence 1 are acknowledged by node "node-3" with host "retention-{{test_id}}.example.com" path "/events"
    Then within "120s" the observed broker receives every generated payload exactly once
    And within "240s" node "node-1" observability metric "nervix_interconnect_relay_channel_watermarks" with labels eventually equals 1
      """
      nervix_interconnect_relay_channel_watermarks
      """

  # Each node a record waits on reports it alive to its source: the receiving node does once it has
  # admitted a delivery, and the delivering node does until then. The source's ACK TIMEOUT is far
  # shorter than the hold, so a delivery that stopped reporting its record would make the source
  # read it again and the emitter send it twice. A one-node cluster delivers nothing to another
  # node, so this behavior has no one-node example.
  Scenario Outline: A record whose delivery <delivery> waits for admission keeps its source acknowledgement alive
    Given Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "held_admission_in_{{test_id}}" exists with 1 partitions
    And Kafka topic "held_admission_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE WIRE JSON CODEC notification_codec
        FROM WIRE SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}'
      };
      CREATE KAFKA TOPIC INGESTOR kafka_notifications
        FROM kafka_main TOPIC 'held_admission_in_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'held_admission_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 500ms RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE KAFKA TOPIC EMITTER kafka_forward FROM notifications
        TO kafka_main TOPIC 'held_admission_out_{{test_id}}'
        MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these Plexus commands are executed on the leader node
      """
      RELOCATE INGESTOR kafka_notifications ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE RELAY notifications ONTO NODE <relay_owner> IGNORE PREFERENCES;
      RELOCATE EMITTER kafka_forward ONTO NODE node-2 IGNORE PREFERENCES;
      """
    Then node "node-1" eventually reports status containing "kind=ingestor name=kafka_notifications unit=instance:0 owner=node-1"
    And node "node-1" eventually reports status containing "kind=relay name=notifications owner=<relay_owner>"
    And node "node-1" eventually reports status containing "kind=emitter name=kafka_forward owner=node-2"
    Given remote relay admission for domain "{{domain}}" is paused
    When Kafka message is published to topic "held_admission_in_{{test_id}}"
      """
      {"user_id":77}
      """
    Then the remote relay admission pause for domain "{{domain}}" is reached
    And the observed broker does not receive a payload within "1200ms"
    When the remote relay admission pause for domain "{{domain}}" is released
    Then within "4s" the observed broker receives payloads
      """
      {"user_id":77}
      """
    And the observed broker does not receive a payload within "1200ms"

    Examples:
      | delivery                        | relay_owner |
      | to the relay's owner            | node-2      |
      | to the relay's attached emitter | node-1      |

  # The relay's owner stays live while every data worker it has is busy for longer than a send
  # attempt ever took before, so it decodes no grant until they are given back. A delivery to it
  # waits that long: the owner's connection answers pings throughout, so nothing has ended the
  # peer, and its slowness costs latency, never a row.
  Scenario: A relay owner whose data workers stay busy keeps every row a NO ACK ingestor sent it
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN slow_owner_{{test_id}};
      """
    Given the active domain is "slow_owner_{{test_id}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY input SCHEMA event UNBRANCHED;
      CREATE ZEROMQ CLIENT emitter CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge slow-owner-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ PUSH EMITTER emitter_output FROM input
        TO emitter MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When these Plexus commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    Then node "node-3" eventually forwards http traffic for host "slow-owner-{{test_id}}.example.com" path "/events" to the observed broker
      """
      {"seq":0}
      """
    When data execution on node "node-1" is occupied for "12s"
    And http payload is posted to node "node-3" with host "slow-owner-{{test_id}}.example.com" path "/events"
      """
      {"seq":1}
      """
    And http payload is posted to node "node-3" with host "slow-owner-{{test_id}}.example.com" path "/events"
      """
      {"seq":2}
      """
    Then within "90s" the observed broker receives payloads
      """
      {"seq":1}
      {"seq":2}
      """
