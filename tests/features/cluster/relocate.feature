Feature: Relocating runtime nodes onto a named cluster node

  @ownership-state-handoff
  Scenario: Immediate materialized relay relocation transfers the final owner state
    Given runtime replication is configured with replica count 1 and snapshot interval "1h"
    And runtime state replica polling is paused
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA state_event ( tenant STRING, id I64 );
      CREATE WIRE JSON SCHEMA state_event_wire MODE STRICT (
        tenant string,
        id integer
      );
      CREATE CODEC state_event_codec
        FROM WIRE JSON SCHEMA state_event_wire
        TO SCHEMA state_event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY moving_state SCHEMA state_event BRANCHED BY by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST edge state-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/state' TYPE HTTP;
      CREATE INGESTOR state_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING state_event_codec
        TO moving_state INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      SHOW RELAY moving_state MATERIALIZED STATE;
      """
    Then the last command output owner is saved as placeholder "state_owner"
    And the first replica in the last command output is saved as placeholder "state_destination"
    When http payload is posted to node "node-1" with host "state-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"acme","id":1}
      """
    Then within "10s" node "{{state_owner}}" eventually reports materialized state for relay "moving_state" containing
      """
      key={"tenant":"acme"} payload={"id":1,"tenant":"acme"}
      """
    When these NSPL commands are executed on the active session
      """
      RELOCATE RELAY moving_state ONTO NODE {{state_destination}} IGNORE PREFERENCES;
      """
    Then within "10s" node "{{state_destination}}" eventually reports materialized state for relay "moving_state" containing
      """
      key={"tenant":"acme"} payload={"id":1,"tenant":"acme"}
      """

  @ownership-state-handoff
  Scenario: A persisted owner checkpoint promptly wakes its replica
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And runtime state replica polling is paused
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA state_event ( tenant STRING, id I64 );
      CREATE WIRE JSON SCHEMA state_event_wire MODE STRICT (
        tenant string,
        id integer
      );
      CREATE CODEC state_event_codec
        FROM WIRE JSON SCHEMA state_event_wire
        TO SCHEMA state_event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY moving_state SCHEMA state_event BRANCHED BY by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST edge state-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/state' TYPE HTTP;
      CREATE INGESTOR state_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING state_event_codec
        TO moving_state INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      SHOW RELAY moving_state MATERIALIZED STATE;
      """
    Then the last command output owner is saved as placeholder "state_owner"
    And the first replica in the last command output is saved as placeholder "state_replica"
    When http payload is posted to node "node-1" with host "state-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"acme","id":1}
      """
    Then within "10s" node "{{state_owner}}" eventually reports materialized state for relay "moving_state" containing
      """
      key={"tenant":"acme"} payload={"id":1,"tenant":"acme"}
      """
    And within "10s" node "{{state_replica}}" eventually reports materialized state for relay "moving_state" containing
      """
      key={"tenant":"acme"} payload={"id":1,"tenant":"acme"}
      """

  @ownership-state-handoff
  Scenario: Relocating a stopped domain preserves branch-local deduplication state
    Given runtime replication is configured with replica count 0 and snapshot interval "1h"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA transaction ( tenant STRING, transaction_id STRING );
      CREATE WIRE JSON SCHEMA transaction_wire MODE STRICT (
        tenant string,
        transaction_id string
      );
      CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY inbound SCHEMA transaction BRANCHED BY by_tenant;
      CREATE RELAY deduped SCHEMA transaction BRANCHED BY by_tenant;
      CREATE VHOST edge stopped-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/stopped' TYPE HTTP;
      CREATE INGESTOR source_txns
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING transaction_codec
        TO inbound INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR dedup_txns FROM inbound
        DEDUPLICATE ON input.transaction_id MAX TIME 10m
        BRANCHED BY by_tenant
        TO deduped INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION deduped_seen TO deduped;
      """
    When http payload is posted to node "node-1" with host "stopped-{{test_id}}.example.com" path "/stopped"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    And http payload is posted to node "node-1" with host "stopped-{{test_id}}.example.com" path "/stopped"
      """
      {"tenant":"beta","transaction_id":"txn-1"}
      """
    Then within "10s" the relay subscription receives a payload
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    And within "10s" the relay subscription receives a payload
      """
      {"tenant":"beta","transaction_id":"txn-1"}
      """
    When these NSPL commands are executed on the active session
      """
      STOP;
      RELOCATE DEDUPLICATOR dedup_txns ONTO NODE node-2 IGNORE PREFERENCES;
      START;
      """
    Then the last command output contains
      """
      starting domain '{{domain}}'
      """
    When http payload is posted to node "node-1" with host "stopped-{{test_id}}.example.com" path "/stopped"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    And http payload is posted to node "node-1" with host "stopped-{{test_id}}.example.com" path "/stopped"
      """
      {"tenant":"beta","transaction_id":"txn-1"}
      """
    Then the relay subscription does not receive a payload within "5s"

  @ownership-state-handoff
  Scenario: Relocating an empty stopped processor prepares an explicit empty checkpoint
    Given runtime replication is configured with replica count 0 and snapshot interval "1h"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA transaction ( tenant STRING, transaction_id STRING );
      CREATE WIRE JSON SCHEMA transaction_wire MODE STRICT (
        tenant string,
        transaction_id string
      );
      CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY inbound SCHEMA transaction BRANCHED BY by_tenant;
      CREATE RELAY deduped SCHEMA transaction BRANCHED BY by_tenant;
      CREATE VHOST edge empty-stopped-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/empty-stopped' TYPE HTTP;
      CREATE INGESTOR source_txns
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING transaction_codec
        TO inbound INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR dedup_txns FROM inbound
        DEDUPLICATE ON input.transaction_id MAX TIME 10m
        BRANCHED BY by_tenant
        TO deduped INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    When these NSPL commands are executed on the leader node
      """
      RELOCATE DEDUPLICATOR dedup_txns ONTO NODE node-2 IGNORE PREFERENCES;
      START;
      """
    Then the last command output contains
      """
      starting domain '{{domain}}'
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION deduped_seen TO deduped;
      """
    When http payload is posted to node "node-1" with host "empty-stopped-{{test_id}}.example.com" path "/empty-stopped"
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """
    Then within "10s" the relay subscription receives a payload
      """
      {"tenant":"acme","transaction_id":"txn-1"}
      """

  @ownership-state-handoff
  Scenario: A destination guest that rejects transferred WASM state aborts relocation
    Given runtime replication is configured with replica count 0 and snapshot interval "1h"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" has state-rejecting WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE rejecting_guest;
      UPLOAD RESOURCE rejecting_guest VERSION '{{wasm_processor}}';
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    Then within "30s" node "node-1" eventually reports describe resource as "cluster_ready: true"
      """
      DESCRIBE RESOURCE rejecting_guest VERSION 1;
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( value I32 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY raw_events SCHEMA event UNBRANCHED;
      CREATE RELAY restored_events SCHEMA event UNBRANCHED;
      CREATE VHOST edge rejecting-wasm-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/rejecting-wasm' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO raw_events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR stateful_guest FROM raw_events
        USING RESOURCE rejecting_guest VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        UNBRANCHED
        TO restored_events SET value = value
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION restored_seen TO restored_events;
      """
    When http payload is posted to node "node-1" with host "rejecting-wasm-{{test_id}}.example.com" path "/rejecting-wasm"
      """
      {"value":1}
      """
    Then within "10s" the relay subscription receives a payload
      """
      {"value":1}
      """
    When these NSPL commands fail with "rejected transferred branch 'none'"
      """
      RELOCATE WASM PROCESSOR stateful_guest ONTO NODE node-2 IGNORE PREFERENCES;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=wasm_processor name=stateful_guest owner=node-1
      """

  @ownership-state-handoff
  Scenario: A destination restart after preparation aborts the ownership handoff
    Given runtime replication is configured with replica count 0 and snapshot interval "1h"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( id I64 );
      CREATE RELAY inbound SCHEMA event UNBRANCHED;
      CREATE RELAY outbound SCHEMA event UNBRANCHED;
      CREATE JUNCTION moving_route FROM inbound UNBRANCHED
        TO outbound INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    Given ownership handoff for domain "{{domain}}" pauses after preparation
    When these NSPL commands begin executing in the background
      """
      RELOCATE JUNCTION moving_route ONTO NODE node-2 IGNORE PREFERENCES;
      """
    Then the ownership handoff preparation pause for domain "{{domain}}" is reached
    When node "node-2" is stopped
    And node "node-2" is started
    And the ownership handoff preparation pause for domain "{{domain}}" is released
    Then the background NSPL execution fails with "destination node 'node-2' changed process incarnation during ownership handoff"
    When these NSPL commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=junction name=moving_route owner=node-1
      """

  @ownership-state-handoff
  Scenario: Owner loss promotes a replica with visible unverified state
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And runtime state replica polling is paused
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA state_event ( tenant STRING, id I64 );
      CREATE WIRE JSON SCHEMA state_event_wire MODE STRICT (
        tenant string,
        id integer
      );
      CREATE CODEC state_event_codec
        FROM WIRE JSON SCHEMA state_event_wire
        TO SCHEMA state_event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY moving_state SCHEMA state_event BRANCHED BY by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST edge forced-state-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/forced-state' TYPE HTTP;
      CREATE INGESTOR state_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING state_event_codec
        TO moving_state INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      SHOW RELAY moving_state MATERIALIZED STATE;
      """
    Then the last command output owner is saved as placeholder "failed_state_owner"
    And the first replica in the last command output is saved as placeholder "promoted_state_replica"
    When http payload is posted to node "node-1" with host "forced-state-{{test_id}}.example.com" path "/forced-state"
      """
      {"tenant":"acme","id":7}
      """
    Then within "10s" node "{{promoted_state_replica}}" eventually reports materialized state for relay "moving_state" containing
      """
      key={"tenant":"acme"} payload={"id":7,"tenant":"acme"}
      """
    When physical time passes for "1s"
    When node "{{failed_state_owner}}" is stopped
    Then node "{{promoted_state_replica}}" eventually observes a stable leader
    And within "20s" node "{{promoted_state_replica}}" eventually reports scheduled "relay" "moving_state" owner equals placeholder "promoted_state_replica"
    And within "10s" node "{{promoted_state_replica}}" eventually reports materialized state for relay "moving_state" containing
      """
      key={"tenant":"acme"} payload={"id":7,"tenant":"acme"}
      """
    When these NSPL commands are executed through the client on node "{{promoted_state_replica}}"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      transition_from={{failed_state_owner}} state_recovery=unverified
      """

  @ownership-state-handoff
  Scenario: Owner loss without a replica publishes an explicit state reset
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA state_event ( tenant STRING, id I64 );
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY moving_state SCHEMA state_event BRANCHED BY by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      START;
      SHOW RELAY moving_state MATERIALIZED STATE;
      """
    Then the last command output owner is saved as placeholder "failed_state_owner"
    And a node other than placeholder "failed_state_owner" is saved as placeholder "surviving_node"
    When node "{{failed_state_owner}}" is stopped
    Then node "{{surviving_node}}" eventually observes a stable leader
    And within "20s" node "{{surviving_node}}" eventually reports scheduled "relay" "moving_state" owner different from placeholder "failed_state_owner"
    When these NSPL commands are executed through the client on node "{{surviving_node}}"
      """
      SHOW RELAY moving_state MATERIALIZED STATE;
      """
    Then the last command output contains
      """
      relay 'moving_state' materialized state is empty
      """
    When these NSPL commands are executed through the client on node "{{surviving_node}}"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      transition_from={{failed_state_owner}} state_recovery=reset resets=materialized_relay:missing_checkpoint
      """

  Scenario: Relocating a deduplicator moves it and resumes deduplication on the destination
    Given runtime replication is configured with replica count 0 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA transaction ( tenant STRING, transaction_id STRING, amount I64 );
      CREATE WIRE JSON SCHEMA transaction_wire MODE STRICT (
        tenant string,
        transaction_id string,
        amount integer
      );
      CREATE CODEC transaction_codec
        FROM WIRE JSON SCHEMA transaction_wire
        TO SCHEMA transaction;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY inbound SCHEMA transaction BRANCHED BY by_tenant;
      CREATE RELAY deduped SCHEMA transaction BRANCHED BY by_tenant;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/relocate' TYPE HTTP;
      CREATE INGESTOR source_txns
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING transaction_codec
        TO inbound INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR dedup_txns FROM inbound
        DEDUPLICATE ON input.transaction_id MAX TIME 10m
        BRANCHED BY by_tenant
        TO deduped INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION deduped_seen TO deduped;
      """
    Then node "node-1" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/relocate"
      """
      {"tenant":"acme","transaction_id":"txn-warmup","amount":1}
      """
    And within "20s" the relay subscription receives a payload
      """
      "transaction_id":"txn-warmup"
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/relocate"
      """
      {"tenant":"acme","transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/relocate"
      """
      {"tenant":"beta","transaction_id":"txn-1","amount":20}
      """
    Then within "10s" the relay subscription receives a payload
      """
      {"amount":10,"tenant":"acme","transaction_id":"txn-1"}
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    And within "10s" the relay subscription receives a payload
      """
      {"amount":20,"tenant":"beta","transaction_id":"txn-1"}
      """
    And the last relay subscription payload contains key fragment '{"tenant":"beta"}'
    When these NSPL commands are executed on the active session
      """
      RELOCATE DEDUPLICATOR dedup_txns ONTO NODE node-2 FOLLOW PREFERENCES;
      """
    Then the last command output contains
      """
      relocated 1 of 1 runtime node(s) onto node 'node-2'
      relocation onto node 'node-2'
      quiesce level: ENTITY_PAUSE
      """
    And the last command output contains
      """
      - kind=deduplicator name=dedup_txns group=1 strategy=follow reason=selected owner=node-1 moves=yes replicas=- promoted_replica=no
      """
    And the last command output contains
      """
      unsatisfied preferences: 0
      """
    And the last command output contains
      """
      hold duration:
      """
    When these NSPL commands are executed on the active session
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=deduplicator name=dedup_txns owner=node-2
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/relocate"
      """
      {"tenant":"acme","transaction_id":"txn-1","amount":10}
      """
    Then the relay subscription does not receive a payload within "5s"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/relocate"
      """
      {"tenant":"acme","transaction_id":"txn-2","amount":11}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/relocate"
      """
      {"tenant":"beta","transaction_id":"txn-2","amount":21}
      """
    Then within "15s" the relay subscription receives a payload
      """
      {"amount":11,"tenant":"acme","transaction_id":"txn-2"}
      """
    And within "15s" the relay subscription receives a payload
      """
      {"amount":21,"tenant":"beta","transaction_id":"txn-2"}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/relocate"
      """
      {"tenant":"acme","transaction_id":"txn-2","amount":11}
      """
    Then the relay subscription does not receive a payload within "5s"

  Scenario: A corridor selection moves every covered runtime node in one relocation
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA corridor_event ( id I64 );
      CREATE RELAY corridor_input SCHEMA corridor_event UNBRANCHED;
      CREATE RELAY corridor_stage_one SCHEMA corridor_event UNBRANCHED;
      CREATE RELAY corridor_stage_two SCHEMA corridor_event UNBRANCHED;
      CREATE RELAY corridor_output SCHEMA corridor_event UNBRANCHED;
      CREATE JUNCTION corridor_source FROM corridor_input UNBRANCHED
        TO corridor_stage_one INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION corridor_middle FROM corridor_stage_one UNBRANCHED
        TO corridor_stage_two INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION corridor_bypass FROM corridor_stage_one UNBRANCHED
        TO corridor_stage_two INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION corridor_sink FROM corridor_stage_two UNBRANCHED
        TO corridor_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      RELOCATE FROM JUNCTION corridor_source TO JUNCTION corridor_sink
        ONTO NODE node-2 IGNORE PREFERENCES;
      """
    Then the last command output contains
      """
      relocated 6 of 6 runtime node(s) onto node 'node-2'
      """
    And the last command output contains
      """
      coverage:
      - junction corridor_source -> junction corridor_sink connected=yes covered=6
      """
    And the last command output contains
      """
      - kind=junction name=corridor_bypass group=1 strategy=ignore reason=selected owner=node-1 moves=yes
      """
    And the last command output contains
      """
      - kind=relay name=corridor_stage_two group=6 strategy=ignore reason=selected owner=node-1 moves=yes
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=junction name=corridor_middle owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=relay name=corridor_stage_one owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=relay name=corridor_input owner=node-1
      """
    When these NSPL commands fail with "relocation covers no runtime node: no FROM/TO pair is connected"
      """
      RELOCATE FROM JUNCTION corridor_sink TO JUNCTION corridor_source
        ONTO NODE node-3 IGNORE PREFERENCES;
      """
    And these NSPL commands are executed on the leader node
      """
      DESCRIBE RELOCATION FROM JUNCTION corridor_source, JUNCTION corridor_sink
        TO JUNCTION corridor_sink
        ONTO NODE node-3 IGNORE PREFERENCES;
      """
    Then the last command output contains
      """
      - junction corridor_source -> junction corridor_sink connected=yes covered=6
      - junction corridor_sink -> junction corridor_sink connected=no covered=0
      """

  Scenario: A hard colocation group moves whole under either preference strategy
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA group_event ( id I64 );
      CREATE RELAY group_input SCHEMA group_event UNBRANCHED;
      CREATE RELAY group_middle SCHEMA group_event UNBRANCHED;
      CREATE RELAY group_output SCHEMA group_event UNBRANCHED;
      CREATE JUNCTION group_head FROM group_input UNBRANCHED
        TO group_middle INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION group_tail FROM group_middle UNBRANCHED
        TO group_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT keep_group FROM group_head TO group_tail REQUIRE COLOCATION;
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      RELOCATE JUNCTION group_head ONTO NODE node-2 IGNORE PREFERENCES;
      """
    Then the last command output contains
      """
      relocated 3 of 3 runtime node(s) onto node 'node-2'
      """
    And the last command output contains
      """
      - kind=junction name=group_head group=1 strategy=ignore reason=selected owner=node-1 moves=yes
      """
    And the last command output contains
      """
      - kind=junction name=group_tail group=1 strategy=ignore reason=required owner=node-1 moves=yes
      """
    And the last command output contains
      """
      - kind=relay name=group_middle group=1 strategy=ignore reason=required owner=node-1 moves=yes
      """
    When these NSPL commands fail with "conflicting preference strategies for hard group [junction group_head, junction group_tail, relay group_middle]"
      """
      RELOCATE JUNCTION group_head ONTO NODE node-3 FOLLOW PREFERENCES
        FOR JUNCTION group_head IGNORE PREFERENCES
        FOR JUNCTION group_tail FOLLOW PREFERENCES;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      RELOCATE JUNCTION group_tail ONTO NODE node-3 FOLLOW PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=junction name=group_head owner=node-3
      """
    And the last command output contains
      """
      - domain={{domain}} kind=junction name=group_tail owner=node-3
      """

  Scenario: Preferences shape the unit and follow the statement default
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA chain_event ( id I64 );
      CREATE RELAY chain_input SCHEMA chain_event UNBRANCHED;
      CREATE RELAY chain_first SCHEMA chain_event UNBRANCHED;
      CREATE RELAY chain_second SCHEMA chain_event UNBRANCHED;
      CREATE RELAY chain_third SCHEMA chain_event UNBRANCHED;
      CREATE JUNCTION chain_head FROM chain_input UNBRANCHED
        TO chain_first INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION chain_partner FROM chain_first UNBRANCHED
        TO chain_second INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION chain_distant FROM chain_second UNBRANCHED
        TO chain_third INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT prefer_first FROM chain_head TO chain_partner PREFER COLOCATION RANK 1;
      CREATE PLACEMENT prefer_second FROM chain_partner TO chain_distant PREFER COLOCATION RANK 1;
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      DESCRIBE RELOCATION JUNCTION chain_head ONTO NODE node-2 FOLLOW PREFERENCES;
      """
    Then the last command output contains
      """
      - kind=junction name=chain_head group=1 strategy=follow reason=selected owner=node-1 moves=yes
      """
    And the last command output contains
      """
      - kind=junction name=chain_partner group=2 strategy=follow reason=preferred owner=node-1 moves=yes
      """
    And the last command output contains
      """
      - kind=relay name=chain_first group=3 strategy=follow reason=preferred owner=node-1 moves=yes
      """
    And the last command output contains
      """
      - kind=junction name=chain_distant group=4 strategy=follow reason=preferred owner=node-1 moves=yes
      """
    And the last command output contains
      """
      - kind=relay name=chain_second group=5 strategy=follow reason=preferred owner=node-1 moves=yes
      """
    And the last command output contains
      """
      unsatisfied preferences: 0
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      DESCRIBE RELOCATION JUNCTION chain_head ONTO NODE node-2 FOLLOW PREFERENCES
        FOR JUNCTION chain_partner IGNORE PREFERENCES;
      """
    Then the last command output contains
      """
      - kind=junction name=chain_partner group=2 strategy=ignore reason=preferred owner=node-1 moves=yes
      """
    And the last command output does not contain
      """
      name=chain_distant
      """
    And the last command output contains
      """
      unsatisfied preferences: 2
      - prefer colocation chain_distant <-> chain_partner (prefer_second)
      - prefer colocation chain_partner <-> chain_second (prefer_second)
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      DESCRIBE RELOCATION JUNCTION chain_head ONTO NODE node-2 IGNORE PREFERENCES
        FOR JUNCTION chain_head FOLLOW PREFERENCES;
      """
    Then the last command output contains
      """
      - kind=junction name=chain_head group=1 strategy=follow reason=selected owner=node-1 moves=yes
      """
    And the last command output contains
      """
      - kind=junction name=chain_partner group=2 strategy=ignore reason=preferred owner=node-1 moves=yes
      """
    And the last command output does not contain
      """
      name=chain_distant
      """
    When these NSPL commands fail with "junction 'chain_distant' is not part of the relocation"
      """
      RELOCATE JUNCTION chain_head ONTO NODE node-2 IGNORE PREFERENCES
        FOR JUNCTION chain_distant FOLLOW PREFERENCES;
      """
    When these NSPL commands are executed on the leader node
      """
      RELOCATE JUNCTION chain_head ONTO NODE node-2 FOLLOW PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=junction name=chain_distant owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=junction name=chain_partner owner=node-2
      """

  Scenario: A separation partner is held out of the unit and reported when it owns the destination
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA split_event ( id I64 );
      CREATE RELAY split_input SCHEMA split_event UNBRANCHED;
      CREATE RELAY split_first SCHEMA split_event UNBRANCHED;
      CREATE RELAY split_second SCHEMA split_event UNBRANCHED;
      CREATE JUNCTION split_head FROM split_input UNBRANCHED
        TO split_first INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION split_partner FROM split_first UNBRANCHED
        TO split_second INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE PLACEMENT prefer_split FROM split_head TO split_partner PREFER COLOCATION RANK 2;
      CREATE PLACEMENT spread_split FROM split_head TO split_partner SUGGEST SEPARATION RANK 1;
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      RELOCATE JUNCTION split_head ONTO NODE node-2 FOLLOW PREFERENCES;
      """
    Then the last command output contains
      """
      relocated 1 of 1 runtime node(s) onto node 'node-2'
      """
    And the last command output does not contain
      """
      name=split_partner
      """
    And the last command output contains
      """
      unsatisfied preferences: 0
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      DESCRIBE RELOCATION JUNCTION split_partner ONTO NODE node-2 FOLLOW PREFERENCES;
      """
    Then the last command output contains
      """
      unsatisfied preferences: 1
      - suggest separation split_head <-> split_partner (spread_split)
      """

  Scenario: A described plan matches the plan the relocation executes
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA plan_event ( id I64 );
      CREATE RELAY plan_input SCHEMA plan_event UNBRANCHED;
      CREATE RELAY plan_output SCHEMA plan_event UNBRANCHED;
      CREATE JUNCTION plan_route FROM plan_input UNBRANCHED
        TO plan_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      DESCRIBE RELOCATION JUNCTION plan_route ONTO NODE node-2 FOLLOW PREFERENCES;
      """
    Then the last command output contains
      """
      relocation onto node 'node-2'
      """
    Then the last command output is saved as the relocation plan
    When these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=junction name=plan_route owner=node-1
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      RELOCATE JUNCTION plan_route ONTO NODE node-2 FOLLOW PREFERENCES;
      """
    Then the last command output contains the saved relocation plan

  Scenario: A selection already on the destination reports a dynamic plan and writes nothing
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA noop_event ( id I64 );
      CREATE RELAY noop_input SCHEMA noop_event UNBRANCHED;
      CREATE RELAY noop_output SCHEMA noop_event UNBRANCHED;
      CREATE JUNCTION noop_route FROM noop_input UNBRANCHED
        TO noop_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      RELOCATE JUNCTION noop_route ONTO NODE node-1 FOLLOW PREFERENCES;
      """
    Then the last command output contains
      """
      relocated 0 of 1 runtime node(s) onto node 'node-1'
      relocation onto node 'node-1'
      quiesce level: DYNAMIC
      gated relays: -
      unit:
      - kind=junction name=noop_route group=1 strategy=follow reason=selected owner=node-1 moves=no replicas=-
      unsatisfied preferences: 0
      """
    And the last command output does not contain
      """
      hold duration:
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=junction name=noop_route owner=node-1
      """

  Scenario: Invalid relocations are rejected without touching the schedule
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA reject_event ( id I64 );
      CREATE WIRE JSON SCHEMA reject_event_wire MODE STRICT ( id integer );
      CREATE CODEC reject_event_codec
        FROM WIRE JSON SCHEMA reject_event_wire
        TO SCHEMA reject_event;
      CREATE RELAY reject_input SCHEMA reject_event UNBRANCHED;
      CREATE RELAY reject_output SCHEMA reject_event UNBRANCHED;
      CREATE VHOST edge reject-{{test_id}}.example.com;
      CREATE ENDPOINT reject_ingress ON edge PATH '/reject' TYPE HTTP;
      CREATE INGESTOR listener_source
        FROM ENDPOINT reject_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reject_event_codec
        TO reject_input INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION reject_route FROM reject_input UNBRANCHED
        TO reject_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    And these NSPL commands fail with "junction 'missing_route' does not exist in domain '{{domain}}'"
      """
      RELOCATE JUNCTION missing_route ONTO NODE node-2 FOLLOW PREFERENCES;
      """
    And these NSPL commands fail with "ingestor 'listener_source' cannot be relocated: server-listener ingestors execute on every cluster node"
      """
      RELOCATE INGESTOR listener_source ONTO NODE node-2 FOLLOW PREFERENCES;
      """
    And these NSPL commands fail with "node 'node-9' is not a raft member"
      """
      RELOCATE JUNCTION reject_route ONTO NODE node-9 FOLLOW PREFERENCES;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-3;
      """
    And these NSPL commands fail with "node 'node-3' is cordoned"
      """
      RELOCATE JUNCTION reject_route ONTO NODE node-3 FOLLOW PREFERENCES;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      BEGIN;
      """
    And these NSPL commands fail on the active session
      """
      RELOCATE JUNCTION reject_route ONTO NODE node-2 FOLLOW PREFERENCES;
      """
    Then the last command error contains
      """
      RELOCATE cannot be queued in a transaction
      """
    When these NSPL commands fail on the active session
      """
      DESCRIBE RELOCATION JUNCTION reject_route ONTO NODE node-2 FOLLOW PREFERENCES;
      """
    Then the last command error contains
      """
      DESCRIBE cannot be queued in a transaction
      """
    When these NSPL commands are executed on the active session
      """
      REVERT;
      """
    And node "node-3" is stopped
    Then within "60s" these NSPL commands on node "node-1" eventually fail with "is not a live raft voter"
      """
      RELOCATE JUNCTION reject_route ONTO NODE node-3 FOLLOW PREFERENCES;
      """

  Scenario: A runtime node is relocated again after failover reassigns it
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-1;
      CORDON NODE node-2;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA failover_event ( id I64 );
      CREATE WIRE JSON SCHEMA failover_event_wire MODE STRICT ( id integer );
      CREATE CODEC failover_event_codec
        FROM WIRE JSON SCHEMA failover_event_wire
        TO SCHEMA failover_event;
      CREATE RELAY failover_input SCHEMA failover_event UNBRANCHED;
      CREATE RELAY failover_output SCHEMA failover_event UNBRANCHED;
      CREATE VHOST edge failover-{{test_id}}.example.com;
      CREATE ENDPOINT failover_ingress ON edge PATH '/failover' TYPE HTTP;
      CREATE INGESTOR failover_source
        FROM ENDPOINT failover_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING failover_event_codec
        TO failover_input INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION failover_route FROM failover_input UNBRANCHED
        TO failover_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-1;
      UNCORDON NODE node-2;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=junction name=failover_route owner=node-3
      """
    And the last cluster status owner for scheduled "junction" "failover_route" is saved as placeholder "failover_owner"
    When node "node-3" is stopped
    Then within "60s" node "node-1" eventually reports scheduled "junction" "failover_route" owner different from placeholder "failover_owner"
    When these NSPL commands are executed through the client on node "node-1"
      """
      RELOCATE JUNCTION failover_route ONTO NODE node-2 FOLLOW PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=junction name=failover_route owner=node-2
      """
    When these NSPL commands are executed on node "node-1"
      """
      CREATE SUBSCRIPTION failover_seen TO failover_output;
      """
    Then node "node-1" eventually accepts http traffic for host "failover-{{test_id}}.example.com" path "/failover"
      """
      {"id":41}
      """
    And within "20s" the relay subscription receives a payload
      """
      "id":41
      """

  Scenario: Relocating onto an existing replica promotes it and demotes the former owner
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And Kafka topic "promote_events_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed through the client on node "node-1"
      """
      CREATE SCHEMA promote_event ( tenant STRING, id I64, source STRING );
      CREATE WIRE JSON SCHEMA promote_event_wire MODE STRICT (
        tenant string,
        id integer,
        source string
      );
      CREATE CODEC promote_event_codec
        FROM WIRE JSON SCHEMA promote_event_wire
        TO SCHEMA promote_event;
      CREATE SCHEMA promote_tenant ( tenant STRING );
      CREATE BRANCH promote_by_tenant SCHEMA promote_tenant TTL 5m;
      CREATE RELAY promote_output SCHEMA promote_event BRANCHED BY promote_by_tenant;
      CREATE CLIENT promote_kafka TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR promote_source
        FROM KAFKA promote_kafka TOPIC promote_events_{{test_id}}
          OFFSET BY DOMAIN MODE ACK SEQUENTIAL ACK TIMEOUT 30s
          RETRY POLICY BACKOFF 100ms MAX 500ms
        ON QUIESCE SUSPEND DECODE USING promote_event_codec
        TIMESTAMP NOW
        TO promote_output INHERIT ALL
        BRANCHED BY promote_by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "ingestor" "promote_source" is saved as placeholder "promote_owner"
    And the first replica for scheduled "ingestor" "promote_source" in the last cluster status is saved as placeholder "promote_replica"
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION promote_seen TO promote_output;
      """
    When Kafka message is published to topic "promote_events_{{test_id}}"
      """
      {"tenant":"acme","id":1,"source":"before-acme"}
      """
    And Kafka message is published to topic "promote_events_{{test_id}}"
      """
      {"tenant":"beta","id":2,"source":"before-beta"}
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "tenant":"acme" | "id":1 | "source":"before-acme"
      key={"tenant":"beta"} | "tenant":"beta" | "id":2 | "source":"before-beta"
      """
    When these NSPL commands are executed on the active session
      """
      RELOCATE INGESTOR promote_source ONTO NODE {{promote_replica}} IGNORE PREFERENCES;
      """
    Then the last command output contains
      """
      - kind=ingestor name=promote_source group=1 strategy=ignore reason=selected owner={{promote_owner}} moves=yes replicas={{promote_owner}} promoted_replica=yes
      """
    Then within "20s" node "node-1" eventually reports scheduled "ingestor" "promote_source" owner equals placeholder "promote_replica"
    When Kafka message is published to topic "promote_events_{{test_id}}"
      """
      {"tenant":"beta","id":3,"source":"after-beta"}
      """
    And Kafka message is published to topic "promote_events_{{test_id}}"
      """
      {"tenant":"acme","id":4,"source":"after-acme"}
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"beta"} | "tenant":"beta" | "id":3 | "source":"after-beta"
      key={"tenant":"acme"} | "tenant":"acme" | "id":4 | "source":"after-acme"
      """
    And the relay subscription does not receive a payload within "1s"

  Scenario: A relocation and a drain of the same domain are mutually exclusive
    Given entity gate deadline is configured as "60s"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA exclusive_event ( id I64 );
      CREATE RELAY exclusive_input SCHEMA exclusive_event UNBRANCHED;
      CREATE RELAY exclusive_output SCHEMA exclusive_event UNBRANCHED;
      CREATE JUNCTION exclusive_route FROM exclusive_input UNBRANCHED
        TO exclusive_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    Given the entity gate for domain "{{domain}}" pauses after engagement
    When these NSPL commands begin executing in the background
      """
      RELOCATE JUNCTION exclusive_route ONTO NODE node-2 FOLLOW PREFERENCES;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    When these NSPL commands fail with "domain '{{domain}}' already has a model alteration in progress"
      """
      ALTER RELAY exclusive_output SET CAPACITY 32;
      """
    And these NSPL commands fail with "domain '{{domain}}' already has a model alteration in progress"
      """
      RELOCATE JUNCTION exclusive_route ONTO NODE node-3 FOLLOW PREFERENCES;
      """
    And the entity gate pause for domain "{{domain}}" is released
    Then the background NSPL execution succeeds
    And the last command output contains
      """
      relocated 1 of 1 runtime node(s) onto node 'node-2'
      """

  @planned-handoff-timeout
  Scenario: A unit that cannot drain aborts whole and succeeds after the stall clears
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA abort_event ( seq I64 );
      CREATE WIRE JSON SCHEMA abort_event_wire MODE STRICT ( seq integer );
      CREATE CODEC abort_event_codec
        FROM WIRE JSON SCHEMA abort_event_wire
        TO SCHEMA abort_event;
      CREATE RELAY abort_input SCHEMA abort_event UNBRANCHED;
      CREATE RELAY abort_staged SCHEMA abort_event UNBRANCHED CAPACITY 1;
      CREATE VHOST edge abort-{{test_id}}.example.com;
      CREATE ENDPOINT abort_ingress ON edge PATH '/abort' TYPE HTTP;
      CREATE INGESTOR abort_source
        FROM ENDPOINT abort_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING abort_event_codec
        TO abort_input INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION abort_route FROM abort_input UNBRANCHED
        TO abort_staged INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE CLIENT abort_sink TYPE ZEROMQ CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE EMITTER stalled_emitter FROM abort_staged
        TO ZEROMQ abort_sink MODE NO_ACK RETRY POLICY BACKOFF 10ms MAX 100ms
          ENCODE USING abort_event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    And emitter "stalled_emitter" enters stall mode
    And http payload is posted to node "node-1" with host "abort-{{test_id}}.example.com" path "/abort"
      """
      {"seq":1}
      """
    Then within "10s" DESCRIBE EMITTER "stalled_emitter" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    Given the next pending entity drain in domain "{{domain}}" is forced to time out
    When these NSPL commands fail with "timed out draining domain"
      """
      RELOCATE FROM JUNCTION abort_route TO EMITTER stalled_emitter
        ONTO NODE node-2 IGNORE PREFERENCES;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=junction name=abort_route owner=node-1
      """
    And the last command output contains
      """
      - domain={{domain}} kind=emitter name=stalled_emitter owner=node-1
      """
    When emitter "stalled_emitter" leaves stall mode
    Then the observed broker receives a payload
      """
      "seq":1
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      RELOCATE FROM JUNCTION abort_route TO EMITTER stalled_emitter
        ONTO NODE node-2 IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=emitter name=stalled_emitter owner=node-2
      """
    And the last command output contains
      """
      - domain={{domain}} kind=junction name=abort_route owner=node-2
      """

  Scenario: Runtime nodes outside the unit keep their state through a relocation
    Given runtime replication is configured with replica count 0 and snapshot interval "10m"
    And Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA wait_event ( id I64, source STRING );
      CREATE WIRE JSON SCHEMA wait_event_wire MODE STRICT ( id integer, source string );
      CREATE CODEC wait_event_codec
        FROM WIRE JSON SCHEMA wait_event_wire
        TO SCHEMA wait_event;
      CREATE RELAY untouched_state SCHEMA wait_event UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY untouched_input SCHEMA wait_event UNBRANCHED;
      CREATE RELAY untouched_output SCHEMA wait_event UNBRANCHED;
      CREATE RELAY moved_input SCHEMA wait_event UNBRANCHED;
      CREATE RELAY moved_output SCHEMA wait_event UNBRANCHED;
      CREATE CLIENT wait_kafka TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR event_source
        FROM KAFKA wait_kafka TOPIC untouched_events_{{test_id}}
          OFFSET BY CONSUMER GROUP nervix_cucumber_untouched_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
          RETRY POLICY BACKOFF 100ms MAX 500ms
        ON QUIESCE SUSPEND DECODE USING wait_event_codec
        TO untouched_input INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INGESTOR state_source
        FROM KAFKA wait_kafka TOPIC untouched_state_{{test_id}}
          OFFSET BY CONSUMER GROUP nervix_cucumber_untouched_state_{{test_id}}
          MODE NO_ACK PARALLEL
        ON QUIESCE SUSPEND DECODE USING wait_event_codec
        TO untouched_state INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION untouched_waiter FROM untouched_input UNBRANCHED
        USING MATERIALIZED STATE untouched_state REQUIRED WAIT
        TO untouched_output INHERIT ALL
        SET source = relay_state.untouched_state.source
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE JUNCTION moved_route FROM moved_input UNBRANCHED
        TO moved_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION untouched_output_seen TO untouched_output;
      """
    And Kafka message is published to topic "untouched_events_{{test_id}}"
      """
      {"id":7,"source":"input"}
      """
    Then the relay subscription does not receive a payload within "5s"
    When these NSPL commands are executed on the active session
      """
      RELOCATE JUNCTION moved_route ONTO NODE node-2 FOLLOW PREFERENCES;
      """
    Then the last command output contains
      """
      relocated 1 of 1 runtime node(s) onto node 'node-2'
      """
    And the relay subscription does not receive a payload within "2s"
    When Kafka message is published to topic "untouched_state_{{test_id}}"
      """
      {"id":7,"source":"state"}
      """
    Then within "20s" the relay subscription receives a payload
      """
      {"id":7,"source":"state"}
      """

  Scenario: A relocated assignment survives reconciliation, a dynamic alteration and a restart
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA durable_event ( seq I64 );
      CREATE RELAY durable_input SCHEMA durable_event UNBRANCHED;
      CREATE RELAY durable_output SCHEMA durable_event UNBRANCHED;
      CREATE JUNCTION durable_route FROM durable_input UNBRANCHED
        TO durable_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      """
    And these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      RELOCATE JUNCTION durable_route ONTO NODE node-2 FOLLOW PREFERENCES;
      """
    Then the last command output contains
      """
      relocated 1 of 1 runtime node(s) onto node 'node-2'
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "junction" "durable_route" is saved as placeholder "relocated_owner"
    And for "5s" node "node-1" keeps reporting scheduled "junction" "durable_route" owner equal to placeholder "relocated_owner"
    When these NSPL commands are executed through the client on node "node-1"
      """
      ALTER RELAY durable_output SET CAPACITY 32;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      STOP;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=junction name=durable_route owner=node-2
      """
    And for "3s" node "node-1" keeps reporting scheduled "junction" "durable_route" owner equal to placeholder "relocated_owner"

  Scenario: Relocating a materialized relay moves it and keeps its readers running
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And Kafka topic "relay_state_{{test_id}}" exists with 1 partitions
    And Kafka topic "relay_reader_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA state_event ( tenant STRING, id I64, source STRING );
      CREATE WIRE JSON SCHEMA state_event_wire MODE STRICT (
        tenant string,
        id integer,
        source string
      );
      CREATE CODEC state_event_codec
        FROM WIRE JSON SCHEMA state_event_wire
        TO SCHEMA state_event;
      CREATE SCHEMA state_tenant ( tenant STRING );
      CREATE BRANCH state_by_tenant SCHEMA state_tenant TTL 5m;
      CREATE RELAY moving_state SCHEMA state_event BRANCHED BY state_by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY reader_input SCHEMA state_event BRANCHED BY state_by_tenant;
      CREATE RELAY reader_output SCHEMA state_event BRANCHED BY state_by_tenant;
      CREATE CLIENT state_kafka TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR state_source
        FROM KAFKA state_kafka TOPIC relay_state_{{test_id}}
          OFFSET BY CONSUMER GROUP nervix_cucumber_relay_state_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
          RETRY POLICY BACKOFF 100ms MAX 500ms
        ON QUIESCE SUSPEND DECODE USING state_event_codec
        TO moving_state INHERIT ALL BRANCHED BY state_by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INGESTOR reader_source
        FROM KAFKA state_kafka TOPIC relay_reader_{{test_id}}
          OFFSET BY CONSUMER GROUP nervix_cucumber_relay_reader_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
          RETRY POLICY BACKOFF 100ms MAX 500ms
        ON QUIESCE SUSPEND DECODE USING state_event_codec
        TO reader_input INHERIT ALL BRANCHED BY state_by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION state_reader FROM reader_input BRANCHED BY state_by_tenant
        USING MATERIALIZED STATE moving_state REQUIRED WAIT
        TO reader_output INHERIT ALL
        SET source = relay_state.moving_state.source
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      SHOW RELAY moving_state MATERIALIZED STATE;
      """
    Then the last command output owner is saved as placeholder "state_owner"
    And the first replica in the last command output is saved as placeholder "state_replica"
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION reader_output_seen TO reader_output;
      """
    And Kafka message is published to topic "relay_state_{{test_id}}"
      """
      {"tenant":"acme","id":1,"source":"before-acme"}
      """
    And Kafka message is published to topic "relay_state_{{test_id}}"
      """
      {"tenant":"beta","id":2,"source":"before-beta"}
      """
    Then within "20s" node "{{state_owner}}" eventually reports materialized state for relay "moving_state" containing
      """
      key={"tenant":"acme"} payload={"id":1,"source":"before-acme","tenant":"acme"}
      """
    And within "20s" node "{{state_owner}}" eventually reports materialized state for relay "moving_state" containing
      """
      key={"tenant":"beta"} payload={"id":2,"source":"before-beta","tenant":"beta"}
      """
    And within "20s" node "{{state_replica}}" eventually reports materialized state for relay "moving_state" containing
      """
      key={"tenant":"acme"} payload={"id":1,"source":"before-acme","tenant":"acme"}
      """
    And within "20s" node "{{state_replica}}" eventually reports materialized state for relay "moving_state" containing
      """
      key={"tenant":"beta"} payload={"id":2,"source":"before-beta","tenant":"beta"}
      """
    When Kafka message is published to topic "relay_reader_{{test_id}}"
      """
      {"tenant":"acme","id":11,"source":"ignored-acme"}
      """
    And Kafka message is published to topic "relay_reader_{{test_id}}"
      """
      {"tenant":"beta","id":12,"source":"ignored-beta"}
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "tenant":"acme" | "id":11 | "source":"before-acme"
      key={"tenant":"beta"} | "tenant":"beta" | "id":12 | "source":"before-beta"
      """
    When these NSPL commands are executed on the active session
      """
      RELOCATE RELAY moving_state ONTO NODE {{state_replica}} IGNORE PREFERENCES;
      """
    Then the last command output contains
      """
      - kind=relay name=moving_state group=1 strategy=ignore reason=selected owner={{state_owner}} moves=yes replicas={{state_owner}} promoted_replica=yes
      """
    When these NSPL commands are executed on the active session
      """
      SHOW RELAY moving_state MATERIALIZED STATE;
      """
    Then the last command output owner equals placeholder "state_replica"
    And the last command output contains
      """
      replicas: {{state_owner}}
      """
    Then within "20s" node "{{state_replica}}" eventually reports materialized state for relay "moving_state" containing
      """
      key={"tenant":"acme"} payload={"id":1,"source":"before-acme","tenant":"acme"}
      """
    And within "20s" node "{{state_replica}}" eventually reports materialized state for relay "moving_state" containing
      """
      key={"tenant":"beta"} payload={"id":2,"source":"before-beta","tenant":"beta"}
      """
    When Kafka message is published to topic "relay_reader_{{test_id}}"
      """
      {"tenant":"beta","id":13,"source":"ignored-beta"}
      """
    And Kafka message is published to topic "relay_reader_{{test_id}}"
      """
      {"tenant":"acme","id":14,"source":"ignored-acme"}
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"beta"} | "tenant":"beta" | "id":13 | "source":"before-beta"
      key={"tenant":"acme"} | "tenant":"acme" | "id":14 | "source":"before-acme"
      """
    When Kafka message is published to topic "relay_state_{{test_id}}"
      """
      {"tenant":"beta","id":3,"source":"after-beta"}
      """
    And Kafka message is published to topic "relay_state_{{test_id}}"
      """
      {"tenant":"acme","id":4,"source":"after-acme"}
      """
    Then within "20s" node "{{state_replica}}" eventually reports materialized state for relay "moving_state" containing
      """
      key={"tenant":"beta"} payload={"id":3,"source":"after-beta","tenant":"beta"}
      """
    And within "20s" node "{{state_replica}}" eventually reports materialized state for relay "moving_state" containing
      """
      key={"tenant":"acme"} payload={"id":4,"source":"after-acme","tenant":"acme"}
      """
    When Kafka message is published to topic "relay_reader_{{test_id}}"
      """
      {"tenant":"acme","id":15,"source":"ignored-acme"}
      """
    And Kafka message is published to topic "relay_reader_{{test_id}}"
      """
      {"tenant":"beta","id":16,"source":"ignored-beta"}
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "tenant":"acme" | "id":15 | "source":"after-acme"
      key={"tenant":"beta"} | "tenant":"beta" | "id":16 | "source":"after-beta"
      """
