Feature: Recovery from a rejected WASM guest snapshot
  A guest that refuses the snapshot it is handed classifies those saved bytes as unusable. The
  default policy keeps them and reports the refusal, so no unrelated failure can erase computation
  state. A processor that opts in replaces that lifetime exactly once, through the coordinated
  reset, and spends its one attempt whether or not the fresh lifetime starts.

  Scenario Outline: A rejected snapshot stays intact under the preserve policy
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has "state restore" failing WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_recovering_guest;
      UPLOAD RESOURCE wasm_recovering_guest VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA limited_input_event ( tenant STRING, message STRING );
      CREATE SCHEMA limited_output_event ( tenant STRING OPTIONAL, note STRING OPTIONAL );
      CREATE WIRE JSON SCHEMA limited_input_wire MODE STRICT ( tenant string, message string );
      CREATE CODEC limited_input_codec FROM WIRE JSON SCHEMA limited_input_wire TO SCHEMA limited_input_event;
      CREATE SCHEMA limited_branch_key ( tenant STRING );
      CREATE BRANCH by_limited_tenant SCHEMA limited_branch_key TTL 5m;
      CREATE RELAY limited_input_events SCHEMA limited_input_event BRANCHED BY by_limited_tenant;
      CREATE RELAY limited_events SCHEMA limited_output_event BRANCHED BY by_limited_tenant;
      CREATE VHOST edge wasm-recovery-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR limited_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING limited_input_codec
        TO limited_input_events
        INHERIT ALL
        BRANCHED BY by_limited_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR recovering_guest FROM limited_input_events
        USING RESOURCE wasm_recovering_guest VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 100000
        MAX MEMORY 1MiB
        BRANCHED BY by_limited_tenant
        TO limited_events
        SET tenant = branch.tenant,
            note = coalesce(note, "ok")
        ON MESSAGE ERROR LOG
        ON REJECTED STATE PRESERVE
        ON GLOBAL ERROR LOG;
      CREATE SUBSCRIPTION limited_events_subscription TO limited_events;
      START;
      """
    When http payload is posted to host "wasm-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","message":"beta-first"}
      """
    And http payload for tenant "alpha" with a 4096 byte message is posted to host "wasm-recovery-{{test_id}}.example.com" path "/events"
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      "tenant":"beta" | "note":"ok"
      "tenant":"alpha" | "note":"ok"
      """
    When http payload for tenant "alpha" with a 16384 byte message is posted to host "wasm-recovery-{{test_id}}.example.com" path "/events"
    Then within "20s" the active session observes a server error containing
      """
      wasm guest exhausted MAX FUEL 100000
      """
    When http payload is posted to host "wasm-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","message":"alpha-after-limit"}
      """
    Then within "20s" the active session observes a server error containing
      """
      wasm processor 'recovering_guest' application state restoration failed (branch {"tenant":"alpha"}, resource 'wasm_recovering_guest' version 1 file 'processors/filter_even.wasm', export 'nervix_load_state', saved state revision 1)
      """
    When http payload is posted to host "wasm-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","message":"beta-second"}
      """
    And http payload is posted to host "wasm-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","message":"alpha-again"}
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      "tenant":"beta" | "note":"ok"
      """
    And the relay subscription does not receive a payload containing fragments within "10s"
      """
      "tenant":"alpha"
      """
    And within "20s" DESCRIBE WASM PROCESSOR "recovering_guest" on the leader node contains
      """
      rejected state policy: PRESERVE
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Opt-in recovery replaces a rejected lifetime once and resumes the branch
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has "state restore" failing WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_recovering_guest;
      UPLOAD RESOURCE wasm_recovering_guest VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA limited_input_event ( tenant STRING, message STRING );
      CREATE SCHEMA limited_output_event ( tenant STRING OPTIONAL, note STRING OPTIONAL );
      CREATE WIRE JSON SCHEMA limited_input_wire MODE STRICT ( tenant string, message string );
      CREATE CODEC limited_input_codec FROM WIRE JSON SCHEMA limited_input_wire TO SCHEMA limited_input_event;
      CREATE SCHEMA limited_branch_key ( tenant STRING );
      CREATE BRANCH by_limited_tenant SCHEMA limited_branch_key TTL 5m;
      CREATE RELAY limited_input_events SCHEMA limited_input_event BRANCHED BY by_limited_tenant;
      CREATE RELAY limited_events SCHEMA limited_output_event BRANCHED BY by_limited_tenant;
      CREATE VHOST edge wasm-recovery-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR limited_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING limited_input_codec
        TO limited_input_events
        INHERIT ALL
        BRANCHED BY by_limited_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR recovering_guest FROM limited_input_events
        USING RESOURCE wasm_recovering_guest VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 100000
        MAX MEMORY 1MiB
        BRANCHED BY by_limited_tenant
        TO limited_events
        SET tenant = branch.tenant,
            note = coalesce(note, "ok")
        ON MESSAGE ERROR LOG
        ON REJECTED STATE RESET
        ON GLOBAL ERROR LOG;
      CREATE SUBSCRIPTION limited_events_subscription TO limited_events;
      START;
      """
    When http payload is posted to host "wasm-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","message":"beta-first"}
      """
    And http payload for tenant "alpha" with a 4096 byte message is posted to host "wasm-recovery-{{test_id}}.example.com" path "/events"
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      "tenant":"beta" | "note":"ok"
      "tenant":"alpha" | "note":"ok"
      """
    When http payload for tenant "alpha" with a 16384 byte message is posted to host "wasm-recovery-{{test_id}}.example.com" path "/events"
    Then within "20s" the active session observes a server error containing
      """
      wasm guest exhausted MAX FUEL 100000
      """
    When http payload is posted to host "wasm-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","message":"alpha-after-limit"}
      """
    Then within "30s" DESCRIBE WASM PROCESSOR "recovering_guest" on the leader node contains
      """
      rejected state recovery: branch, application state rejected, recovered
      """
    When http payload is posted to host "wasm-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","message":"beta-second"}
      """
    And http payload is posted to host "wasm-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","message":"alpha-recovered"}
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      "tenant":"beta" | "note":"ok"
      "tenant":"alpha" | "note":"ok"
      """
    And within "20s" DESCRIBE WASM PROCESSOR "recovering_guest" on the leader node contains
      """
      rejected state policy: RESET
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A failed fresh initialization spends the one recovery attempt for good
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has "state restore" failing WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_recovering_guest;
      UPLOAD RESOURCE wasm_recovering_guest VERSION '{{wasm_processor}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA limited_input_event ( tenant STRING, message STRING );
      CREATE SCHEMA limited_output_event ( tenant STRING OPTIONAL, note STRING OPTIONAL );
      CREATE WIRE JSON SCHEMA limited_input_wire MODE STRICT ( tenant string, message string );
      CREATE CODEC limited_input_codec FROM WIRE JSON SCHEMA limited_input_wire TO SCHEMA limited_input_event;
      CREATE SCHEMA limited_branch_key ( tenant STRING );
      CREATE BRANCH by_limited_tenant SCHEMA limited_branch_key TTL 5m;
      CREATE RELAY limited_input_events SCHEMA limited_input_event BRANCHED BY by_limited_tenant;
      CREATE RELAY limited_events SCHEMA limited_output_event BRANCHED BY by_limited_tenant;
      CREATE VHOST edge wasm-recovery-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR limited_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING limited_input_codec
        TO limited_input_events
        INHERIT ALL
        BRANCHED BY by_limited_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR recovering_guest FROM limited_input_events
        USING RESOURCE wasm_recovering_guest VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 100000
        MAX MEMORY 1MiB
        BRANCHED BY by_limited_tenant
        TO limited_events
        SET tenant = branch.tenant,
            note = coalesce(note, "ok")
        ON MESSAGE ERROR LOG
        ON REJECTED STATE RESET
        ON GLOBAL ERROR LOG;
      CREATE SUBSCRIPTION limited_events_subscription TO limited_events;
      START;
      """
    When fresh WASM reset guest initialization fails on every node
    And http payload is posted to host "wasm-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","message":"beta-first"}
      """
    And http payload for tenant "alpha" with a 4096 byte message is posted to host "wasm-recovery-{{test_id}}.example.com" path "/events"
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      "tenant":"beta" | "note":"ok"
      "tenant":"alpha" | "note":"ok"
      """
    When http payload for tenant "alpha" with a 16384 byte message is posted to host "wasm-recovery-{{test_id}}.example.com" path "/events"
    And http payload is posted to host "wasm-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","message":"alpha-after-limit"}
      """
    Then within "30s" the active session observes a server error containing
      """
      wasm processor 'recovering_guest' rejected-state recovery failed
      """
    And within "30s" DESCRIBE WASM PROCESSOR "recovering_guest" on the leader node contains
      """
      rejected state recovery: branch, application state rejected, failed
      """
    When fresh WASM reset guest initialization succeeds again on every node
    And the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION limited_events_subscription TO limited_events;
      """
    And http payload is posted to host "wasm-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","message":"alpha-after-restart"}
      """
    And http payload is posted to host "wasm-recovery-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","message":"beta-after-restart"}
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      "tenant":"beta" | "note":"ok"
      """
    And the relay subscription does not receive a payload containing fragments within "10s"
      """
      "tenant":"alpha"
      """
    And within "20s" DESCRIBE WASM PROCESSOR "recovering_guest" on the leader node contains
      """
      rejected state recovery: branch, application state rejected, failed
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
