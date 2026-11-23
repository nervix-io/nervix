Feature: Guest-rendered WASM state sections
  A WASM guest describes its own state through the required nervix_describe_state export. SHOW WASM
  PROCESSOR ... STATE renders every returned branch entry on a scratch instance restored from the
  branch's committed checkpoint, and reports what the guest described beside the checkpoint facts:
  each section's name, fields and types, and row count, with the generation and revision the render
  restored. Guest state is always sensitive, so a reply never carries a value of a section. An owner
  whose page does not fit one reply drops its trailing entries, and a page reduced to one entry
  carries that entry's description failure instead; the testing build's page reply overflow stands
  in for replies whose entries pad beyond the payload limit.

  @wasm_state_inspection
  Scenario Outline: Each branch reports the sections its guest renders from the committed checkpoint, with every value withheld
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":1}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":2}
      """
    Then within "30s" the 2 branches of WASM PROCESSOR "counting_guest" have committed revision 2 with <replica_count> confirmed replicas
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE FOR ALL BRANCHES FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /outcome = "page"
      /counts/branches = 2
      /units/0/wasm/branches/0/checkpoint/committed_revision = 2
      /units/0/wasm/branches/0/description/generation = 1
      /units/0/wasm/branches/0/description/revision = 2
      /units/0/wasm/branches/0/description/outcome = "described"
      /units/0/wasm/branches/0/description/sections/0/name = "counters"
      /units/0/wasm/branches/0/description/sections/0/rows = 1
      /units/0/wasm/branches/0/description/sections/0/fields/0/name = "count"
      /units/0/wasm/branches/0/description/sections/0/fields/0/type = "U32"
      /units/0/wasm/branches/0/description/sections/0/fields/0/optional = false
      /units/0/wasm/branches/0/description/sections/0/fields/0/sensitive = true
      /units/0/wasm/branches/1/description/generation = 1
      /units/0/wasm/branches/1/description/revision = 2
      /units/0/wasm/branches/1/description/outcome = "described"
      /units/0/wasm/branches/1/description/sections/0/name = "counters"
      /units/0/wasm/branches/1/description/sections/0/rows = 1
      /units/0/wasm/branches/1/description/sections/0/fields/0/sensitive = true
      """
    And the last WASM state inspection JSON descriptions restored their checkpoints' committed revisions
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE FOR BRANCH KEY { tenant = 'alpha' };
      """
    Then the last command output contains
      """
      guest state branch=40238306d5ebf339fe640ae723e4705c5bb1743e39d0ec207ed8de19fead54b1 generation=1 revision=2: values withheld, guest state is always sensitive
      guest section branch=40238306d5ebf339fe640ae723e4705c5bb1743e39d0ec207ed8de19fead54b1 name=counters rows=1 fields=count U32
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @wasm_state_inspection
  Scenario Outline: A render that fails reports that branch's entry while the branch keeps processing and checkpointing
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has "state description" failing WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_failing_guest;
      UPLOAD RESOURCE wasm_failing_guest VERSION '{{wasm_processor}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA limited_input_event ( tenant STRING, message STRING );
      CREATE SCHEMA limited_output_event ( tenant STRING OPTIONAL, note STRING OPTIONAL );
      CREATE WIRE JSON SCHEMA limited_input_wire MODE STRICT ( tenant string, message string );
      CREATE WIRE JSON CODEC limited_input_codec FROM WIRE SCHEMA limited_input_wire TO SCHEMA limited_input_event;
      CREATE SCHEMA limited_branch_key ( tenant STRING );
      CREATE PARAMETRIZER by_limited_tenant SCHEMA limited_branch_key TTL 5m;
      CREATE RELAY limited_input_events SCHEMA limited_input_event BRANCHED BY by_limited_tenant;
      CREATE RELAY limited_events SCHEMA limited_output_event BRANCHED BY by_limited_tenant;
      CREATE VHOST edge wasm-description-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR limited_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING limited_input_codec
        TO limited_input_events
        INHERIT ALL
        BRANCHED BY by_limited_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR failing_guest FROM limited_input_events
        USING RESOURCE wasm_failing_guest VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 1MiB
        MAX TABLE ELEMENTS 4096
        BRANCHED BY by_limited_tenant
        TO limited_events
        SET tenant = branch.tenant,
            note = coalesce(note, "ok")
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      CREATE SUBSCRIPTION limited_events_subscription TO limited_events;
      START;
      """
    When http payload is posted to host "wasm-description-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","message":"first"}
      """
    And http payload is posted to host "wasm-description-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","message":"first"}
      """
    Then within "30s" the 2 branches of WASM PROCESSOR "failing_guest" have committed revision 1 with <replica_count> confirmed replicas
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR failing_guest STATE FOR BRANCH KEY { tenant = 'alpha' } FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /outcome = "page"
      /units/0/wasm/branches/0/checkpoint/stage = "replica_confirmed"
      /units/0/wasm/branches/0/description/generation = 1
      /units/0/wasm/branches/0/description/revision = 1
      /units/0/wasm/branches/0/description/outcome = "failed"
      /units/0/wasm/branches/0/description/failure/stage = "state_description"
      """
    And the last command output contains
      """
      wasm processor 'failing_guest' state description failed (branch 40238306d5ebf339fe640ae723e4705c5bb1743e39d0ec207ed8de19fead54b1, resource 'wasm_failing_guest' version 1 file 'processors/filter_even.wasm', export 'nervix_describe_state', saved state revision 1): wasm guest state description failed: wasm guest reported global error: guest cannot describe its state
      """
    When http payload is posted to host "wasm-description-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","message":"second"}
      """
    And http payload is posted to host "wasm-description-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","message":"second"}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      "tenant":"alpha" | "note":"ok"
      "tenant":"beta" | "note":"ok"
      "tenant":"alpha" | "note":"ok"
      "tenant":"beta" | "note":"ok"
      """
    And within "30s" the 2 branches of WASM PROCESSOR "failing_guest" have committed revision 2 with <replica_count> confirmed replicas

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @wasm_state_inspection
  Scenario Outline: A description larger than any read carries is reported as a failed state description
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has "oversized state description" failing WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_failing_guest;
      UPLOAD RESOURCE wasm_failing_guest VERSION '{{wasm_processor}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA limited_input_event ( tenant STRING, message STRING );
      CREATE SCHEMA limited_output_event ( tenant STRING OPTIONAL, note STRING OPTIONAL );
      CREATE WIRE JSON SCHEMA limited_input_wire MODE STRICT ( tenant string, message string );
      CREATE WIRE JSON CODEC limited_input_codec FROM WIRE SCHEMA limited_input_wire TO SCHEMA limited_input_event;
      CREATE SCHEMA limited_branch_key ( tenant STRING );
      CREATE PARAMETRIZER by_limited_tenant SCHEMA limited_branch_key TTL 5m;
      CREATE RELAY limited_input_events SCHEMA limited_input_event BRANCHED BY by_limited_tenant;
      CREATE RELAY limited_events SCHEMA limited_output_event BRANCHED BY by_limited_tenant;
      CREATE VHOST edge wasm-description-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR limited_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING limited_input_codec
        TO limited_input_events
        INHERIT ALL
        BRANCHED BY by_limited_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR failing_guest FROM limited_input_events
        USING RESOURCE wasm_failing_guest VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 1MiB
        MAX TABLE ELEMENTS 4096
        BRANCHED BY by_limited_tenant
        TO limited_events
        SET tenant = branch.tenant,
            note = coalesce(note, "ok")
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      CREATE SUBSCRIPTION limited_events_subscription TO limited_events;
      START;
      """
    When http payload is posted to host "wasm-description-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","message":"first"}
      """
    And http payload is posted to host "wasm-description-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","message":"first"}
      """
    Then within "30s" the 2 branches of WASM PROCESSOR "failing_guest" have committed revision 1 with <replica_count> confirmed replicas
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR failing_guest STATE FOR BRANCH KEY { tenant = 'alpha' } FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /outcome = "page"
      /units/0/wasm/branches/0/checkpoint/stage = "replica_confirmed"
      /units/0/wasm/branches/0/description/generation = 1
      /units/0/wasm/branches/0/description/revision = 1
      /units/0/wasm/branches/0/description/outcome = "failed"
      /units/0/wasm/branches/0/description/failure/stage = "state_description"
      """
    And the last command output contains
      """
      wasm processor 'failing_guest' state description failed (branch 40238306d5ebf339fe640ae723e4705c5bb1743e39d0ec207ed8de19fead54b1, resource 'wasm_failing_guest' version 1 file 'processors/filter_even.wasm', export 'nervix_describe_state', saved state revision 1): the description does not fit one read: it carries 129 sections in
      """
    And the last command output contains
      """
      bytes, where one read carries at most 128 sections in
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @wasm_state_inspection
  Scenario Outline: An owner page whose reply overflows keeps its leading entries, and the continuation returns the rest
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":1}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"gamma","sequence":1}
      """
    Then within "30s" the 3 branches of WASM PROCESSOR "counting_guest" have committed revision 1 with <replica_count> confirmed replicas
    Given every owner page reply of WASM PROCESSOR "counting_guest" holding more than one entry overflows
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE LIMIT 3 FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /outcome = "page"
      /counts/branches = 3
      /units/0/wasm/checkpoints/total = 3
      /units/0/wasm/branches/0/description/outcome = "described"
      /units/0/wasm/branches/0/description/sections/0/name = "counters"
      """
    And the last WASM state inspection JSON lists 1 branch entries
    And the last WASM state inspection JSON branch fingerprints are saved as placeholder "first_page"
    And the last command output JSON value at "/continuation" is saved as placeholder "continuation"
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE LIMIT 3 AFTER '{{continuation}}' FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /outcome = "page"
      /counts/branches = 3
      /units/0/wasm/branches/0/description/outcome = "described"
      /units/0/wasm/branches/0/description/sections/0/name = "counters"
      """
    And the last WASM state inspection JSON lists 1 branch entries
    And the last WASM state inspection JSON branch fingerprints are disjoint from placeholder "first_page"
    And the last WASM state inspection JSON branch fingerprints are saved as placeholder "second_page"
    And the last command output JSON value at "/continuation" is saved as placeholder "continuation"
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE LIMIT 3 AFTER '{{continuation}}' FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /outcome = "page"
      /counts/branches = 3
      /continuation = null
      /units/0/wasm/branches/0/description/outcome = "described"
      /units/0/wasm/branches/0/description/sections/0/name = "counters"
      """
    And the last WASM state inspection JSON lists 1 branch entries
    And the last WASM state inspection JSON branch fingerprints are disjoint from placeholder "first_page"
    And the last WASM state inspection JSON branch fingerprints are disjoint from placeholder "second_page"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @wasm_state_inspection
  Scenario Outline: A page reduced to one entry whose reply still overflows carries that entry's description failure instead
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":1}
      """
    Then within "30s" the 2 branches of WASM PROCESSOR "counting_guest" have committed revision 1 with <replica_count> confirmed replicas
    Given every owner page reply of WASM PROCESSOR "counting_guest" holding a described entry overflows
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE FOR BRANCH KEY { tenant = 'alpha' } FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /outcome = "page"
      /counts/branches = 1
      /continuation = null
      /units/0/wasm/branches/0/checkpoint/committed_revision = 1
      /units/0/wasm/branches/0/description/generation = 1
      /units/0/wasm/branches/0/description/revision = 1
      /units/0/wasm/branches/0/description/outcome = "failed"
      /units/0/wasm/branches/0/description/failure/stage = "state_description"
      """
    And the last command output contains
      """
      wasm processor 'counting_guest' state description failed (branch 40238306d5ebf339fe640ae723e4705c5bb1743e39d0ec207ed8de19fead54b1, resource 'wasm_reset_guest' version 1 file 'processors/filter_even.wasm', export 'nervix_describe_state', saved state revision 1): the description does not fit one read: it carries 1 sections in
      """
    And the last command output contains
      """
      bytes, where one read carries at most 128 sections in
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE LIMIT 2 FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /outcome = "page"
      /counts/branches = 2
      /units/0/wasm/branches/0/description/outcome = "failed"
      /units/0/wasm/branches/0/description/failure/stage = "state_description"
      """
    And the last WASM state inspection JSON lists 1 branch entries
    And the last WASM state inspection JSON branch fingerprints are saved as placeholder "first_page"
    And the last command output JSON value at "/continuation" is saved as placeholder "continuation"
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE LIMIT 2 AFTER '{{continuation}}' FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /outcome = "page"
      /counts/branches = 2
      /continuation = null
      /units/0/wasm/branches/0/description/outcome = "failed"
      /units/0/wasm/branches/0/description/failure/stage = "state_description"
      """
    And the last WASM state inspection JSON lists 1 branch entries
    And the last WASM state inspection JSON branch fingerprints are disjoint from placeholder "first_page"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @wasm_state_inspection
  Scenario Outline: An owner whose page reply overflows even after its description gave way refuses the read as too large
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":1}
      """
    Then within "30s" the 2 branches of WASM PROCESSOR "counting_guest" have committed revision 1 with <replica_count> confirmed replicas
    Given every owner page reply of WASM PROCESSOR "counting_guest" overflows
    And client "reader" is connected to node "node-1"
    When client "reader" submits this Plexus command request
      """
      SHOW WASM PROCESSOR counting_guest STATE FORMAT JSON;
      """
    Then the typed inspection refuses the read with "reply_too_large"
    When client "reader" submits this Plexus command request
      """
      SHOW WASM PROCESSOR counting_guest STATE FOR BRANCH KEY { tenant = 'beta' } FORMAT JSON;
      """
    Then the typed inspection refuses the read with "reply_too_large"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @wasm_state_inspection
  Scenario: A read sent to a node that does not own the processor is rendered by the owner
    Given runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And a 3 node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":1}
      """
    Then within "30s" the 2 branches of WASM PROCESSOR "counting_guest" have committed revision 1 with 1 confirmed replicas
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE FORMAT JSON;
      """
    Then the last WASM state inspection JSON owner of unit 0 is saved as placeholder "owner"
    And a cluster node other than placeholder "owner" is saved as placeholder "reader"
    When these Plexus commands are attempted on node "{{reader}}"
      """
      SHOW WASM PROCESSOR counting_guest STATE FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /outcome = "page"
      /units/0/owner = "{{owner}}"
      /units/0/wasm/branches/0/description/outcome = "described"
      /units/0/wasm/branches/0/description/revision = 1
      /units/0/wasm/branches/0/description/sections/0/name = "counters"
      /units/0/wasm/branches/1/description/outcome = "described"
      /units/0/wasm/branches/1/description/revision = 1
      /units/0/wasm/branches/1/description/sections/0/name = "counters"
      """

  @wasm_state_inspection
  Scenario Outline: A branch whose guest state was reset renders the new lifetime's checkpoint
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":1}
      """
    Then within "30s" the 2 branches of WASM PROCESSOR "counting_guest" have committed revision 1 with <replica_count> confirmed replicas
    When these Plexus commands are executed through the client on the leader node
      """
      RESET WASM PROCESSOR counting_guest STATE FOR BRANCH KEY { tenant = 'alpha' };
      """
    Then within "10s" SHOW WASM PROCESSOR "counting_guest" STATE on the leader node contains
      """
      state reset: READY, branch, generation 2
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR counting_guest STATE FOR BRANCH KEY { tenant = 'alpha' } FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /outcome = "page"
      /units/0/wasm/branches/0/checkpoint/generation = 2
      /units/0/wasm/branches/0/description/generation = 2
      /units/0/wasm/branches/0/description/outcome = "described"
      /units/0/wasm/branches/0/description/sections/0/name = "counters"
      """
    And the last WASM state inspection JSON descriptions restored their checkpoints' committed revisions

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @wasm_state_inspection
  Scenario Outline: A guest module without the state description export fails instantiation with the export named
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has "missing state description export" failing WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_failing_guest;
      UPLOAD RESOURCE wasm_failing_guest VERSION '{{wasm_processor}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA limited_input_event ( tenant STRING, message STRING );
      CREATE SCHEMA limited_output_event ( tenant STRING OPTIONAL, note STRING OPTIONAL );
      CREATE WIRE JSON SCHEMA limited_input_wire MODE STRICT ( tenant string, message string );
      CREATE WIRE JSON CODEC limited_input_codec FROM WIRE SCHEMA limited_input_wire TO SCHEMA limited_input_event;
      CREATE SCHEMA limited_branch_key ( tenant STRING );
      CREATE PARAMETRIZER by_limited_tenant SCHEMA limited_branch_key TTL 5m;
      CREATE RELAY limited_input_events SCHEMA limited_input_event BRANCHED BY by_limited_tenant;
      CREATE RELAY limited_events SCHEMA limited_output_event BRANCHED BY by_limited_tenant;
      CREATE VHOST edge wasm-description-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR limited_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING limited_input_codec
        TO limited_input_events
        INHERIT ALL
        BRANCHED BY by_limited_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR failing_guest FROM limited_input_events
        USING RESOURCE wasm_failing_guest VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 1MiB
        MAX TABLE ELEMENTS 4096
        BRANCHED BY by_limited_tenant
        TO limited_events
        SET tenant = branch.tenant,
            note = coalesce(note, "ok")
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      CREATE SUBSCRIPTION limited_events_subscription TO limited_events;
      START;
      """
    When http payload is posted to host "wasm-description-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","message":"first"}
      """
    Then within "10s" the active session observes a server error
    And the last server error contains
      """
      wasm processor 'failing_guest' instantiation failed (branch 40238306d5ebf339fe640ae723e4705c5bb1743e39d0ec207ed8de19fead54b1, resource 'wasm_failing_guest' version 1 file 'processors/filter_even.wasm', export 'nervix_describe_state'): wasm guest instantiation failed: missing required export 'nervix_describe_state'
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  @wasm_state_inspection
  Scenario Outline: The Rust SDK reference guest describes its progress through the default export wiring
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_filter;
      UPLOAD RESOURCE wasm_filter VERSION '{{wasm_processor}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA metric ( value I32 );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer );
      CREATE WIRE JSON CODEC metric_codec FROM WIRE SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY filtered_metrics SCHEMA metric UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/metrics';
      CREATE HTTP ENDPOINT INGESTOR metric_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING metric_codec
        TO raw_metrics
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR filter_even_rows FROM raw_metrics
        USING RESOURCE wasm_filter VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        MAX TABLE ELEMENTS 4096
        UNBRANCHED
        TO filtered_metrics
        SET value = value
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      CREATE SUBSCRIPTION filtered_metrics_subscription TO filtered_metrics;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":2}
      """
    Then the relay subscription receives a payload
      """
      "value":2
      """
    And within "30s" SHOW WASM PROCESSOR "filter_even_rows" STATE on the leader node contains
      """
      guest section branch=unbranched name=progress rows=1 fields=processed_rows U64
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR filter_even_rows STATE FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /outcome = "page"
      /units/0/wasm/branches/0/branch = null
      /units/0/wasm/branches/0/description/generation = 1
      /units/0/wasm/branches/0/description/outcome = "described"
      /units/0/wasm/branches/0/description/sections/0/name = "progress"
      /units/0/wasm/branches/0/description/sections/0/rows = 1
      /units/0/wasm/branches/0/description/sections/0/fields/0/name = "processed_rows"
      /units/0/wasm/branches/0/description/sections/0/fields/0/type = "U64"
      /units/0/wasm/branches/0/description/sections/0/fields/0/sensitive = true
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
