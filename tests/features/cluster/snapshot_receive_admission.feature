@snapshot_receive_admission @deloxide_stress_restore
Feature: Snapshot receive decoding under overlapping bulk memory

  Scenario Outline: A snapshot receive refuses overlapping decode memory and preserves both branches on <cluster_size> nodes with <key_bytes> byte keys
    Given runtime replication is configured with replica count 0 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And two snapshot tenant placeholders contain <key_bytes> bytes each
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( tenant STRING, value I64, source STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, value integer, source string );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 30m;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY accepted SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge snapshot-receive-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR source FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR unique_events FROM events DEDUPLICATE ON input.value, input.source
        MAX TIME 30m BRANCHED BY by_tenant
        TO accepted INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      RELOCATE DEDUPLICATOR unique_events ONTO NODE node-1 IGNORE PREFERENCES;
      CREATE SUBSCRIPTION observed TO accepted;
      """
    And http payload is posted to host "snapshot-receive-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"{{snapshot_alpha}}","value":1,"source":"alpha-one"}
      """
    And http payload is posted to host "snapshot-receive-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"{{snapshot_beta}}","value":1,"source":"beta-one"}
      """
    And http payload is posted to host "snapshot-receive-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"{{snapshot_alpha}}","value":2,"source":"alpha-two"}
      """
    And http payload is posted to host "snapshot-receive-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"{{snapshot_beta}}","value":2,"source":"beta-two"}
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"{{snapshot_alpha}}"} | "tenant":"{{snapshot_alpha}}" | "value":1 | "source":"alpha-one"
      key={"tenant":"{{snapshot_beta}}"} | "tenant":"{{snapshot_beta}}" | "value":1 | "source":"beta-one"
      key={"tenant":"{{snapshot_alpha}}"} | "tenant":"{{snapshot_alpha}}" | "value":2 | "source":"alpha-two"
      key={"tenant":"{{snapshot_beta}}"} | "tenant":"{{snapshot_beta}}" | "value":2 | "source":"beta-two"
      """
    And the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --timeout 60s" from node "{{leader}}" into "snapshot.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And snapshot checkpoints in backup "snapshot.nvxb" refuse overlapping receive memory on node "node-1" and transfer completely after release
    And within "60s" every node reserves only the bulk memory the scenario holds

    Examples:
      | cluster_size | key_bytes |
      | 1            | 1400      |
      | 3            | 1400      |
      | 1            | 2400      |
      | 3            | 2400      |
