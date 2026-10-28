Feature: Materialized capture under bulk memory contention

  @restore_installation @deloxide_stress_restore
  Scenario Outline: A stopped materialized capture refuses overlapping bulk memory and succeeds after release on <cluster_size> nodes
    Given runtime replication is configured with replica count 0 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( tenant STRING, value I64, source STRING );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( tenant string, value integer, source string );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 30m;
      CREATE RELAY state SCHEMA event BRANCHED BY by_tenant WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY generated SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge backup-bulk-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/state' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR source FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO state INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE GENERATOR produce USING MATERIALIZED STATE state EACH 100ms BRANCHED BY by_tenant
        TO generated SET tenant = branch.tenant, value = relay_state.state.value, source = relay_state.state.source
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      START;
      CREATE SUBSCRIPTION initial_output TO generated;
      """
    And http payload is posted to host "backup-bulk-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"alpha","value":1,"source":"alpha-first"}
      """
    And http payload is posted to host "backup-bulk-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"beta","value":2,"source":"beta-state"}
      """
    And http payload is posted to host "backup-bulk-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"alpha","value":3,"source":"alpha-state"}
      """
    Then within "60s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "value":3 | "source":"alpha-state"
      key={"tenant":"beta"} | "tenant":"beta" | "value":2 | "source":"beta-state"
      """
    When these Plexus commands are executed on the leader node
      """
      STOP;
      """
    Then the current leader node is saved as placeholder "leader"
    When bulk memory on every node keeps only "3145728" bytes free
    And the CLI backs up "domain {{domain}} --timeout 30s" from node "{{leader}}" into "refused.nvxb" reporting JSON
    Then the CLI backup failed with JSON error code "BACKUP_REFUSED"
    And the CLI backup failure mentions "materialized snapshot memory admission was refused"
    And within "60s" every node reserves only the bulk memory the scenario holds
    When bulk memory on every node is released
    And the CLI backs up "domain {{domain}} --timeout 60s" from node "{{leader}}" into "accepted.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given the restore coordinator uses remote placement in a multi-node cluster
    When the CLI restores "domain {{domain}} --as {{domain}}_copy --resume" from "accepted.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}_copy" with 0 resource versions and 11 models
    Given the active domain is "{{domain}}_copy"
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION restored_output TO generated;
      """
    Then within "60s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "value":3 | "source":"alpha-state"
      key={"tenant":"beta"} | "tenant":"beta" | "value":2 | "source":"beta-state"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
