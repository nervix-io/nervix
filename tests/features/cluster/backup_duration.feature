Feature: Backup and restore of DURATION values
  Scenario Outline: A resumed backup restores DURATION schema fields, branch keys and materialized values
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA policy_update ( tenant STRING, level I64 );
      CREATE SCHEMA policy ( tenant STRING, timeout DURATION, retries <duration_list> );
      CREATE WIRE JSON SCHEMA policy_wire MODE STRICT ( tenant string, level integer );
      CREATE WIRE JSON CODEC policy_codec FROM WIRE SCHEMA policy_wire TO SCHEMA policy_update;
      CREATE SCHEMA policy_key ( tenant STRING, timeout DURATION );
      CREATE BRANCH by_policy SCHEMA policy_key TTL 30m;
      CREATE RELAY state SCHEMA policy BRANCHED BY by_policy WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST edge backup-duration-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/state';
      CREATE HTTP ENDPOINT INGESTOR ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING policy_codec
        TO state
          SET tenant = input.tenant,
              timeout = IF input.level > 1 THEN 1m 30s ELSE 10s END,
              retries = vec(100ms, 1s)
          BRANCHED BY by_policy
          SET tenant = message.tenant,
              timeout = message.timeout
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to host "backup-duration-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"alpha","level":2}
      """
    And http payload is posted to host "backup-duration-{{test_id}}.example.com" path "/state"
      """
      {"tenant":"beta","level":0}
      """
    Then within "30s" node "node-1" eventually reports materialized state for relay "state" containing
      """
      {"branch":{"tenant":"alpha","timeout":"1m 30s"},"fields":{"retries":["100ms","1s"],"tenant":"alpha","timeout":"1m 30s"}}
      """
    And within "30s" node "node-1" eventually reports materialized state for relay "state" containing
      """
      {"branch":{"tenant":"beta","timeout":"10s"},"fields":{"retries":["100ms","1s"],"tenant":"beta","timeout":"10s"}}
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "duration.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "domain {{domain}} --resume" from "duration.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 0 resource versions and 10 models
    And the CLI restore reports domain "{{domain}}" as "RUNNING" at start version 1
    And within "30s" node "restored-1" eventually reports materialized state for relay "state" containing
      """
      {"branch":{"tenant":"alpha","timeout":"1m 30s"},"fields":{"retries":["100ms","1s"],"tenant":"alpha","timeout":"1m 30s"}}
      """
    And within "30s" node "restored-1" eventually reports materialized state for relay "state" containing
      """
      {"branch":{"tenant":"beta","timeout":"10s"},"fields":{"retries":["100ms","1s"],"tenant":"beta","timeout":"10s"}}
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE SCHEMA policy_key;
      """
    Then the last command output contains
      """
      timeout DURATION
      """

    Examples:
      | cluster_size | replica_count | duration_list |
      | 1            | 0             | VEC<DURATION> |
      | 3            | 1             | VEC<DURATION> |
