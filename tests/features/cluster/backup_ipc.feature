Feature: Bounded Arrow IPC in restored materialized state

  Scenario Outline: A restore refuses a self-consistent archive with an Arrow body outside its section
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I32 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY state SCHEMA event UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST edge backup-ipc-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/state' TYPE HTTP;
      CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO state INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to host "backup-ipc-{{test_id}}.example.com" path "/state"
      """
      {"value":42}
      """
    Then within "30s" node "node-1" eventually reports materialized state for relay "state" containing
      """
      payload={"value":42}
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "source.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given backup archive "source.nvxb" is copied to "malformed.nvxb" with a materialized Arrow body declaring <declared> bytes
    And the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "domain {{domain}} --as {{domain}}_restored --resume <mode>" from "malformed.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_REFUSED" and a message containing "materialized state"
    And every node still answers cluster status

    Examples:
      | cluster_size | mode      | declared            |
      | 1            | --dry-run | 1152921504606846976 |
      | 3            | --dry-run | 1152921504606846976 |
      | 1            |           | 1152921504606846976 |
      | 3            |           | 1152921504606846976 |
