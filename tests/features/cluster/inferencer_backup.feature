Feature: Inferencer state in backup archives

  @inferencer_backup @restore_installation @deloxide_stress @deloxide_stress_restore
  Scenario Outline: A <cut> backup and <resume_mode> restore <state_mode> both inferencer branch sums on <cluster_size> nodes
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA features ( tenant STRING, vector <input_type> );
      CREATE SCHEMA scored ( score <output_type> );
      CREATE WIRE JSON SCHEMA features_wire MODE STRICT ( tenant string, vector array );
      CREATE WIRE JSON CODEC features_codec FROM WIRE SCHEMA features_wire TO SCHEMA features;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 30m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE VHOST edge infer-backup-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/features';
      CREATE HTTP ENDPOINT INGESTOR feature_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING features_codec
        TO features INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' <tensor_type>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' <tensor_type>[2] INITIAL ZEROS }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        BRANCHED BY by_tenant
        TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION scores_before TO scored;
      START;
      """
    When http payload is posted to node "node-1" with host "infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[100.0,1000.0]}
      """
    And http payload is posted to node "node-1" with host "infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[2.0,20.0]}
      """
    And http payload is posted to node "node-1" with host "infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[200.0,2000.0]}
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[11.0]
      key={"tenant":"beta"} | "score":[1100.0]
      key={"tenant":"acme"} | "score":[33.0]
      key={"tenant":"beta"} | "score":[3300.0]
      """
    When these Plexus commands are executed on the leader node
      """
      DELETE SUBSCRIPTION scores_before;
      <cut_command>
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} <backup_flags>" from node "{{leader}}" into "inferencer.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "inferencer.nvxb" as json
    Then the described backup has exactly <sections> "inferencer" state sections
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    And the restore coordinator uses remote placement in a multi-node cluster
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "domain {{domain}} <restore_flags>" from "inferencer.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 1 resource versions and 12 models
    And the CLI restore reports domain "{{domain}}" as "<status>" at start version 1
    And the CLI restore reports no warnings
    When these Plexus commands are executed on the leader node
      """
      <start_command>
      CREATE SUBSCRIPTION scores_after TO scored;
      """
    And http payload is posted to node "restored-1" with host "infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[3.0,30.0]}
      """
    And http payload is posted to node "restored-1" with host "infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[300.0,3000.0]}
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[<acme>]
      key={"tenant":"beta"} | "score":[<beta>]
      """

    Examples:
      | cluster_size | replica_count | input_type    | output_type   | tensor_type       | cut                | resume_mode          | state_mode | cut_command      | backup_flags    | restore_flags            | sections | status  | start_command    | acme | beta   |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> | quiesced           | RESUME               | preserve   | DESCRIBE DOMAIN; | --timeout 120s  | --resume                 | 2        | RUNNING | DESCRIBE DOMAIN; | 66.0 | 6600.0 |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> | live               | RESUME               | preserve   | DESCRIBE DOMAIN; | --without-pause | --resume                 | 2        | RUNNING | DESCRIBE DOMAIN; | 66.0 | 6600.0 |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> | stopped            | START                | preserve   | STOP;            | --timeout 120s  |                          | 2        | STOPPED | START;           | 66.0 | 6600.0 |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> | quiesced           | START                | preserve   | DESCRIBE DOMAIN; | --timeout 120s  |                          | 2        | STOPPED | START;           | 66.0 | 6600.0 |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> | configuration-only | RESUME               | initialize | DESCRIBE DOMAIN; | --without-state | --resume                 | 0        | RUNNING | DESCRIBE DOMAIN; | 33.0 | 3300.0 |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> | quiesced           | RESUME WITHOUT STATE | initialize | DESCRIBE DOMAIN; | --timeout 120s  | --resume --without-state | 2        | RUNNING | DESCRIBE DOMAIN; | 33.0 | 3300.0 |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> | quiesced           | RESUME               | preserve   | DESCRIBE DOMAIN; | --timeout 120s  | --resume                 | 2        | RUNNING | DESCRIBE DOMAIN; | 66.0 | 6600.0 |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> | live               | RESUME               | preserve   | DESCRIBE DOMAIN; | --without-pause | --resume                 | 2        | RUNNING | DESCRIBE DOMAIN; | 66.0 | 6600.0 |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> | stopped            | START                | preserve   | STOP;            | --timeout 120s  |                          | 2        | STOPPED | START;           | 66.0 | 6600.0 |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> | quiesced           | START                | preserve   | DESCRIBE DOMAIN; | --timeout 120s  |                          | 2        | STOPPED | START;           | 66.0 | 6600.0 |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> | configuration-only | RESUME               | initialize | DESCRIBE DOMAIN; | --without-state | --resume                 | 0        | RUNNING | DESCRIBE DOMAIN; | 33.0 | 3300.0 |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> | quiesced           | RESUME WITHOUT STATE | initialize | DESCRIBE DOMAIN; | --timeout 120s  | --resume --without-state | 2        | RUNNING | DESCRIBE DOMAIN; | 33.0 | 3300.0 |

  @inferencer_backup @restore_installation @deloxide_stress @deloxide_stress_restore
  Scenario Outline: Stopped inferencer restore preserves complete archive values and installs replicas on <cluster_size> nodes
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA features ( tenant STRING, vector <input_type> );
      CREATE SCHEMA scored ( score <output_type> );
      CREATE WIRE JSON SCHEMA features_wire MODE STRICT ( tenant string, vector array );
      CREATE WIRE JSON CODEC features_codec FROM WIRE SCHEMA features_wire TO SCHEMA features;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 30m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE VHOST edge infer-backup-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/features';
      CREATE HTTP ENDPOINT INGESTOR feature_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING features_codec
        TO features INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' <tensor_type>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' <tensor_type>[2] INITIAL ZEROS }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        BRANCHED BY by_tenant
        TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION scores_before TO scored;
      START;
      """
    When http payload is posted to node "node-1" with host "infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[100.0,1000.0]}
      """
    And http payload is posted to node "node-1" with host "infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[2.0,20.0]}
      """
    And http payload is posted to node "node-1" with host "infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[200.0,2000.0]}
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[11.0]
      key={"tenant":"beta"} | "score":[1100.0]
      key={"tenant":"acme"} | "score":[33.0]
      key={"tenant":"beta"} | "score":[3300.0]
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "inferencer.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "inferencer.nvxb" as json
    Then the described backup has exactly 2 "inferencer" state sections
    When the CLI describes backup archive "inferencer.nvxb" as text
    Then the CLI exited with status 0 and printed "inferencer=running unit=Single"
    And the CLI exited with status 0 and printed "shapes=[[2]]"
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    And the restore coordinator uses remote placement in a multi-node cluster
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "domain {{domain}}" from "inferencer.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 1 resource versions and 12 models
    And the CLI restore reports domain "{{domain}}" as "STOPPED" at start version 1
    And the CLI restore reports no warnings
    And in a multi-node cluster backup "inferencer.nvxb" inferencer "running" is installed on its scheduled owner and replicas
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "stopped.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup archives "inferencer.nvxb" and "stopped.nvxb" preserve complete domain "{{domain}}" restored as stopped domain "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scores_after TO scored;
      START;
      """
    And http payload is posted to node "restored-1" with host "infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[3.0,30.0]}
      """
    And http payload is posted to node "restored-1" with host "infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[300.0,3000.0]}
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[66.0]
      key={"tenant":"beta"} | "score":[6600.0]
      """

    Examples:
      | cluster_size | replica_count | input_type    | output_type   | tensor_type       |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |

  @inferencer_backup @restore_installation @deloxide_stress @deloxide_stress_restore
  Scenario Outline: An inferencer descriptor with a foreign branch field is refused by branch name on <cluster_size> nodes
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA features ( tenant STRING, vector <input_type> );
      CREATE SCHEMA scored ( score <output_type> );
      CREATE WIRE JSON SCHEMA features_wire MODE STRICT ( tenant string, vector array );
      CREATE WIRE JSON CODEC features_codec FROM WIRE SCHEMA features_wire TO SCHEMA features;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 30m;
      CREATE RELAY features SCHEMA features BRANCHED BY by_tenant;
      CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant;
      CREATE VHOST edge infer-backup-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/features';
      CREATE HTTP ENDPOINT INGESTOR feature_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING features_codec
        TO features INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS { 'features' <tensor_type>[2] = input.vector }
        STATE { 'state_in' FROM 'state_out' <tensor_type>[2] INITIAL ZEROS }
        OUTPUT SCHEMA { 'score' <tensor_type>[1] }
        BRANCHED BY by_tenant
        TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION scores_before TO scored;
      START;
      """
    When http payload is posted to node "node-1" with host "infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[100.0,1000.0]}
      """
    And http payload is posted to node "node-1" with host "infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"acme","vector":[2.0,20.0]}
      """
    And http payload is posted to node "node-1" with host "infer-backup-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[200.0,2000.0]}
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "score":[11.0]
      key={"tenant":"beta"} | "score":[1100.0]
      key={"tenant":"acme"} | "score":[33.0]
      key={"tenant":"beta"} | "score":[3300.0]
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "inferencer.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "inferencer.nvxb" as json
    Then the described backup has exactly 2 "inferencer" state sections
    Given backup archive "inferencer.nvxb" is copied to "foreign-key.nvxb" with field "tenant" renamed to "zone" in every branch key under "inferencer/running"
    And the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "domain {{domain}} --resume --dry-run" from "foreign-key.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_REFUSED" and a message containing "/descriptor.rkyv' does not belong to the branching of inferencer 'running' in domain '{{domain}}': it is not a key of branch 'by_tenant': the key has no field 'tenant'"

    Examples:
      | cluster_size | replica_count | input_type    | output_type   | tensor_type       |
      | 1            | 0             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | 1             | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
