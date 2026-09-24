Feature: Resource lifecycle

  Scenario: Describing a missing resource version reports a clear error
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      """
    When these NSPL commands fail with "resource 'fraud_model@1' does not exist"
      """
      DESCRIBE RESOURCE fraud_model VERSION 1;
      """

  Scenario: Created resource is visible before upload
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE fraud_model;
      DESCRIBE RESOURCE fraud_model;
      """
    Then the last command output contains
      """
      resource: fraud_model
      latest: (none)
      versions: (none)
      """

  Scenario: Rust client can upload local resource directories
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "schema/root.proto": "syntax = \"proto3\";",
        "schema/types/common.proto": "message Common {}"
      }
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE proto;
      UPLOAD RESOURCE proto VERSION '{{proto_dir}}';
      """
    Then the last command output contains
      """
      uploaded resource version 1
      """

  Scenario: Uploading an unknown resource fails
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "schema.proto": "syntax = \"proto3\";"
      }
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      """
    When these NSPL commands fail with "resource 'proto' does not exist"
      """
      UPLOAD RESOURCE proto VERSION '{{proto_dir}}';
      """

  @command_completion
  Scenario: An incomplete upload does not admit content or consume its identity
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "schema.proto": "syntax = \"proto3\";"
      }
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE proto;
      """
    And an incomplete upload of resource "proto" with identity "partial-body" is sent to the leader node
    Then the last command error contains
      """
      upload size mismatch: expected 2, received 1
      """
    Given client "uploader" is connected to the leader node
    When client "uploader" selects domain "{{domain}}"
    And client "uploader" uploads resource "proto" from "{{proto_dir}}" with identity "partial-body"
    Then the last command output contains
      """
      uploaded resource version 1
      """
    When client "uploader" executes these NSPL commands
      """
      DESCRIBE RESOURCE proto;
      """
    Then the last command output contains
      """
      versions: 1
      """

  Scenario Outline: Uploaded resource is describable after replication
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "schema/root.proto": "syntax = \"proto3\";",
        "schema/types/common.proto": "message Common {}",
        "assets/lookup.csv": "id,name\n1,Alice\n2,Bob\n"
      }
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE proto;
      UPLOAD RESOURCE proto VERSION '{{proto_dir}}';
      """
    Then the last command output contains
      """
      uploaded resource version 1
      """
    When these NSPL commands are executed on node "node-1"
      """
      DESCRIBE RESOURCE proto;
      """
    Then the last command output contains
      """
      versions: 1
      """
    When these NSPL commands are executed on node "node-1"
      """
      DESCRIBE RESOURCE proto VERSION 1;
      """
    Then the last command output contains
      """
      state=ready
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Uploaded resource is replicated to every node in a 3 node cluster
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "schema/root.proto": "syntax = \"proto3\";",
        "schema/types/common.proto": "message Common {}",
        "assets/lookup.csv": "id,name\n1,Alice\n2,Bob\n"
      }
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE proto;
      UPLOAD RESOURCE proto VERSION '{{proto_dir}}';
      """
    When these NSPL commands are executed on node "node-2"
      """
      DESCRIBE RESOURCE proto VERSION 1;
      """
    Then the last command output contains
      """
      - node-2 topology=alive state=ready
      """
    And the last command output contains
      """
      - node-3 topology=alive state=ready
      """
    And the last command output contains
      """
      - node-1 topology=alive state=ready
      """

  @command_completion
  Scenario: Upload remains pending until every live node installs the resource
    Given a 3 node nervix cluster is started
    And resource installation on node "node-3" pauses before promotion
    And node "node-1" has resource directory "lookup_dir" containing
      """
      {
        "lookup.jsonl": "{\"zip\":\"60601\",\"city\":\"Chicago\"}\n"
      }
      """
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE zip_codes;
      CREATE SCHEMA zip_entry (zip STRING, city STRING);
      CREATE WIRE JSON SCHEMA zip_wire MODE STRICT (zip string, city string);
      CREATE CODEC zip_codec FROM WIRE JSON SCHEMA zip_wire TO SCHEMA zip_entry;
      """
    Given client "uploader" is connected to the leader node
    When client "uploader" selects domain "{{domain}}"
    And client "uploader" begins uploading resource "zip_codes" from "{{lookup_dir}}" with identity "delayed-install" in the background
    Then the resource installation pause on node "node-3" is reached
    When these NSPL commands are executed on node "node-2"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      current_leader:
      """
    When the background resource upload connection is dropped
    And the resource installation pause on node "node-3" is released
    Given client "retrying-uploader" is connected to the leader node
    When client "retrying-uploader" selects domain "{{domain}}"
    And client "retrying-uploader" uploads resource "zip_codes" from "{{lookup_dir}}" with identity "delayed-install"
    Then the last command output contains
      """
      uploaded resource version 1
      """
    When these NSPL commands are executed through the client on node "node-2"
      """
      CREATE HASH MAP zip_by_code
        KEY zip
        FROM RESOURCE zip_codes VERSION 1
        PATH 'lookup.jsonl'
        DECODE USING zip_codec;
      """
    And these NSPL commands are executed on node "node-1"
      """
      LOOKUP zip_by_code KEY '60601';
      """
    Then the last command output contains
      """
      "city":"Chicago"
      """
    When these NSPL commands are executed on node "node-2"
      """
      LOOKUP zip_by_code KEY '60601';
      """
    Then the last command output contains
      """
      "city":"Chicago"
      """
    When these NSPL commands are executed on node "node-3"
      """
      LOOKUP zip_by_code KEY '60601';
      """
    Then the last command output contains
      """
      "city":"Chicago"
      """

  @command_completion
  Scenario Outline: Resource bindings use only completed versions
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has TLS resource directory "tls_v1" for hosts "api.example.com"
    And node "node-1" has TLS resource directory "tls_v2" for hosts "api.example.com"
    And node "node-1" has TLS resource directory "tls_v3" for hosts "api.example.com"
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE tls_bundle;
      UPLOAD RESOURCE tls_bundle VERSION '{{tls_v1}}';
      DESCRIBE RESOURCE tls_bundle;
      """
    Then the last command output contains
      """
      resource: tls_bundle
      latest: 1
      versions: 1
      """
    Given resource installation on node "<held_node>" pauses before promotion
    And client "uploader" is connected to the leader node
    When client "uploader" selects domain "{{domain}}"
    And client "uploader" begins uploading resource "tls_bundle" from "{{tls_v2}}" with identity "applying-version" in the background
    Then the resource installation pause on node "<held_node>" is reached
    When these NSPL commands fail with "resource 'tls_bundle@2' is not a completed version in domain '{{domain}}'"
      """
      CREATE VHOST applying_edge api.example.com WITH TLS tls_bundle VERSION 2;
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE RESOURCE tls_bundle;
      """
    Then the last command output contains
      """
      resource: tls_bundle
      latest: 1
      """
    When the resource installation pause on node "<held_node>" is released
    Then the background NSPL execution succeeds
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE RESOURCE tls_bundle;
      """
    Then the last command output contains
      """
      resource: tls_bundle
      latest: 2
      versions: 1,2
      """
    Then the current leader node is saved as placeholder "upload_leader"
    Given resource installation on node "{{upload_leader}}" fails before promotion
    When client "uploader" upload of resource "tls_bundle" from "{{tls_v3}}" with identity "failed-version" fails with "for version 3 failed"
    Then the last command output contains
      """
      injected resource installation failure
      """
    When these NSPL commands fail with "resource 'tls_bundle@3' is not a completed version in domain '{{domain}}'"
      """
      CREATE VHOST failed_edge failed.example.com WITH TLS tls_bundle VERSION 3;
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE RESOURCE tls_bundle;
      """
    Then the last command output contains
      """
      resource: tls_bundle
      latest: 2
      versions: 1,2,3
      """

    Examples:
      | cluster_size | held_node |
      | 1            | node-1    |
      | 3            | node-3    |

  @resource_upload_idempotency
  Scenario Outline: Upload retry reports one assigned version
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "schema/root.proto": "syntax = \"proto3\";",
        "schema/types/common.proto": "message Common {}"
      }
      """
    And node "node-1" has resource directory "different_proto_dir" containing
      """
      {
        "schema/root.proto": "syntax = \"proto3\"; message Different {}"
      }
      """
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE proto;
      """
    Given client "uploader" is connected to the leader node
    When client "uploader" selects domain "{{domain}}"
    And client "uploader" uploads resource "proto" from "{{proto_dir}}" with identity "retry-one"
    Then the last command output contains
      """
      uploaded resource version 1
      """
    And the last client upload installed version 1
    When client "uploader" uploads resource "proto" from "{{proto_dir}}" with identity "retry-one"
    Then the last command output contains
      """
      uploaded resource version 1
      """
    And the last client upload recovered version 1 from an earlier upload
    When client "uploader" upload of resource "proto" from "{{different_proto_dir}}" with identity "retry-one" fails with "already assigned version 1 with digest"
    When client "uploader" executes these NSPL commands
      """
      DESCRIBE RESOURCE proto;
      """
    Then the last command output contains
      """
      versions: 1
      """
    And the last command output does not contain
      """
      versions: 1,2
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @resource_upload_idempotency
  Scenario: Upload retry after leader change reports the assigned version
    Given a 3 node nervix cluster is started
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "schema/root.proto": "syntax = \"proto3\";",
        "schema/types/common.proto": "message Common {}"
      }
      """
    And the active domain is "{{domain}}"
    And node "node-1" eventually reports leader "node-1"
    When these NSPL commands are executed on node "node-1"
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE proto;
      """
    Given client "uploader" is connected to the leader node
    When client "uploader" selects domain "{{domain}}"
    And client "uploader" uploads resource "proto" from "{{proto_dir}}" with identity "leader-retry"
    Then the last command output contains
      """
      uploaded resource version 1
      """
    When node "node-1" is stopped
    Then node "node-2" eventually reports a leader other than "node-1"
    Given client "retrying-uploader" is connected to node "node-2"
    When client "retrying-uploader" selects domain "{{domain}}"
    And client "retrying-uploader" uploads resource "proto" from "{{proto_dir}}" with identity "leader-retry"
    Then the last command output contains
      """
      uploaded resource version 1
      """
    When client "retrying-uploader" executes these NSPL commands
      """
      DESCRIBE RESOURCE proto;
      """
    Then the last command output contains
      """
      versions: 1
      """

  Scenario: Uploading a second resource version appends to the version list
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "proto_v1" containing
      """
      {
        "schema.proto": "syntax = \"proto3\";",
        "data.csv": "id,name\n1,Alice\n"
      }
      """
    And node "node-1" has resource directory "proto_v2" containing
      """
      {
        "schema.proto": "syntax = \"proto3\";",
        "data.csv": "id,name\n1,Alice\n2,Bob\n"
      }
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE proto;
      UPLOAD RESOURCE proto VERSION '{{proto_v1}}';
      UPLOAD RESOURCE proto VERSION '{{proto_v2}}';
      DESCRIBE RESOURCE proto;
      """
    Then the last command output contains
      """
      resource: proto
      latest: 2
      versions: 1,2
      """

  Scenario: Uploading a resource through a follower client redirects to the leader
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "schema/root.proto": "syntax = \"proto3\";",
        "schema/types/common.proto": "message Common {}"
      }
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE proto;
      """
    And these NSPL commands are executed through the client on a follower node
      """
      UPLOAD RESOURCE proto VERSION '{{proto_dir}}';
      """
    Then the last command output contains
      """
      uploaded resource version 1
      """

  Scenario Outline: Uploaded resources survive a full cluster restart
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "schema/root.proto": "syntax = \"proto3\";",
        "schema/types/common.proto": "message Common {}",
        "assets/lookup.csv": "id,name\n1,Alice\n2,Bob\n"
      }
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE proto;
      UPLOAD RESOURCE proto VERSION '{{proto_dir}}';
      """
    And the cluster is restarted
    Then within "15s" node "node-1" eventually reports describe resource as "versions: 1"
      """
      DESCRIBE RESOURCE proto;
      """
    And within "15s" node "node-1" eventually reports describe resource as "state=ready"
      """
      DESCRIBE RESOURCE proto VERSION 1;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Uploaded resources converge after a node rejoins the cluster
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "schema/root.proto": "syntax = \"proto3\";",
        "schema/types/common.proto": "message Common {}",
        "assets/lookup.csv": "id,name\n1,Alice\n2,Bob\n"
      }
      """
    And node "node-1" eventually reports leader "node-1"
    And node "node-3" is stopped
    Then node "node-1" eventually reports status containing "raft member 'node-3' is marked unavailable"
    When these NSPL commands are executed on node "node-1"
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE proto;
      UPLOAD RESOURCE proto VERSION '{{proto_dir}}';
      """
    Then within "10s" node "node-1" eventually reports describe resource as "- node-1 topology=alive state=ready"
      """
      DESCRIBE RESOURCE proto VERSION 1;
      """
    When node "node-3" is started
    Then within "20s" node "node-1" eventually reports describe resource as "- node-3 topology=alive state=ready"
      """
      DESCRIBE RESOURCE proto VERSION 1;
      """
    And within "20s" node "node-3" eventually reports describe resource as "- node-3 topology=alive state=ready"
      """
      DESCRIBE RESOURCE proto VERSION 1;
      """

  @resource_streaming
  Scenario: Large resource replication preserves control responsiveness
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "large_resource" with file "model.bin" of 48 MiB
    And node "node-1" eventually reports leader "node-1"
    And node "node-3" is stopped
    When these NSPL commands are executed on node "node-1"
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE large_model;
      UPLOAD RESOURCE large_model VERSION '{{large_resource}}';
      """
    Then the last command output contains
      """
      uploaded resource version 1
      """
    When node "node-3" is started
    Then within "1s" these NSPL commands complete on node "node-3"
      """
      SHOW CLUSTER STATUS;
      """
    And within "30s" node "node-1" eventually reports describe resource as "- node-3 topology=alive state=ready"
      """
      DESCRIBE RESOURCE large_model VERSION 1;
      """
