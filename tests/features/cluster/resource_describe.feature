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
      published resource version 1
      """
    When these NSPL commands are executed through the client on the leader node
      """
      DESCRIBE RESOURCE proto VERSION 1;
      """
    Then the last command output contains
      """
      cluster_ready: true
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
      published resource version 1
      """
    And within "10s" node "node-1" eventually reports describe resource as "versions: 1"
      """
      DESCRIBE RESOURCE proto;
      """
    And within "10s" node "node-1" eventually reports describe resource as "cluster_ready: true"
      """
      DESCRIBE RESOURCE proto VERSION 1;
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
    Then within "10s" node "node-1" eventually reports describe resource as "- node-2 topology=alive state=ready"
      """
      DESCRIBE RESOURCE proto VERSION 1;
      """
    And within "10s" node "node-1" eventually reports describe resource as "- node-3 topology=alive state=ready"
      """
      DESCRIBE RESOURCE proto VERSION 1;
      """
    And within "10s" node "node-2" eventually reports describe resource as "cluster_ready: true"
      """
      DESCRIBE RESOURCE proto VERSION 1;
      """
    And within "10s" node "node-2" eventually reports describe resource as "- node-2 topology=alive state=ready"
      """
      DESCRIBE RESOURCE proto VERSION 1;
      """

  @resource_upload_idempotency
  Scenario Outline: Upload retry reports one published version
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
      published resource version 1
      """
    When client "uploader" uploads resource "proto" from "{{proto_dir}}" with identity "retry-one"
    Then the last command output contains
      """
      published resource version 1
      """
    When client "uploader" upload of resource "proto" from "{{different_proto_dir}}" with identity "retry-one" fails with "already published version 1 with digest"
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
  Scenario: Upload retry after leader change reports the published version
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
      published resource version 1
      """
    When node "node-1" is stopped
    Then node "node-2" eventually reports a leader other than "node-1"
    Given client "retrying-uploader" is connected to node "node-2"
    When client "retrying-uploader" selects domain "{{domain}}"
    And client "retrying-uploader" uploads resource "proto" from "{{proto_dir}}" with identity "leader-retry"
    Then the last command output contains
      """
      published resource version 1
      """
    When client "retrying-uploader" executes these NSPL commands
      """
      DESCRIBE RESOURCE proto;
      """
    Then the last command output contains
      """
      versions: 1
      """

  @resource_readiness
  Scenario: Readiness deadline returns the published version while a replica is pending
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
    And node "node-3" is stopped
    When these NSPL commands are executed on node "node-1"
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE proto;
      """
    Given client "uploader" is connected to node "node-1"
    When client "uploader" selects domain "{{domain}}"
    And client "uploader" uploads resource "proto" from "{{proto_dir}}" with identity "readiness-wait"
    And node "node-3" is started
    And client "uploader" waits "0ms" for resource "proto" version 1 readiness
    Then the last command output contains
      """
      version: 1
      cluster_ready: false
      """
    And the last command output contains
      """
      published but not ready on every live node before the deadline
      """
    And within "20s" node "node-1" eventually reports describe resource as "- node-3 topology=alive state=ready"
      """
      DESCRIBE RESOURCE proto VERSION 1;
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
      published resource version 1
      """
    And within "10s" node "node-1" eventually reports describe resource as "cluster_ready: true"
      """
      DESCRIBE RESOURCE proto VERSION 1;
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
    And within "15s" node "node-1" eventually reports describe resource as "cluster_ready: true"
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
    When these NSPL commands are executed on node "node-1"
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE proto;
      UPLOAD RESOURCE proto VERSION '{{proto_dir}}';
      """
    Then within "10s" node "node-1" eventually reports describe resource as "cluster_ready: true"
      """
      DESCRIBE RESOURCE proto VERSION 1;
      """
    When node "node-3" is started
    Then within "20s" node "node-1" eventually reports describe resource as "- node-3 topology=alive state=ready"
      """
      DESCRIBE RESOURCE proto VERSION 1;
      """
    And within "20s" node "node-3" eventually reports describe resource as "cluster_ready: true"
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
      published resource version 1
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
