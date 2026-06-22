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

  Scenario: Rust client batches can upload local resource directories
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "schema/root.proto": "syntax = \"proto3\";",
        "schema/types/common.proto": "message Common {}"
      }
      """
    When these NSPL commands are executed as one batch through the client on the leader node
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE proto;
      UPLOAD RESOURCE proto VERSION '{{proto_dir}}';
      DESCRIBE RESOURCE proto VERSION 1;
      """
    Then the last command output contains
      """
      uploaded resource version 1
      """
    And the last command output contains
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
      uploaded resource version 1
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
      uploaded resource version 1
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
