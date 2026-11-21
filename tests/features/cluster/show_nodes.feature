Feature: Previewing node selectors
  An operator can inspect the cluster's committed node records and see which
  nodes a selector would match from a session on any node.

  Scenario Outline: Preview the committed node records on a cluster of <cluster_size>
    Given node-1 starts with node-tags = zone=eu
    And node-1 reports gpu.count = 2
    And node-1 reports onnx.cuda = true
    And a <cluster_size> node nervix cluster is started
    Then every node eventually describes node "node-1" with tags "zone=eu"
    And every node eventually describes node "node-<cluster_size>" with property "cpu.model" as "Scenario CPU"
    When this Plexus command is sent without a domain to a follower or the only node
      """
      SHOW NODES FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /nodes/0/name = "node-1"
      /nodes/0/live = true
      /nodes/0/voter = true
      /nodes/0/cordoned = false
      /nodes/0/terminating = false
      /nodes/0/record/tags/zone = "eu"
      /nodes/0/record/properties/cpu_model/Value = "Scenario CPU"
      /nodes/<last_index>/name = "node-<cluster_size>"
      """
    And the typed node inspection lists "<all_names>"
    When these Plexus commands are executed through the client on node "node-<cluster_size>"
      """
      SHOW NODES WHERE tag('zone') = 'eu';
      """
    Then the typed node inspection lists "node-1"
    When these Plexus commands are executed through the client on node "node-<cluster_size>"
      """
      SHOW NODES WHERE tag('zone') = 'eu' AND node.cuda.available FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /nodes/0/name = "node-1"
      """
    And the typed node inspection lists "node-1"
    When these Plexus commands are executed through the client on node "node-<cluster_size>"
      """
      SHOW NODES WHERE node.cuda.available;
      """
    Then the typed node inspection lists "node-1"
    When these Plexus commands are executed through the client on node "node-<cluster_size>"
      """
      SHOW NODES WHERE tag('missing') = 'eu' FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /nodes = []
      """
    And the typed node inspection lists ""
    When these Plexus commands are executed through the client on node "node-<cluster_size>"
      """
      SHOW NODES WHERE (tag('zone') AS I64) = 1 OR node.name = 'node-3';
      """
    Then the typed node inspection lists "<matches_with_error>"
    And the last client request diagnostic message contains "node selector failed on 'node-1'"
    When the CLI successfully executes this JSON inspection
      """
      SHOW NODES WHERE node.name IN ('node-1', 'node-3') FORMAT JSON;
      """
    Then the last command output is a JSON document where
      """
      /nodes/0/name = "node-1"
      /nodes/<last_match>/name = "node-<cluster_size>"
      """

    Examples:
      | cluster_size | last_index | last_match | all_names            | matches_with_error |
      | 1            | 0          | 0          | node-1               |                    |
      | 3            | 2          | 1          | node-1,node-2,node-3 | node-3             |

  Scenario Outline: Refuse an invalid selector on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands fail through the client on node "node-1" with "node.disk.free"
      """
      SHOW NODES WHERE node.disk.free;
      """
    And these Plexus commands fail through the client on node "node-1" with "unknown function 'zone_of'"
      """
      SHOW NODES WHERE zone_of('node-1') = 'eu';
      """
    And these Plexus commands fail through the client on node "node-1" with "WHERE expression must evaluate to Boolean"
      """
      SHOW NODES WHERE node.name;
      """
    And these Plexus commands fail through the client on node "node-1" with "tag() requires a literal STRING key"
      """
      SHOW NODES WHERE tag(node.name) = 'eu';
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
