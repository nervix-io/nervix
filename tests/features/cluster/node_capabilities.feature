Feature: Committed node capabilities
  Every joined node announces what it can carry through consensus. Placement and peer delivery
  use that committed claim, and a restart replaces the claim for the same node.

  Scenario Outline: Joined nodes describe their committed detected capabilities on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    Then node "node-1" eventually describes committed capabilities for node "node-<cluster_size>"
    And node "node-<cluster_size>" eventually describes committed capabilities for node "node-1"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Restarting a node replaces its committed CPU and memory claim on a cluster of <cluster_size>
    Given node-1 starts with cpu-count = 2
    And node-1 starts with memory-limit = 8GiB
    And a <cluster_size> node nervix cluster is started
    Then node "node-<cluster_size>" eventually describes node "node-1" with CPU count 2 and memory "8GiB"
    When node "node-1" is stopped
    Given node-1 starts with cpu-count = 4
    And node-1 starts with memory-limit = 16GiB
    When node "node-1" is started
    Then node "node-<cluster_size>" eventually describes node "node-1" with CPU count 4 and memory "16GiB"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A departed node keeps its last committed capabilities until it is dropped
    Given node-3 starts with cpu-count = 3
    And node-3 starts with memory-limit = 8GiB
    And a 3 node nervix cluster is started
    Then node "node-1" eventually describes node "node-3" with CPU count 3 and memory "8GiB"
    When node "node-3" is stopped
    Then node "node-1" eventually describes node "node-3" with CPU count 3 and memory "8GiB"

  Scenario: Placement gives a node twice the CPU and memory twice the independent partitions
    Given the production sticky scheduler is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And node-1 starts with cpu-count = 2
    And node-1 starts with memory-limit = 8GiB
    And node-2 starts with cpu-count = 4
    And node-2 starts with memory-limit = 16GiB
    And node-3 starts with cpu-count = 2
    And node-3 starts with memory-limit = 8GiB
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA partition_key (key I64);
      CREATE SCHEMA event (value I64);
      CREATE BRANCH by_capacity SCHEMA partition_key TTL 5m PARTITIONS 32;
      CREATE RELAY events SCHEMA event BRANCHED BY by_capacity;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 32 partitions of relay "events"
    And the last cluster status places 8 partitions of relay "events" on node "node-1"
    And the last cluster status places 16 partitions of relay "events" on node "node-2"
    And the last cluster status places 8 partitions of relay "events" on node "node-3"
