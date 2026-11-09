Feature: The node record
  Every node commits one record through consensus: its capacity claim, the properties it detected
  at startup and the tags its operator provisioned it with. DESCRIBE CLUSTER prints every node's
  record on every node, a restart replaces it, and DROP NODE removes it.

  Scenario Outline: A node's tags appear in its record on every node of a cluster of <cluster_size>
    Given node-1 starts with node-tags = zone=eu,pool=gpu
    And a <cluster_size> node nervix cluster is started
    Then every node eventually describes node "node-1" with tags "pool=gpu,zone=eu"
    And every node eventually describes node "node-<cluster_size>" with property "cpu.model" as "Scenario CPU"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A node without tags describes none on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    Then every node eventually describes node "node-<cluster_size>" with tags "none"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A GPU and a registered CUDA provider make CUDA available on a cluster of <cluster_size>
    Given node-1 reports gpu.count = 2
    And node-1 reports onnx.cuda = true
    And a <cluster_size> node nervix cluster is started
    Then every node eventually describes node "node-1" with property "gpu.count" as "2"
    And every node eventually describes node "node-1" with property "onnx.cuda" as "true"
    And every node eventually describes node "node-1" with property "cuda.available" as "true"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A GPU without the CUDA provider leaves CUDA unavailable on a cluster of <cluster_size>
    Given node-1 reports gpu.count = 1
    And node-1 reports onnx.cuda = false
    And a <cluster_size> node nervix cluster is started
    Then every node eventually describes node "node-1" with property "gpu.count" as "1"
    And every node eventually describes node "node-1" with property "onnx.cuda" as "false"
    And every node eventually describes node "node-1" with property "cuda.available" as "false"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Restarting a node with other tags replaces its record on a cluster of <cluster_size>
    Given node-1 starts with node-tags = pool=gpu,zone=eu
    And a <cluster_size> node nervix cluster is started
    Then every node eventually describes node "node-1" with tags "pool=gpu,zone=eu"
    When node "node-1" is stopped
    Given node-1 starts with node-tags = pool=cpu,rack=r7
    When node "node-1" is started
    Then every node eventually describes node "node-1" with tags "pool=cpu,rack=r7"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Dropping a departed node removes its record
    Given node-3 starts with node-tags = pool=edge
    And a 3 node nervix cluster is started
    Then node "node-1" eventually describes node "node-3" with tags "pool=edge"
    When node "node-3" is stopped
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually reports status containing "raft member 'node-3' is marked unavailable"
    And node "node-1" eventually describes node "node-3" with tags "pool=edge"
    When these Plexus commands are executed through the client on node "node-1"
      """
      DROP NODE node-3;
      """
    Then node "node-1" eventually describes no record of node "node-3"

  Scenario: A server process refuses a tag key outside the key alphabet and names it
    When a nervix-server process is started with command-line option "--node-tags" set to "Pool=gpu"
    Then the server process exits with status 1
    And the server process log contains "invalid --node-tags"
    And the server process log contains "node tag key 'Pool' must start with a lower-case letter or a digit"
    And the server process log does not contain "panicked"
