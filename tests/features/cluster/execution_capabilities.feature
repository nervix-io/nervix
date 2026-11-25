Feature: Capability-derived execution capacity
  A node sizes its independent execution classes and transient memory from its resolved CPU and
  memory capabilities. Explicit capacities pass the same validation as derived defaults.

  Scenario Outline: A one-GiB node starts with execution capacity derived from its claim on a cluster of <cluster_size>
    Given node-1 starts with cpu-count = 8
    And node-1 starts with memory-limit = 1GiB
    And a <cluster_size> node nervix cluster is started
    Then node "node-1" eventually observes a stable leader
    And node "node-<cluster_size>" eventually describes node "node-1" with CPU count 8 and memory "1GiB"
    And node "node-1" observability metric "nervix_execution_workers" with labels eventually equals 2
      """
      nervix_execution_workers{class="data_cpu"}
      """

    And node "node-1" observability metric "nervix_execution_memory_capacity_bytes" with labels eventually equals 67108864
      """
      nervix_execution_memory_capacity_bytes{class="relay"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An explicit execution worker capacity is used by the running node on a cluster of <cluster_size>
    Given node-1 starts with cpu-count = 8
    And node-1 starts with execution-data-cpu-workers = 3
    And a <cluster_size> node nervix cluster is started
    Then node "node-1" eventually observes a stable leader
    And node "node-1" observability metric "nervix_execution_workers" with labels eventually equals 3
      """
      nervix_execution_workers{class="data_cpu"}
      """
    When these Plexus commands are executed through the client on node "node-1"
      """
      DESCRIBE CLUSTER;
      """
    Then the last command output contains
      """
      execution workers: class=data_cpu workers=3
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A node given a management memory budget below one consensus vote refuses to start on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And node "node-<cluster_size>" is stopped
    And node-<cluster_size> starts with execution-management-memory-bytes = 128KiB
    When node "node-<cluster_size>" attempts to start
    Then node "node-<cluster_size>" refused to start because "the management memory budget of 131072 bytes cannot hold its largest consensus vote or metadata write of 278528 bytes"
    Given node-<cluster_size> starts with execution-management-memory-bytes = 272KiB
    When node "node-<cluster_size>" is started
    Then node "node-<cluster_size>" eventually observes a stable leader
    And node "node-<cluster_size>" observability metric "nervix_execution_memory_capacity_bytes" with labels eventually equals 278528
      """
      nervix_execution_memory_capacity_bytes{class="management"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
