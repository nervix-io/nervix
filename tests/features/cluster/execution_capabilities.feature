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
      execution workers: class=data_cpu workers=3 pending_jobs=192
      """
    And node "node-1" observability metric "nervix_execution_queue_capacity" with labels eventually equals 192
      """
      nervix_execution_queue_capacity{class="data_cpu"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
