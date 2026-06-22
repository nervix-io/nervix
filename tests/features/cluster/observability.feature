Feature: Cluster observability

  Scenario Outline: Observability endpoints report node health and allocator metrics
    Given a <cluster_size> node nervix cluster is started
    Then node "<node_id>" observability path "/livez" eventually responds with 200 and "live"
    And node "<node_id>" observability path "/readyz" eventually responds with 200 and "ready"
    And node "<node_id>" observability path "/metrics" eventually responds with 200 and contains "nervix_jemalloc_active_bytes"

    Examples:
      | cluster_size | node_id |
      | 1            | node-1  |
      | 3            | node-1  |
      | 3            | node-2  |
      | 3            | node-3  |
