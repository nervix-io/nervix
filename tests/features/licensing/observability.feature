Feature: Free licensing observability
  Free clusters expose the current policy and zero-valued admission series without an activation.

  Scenario Outline: The free state and cluster quota series are initialized on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    Then node "node-1" observability metric "nervix_licensing_state" with labels eventually equals 1
      """
      state="free"
      """
    And node "node-1" observability metric "nervix_licensing_state" with labels eventually equals 0
      """
      state="licensed_fresh"
      """
    And node "node-1" observability metric "nervix_licensing_over_limit" with labels eventually equals 0
      """
      condition="cluster.nodes"
      """
    And node "node-1" observability metric "nervix_licensing_over_limit" with labels eventually equals 0
      """
      condition="cluster.domains"
      """
    And node "node-1" observability metric "nervix_licensing_admissions_refused_total" with labels eventually equals 0
      """
      condition="cluster.domains"
      state="over_limit"
      """
    And the current leader node is saved as placeholder "licensing_leader"
    And node "{{licensing_leader}}" licensing metrics match the free policy

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
