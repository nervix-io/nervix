Feature: Free licensing observability
  Free clusters expose the current policy and zero-valued request and transition series without an
  activation.

  Scenario Outline: The free state series are initialized on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    Then node "node-1" observability metric "nervix_licensing_state" with labels eventually equals 1
      """
      state="free"
      """
    And node "node-1" observability metric "nervix_licensing_state" with labels eventually equals 0
      """
      state="licensed_fresh"
      """
    And the current leader node is saved as placeholder "licensing_leader"
    And node "{{licensing_leader}}" licensing metrics match the free policy

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
