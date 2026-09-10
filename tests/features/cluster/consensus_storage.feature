Feature: Durable consensus storage

  Scenario Outline: Committed administrative changes recover atomically after storage failure
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE DOMAIN durable_preceding;
      """
    And consensus storage on the leader fails <boundary> committing domain "durable_change"
    When these NSPL commands fail with "consensus storage"
      """
      CREATE DOMAIN durable_change;
      """
    Then the storage-failed node has no published domain "durable_change"
    When the cluster is restarted
    And these NSPL commands are executed on the leader node
      """
      USE durable_preceding;
      USE durable_change;
      """
    Then the last command output contains
      """
      durable_change
      """

    Examples:
      | cluster_size | boundary |
      | 1            | before   |
      | 3            | before   |
      | 1            | after    |
      | 3            | after    |
