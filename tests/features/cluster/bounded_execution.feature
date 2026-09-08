Feature: Bounded execution and transient memory

  Scenario Outline: Occupied bulk execution leaves management work responsive
    Given a <cluster_size> node nervix cluster is started
    When bulk execution on node "<node_id>" is occupied
    Then node "<node_id>" observability path "/readyz" eventually responds with 200 and "ready"
    And within "10s" these NSPL commands complete on node "<node_id>"
      """
      CREATE DOMAIN bounded_execution;
      """
    When bulk execution on node "<node_id>" is released
    Then node "<node_id>" eventually observes a stable leader

    Examples:
      | cluster_size | node_id |
      | 1            | node-1  |
      | 3            | node-2  |
