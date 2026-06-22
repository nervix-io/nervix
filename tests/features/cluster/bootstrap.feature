Feature: Cluster bootstrap

  Scenario: Bootstrap node forms the initial cluster
    Given a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually reports leader "node-1"
    And node "node-2" eventually reports leader "node-1"
    And node "node-3" eventually reports leader "node-1"
    And node "node-1" eventually reports raft voters "node-1,node-2,node-3"
