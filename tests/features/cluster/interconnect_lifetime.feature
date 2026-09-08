Feature: Interconnect connection lifetime

  Scenario: Peer churn and silent handshakes leave the node responsive
    Given a 3 node nervix cluster is started
    When node "node-3" is restarted 3 times with a new interconnect address
    Then node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    When a silent peer starts an interconnect handshake with node "node-3"
    And node "node-3" is stopped while timing shutdown
    Then the last cluster operation completes within "12s"
    And node "node-1" eventually observes a stable leader
