Feature: Web console connection status
  Scenario Outline: Web console reports a connected leader websocket session
    Given a <cluster_size> node nervix cluster is started
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Web console opened on a follower connects to the leader
    Given a 3 node nervix cluster is started
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "follower"
    When the web console is opened on node "{{follower}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".terminal" contains "connected to leader '{{leader}}'"

  Scenario: Web console reconnects after leader switchover
    Given a 3 node nervix cluster is started
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    When the web console is opened on node "{{old_leader}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".terminal" contains "connected to leader '{{old_leader}}'"
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{old_leader}}" eventually reports leader "{{new_leader}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".terminal" contains "connected to leader '{{new_leader}}'"

  Scenario Outline: A server with an active web console session fully terminates before restart
    Given a <cluster_size> node nervix cluster is started
    Then the current leader node is saved as placeholder "stopped_node"
    When the web console is opened on node "{{stopped_node}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When node "{{stopped_node}}" is stopped
    And node "{{stopped_node}}" is started
    Then node "{{stopped_node}}" eventually observes a stable leader

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
