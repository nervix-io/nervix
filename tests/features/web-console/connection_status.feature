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

  Scenario: Web console keeps the session during a server outage
    Given a 1 node nervix cluster is started
    Then the current leader node is saved as placeholder "stopped_node"
    When the web console is opened on node "{{stopped_node}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When node "{{stopped_node}}" is stopped
    Then selector ".topbar-status .pill.waiting" contains "WAITING"
    And selector ".auth-panel" does not contain "Connect"

  Scenario: Web console asks for credentials after authentication fails
    Given a 1 node nervix cluster is started
    When the web console is opened on the leader node with password "incorrect_password"
    Then selector ".auth-panel" contains "Connect"
    And selector ".auth-error" contains "Authentication failed"

  Scenario: Web console keeps a command submitted immediately after authentication
    Given a 1 node nervix cluster is started
    When the web console is opened on the leader node with password "incorrect_password"
    Then selector ".auth-error" contains "Authentication failed"
    When selector ".auth-password" is filled with "nervix-test-password"
    And selector ".auth-submit" is clicked
    And selector ".prompt-row input" is filled with "CREATE DOMAIN after_login;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".terminal" contains "created domain 'after_login'"

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
