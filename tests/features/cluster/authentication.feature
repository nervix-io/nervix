Feature: Authentication

  Scenario Outline: Client authentication requires a valid user password
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE USER auth_user WITH PASSWORD 'created-password';
      """
    Then the last command output contains
      """
      created user 'auth_user'
      """
    When the client connects to the leader node as user "auth_user" with password "created-password"
    Then the last command output contains
      """
      raft.current_leader:
      """
    When the client connects to the leader node as user "auth_user" with password "wrong-password"
    Then the last command error contains
      """
      authentication failed
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Web console rejects an invalid password
    Given a 1 node nervix cluster is started
    When the web console is opened on the leader node with password "wrong-password"
    Then selector ".auth-panel" contains "Authentication failed"

  Scenario: Web console accepts the configured password
    Given a 1 node nervix cluster is started
    When the web console is opened on the leader node
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"

  Scenario Outline: Client authentication applies async backoff to repeated attempts
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE USER throttled_user WITH PASSWORD 'created-password';
      """
    And the client attempts to connect to the leader node as user "throttled_user" with password "wrong-password" 18 times
    Then the last command error contains
      """
      authentication failed
      """
    And the last authentication attempts take at least "500ms"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
