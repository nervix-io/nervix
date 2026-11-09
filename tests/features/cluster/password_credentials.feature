@password_credentials @deloxide_stress_restore
Feature: Bounded password credentials

  Scenario Outline: A committed password policy creates credentials that survive restart and restore
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed through the client on the leader node
      """
      ALTER CLUSTER SET PASSWORD_ITERATIONS = 210000;
      CREATE USER credential_user WITH PASSWORD 'created-password';
      """
    Then the last command output contains
      """
      created user 'credential_user'
      """
    When the client connects to the leader node as user "credential_user" with password "created-password"
    Then the last command output contains
      """
      raft.current_leader:
      """
    And user "credential_user" with password "created-password" is accepted by the HTTP and WebSocket listeners
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "cluster" from node "{{leader}}" into "credentials.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming no domain
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "cluster --on-existing-user skip" from "credentials.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded with users 1 created, 1 skipped and 0 replaced
    When the client connects to the leader node as user "credential_user" with password "created-password"
    Then the last command output contains
      """
      raft.current_leader:
      """
    And user "credential_user" with password "created-password" is accepted by the HTTP and WebSocket listeners
    When the client connects to the leader node as user "credential_user" with password "wrong-password"
    Then the last command error contains
      """
      authentication failed
      """
    And user "credential_user" with password "wrong-password" is refused by the HTTP and WebSocket listeners
    When the cluster is restarted
    And the client connects to the leader node as user "credential_user" with password "created-password"
    Then the last command output contains
      """
      raft.current_leader:
      """
    And user "credential_user" with password "created-password" is accepted by the HTTP and WebSocket listeners
    When the web console is opened as user "credential_user" with password "created-password"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
