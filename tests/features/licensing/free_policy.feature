Feature: Embedded free licensing policy
  A cluster without an activation derives its permissions from the embedded free policy: every
  capability is disabled, licensing limits no base feature, and the policy is observable on every
  node.

  Scenario: A follower describes its committed license without a selected domain
    Given a 3 node nervix cluster is started
    Then the current leader node is saved as placeholder "licensing_leader"
    And a node other than placeholder "licensing_leader" is saved as placeholder "licensing_follower"
    And node "{{licensing_follower}}" eventually describes this license without a selected domain
      """
      mode: free
      policy: free
      capability licensing.dummy: disabled
      """
    And the last license inspection has typed cluster scope

  Scenario Outline: The free policy is published without a selected domain on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      policy: free
      processing: running
      conditions not understood: 0
      capability licensing.dummy: disabled
      """
    And the last license inspection has typed cluster scope
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN {{domain}}_second;
      """
    And the cluster is restarted
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      policy: free
      processing: running
      """
    And the licensing service has received no requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
