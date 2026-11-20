Feature: Publish a prepared failover when eligible nodes stay the same

  Background:
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "owner"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA recovery_event ( id I64 );
      CREATE RELAY recovered_events SCHEMA recovery_event UNBRANCHED;
      START;
      RELOCATE RELAY recovered_events ONTO NODE {{owner}} IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=relay name=recovered_events owner={{owner}}
      """

  Scenario: Retiring the stopped owner's probe target does not repeat recovery preparation
    Given the first forced recovery publication is paused
    When application health probes from node "{{leader}}" to node "{{owner}}" fail
    Then the first forced recovery publication reaches its pause
    And node "{{leader}}" reports interconnect to "{{owner}}" as "unavailable"
    When node "{{owner}}" is stopped
    Then node "{{leader}}" eventually stops probing "{{owner}}"
    When the first forced recovery publication is released
    Then within "30s" node "{{leader}}" reports no scheduled work on "{{owner}}"
    And 1 forced recovery preparation round reached publication

  Scenario: A recovered owner keeps its work and the destination discards the rejected preparation
    When these Plexus commands are executed on the leader node
      """
      CORDON NODE {{leader}};
      """
    Given the first forced recovery publication is paused
    When application health probes from node "{{leader}}" to node "{{owner}}" fail
    Then the first forced recovery publication reaches its pause
    When application health probes from node "{{leader}}" to node "{{owner}}" are restored
    Then node "{{leader}}" eventually reports interconnect to "{{owner}}" as "connected"
    When the first forced recovery publication is released
    Then the destination discards the rejected forced recovery preparation
    And within "10s" node "{{leader}}" eventually reports scheduled "relay" "recovered_events" owner equals placeholder "owner"
