Feature: Licensing transitions and grace
  Signed reductions protect existing operation for committed, receipt-anchored grace without admitting growth.

  Scenario Outline: A signed downgrade protects existing usage without admitting growth on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When a persistent license is activated through the running quorum
    And these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN {{domain}}_second;
      """
    And the licensing service sets the domain quota to 1 with 300 seconds of grace
    And these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      transitions active: 1
      """
    When these Plexus commands fail with "within_grace"
      """
      CREATE UNPACED DOMAIN {{domain}}_third;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A reduction above usage applies immediately on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When a persistent license is activated through the running quorum
    And these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the licensing service sets the domain quota to 4 with 300 seconds of grace
    And these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      transitions active: 0
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}}_second;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An upgrade ends an unnecessary grant immediately on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When a persistent license is activated through the running quorum
    And these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN {{domain}}_second;
      """
    And the licensing service sets the domain quota to 1 with 300 seconds of grace
    And these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      transitions active: 1
      """
    When the licensing service sets the domain quota to 4 with 300 seconds of grace
    And these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      transitions active: 0
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}}_third;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Acknowledgements and extensions preserve the original start through recovery on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When a persistent license is activated through the running quorum
    And these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN {{domain}}_second;
      """
    And the licensing service sets the domain quota to 1 with 3600 seconds of grace
    And the next licensing response is delayed by two seconds
    And these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    Then the committed licensing transition is saved as placeholder "original_grant"
    And the transition begins at the decision receipt after delayed delivery
    When these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    Then the licensing service acknowledges transition placeholder "original_grant"
    When the cluster is restarted
    And licensing leadership moves to another node when one exists
    Then the committed licensing transition matches placeholder "original_grant"
    When the licensing administrator extends transitions by 600 seconds
    And these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    Then the licensing transition extends placeholder "original_grant" without restarting

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Expired grace reports excess usage and leaves domains intact on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When a persistent license is activated through the running quorum
    And these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN {{domain}}_second;
      """
    And a native session begins observing licensing transition notices
    And the licensing service sets the domain quota to 1 with 300 seconds of grace
    And these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    Then a licensing Warning notice saying "transition_started" is observed
    And node "node-1" eventually describes this license without a selected domain
      """
      transitions active: 1
      """
    And the current leader node is saved as placeholder "licensing_leader"
    And node "{{licensing_leader}}" observability metric "nervix_licensing_over_limit" with labels eventually equals 0
      """
      condition="cluster.domains"
      """
    When the licensing service becomes unreachable
    And the cluster licensing clock passes the "transition" deadline
    Then node "node-1" eventually describes this license without a selected domain
      """
      transitions active: 0
      state: over_limit
      quota cluster.domains: 2 of 1
      processing: running
      """
    And node "{{licensing_leader}}" observability metric "nervix_licensing_over_limit" with labels eventually equals 1
      """
      condition="cluster.domains"
      """
    And a licensing Warning notice saying "over_limit" is observed
    When these Plexus commands fail with "over_limit"
      """
      CREATE UNPACED DOMAIN {{domain}}_third;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: The term tick returns to the actual unlimited free policy on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When a persistent license is activated through the running quorum
    And these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN {{domain}}_second;
      """
    And the licensing service sets the domain quota to 1 with 300 seconds of grace
    And these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    And the licensing service becomes unreachable
    And the cluster licensing clock passes the "term" deadline
    Then node "node-1" eventually describes this license without a selected domain
      """
      quota cluster.domains: 2 of unlimited
      transitions active: 0
      processing: running
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}}_third;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Restore cannot expand the existing allowance on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When a persistent license is activated through the running quorum
    And these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN {{domain}}_second;
      """
    And the licensing service sets the domain quota to 1 with 3600 seconds of grace
    And these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    Then the current leader node is saved as placeholder "licensing_leader"
    When the CLI backs up "domain {{domain}}" from node "{{licensing_leader}}" into "transition.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI restores "domain {{domain}} --as {{domain}}_copy" from "transition.nvxb" on node "{{licensing_leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_INCOMPLETE" and a message containing "within_grace"
    And node "node-1" eventually describes this license without a selected domain
      """
      quota cluster.domains: 2 of 1
      transitions active: 1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A protected member may rejoin during grace while a newcomer is refused
    Given an HTTPS licensing test service is available
    And a 3 node nervix cluster is started
    When a persistent license is activated through the running quorum
    And the licensing service sets the node quota to 1 with 3600 seconds of grace
    And these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    Then the current leader node is saved as placeholder "licensing_leader"
    And a node other than placeholder "licensing_leader" is saved as placeholder "recovering_node"
    When node "{{recovering_node}}" is stopped
    Then node "{{licensing_leader}}" eventually reports status containing "raft member '{{recovering_node}}' is marked unavailable"
    When these Plexus commands are executed through the client on node "{{licensing_leader}}"
      """
      DROP NODE {{recovering_node}};
      """
    When node "{{recovering_node}}" attempts to rejoin
    Then node "{{recovering_node}}" joined the cluster
    When node "node-4" attempts to join
    Then node "node-4" refused to join because "within_grace"
    And node "{{licensing_leader}}" eventually describes this license without a selected domain
      """
      quota cluster.nodes: 3 of 1
      transitions active: 1
      """

  Scenario Outline: Ephemeral expiry caps grants before and after acknowledgement on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN {{domain}}_second;
      """
    And the licensing service sets the domain quota to 1 with 3600 seconds of grace
    And these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    And the licensing transition is "<acknowledgement>"
    Then node "node-1" eventually describes this license without a selected domain
      """
      transitions active: 1
      acknowledged: <acknowledged>
      """
    When the licensing service becomes unreachable
    And the cluster licensing clock passes the "ephemeral" deadline
    Then node "node-1" eventually describes this license without a selected domain
      """
      transitions active: 0
      quota cluster.domains: 2 of 1
      """
    And creating another domain cannot use a grant after activation expiry

    Examples:
      | cluster_size | acknowledgement | acknowledged |
      | 1            | pending         | false        |
      | 3            | pending         | false        |
      | 1            | acknowledged    | true         |
      | 3            | acknowledged    | true         |
