Feature: Cluster licensing release and transfer
  Release gives up licensed usage in committed state before asking the service to release capacity.

  Scenario Outline: A released cluster returns to the free policy on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      RELEASE LICENSE;
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      policy: free
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Release is committed while the service is unreachable and local rotation remains available on <cluster_size> nodes
    Given an HTTPS licensing test service has bounded offline allowance
    And a <cluster_size> node nervix cluster is started
    When a persistent license is activated through the running quorum
    And the licensing service becomes unreachable
    And these Plexus commands are executed on the leader node
      """
      RELEASE LICENSE;
      ROTATE LICENSE CLUSTER KEY;
      ROTATE LICENSE CLUSTER KEY;
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: released
      policy: releasing
      release: awaiting service confirmation
      """
    And node "node-1" eventually has cluster key generation 3
    When the cluster licensing clock passes the "valid_until" deadline
    And these Plexus commands are executed on the leader node
      """
      CREATE DOMAIN released_free_policy CLOCK UNPACED START '2026-11-06T00:00:00Z';
      """
    When the cluster is restarted
    And the licensing service becomes reachable
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      policy: free
      """
    And node "node-1" eventually has cluster key generation 3

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Release retains a previously granted transition on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When a persistent license is activated through the running quorum
    And these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN {{domain}}_second;
      """
    And the licensing service reduces the domain quota with a transition
    And these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      RELEASE LICENSE;
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: released
      release: confirmed
      policy: releasing
      """
    And the released cluster retains a grant until its signed deadline
    When the cluster licensing clock passes the "grants_until" deadline
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      policy: free
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: The former cluster observes transfer and a rebuilt cluster activates anew on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And the licensing service allows one persistent activation
    And a <cluster_size> node nervix cluster is started
    When a persistent license is activated through the running quorum
    And the licensing administrator transfers the activation
    And these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      status: transferred
      """
    When the transferred installation is rebuilt and activates anew
    Then the licensing service holds one active activation after transfer

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Expired ephemeral release is refused on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the committed license decision is saved as placeholder "before_expiry"
    When the cluster licensing clock passes the "ephemeral" deadline
    And these Plexus commands fail with "expired ephemeral activation cannot be released"
      """
      RELEASE LICENSE;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
