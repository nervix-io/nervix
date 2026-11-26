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
    # Keep the retry delay inside the scenario's 40-second observation window after repeated failures.
    When these Plexus commands are executed on the leader node
      """
      ALTER CLUSTER SET LICENSING_RETRY_BACKOFF_MAX = 2s;
      """
    And a persistent license is activated through the running quorum
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
      CREATE UNPACED DOMAIN released_free_policy;
      """
    When the cluster is restarted
    # Reproduce the two transport failures after restart that exposed the timing bug.
    Then the current leader node is saved as placeholder "release_leader"
    And node "{{release_leader}}" observability metric "nervix_licensing_requests_total" with labels eventually reaches at least 2
      """
      operation="activation.release"
      outcome="unreachable"
      """
    When the licensing service clock catches up with the cluster
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

  Scenario Outline: Release returns after its local commit while an automatic refresh retries an unreachable service on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    # Keep the refresh retrying every few seconds, and the leader's pause after an ended attempt
    # short, so the release's confirmation follows the reconnection within the observation window.
    When these Plexus commands are executed on the leader node
      """
      ALTER CLUSTER SET LICENSING_RETRY_BACKOFF_MAX = 2s, LICENSING_REQUEST_TIMEOUT = 5s;
      """
    And a persistent license is activated through the running quorum
    And the licensing service becomes unreachable
    And the cluster licensing clock passes the "refresh_after" deadline
    Then the current leader node is saved as placeholder "refresh_leader"
    And node "{{refresh_leader}}" observability metric "nervix_licensing_requests_total" with labels eventually reaches at least 2
      """
      operation="activation.refresh"
      outcome="unreachable"
      """
    When these Plexus commands begin executing in the background as "release"
      """
      RELEASE LICENSE;
      """
    Then within "30s" background Plexus execution "release" reaches a successful outcome
    And node "node-1" eventually describes this license without a selected domain
      """
      mode: released
      policy: releasing
      release: awaiting service confirmation
      """
    # The refresh ended with the activation it was for, so a local rotation no longer waits for it.
    When these Plexus commands begin executing in the background as "rotation"
      """
      ROTATE LICENSE CLUSTER KEY;
      """
    Then within "30s" background Plexus execution "rotation" reaches a successful outcome
    And node "node-1" eventually has cluster key generation 2
    When the licensing service clock catches up with the cluster
    And the licensing service becomes reachable
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      policy: free
      """
    And node "node-1" eventually has cluster key generation 2
    And node "{{refresh_leader}}" observability metric "nervix_licensing_requests_total" with labels eventually equals 0
      """
      operation="activation.refresh"
      outcome="rejected_by_cluster"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A release ends an operator's refresh that waits for an unreachable service on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ALTER CLUSTER SET LICENSING_RETRY_BACKOFF_MAX = 2s, LICENSING_REQUEST_TIMEOUT = 5s;
      """
    And a persistent license is activated through the running quorum
    And the licensing service becomes unreachable
    And these Plexus commands begin executing in the background as "refresh"
      """
      REFRESH LICENSE;
      """
    Then the current leader node is saved as placeholder "refresh_leader"
    And node "{{refresh_leader}}" observability metric "nervix_licensing_requests_total" with labels eventually reaches at least 1
      """
      operation="activation.refresh"
      outcome="unreachable"
      """
    When these Plexus commands begin executing in the background as "release"
      """
      RELEASE LICENSE;
      """
    Then within "30s" background Plexus execution "release" reaches a successful outcome
    And within "30s" background Plexus execution "refresh" fails with "the cluster no longer holds the activation this refresh was for"
    When the licensing service becomes reachable
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      policy: free
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A release waits for a key rotation still pending and commits once the rotation finishes on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ALTER CLUSTER SET LICENSING_RETRY_BACKOFF_MAX = 2s, LICENSING_REQUEST_TIMEOUT = 5s;
      """
    And a persistent license is activated through the running quorum
    And the licensing service becomes unreachable
    And these Plexus commands begin executing in the background as "rotation"
      """
      ROTATE LICENSE CLUSTER KEY;
      """
    Then the current leader node is saved as placeholder "rotation_leader"
    And node "{{rotation_leader}}" observability metric "nervix_licensing_requests_total" with labels eventually reaches at least 1
      """
      operation="activation.rotate_key"
      outcome="unreachable"
      """
    When these Plexus commands begin executing in the background as "release"
      """
      RELEASE LICENSE;
      """
    # The pending rotation finishes with the service first, so the release cannot commit while
    # the service stays unreachable.
    Then background Plexus execution "release" is still running after "5s"
    When the licensing service becomes reachable
    Then within "30s" background Plexus execution "rotation" reaches a successful outcome
    And within "30s" background Plexus execution "release" reaches a successful outcome
    And node "node-1" eventually has cluster key generation 2
    And node "node-1" eventually describes this license without a selected domain
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
