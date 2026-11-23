Feature: Verification of licensing decisions
  A response grants nothing until both the coordinator and Raft validate its signed document.

  Scenario Outline: A sync reporting an in-flight decision does not add a refresh on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And the licensing notification stream is "blocked"
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      ALTER CLUSTER SET LICENSING_RETRY_BACKOFF_MAX = 1s;
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the licensing service holds exactly one authenticated notification stream
    Given the next licensing response is held at the service
    When this Plexus command request begins executing in the background on the leader node
      """
      REFRESH LICENSE;
      """
    Then the licensing refresh response is held after its decision is issued
    When the licensing notification stream is "normal"
    Then the licensing service holds exactly one authenticated notification stream
    When the licensing notification stream is disconnected
    Then the licensing notification reconnect uses a fresh ticket and its last event identifier
    When the held licensing response is released
    Then the background command request succeeds
    And the committed license decision is saved as placeholder "held_decision"
    Given the next licensing response has "earlier_revision" tampering
    When an explicit license refresh is refused by decision verification
    Then the committed license decision matches placeholder "held_decision"
    And only the held refresh and the explicit replay reached the licensing service
    And the licensing request outcome "rejected_by_cluster" was reported

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A newer change received while a decision is in flight still refreshes on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And the licensing notification stream is "blocked"
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      ALTER CLUSTER SET LICENSING_RETRY_BACKOFF_MAX = 1s;
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the licensing service holds exactly one authenticated notification stream
    Given the next licensing response is held at the service
    When this Plexus command request begins executing in the background on the leader node
      """
      REFRESH LICENSE;
      """
    Then the licensing refresh response is held after its decision is issued
    When the licensing service changes the domain quota to 12
    And the licensing notification stream is "normal"
    Then the licensing service holds exactly one authenticated notification stream
    When the licensing notification stream is disconnected
    Then the licensing notification reconnect uses a fresh ticket and its last event identifier
    When the held licensing response is released
    Then the background command request succeeds
    And node "node-1" eventually describes this license without a selected domain
      """
      quota cluster.domains: 1 of 12
      verification: verified
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Reapplying the current decision is a no-op on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When a persistent license is activated with a stable reference
    Then the committed license decision is saved as placeholder "current_decision"
    When the license activation with the same reference is repeated
    Then the committed license decision matches placeholder "current_decision"
    And the repeated activation recovers its outcome without a service request

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Replaying an earlier valid decision is refused on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      REFRESH LICENSE;
      """
    Then the committed license decision is saved as placeholder "newest_decision"
    Given the next licensing response has "earlier_revision" tampering
    When an explicit license refresh is refused by decision verification
    Then the committed license decision matches placeholder "newest_decision"
    And the licensing request outcome "rejected_by_cluster" was reported

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A <tamper> response keeps the committed decision on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    # The stream's first sync goes out before the tampered refresh, so it reports the saved
    # decision. A sync sent after that refresh reports the decision the service issued for it,
    # and the cluster then rightly moves to the service's next decision (see the next outline).
    Then the licensing service holds exactly one authenticated notification stream
    And the committed license decision is saved as placeholder "accepted_license"
    Given the next licensing response has "<tamper>" tampering
    When an explicit license refresh is refused by decision verification
    Then the committed license decision matches placeholder "accepted_license"
    And the licensing request outcome "rejected_by_cluster" was reported

    Examples:
      | cluster_size | tamper           |
      | 1            | signature        |
      | 3            | signature        |
      | 1            | unauthorized_key |
      | 3            | unauthorized_key |
      | 1            | cluster          |
      | 3            | cluster          |
      | 1            | key              |
      | 3            | key              |
      | 1            | generation       |
      | 3            | generation       |
      | 1            | activation       |
      | 3            | activation       |
      | 1            | revision         |
      | 3            | revision         |
      | 1            | timing           |
      | 3            | timing           |
      | 1            | service          |
      | 3            | service          |
      | 1            | request          |
      | 3            | request          |

  Scenario Outline: A sync after a refused <tamper> response moves the cluster to the service's next decision on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And the licensing notification stream is "blocked"
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the licensing service holds exactly one authenticated notification stream
    And the committed license decision is saved as placeholder "accepted_license"
    Given the next licensing response has "<tamper>" tampering
    When an explicit license refresh is refused by decision verification
    And the licensing notification stream is "normal"
    Then the committed license decision is newer than placeholder "accepted_license"
    And the committed license decision is the licensing service's current decision
    And the licensing stream's first sync reported a newer decision than placeholder "accepted_license"
    And the licensing request outcome "rejected_by_cluster" was reported

    Examples:
      | cluster_size | tamper    |
      | 1            | signature |
      | 3            | signature |
      | 1            | revision  |
      | 3            | revision  |

  Scenario Outline: A revoked-key response retains the decision and marks its signing key on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When a persistent license is activated through the running quorum
    Then the committed license decision is saved as placeholder "accepted_license"
    Given the next licensing response has "revoked_key" tampering
    When an explicit license refresh is refused by decision verification
    Then the committed license decision matches placeholder "accepted_license" with a revoked signing key
    And the licensing request outcome "rejected_by_cluster" was reported

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # An activation's decision is checked by the node that received it, before it is proposed; a
  # refresh's decision is checked when the cluster applies it. Both refusals name the reason once.
  Scenario Outline: A <tamper> decision is refused with its reason named once on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    And the next licensing response has "<tamper>" tampering
    When these Plexus commands fail with "licensing decision was rejected: <reason>"
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the last command error message contains "<reason>" exactly once
    And node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      policy: free
      """
    And the licensing request outcome "rejected_by_cluster" was reported
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the licensing service holds exactly one authenticated notification stream
    Given the next licensing response has "<tamper>" tampering
    When an explicit license refresh is refused by decision verification
    Then the last command output contains
      """
      licensing decision was rejected: <reason>
      """

    Examples:
      | cluster_size | tamper    | reason                                                    |
      | 1            | signature | the decision envelope or signing authorization is invalid |
      | 3            | signature | the decision envelope or signing authorization is invalid |
      | 1            | service   | the decision names a different licensing service          |
      | 3            | service   | the decision names a different licensing service          |
