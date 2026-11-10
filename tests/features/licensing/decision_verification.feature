Feature: Verification of licensing decisions
  A response grants nothing until both the coordinator and Raft validate its signed document.

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
    Then the committed license decision is saved as placeholder "accepted_license"
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
      | 1            | revoked_key      |
      | 3            | revoked_key      |
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
