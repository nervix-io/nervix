Feature: Cluster licensing key rotation
  Rotation changes the signing key without replacing the cluster or restarting a signed deadline.

  Scenario Outline: An unactivated cluster rotates without provisioned service trust on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ROTATE LICENSE CLUSTER KEY;
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      policy: free
      """
    And node "node-1" eventually has cluster key generation 2

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A free cluster rotates its key without contacting the service on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ROTATE LICENSE CLUSTER KEY;
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      policy: free
      """
    And the licensing service has received no requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An activated cluster rotates and refreshes with its new key on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the committed license decision is saved as placeholder "before_rotation"
    When these Plexus commands are executed on the leader node
      """
      ROTATE LICENSE CLUSTER KEY;
      """
    Then the committed license decision is newer than placeholder "before_rotation"
    And the rotation preserves identity and every signed deadline from placeholder "before_rotation"
    And the committed license decision is saved as placeholder "after_rotation"
    And every node has the license decision saved as placeholder "after_rotation"
    When these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: activated
      verification: verified
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A lost rotation response converges through its retired binding on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When a persistent license is activated through the running quorum
    Then the committed license decision is saved as placeholder "before_rotation"
    Given the next licensing operation response is lost after processing
    When these Plexus commands are executed on the leader node
      """
      ROTATE LICENSE CLUSTER KEY;
      """
    Then the rotation preserves identity and every signed deadline from placeholder "before_rotation"
    And the licensing service replayed one rotation request identity

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A rejected rotation response keeps its command recoverable on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When a persistent license is activated through the running quorum
    Then the committed license decision is saved as placeholder "before_rotation"
    Given the next licensing response has "signature" tampering
    When these Plexus commands are executed on the leader node
      """
      ROTATE LICENSE CLUSTER KEY;
      """
    Then the rotation preserves identity and every signed deadline from placeholder "before_rotation"
    And the licensing service replayed one rotation request identity

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A definitive refusal abandons the pending key on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When a persistent license is activated through the running quorum
    Then the committed license decision is saved as placeholder "before_rotation"
    Given the next licensing rotation is refused
    When these Plexus commands fail with "proof_invalid"
      """
      ROTATE LICENSE CLUSTER KEY;
      """
    Then the committed license decision matches placeholder "before_rotation"
    When these Plexus commands are executed on the leader node
      """
      ROTATE LICENSE CLUSTER KEY;
      """
    Then the rotation preserves identity and every signed deadline from placeholder "before_rotation"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Root succession permits a later working key on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When a persistent license is activated through the running quorum
    Then the committed license decision is saved as placeholder "before_succession"
    When the licensing service advances its root epoch with succession "provided"
    And these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    Then the committed license decision is newer than placeholder "before_succession"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An unknown root epoch cannot replace a committed decision on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When a persistent license is activated through the running quorum
    Then the committed license decision is saved as placeholder "before_unknown_epoch"
    When the licensing service advances its root epoch with succession "missing"
    And an explicit license refresh is refused by decision verification
    Then the committed license decision matches placeholder "before_unknown_epoch"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Revocation retains a committed decision and rejects a new signature on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When a persistent license is activated through the running quorum
    Then the committed license decision is saved as placeholder "before_revocation"
    When the licensing service revokes its working key
    And an explicit license refresh is refused by decision verification
    Then node "node-1" eventually describes this license without a selected domain
      """
      verification: verified (signing key revoked)
      """
    And the revocation triggers a background refresh before its ordinary deadline
    When the licensing service installs a replacement working key
    And these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    Then the committed license decision is newer than placeholder "before_revocation"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
