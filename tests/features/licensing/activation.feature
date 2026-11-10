Feature: Signed license activation
  A cluster commits a verified decision before granting licensed usage.

  Scenario Outline: A persistent activation commits a verified decision on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: activated
      policy: licensed fresh
      processing: running
      quota cluster.nodes: <cluster_size> of 16
      quota cluster.domains: 0 of 64
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An ephemeral activation commits its fixed binding on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: activated
      policy: licensed fresh
      verification: verified
      processing: running
      """
    And the licensing service holds one activation and one creation request identity

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Activation refuses <code> without changing free mode on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And the licensing service refuses with "<code>"
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands fail with "<code>"
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      policy: free
      """
    And the licensing request outcome "refused_by_service" was reported

    Examples:
      | cluster_size | code                |
      | 1            | license_key_invalid |
      | 3            | license_key_invalid |
      | 1            | license_key_revoked |
      | 3            | license_key_revoked |
      | 1            | kind_not_allowed    |
      | 3            | kind_not_allowed    |
      | 1            | license_not_active  |
      | 3            | license_not_active  |
      | 1            | quota_exhausted     |
      | 3            | quota_exhausted     |

  Scenario Outline: A lost response reuses the creation identity on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And the first activation response is lost after processing
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: activated
      verification: verified
      """
    And the licensing service holds one activation and one creation request identity

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An already processed activation is adopted on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And the licensing service has already processed the activation when it answers
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: activated
      verification: verified
      """
    And the licensing service received an adoption refresh without committed revisions
    And the licensing service holds one activation and one creation request identity

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Activation completes once after leadership changes after durable admission
    Given an HTTPS licensing test service is available
    And a 3 node nervix cluster is started
    Then the current leader node is saved as placeholder "activation_leader"
    And a node other than placeholder "activation_leader" is saved as placeholder "activation_successor"
    Given leadership moves from node "{{activation_leader}}" to node "{{activation_successor}}" under its next durably admitted command
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the leadership move under the durably admitted command completed
    And node "{{activation_successor}}" eventually describes this license without a selected domain
      """
      mode: activated
      verification: verified
      """
    And the licensing service holds one activation and one creation request identity

  @slots_2
  Scenario: Two independent three-node quorums compete for one activation capacity slot
    Given an HTTPS licensing test service is available
    And a 3 node nervix cluster is started
    When two three-node clusters race for the last persistent activation slot

  Scenario Outline: An adoption response must match the requested activation on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And the licensing service has already processed the activation when it answers
    And the next licensing response has "activation" tampering
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands fail with "does not match its independent stored binding"
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      policy: free
      """
    And the licensing request outcome "rejected_by_cluster" was reported

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
