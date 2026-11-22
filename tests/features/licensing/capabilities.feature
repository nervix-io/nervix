Feature: License capabilities
  A capability is a feature a license switches on. The catalog's capability licensing.dummy changes
  nothing: it shows that a verified license reached the cluster. DESCRIBE LICENSE lists every
  catalog capability as enabled or disabled. A capability a decision does not list is disabled, and
  a capability the cluster does not know is ignored and counted as not understood, so a cluster and
  a licensing service built from different catalogs keep working together.

  Scenario Outline: A free cluster shows every capability disabled on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      capability licensing.dummy: disabled
      """
    And the last license inspection has typed cluster scope

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A license that enables licensing.dummy shows it until the cluster releases the license on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And the licensing service enables the capability "licensing.dummy"
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: activated
      verification: verified
      capability licensing.dummy: enabled
      """
    When these Plexus commands are executed on the leader node
      """
      RELEASE LICENSE;
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      capability licensing.dummy: disabled
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A decision from a service that does not know licensing.dummy leaves it disabled on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And the licensing service does not know the capability "licensing.dummy"
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: activated
      verification: verified
      capability licensing.dummy: disabled
      conditions not understood: 0
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A decision with a capability the cluster does not know applies, and the capability counts as not understood on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And the licensing service enables the capability "licensing.unreleased", which the cluster does not know
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: activated
      verification: verified
      capability licensing.dummy: disabled
      conditions not understood: 1
      """
    And the last command output does not contain
      """
      licensing.unreleased
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
