Feature: License capabilities
  A capability is a feature a license switches on. The catalog's capability licensing.dummy changes
  nothing: it shows that a verified license reached the cluster. DESCRIBE LICENSE lists every
  catalog capability as enabled or disabled.

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
