Feature: Licensing refresh and committed deadline crossings
  Failed service requests do not renew a committed decision or reset its deadlines.

  Scenario Outline: The leader automatically refreshes a due decision on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the committed license decision is saved as placeholder "automatic_license"
    When both licensing clocks pass the refresh deadline
    Then the committed license decision is newer than placeholder "automatic_license"
    And node "node-1" eventually describes this license without a selected domain
      """
      policy: licensed fresh
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An explicit refresh commits a newer decision on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the committed license decision is saved as placeholder "first_license"
    When these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    Then the committed license decision is newer than placeholder "first_license"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Lapse freezes new usage while existing domains remain operable on <cluster_size> nodes
    Given an HTTPS licensing test service has bounded offline allowance
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      START;
      """
    Then the committed license decision is saved as placeholder "before_outage"
    When the licensing service becomes unreachable
    And the cluster licensing clock passes the "overdue" deadline
    Then node "node-1" eventually describes this license without a selected domain
      """
      policy: licensed overdue
      processing: running
      """
    And a licensing Warning notice saying "Licensing refresh is overdue" is observed
    When the cluster licensing clock passes the "valid_until" deadline
    Then node "node-1" eventually describes this license without a selected domain
      """
      policy: lapsed freeze
      processing: running
      """
    And node "node-1" eventually reports status containing "{{domain}} status=Running"
    And a licensing Warning notice saying "new usage is refused" is observed
    And the signed license deadlines match placeholder "before_outage"
    When the cluster is restarted
    Then node "node-1" eventually describes this license without a selected domain
      """
      policy: lapsed freeze
      """
    And node "node-1" eventually reports status containing "{{domain}} status=Running"
    When these Plexus commands fail with "lapsed"
      """
      CREATE UNPACED DOMAIN {{domain}}_refused;
      """
    Then the current leader node is saved as placeholder "licensing_leader"
    When node "<joining_node>" attempts to join
    Then node "<joining_node>" refused to join because "lapsed"
    When these Plexus commands are executed through the client on node "{{licensing_leader}}"
      """
      STOP;
      START;
      """
    When the CLI backs up "domain {{domain}}" from node "{{licensing_leader}}" into "licensed.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the licensing service becomes reachable
    And the licensing service clock catches up with the cluster
    And these Plexus commands are executed through the client on node "{{licensing_leader}}"
      """
      REFRESH LICENSE;
      CREATE UNPACED DOMAIN {{domain}}_admitted;
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      policy: licensed fresh
      """
    And node "node-1" eventually reports raft voters "<voters>"

    Examples:
      | cluster_size | joining_node | voters               |
      | 1            | node-2       | node-1               |
      | 3            | node-4       | node-1,node-2,node-3 |

  Scenario Outline: A service refusal is reported without renewing the decision on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the committed license decision is saved as placeholder "before_refusal"
    When the licensing service refuses with "license_key_revoked"
    And these Plexus commands fail with "license_key_revoked"
      """
      REFRESH LICENSE;
      """
    Then node "<observer>" reports the licensing service refusal "license_key_revoked"
    And the committed license decision matches placeholder "before_refusal"
    And the licensing request outcome "refused_by_service" was reported

    Examples:
      | cluster_size | observer |
      | 1            | node-1   |
      | 3            | node-3   |
