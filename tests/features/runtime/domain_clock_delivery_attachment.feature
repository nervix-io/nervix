Feature: Receiver-opened domain clock delivery

  @domain_clock_delivery_attachment
  Scenario Outline: Installed clocks receive progress while their gossip exchanges are blocked
    Given the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "authority_domain"
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD <period> SKEW 10ms;
      """
    And a clock session is opened on node "<target>"
    When gossip exchanges involving node "<target>" are blocked with a "30s" send delay
    And these Plexus commands are executed on the leader node
      """
      START AT '2000-01-01T00:00:00Z' TIME RATE 1.0;
      """
    And the clock session attaches to the clock of domain "{{domain}}"
    Then the clock session is attached at generation 1 to a paced clock with period "<period>", skew "10ms", logical origin "2000-01-01T00:00:00Z" and time rate "1"
    And within "10s" the clock session receives <ticks> increasing ticks for generation 1 with period "<period>" and time rate "1"
    When gossip exchanges involving node "<target>" are restored
    And node "<target>" is restarted 1 times with a new interconnect address
    Given a clock session is opened on node "<target>"
    When the clock session attaches to the clock of domain "{{domain}}"
    Then the clock session is attached at generation 1 to a paced clock with period "<period>", skew "10ms", logical origin "2000-01-01T00:00:00Z" and time rate "1"
    And within "10s" the clock session receives <ticks> increasing ticks for generation 1 with period "<period>" and time rate "1"

    Examples:
      | cluster_size | target | period | ticks |
      | 1            | node-1 | 100ms  | 2     |
      | 3            | node-3 | 100ms  | 2     |
      | 3            | node-3 | 1h     | 1     |

  @domain_clock_delivery_attachment
  Scenario: A receiver attaches to a reassigned authority while its graph preparation is held
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "authority_domain"
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 10ms;
      START AT '2000-01-01T00:00:00Z' TIME RATE 4.0;
      """
    And domain clock progress for domain "{{domain}}" on node "node-2" is paused before delivery
    Then domain clock progress for domain "{{domain}}" on node "node-2" reaches the delivery pause within the authority observation budget
    When runtime preparation on node "node-3" is paused
    And application health probes from node "node-1" to node "node-2" fail
    And application health probes from node "node-3" to node "node-2" fail
    Then node "node-1" eventually reports interconnect to "node-2" as "unavailable"
    When consensus connectivity for node "node-2" is blocked
    And gossip exchanges involving node "node-2" are blocked with a "5s" send delay
    Then node "node-3" reaches its runtime preparation pause
    Given a clock session is opened on node "node-3"
    When the clock session attaches to the clock of domain "{{domain}}"
    Then the clock session is attached at generation 1 to a paced clock with period "100ms", skew "10ms", logical origin "2000-01-01T00:00:00Z" and time rate "4"
    And within "10s" the clock session receives 2 increasing ticks for generation 1 with period "100ms" and time rate "4"
    When runtime preparation on node "node-3" is released
    And gossip exchanges involving node "node-2" are restored
    And consensus connectivity for node "node-2" is restored
    And application health probes from node "node-1" to node "node-2" are restored
    And application health probes from node "node-3" to node "node-2" are restored
    And domain clock progress for domain "{{domain}}" on node "node-2" resumes
    When node "node-3" is stopped
    Then node "node-1" eventually reports status containing "raft member 'node-3' is marked unavailable"
    Given a clock session is opened on node "node-1"
    When the clock session attaches to the clock of domain "{{domain}}"
    Then the clock session is attached at generation 1 to a paced clock with period "100ms", skew "10ms", logical origin "2000-01-01T00:00:00Z" and time rate "4"
    And within "10s" the clock session receives 1 increasing ticks for generation 1 with period "100ms" and time rate "4"
