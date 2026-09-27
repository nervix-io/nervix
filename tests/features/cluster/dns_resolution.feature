Feature: Cluster peer name resolution

  Nodes reach one another at the interconnect endpoints they advertise. A node resolves a peer's
  advertised host through its own resolver for every connection attempt, using the resolver
  configuration and hosts file it loaded at startup. These scenarios point every node at a DNS
  fixture the scenario controls, so the names, answers and TTLs are the scenario's own.

  Scenario Outline: Nodes form a cluster through <addressing>
    Given cluster peers are addressed by "<addressing>"
    And a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually reports leader "node-1"
    And node "node-1" eventually reports raft voters "<voters>"

    Examples:
      | addressing             | cluster_size | voters               |
      | DNS names              | 1            | node-1               |
      | DNS names              | 3            | node-1,node-2,node-3 |
      | literal IPv6 endpoints | 1            | node-1               |
      | literal IPv6 endpoints | 3            | node-1,node-2,node-3 |

  Scenario: Peers reach a node through the answer that connects
    Given cluster peers are addressed by "DNS names behind an unreachable address"
    And a 3 node nervix cluster is started
    Then node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-2" eventually reports interconnect to "node-3" as "connected"
    And node "node-3" eventually reports interconnect to "node-1" as "connected"
    And node "node-1" eventually reports raft voters "node-1,node-2,node-3"

  Scenario: Single-label names are completed by the search domain
    Given cluster peers are addressed by "single-label DNS names"
    And a 3 node nervix cluster is started
    Then node "node-1" eventually reports interconnect to "node-3" as "connected"
    And node "node-3" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports raft voters "node-1,node-2,node-3"

  Scenario: Names the hosts file lists resolve without asking DNS
    Given cluster peers are addressed by "hosts file names"
    And a 3 node nervix cluster is started
    Then node "node-2" eventually reports interconnect to "node-1" as "connected"
    And node "node-3" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports raft voters "node-1,node-2,node-3"
    And the DNS fixture received no questions for node names

  Scenario Outline: Peers follow a node that returns at a new address after its name stopped resolving
    Given cluster peers are addressed by "DNS names"
    And a 3 node nervix cluster is started
    When the DNS fixture answers the name of node "node-2" with "<answer>"
    And node "node-2" is stopped
    Then node "node-1" observability metric "nervix_interconnect_connection_failures_total" with labels eventually reaches at least 1
      """
      reason="resolution"
      """
    When node "node-2" moves to another address behind its name
    And node "node-2" is started
    Then node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-3" eventually reports interconnect to "node-2" as "connected"
    And node "node-2" eventually reports interconnect to "node-1" as "connected"
    And node "node-1" eventually reports raft voters "node-1,node-2,node-3"

    Examples:
      | answer         |
      | name not found |
      | no addresses   |
      | silence        |

  Scenario: A stopped leader rejoins through its advertised name
    Given cluster peers are addressed by "DNS names"
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-2" eventually reports leader "node-1"
    When node "node-1" is stopped
    Then node "node-2" eventually reports a leader other than "node-1"
    And node "node-3" eventually reports a leader other than "node-1"
    When node "node-1" is started
    Then node "node-1" eventually observes a stable leader
    And node "node-2" eventually reports interconnect to "node-1" as "connected"
    And node "node-3" eventually reports interconnect to "node-1" as "connected"
    And node "node-2" eventually reports raft voters "node-1,node-2,node-3"
