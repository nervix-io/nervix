Feature: Node prerequisites

  A node brings what it needs to reach the live cluster: the cluster it belongs to, the trust root
  it verifies its peers with, the wire contract it speaks, the consensus floors and durability
  promise it keeps, and its timings. A joining node and every member it handshakes with compare
  these before anything else. A node that cannot run beside a member is refused with the
  prerequisite and both values, and only that node stops. A node whose timings stay within the
  compatibility bound joins, and the cluster reports how its timings drift from the leader's.

  Scenario Outline: A node that claims another cluster is refused naming the cluster identity on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And node "node-<joiner>" claims cluster "another-cluster"
    When node "node-<joiner>" attempts to join
    Then node "node-<joiner>" refused to join because "cluster identity differs: this node is in cluster 'another-cluster', the peer in 'cucumber'"
    And node "node-1" eventually reports raft voters "<voters>"
    And node "node-1" eventually observes a stable leader

    Examples:
      | cluster_size | joiner | voters               |
      | 1            | 2      | node-1               |
      | 3            | 4      | node-1,node-2,node-3 |

  Scenario Outline: A node that trusts another root is refused naming both trust roots on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And node "node-<joiner>" also trusts another certificate authority
    When node "node-<joiner>" attempts to join
    Then node "node-<joiner>" refused to join because its trust root differs from the trust root of node "node-1"
    And node "node-1" eventually reports raft voters "<voters>"
    And node "node-1" eventually observes a stable leader

    Examples:
      | cluster_size | joiner | voters               |
      | 1            | 2      | node-1               |
      | 3            | 4      | node-1,node-2,node-3 |

  @node_prerequisite_refusal @deloxide_stress
  Scenario Outline: A node whose heartbeat is outside the compatibility bound is refused on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And node-<joiner> starts with raft-heartbeat-interval = 1s
    When node "node-<joiner>" attempts to join
    Then node "node-<joiner>" refused to join because "raft-heartbeat-interval is 1s on this node and 166ms on the peer, not within a factor of 2"
    And node "node-1" eventually reports raft voters "<voters>"
    And node "node-1" eventually observes a stable leader

    Examples:
      | cluster_size | joiner | voters               |
      | 1            | 2      | node-1               |
      | 3            | 4      | node-1,node-2,node-3 |

  Scenario Outline: A node whose heartbeat is within the bound joins and its drift is described on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And node-<joiner> starts with raft-heartbeat-interval = 250ms
    When node "node-<joiner>" attempts to join
    Then node "node-<joiner>" joined the cluster
    And node "node-1" eventually reports raft voters "<voters>"
    And node "node-1" eventually reports cluster timing drift "node-<joiner>"
    And node "node-1" eventually describes node "node-<joiner>" with "raft-heartbeat-interval: 250ms, the leader's 166ms"
    And node "node-1" eventually describes node "node-<joiner>" with "cluster identity: cucumber"
    And node "node-1" eventually describes node "node-<joiner>" with "durability promise: fsync every append"
    And node "node-1" eventually describes node "node-1" with "raft-heartbeat-interval: 166ms"
    And node "node-1" observability metric "nervix_cluster_timing_drift_nodes" with labels eventually equals 1
      """
      nervix_cluster_timing_drift_nodes
      """

    Examples:
      | cluster_size | joiner | voters                      |
      | 1            | 2      | node-1,node-2               |
      | 3            | 4      | node-1,node-2,node-3,node-4 |

  @node_prerequisite_refusal @deloxide_stress
  Scenario Outline: A node whose memory limit cannot hold its memory budgets refuses to start at preflight on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And node "node-<cluster_size>" is stopped
    And node-<cluster_size> starts with memory-limit = 1GiB
    When node "node-<cluster_size>" attempts to start
    Then node "node-<cluster_size>" refused to start because "memory-limit (1GiB) cannot hold the node's memory budgets of 2.27GiB"
    Given node-<cluster_size> starts with memory-limit = 4GiB
    When node "node-<cluster_size>" is started
    Then node "node-<cluster_size>" eventually observes a stable leader

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
