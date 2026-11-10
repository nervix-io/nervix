Feature: Control-plane and session settings
  Each node advertises the checked limits of the session it opens. A new session after a restart
  reads that node's current limits, and control-plane cadences are declared node capabilities.

  Scenario Outline: Coordination timing parameters commit on every node of a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    When these Plexus statements are executed on the leader node
      """
      ALTER CLUSTER SET FORCED_RECOVERY_BUDGET = 7s,
        OWNERSHIP_HANDOFF_POLL_INTERVAL = 150ms,
        ENTITY_GATE_POLL_INTERVAL = 20ms,
        DOMAIN_DRAIN_POLL_INTERVAL = 40ms,
        RUNTIME_READINESS_PROPAGATION_BOUND = 25s;
      """
    Then node "node-1" eventually reports status containing "forced-recovery-budget committed=7s"
    And node "node-<cluster_size>" eventually reports status containing "ownership-handoff-poll-interval committed=150ms"
    And node "node-<cluster_size>" eventually reports status containing "entity-gate-poll-interval committed=20ms"
    And node "node-<cluster_size>" eventually reports status containing "domain-drain-poll-interval committed=40ms"
    And node "node-<cluster_size>" eventually reports status containing "runtime-readiness-propagation-bound committed=25s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Producer and consumer links survive different announced heartbeat cadences
    Given node-1 starts with client-link-heartbeat-interval = 2s
    And node-3 starts with client-link-heartbeat-interval = 12s
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 10ms;
      CREATE SCHEMA incoming (id STRING, amount I64);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE RELAY orders SCHEMA incoming UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR orders_in
        FROM SCHEMA incoming
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT CONSUMER EMITTER app_output FROM orders
        TO SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      RELOCATE INGESTOR orders_in ONTO NODE node-1 IGNORE PREFERENCES;
      RELOCATE EMITTER app_output ONTO NODE node-1 IGNORE PREFERENCES;
      """
    And client "app" is connected to node "node-3"
    When client "app" opens producer "input" on ingestor "orders_in" expecting fields "id STRING, amount I64"
    And client "app" opens consumer "output" on emitter "app_output" expecting fields "id STRING, cents I64"
    Then within "30s" the leader node describes ingestor "orders_in" with
      """
      forwarded producers: 1
      """
    And within "30s" the leader node describes emitter "app_output" with
      """
      forwarded consumers: 1
      """
    When client "app" observes clock progress for at least "13s" while producer "input" and consumer "output" stay idle
    And producer "input" submits batch "after idle" with rows
      | id  | amount |
      | one | 7      |
    And consumer "output" reads output batch "after idle output"
    Then output batch "after idle output" contains id "one" and cents 700
    When output batch "after idle output" is acknowledged
    Then batch "after idle" completes

  Scenario Outline: A session announces its complete default bounds on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    Then node "node-1" advertises session bounds at open
      | frame_bytes | transfer_bytes | nesting_depth | collection_entries | string_bytes |
      | 4194304     | 67108864       | 64            | 262144             | 67108864     |

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Configured control-plane cadences are visible through inspection and metrics on a cluster of <cluster_size>
    Given node-1 starts with control-plane-membership-interval = 100ms
    And node-1 starts with control-plane-scheduling-interval = 200ms
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed through the client on node "node-1"
      """
      DESCRIBE CLUSTER;
      """
    Then the last command output contains
      """
      control-plane-membership-interval=100ms control-plane-scheduling-interval=200ms
      """
    And node "node-1" control-plane cadence metric for "membership" eventually equals 0.1
    And node "node-1" control-plane cadence metric for "scheduling" eventually equals 0.2

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Session bounds are read from the contacted node after it restarts on a cluster of <cluster_size>
    Given node-1 starts with session-frame-bytes = 2MiB
    And node-1 starts with session-transfer-bytes = 32MiB
    And node-1 starts with session-string-bytes = 16MiB
    And a <cluster_size> node nervix cluster is started
    When leadership is transferred to node "node-1"
    Then a native client on node "node-1" executes a command with 0 bytes of padding
    And the same native client on node "node-1" refuses 3145728 padding bytes above its 2097152 byte frame limit
    When node "node-1" is stopped
    Given node-1 starts with session-frame-bytes = 8MiB
    And node-1 starts with session-transfer-bytes = 128MiB
    And node-1 starts with session-string-bytes = 96MiB
    When node "node-1" is started
    And leadership is transferred to node "node-1"
    Then node "node-1" advertises session bounds at open
      | frame_bytes | transfer_bytes | nesting_depth | collection_entries | string_bytes |
      | 8388608     | 134217728      | 64            | 262144             | 100663296    |
    And the same native client on node "node-1" executes a command with 5242880 bytes of padding
    When node "node-1" is stopped
    Given node-1 starts with session-frame-bytes = 2MiB
    And node-1 starts with session-transfer-bytes = 32MiB
    And node-1 starts with session-string-bytes = 16MiB
    When node "node-1" is started
    And leadership is transferred to node "node-1"
    Then node "node-1" advertises session bounds at open
      | frame_bytes | transfer_bytes | nesting_depth | collection_entries | string_bytes |
      | 2097152     | 33554432       | 64            | 262144             | 16777216     |
    And the same native client on node "node-1" refuses 3145728 padding bytes above its 2097152 byte frame limit

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
