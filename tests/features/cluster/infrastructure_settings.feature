Feature: Declared infrastructure settings
  Infrastructure capacity belongs to each node. Liveness follows the committed cluster policy,
  and reserved append admission lets the leader retain a stream to every follower.

  Scenario: A ten-member cluster retains append capacity for every follower
    Given a 10 node nervix cluster is started
    When 16 domains named "infrastructure_append" are created on the leader node
    Then within "120s" every node has applied 16 domains named "infrastructure_append"
    And node "node-1" eventually describes node "node-10" with "interconnect: max-peers=64 request-quota=128 health-probes=64 max-connections=768 handshakes=32 queue=1024 completion=4 watermarks=8192 stream-window=65536 connection-window=262144 setup=5s request=10s progress=5s backoff=200ms max-backoff=5s jitter-permille=0"

  Scenario Outline: A configured request quota is visible on a cluster of <cluster_size>
    Given node-1 starts with interconnect-max-peers = 16
    And node-1 starts with interconnect-request-quota = 4
    And node-1 starts with interconnect-request-timeout = 20s
    And node-1 starts with partition-topology-poll-interval = 250ms
    And a <cluster_size> node nervix cluster is started
    Then node "node-<cluster_size>" eventually describes node "node-1" with "interconnect: max-peers=16 request-quota=4 health-probes=16 max-connections=192 handshakes=32 queue=1024 completion=1 watermarks=8192 stream-window=65536 connection-window=262144 setup=5s request=20s progress=5s backoff=200ms max-backoff=5s jitter-permille=0"
    And the last command output contains
      """
      partition-topology-poll-interval=250ms
      """
    When 4 domains named "configured_infrastructure" are created on the leader node
    Then within "60s" every node has applied 4 domains named "configured_infrastructure"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Committed unavailability changes transport liveness on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = 4s;
      """
    Then node "node-1" eventually describes node "node-<cluster_size>" with "interconnect liveness: ping-interval=2s ping-timeout=1s probe-timeout=400ms"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Lowering committed unavailability shortens failover while preserving healthy work
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And node "node-1" eventually reports leader "node-1"
    And the leader node is configured with these Plexus commands
      """
      ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = 4s;
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA settings_event ( id I64 );
      CREATE RELAY settings_events_1 SCHEMA settings_event UNBRANCHED;
      CREATE RELAY settings_events_2 SCHEMA settings_event UNBRANCHED;
      CREATE RELAY settings_events_3 SCHEMA settings_event UNBRANCHED;
      CREATE RELAY settings_events_4 SCHEMA settings_event UNBRANCHED;
      CREATE RELAY settings_events_5 SCHEMA settings_event UNBRANCHED;
      CREATE RELAY settings_events_6 SCHEMA settings_event UNBRANCHED;
      CREATE RELAY settings_events_7 SCHEMA settings_event UNBRANCHED;
      CREATE RELAY settings_events_8 SCHEMA settings_event UNBRANCHED;
      CREATE RELAY settings_events_9 SCHEMA settings_event UNBRANCHED;
      CREATE RELAY settings_events_10 SCHEMA settings_event UNBRANCHED;
      CREATE RELAY settings_events_11 SCHEMA settings_event UNBRANCHED;
      CREATE RELAY settings_events_12 SCHEMA settings_event UNBRANCHED;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status schedules nodes on at least 3 distinct owners
    And node "node-1" eventually describes node "node-3" with "interconnect liveness: ping-interval=2s ping-timeout=1s probe-timeout=400ms"
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status work on healthy nodes "node-1,node-3" is saved
    When application health responses from node "node-2" fail
    And consensus connectivity for node "node-2" is blocked
    And gossip exchanges involving node "node-2" are blocked with a "5s" send delay
    Then for "2s" healthy nodes "node-1,node-3" keep each other live and their scheduled work while node "node-2" waits at least "4s" for failover
    And within "6s" node "node-1" reports no scheduled work on "node-2"

  Scenario Outline: Local database and resolver capacities are visible on a cluster of <cluster_size>
    Given node-1 starts with database-cache-bytes = 1MiB
    And node-1 starts with database-journal-bytes = 64MiB
    And node-1 starts with database-workers = 1
    And node-1 starts with raft-database-cache-bytes = 2MiB
    And node-1 starts with raft-database-journal-bytes = 64MiB
    And node-1 starts with raft-database-workers = 1
    And node-1 starts with dns-cache-entries = 16
    And node-1 starts with dns-positive-max-ttl = 2s
    And node-1 starts with dns-negative-max-ttl = 1s
    And a <cluster_size> node nervix cluster is started
    Then node "node-<cluster_size>" eventually describes node "node-1" with "database: cache-bytes=1048576 journal-bytes=67108864 workers=1"
    And node "node-<cluster_size>" eventually describes node "node-1" with "raft database: cache-bytes=2097152 journal-bytes=67108864 workers=1"
    And node "node-<cluster_size>" eventually describes node "node-1" with "dns: cache-entries=16 positive-max-ttl=2s negative-max-ttl=1s hook-budget=30s families=dual-stack trust-negative=true concurrent-lookups=64"
    When 2 domains named "configured_stores" are created on the leader node
    Then within "60s" every node has applied 2 domains named "configured_stores"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
