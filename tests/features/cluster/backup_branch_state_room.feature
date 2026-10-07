Feature: Deduplicator and window restore conversions refused for room
  A restoring node that has no room for one unit of a deduplicator or window conversion converts
  that unit again instead of failing the restore.

  Background:
    Given these NSPL commands are saved as placeholder "branch_state_models"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA payment ( tenant STRING, transaction_id STRING, amount I64 );
      CREATE WIRE JSON SCHEMA payment_wire MODE STRICT ( tenant string, transaction_id string, amount integer );
      CREATE CODEC payment_codec FROM WIRE JSON SCHEMA payment_wire TO SCHEMA payment;
      CREATE SCHEMA metric ( tenant STRING, latency I64 );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( tenant string, latency integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA metric_summary (
        tenant STRING,
        samples I64,
        total I64 OPTIONAL,
        first_latency I64 OPTIONAL,
        last_latency I64 OPTIONAL,
        latency_p0 F64 OPTIONAL
      );
      CREATE SCHEMA tenant_key ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 30m;
      CREATE RELAY payments SCHEMA payment BRANCHED BY by_tenant;
      CREATE RELAY unique_payments SCHEMA payment BRANCHED BY by_tenant;
      CREATE RELAY metrics SCHEMA metric BRANCHED BY by_tenant;
      CREATE RELAY metric_summaries SCHEMA metric_summary BRANCHED BY by_tenant;
      CREATE VHOST edge backup-branch-room-{{test_id}}.example.com;
      CREATE ENDPOINT payment_ingress ON edge PATH '/payments' TYPE HTTP;
      CREATE ENDPOINT metric_ingress ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR payment_source FROM ENDPOINT payment_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING payment_codec
        TO payments INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR metric_source FROM ENDPOINT metric_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING metric_codec
        TO metrics INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR unique_payment_filter FROM payments
        DEDUPLICATE ON input.transaction_id, input.amount
        MAX TIME 30m
        BRANCHED BY by_tenant
        TO unique_payments INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE WINDOW PROCESSOR latency_window FROM metrics
        WIDTH 2 MESSAGES
        STEP 1 MESSAGES
        BRANCHED BY by_tenant
        TO metric_summaries
          SET tenant = FIRST(input.tenant), samples = COUNT(input.latency), total = SUM(input.latency),
              first_latency = FIRST(input.latency), last_latency = LAST(input.latency),
              latency_p0 = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 0, 10, 0, 100, '10m')
          ON MESSAGE ERROR LOG;
      START;
      """

  Scenario Outline: A deduplicator keyspace the restoring node refuses for room is converted again and resumes exactly on <cluster_size> nodes
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      {{branch_state_models}}
      CREATE SUBSCRIPTION source_payments TO unique_payments;
      """
    And http payload is posted to host "backup-branch-room-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to host "backup-branch-room-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"beta","transaction_id":"txn-2","amount":20}
      """
    And http payload is posted to host "backup-branch-room-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-3","amount":30}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "transaction_id":"txn-1"
      key={"tenant":"beta"} | "transaction_id":"txn-2"
      key={"tenant":"alpha"} | "transaction_id":"txn-3"
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "refused-keyspace.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "refused-keyspace.nvxb" as json
    Then the described backup has exactly 2 "deduplicator" state sections
    And the described backup has exactly 0 "window_processor" state sections
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    And restoring domain "{{domain}}" pauses before it converts its first deduplicator or window state
    When restore "DOMAIN {{domain}} RESUME" of backup archive "refused-keyspace.nvxb" is streamed to node "{{leader}}" under execution reference "refused_keyspace" in the background
    Then the restore of domain "{{domain}}" pauses before it converts its first deduplicator or window state
    When bulk execution on node "{{leader}}" is saturated
    And the paused restore conversion of domain "{{domain}}" is released
    Then within "60s" node "{{leader}}" observability metric "nervix_execution_job_rejections_total" with labels eventually reaches at least 10
      """
      class="bulk_cpu"
      """
    When bulk execution on node "{{leader}}" is released
    Then the background restore stream's outcome is "completed" as "executed"
    And the restore stream's report shows every step applied
    And within "60s" node "restored-1" accepts http payload for host "backup-branch-room-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"probe","latency":1}
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION restored_payments TO unique_payments;
      """
    And http payload is posted to node "restored-1" with host "backup-branch-room-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-room-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"beta","transaction_id":"txn-1","amount":10}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-room-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"beta","transaction_id":"txn-2","amount":20}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-room-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-3","amount":30}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-room-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"alpha","transaction_id":"txn-2","amount":20}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-room-{{test_id}}.example.com" path "/payments"
      """
      {"tenant":"beta","transaction_id":"txn-5","amount":50}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"beta"} | "transaction_id":"txn-1"
      key={"tenant":"alpha"} | "transaction_id":"txn-2"
      key={"tenant":"beta"} | "transaction_id":"txn-5"
      """
    And the relay subscription does not receive a payload within "3s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: A window the restoring node refuses for room is converted again and resumes exactly on <cluster_size> nodes
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      {{branch_state_models}}
      CREATE SUBSCRIPTION source_summaries TO metric_summaries;
      """
    And http payload is posted to host "backup-branch-room-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"alpha","latency":10}
      """
    And http payload is posted to host "backup-branch-room-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":20}
      """
    And http payload is posted to host "backup-branch-room-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"alpha","latency":70}
      """
    And http payload is posted to host "backup-branch-room-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":80}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "samples":2 | "total":80 | "first_latency":10 | "last_latency":70 | "latency_p0":15.0
      key={"tenant":"beta"} | "samples":2 | "total":100 | "first_latency":20 | "last_latency":80 | "latency_p0":25.0
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "refused-window.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "refused-window.nvxb" as json
    Then the described backup has exactly 0 "deduplicator" state sections
    And the described backup has exactly 2 "window_processor" state sections
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given the restore coordinator uses remote placement in a multi-node cluster
    And restoring domain "{{domain}}" pauses before it converts its first deduplicator or window state
    When restore "DOMAIN {{domain}} RESUME" of backup archive "refused-window.nvxb" is streamed to node "{{leader}}" under execution reference "refused_window" in the background
    Then the restore of domain "{{domain}}" pauses before it converts its first deduplicator or window state
    When bulk execution on node "{{leader}}" is saturated
    And the paused restore conversion of domain "{{domain}}" is released
    Then within "60s" node "{{leader}}" observability metric "nervix_execution_job_rejections_total" with labels eventually reaches at least 10
      """
      class="bulk_cpu"
      """
    When bulk execution on node "{{leader}}" is released
    Then the background restore stream's outcome is "completed" as "executed"
    And the restore stream's report shows every step applied
    And within "60s" node "restored-1" accepts http payload for host "backup-branch-room-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"probe","latency":1}
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION restored_summaries TO metric_summaries;
      """
    And http payload is posted to node "restored-1" with host "backup-branch-room-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"alpha","latency":30}
      """
    And http payload is posted to node "restored-1" with host "backup-branch-room-{{test_id}}.example.com" path "/metrics"
      """
      {"tenant":"beta","latency":40}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "samples":2 | "total":100 | "first_latency":70 | "last_latency":30 | "latency_p0":15.0
      key={"tenant":"beta"} | "samples":2 | "total":120 | "first_latency":80 | "last_latency":40 | "latency_p0":25.0
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |
