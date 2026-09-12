Feature: Interconnection observability

  Scenario Outline: Interconnection series are exposed with bounded dimensions
    Given a <cluster_size> node nervix cluster is started
    Then node "<node_id>" observability path "/metrics" eventually responds with 200 and contains "nervix_interconnect_connections"
    And node "<node_id>" observability path "/metrics" eventually responds with 200 and contains "nervix_interconnect_streams"
    And node "<node_id>" observability path "/metrics" eventually responds with 200 and contains "nervix_interconnect_pending_operations"
    And node "<node_id>" observability path "/metrics" eventually responds with 200 and contains "nervix_interconnect_unresolved_outcome_age_seconds"
    And node "<node_id>" observability path "/metrics" eventually responds with 200 and contains "nervix_interconnect_request_seconds_total"
    And node "<node_id>" observability path "/metrics" eventually responds with 200 and contains "nervix_interconnect_relay_admission_wait_seconds_total"
    And node "<node_id>" observability path "/metrics" eventually responds with 200 and contains "nervix_interconnect_bulk_bytes_total"
    And node "<node_id>" observability path "/metrics" eventually responds with 200 and contains "nervix_interconnect_stream_resets_total"
    And node "<node_id>" observability path "/metrics" eventually responds with 200 and contains "nervix_interconnect_quota_failures_total"
    And node "<node_id>" observability path "/metrics" eventually responds with 200 and contains "nervix_execution_memory_reserved_bytes"
    And node "<node_id>" observability path "/metrics" eventually responds with 200 and contains "nervix_execution_memory_rejections_total"
    And node "<node_id>" observability path "/metrics" eventually responds with 200 and contains "nervix_execution_job_queue_seconds_total"
    And node "<node_id>" observability path "/metrics" eventually responds with 200 and contains "nervix_execution_job_work_seconds_total"
    And node "<node_id>" observability path "/metrics" eventually responds with 200 and contains "nervix_node_scheduler_delay_seconds_total"
    And node "<node_id>" observability path "/metrics" eventually responds with 200 and contains "nervix_consensus_log_retained_bytes"
    And node "<node_id>" observability path "/metrics" eventually responds with 200 and contains "nervix_consensus_snapshot_pinned_generations"
    And node "<node_id>" observability metric "nervix_interconnect_requests_total" with labels eventually reaches at least 0
      """
      operation="liveness"
      """
    And node "<node_id>" interconnection metrics use only bounded dimensions

    Examples:
      | cluster_size | node_id |
      | 1            | node-1  |
      | 3            | node-1  |
      | 3            | node-3  |

  Scenario: Occupied bulk execution leaves the reserved management budget intact
    Given a 3 node nervix cluster is started
    When bulk execution on node "node-2" is occupied
    Then node "node-2" observability metric "nervix_execution_jobs_running" with labels eventually reaches at least 1
      """
      class="bulk_cpu"
      """
    And node "node-2" observability metric "nervix_execution_memory_capacity_bytes" with labels eventually equals 8388608
      """
      class="management"
      """
    And within "10s" these NSPL commands complete on node "node-2"
      """
      CREATE DOMAIN interconnect_observability;
      """
    When bulk execution on node "node-2" is released
    Then node "node-2" eventually observes a stable leader

  Scenario: Cross-node relay delivery advances transport and admission series
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      CREATE UNPACED DOMAIN {{domain}};
      """
    Given the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY input SCHEMA event UNBRANCHED;
      CREATE CLIENT sink TYPE ZEROMQ CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE VHOST edge observability-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER sink_output FROM input
        TO ZEROMQ sink MODE NO_ACK RETRY POLICY BACKOFF 100ms MAX 5s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When these NSPL commands are executed through the client on node "node-1"
      """
      UNCORDON NODE node-2;
      UNCORDON NODE node-3;
      """
    Then node "node-3" eventually forwards http traffic for host "observability-{{test_id}}.example.com" path "/events" to the observed broker
      """
      {"seq":1}
      """
    And node "node-1" observability metric "nervix_interconnect_relay_admissions_total" with labels eventually reaches at least 1
      """
      outcome="admitted"
      """
    And node "node-1" observability metric "nervix_interconnect_connections" with labels eventually reaches at least 1
      """
      class="relay"
      """
    And node "node-3" observability metric "nervix_execution_jobs_total" with labels eventually reaches at least 1
      """
      class="data_cpu"
      """

  Scenario: A bulk transfer reports its progress without spending management capacity
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "large_resource" with file "model.bin" of 48 MiB
    And node "node-1" eventually reports leader "node-1"
    And node "node-3" is stopped
    When these NSPL commands are executed on node "node-1"
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE large_model;
      UPLOAD RESOURCE large_model VERSION '{{large_resource}}';
      """
    Then the last command output contains
      """
      published resource version 1
      """
    When node "node-3" is started
    Then within "30s" node "node-1" eventually reports describe resource as "- node-3 topology=alive state=ready"
      """
      DESCRIBE RESOURCE large_model VERSION 1;
      """
    And node "node-1" observability metric "nervix_interconnect_bulk_bytes_total" with labels eventually reaches at least 1048576
      """
      class="bulk"
      direction="sent"
      """
    And node "node-3" observability metric "nervix_interconnect_bulk_bytes_total" with labels eventually reaches at least 1048576
      """
      class="bulk"
      direction="received"
      """
    And node "node-1" observability metric "nervix_interconnect_connection_failures_total" with labels eventually equals 0
      """
      class="management"
      reason="capacity"
      """
    And node "node-1" observability metric "nervix_interconnect_quota_failures_total" with labels eventually equals 0
      """
      direction="outbound"
      operation="liveness"
      """
    And node "node-1" observability metric "nervix_interconnect_connections" with labels eventually reaches at least 2
      """
      class="management"
      direction="outbound"
      """
