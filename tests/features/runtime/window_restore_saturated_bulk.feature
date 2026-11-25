Feature: A window processor reopens its windows while the bulk workers are full
  A window processor that starts reopens the windows its checkpoint holds before it takes any input.
  After a restart that checkpoint comes from storage, so reopening it decodes the window's rows on
  the node's bulk workers. When every bulk worker is busy, the reopen waits for a place on them,
  and the node counts it among the jobs waiting for a worker. The first worker that frees takes it,
  and the window then continues with the rows it held when the node stopped.

  Scenario Outline: A window processor that restarts while bulk execution on its node is saturated waits for a place and keeps its open window on <cluster_size> nodes
    Given runtime replication is configured with replica count 0 and snapshot interval "10m"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA reading ( value I64 );
      CREATE SCHEMA reading_summary ( samples I64, total I64 );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT ( value integer );
      CREATE WIRE JSON CODEC reading_codec FROM WIRE SCHEMA reading_wire TO SCHEMA reading;
      CREATE RELAY readings SCHEMA reading UNBRANCHED;
      CREATE RELAY reading_summaries SCHEMA reading_summary UNBRANCHED;
      CREATE VHOST edge saturated-restore-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/readings';
      CREATE HTTP ENDPOINT INGESTOR reading_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING reading_codec
        TO readings INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR sliding_readings FROM readings
        WIDTH 2 MESSAGES STEP 1 MESSAGES UNBRANCHED
        TO reading_summaries SET samples = COUNT(input.value), total = SUM(input.value)
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION reading_summaries_subscription TO reading_summaries;
      START;
      """
    Then node "node-1" eventually accepts http traffic for host "saturated-restore-{{test_id}}.example.com" path "/readings"
      """
      {"value":10}
      """
    When http payload is posted to node "node-1" with host "saturated-restore-{{test_id}}.example.com" path "/readings"
      """
      {"value":20}
      """
    Then within "30s" the relay subscription receives a payload
      """
      "samples":2,"total":30
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE WINDOW PROCESSOR sliding_readings;
      """
    Then the last command output owner is saved as placeholder "window_owner"
    # After the restart the owner reopens the window from storage. Its restore is held once it has
    # read the window's lifecycle, so the bulk workers are full before it opens the window.
    When the next branch restore of window processor "sliding_readings" is held
    And the cluster is restarted
    Then within "60s" the branch restore of window processor "sliding_readings" reaches its hold
    When bulk execution on node "{{window_owner}}" is saturated
    And the held branch restore of window processor "sliding_readings" is released
    Then within "60s" a bulk job waits for a place on node "{{window_owner}}"
    When bulk execution is released on every node
    Then node "{{window_owner}}" observability metric "nervix_execution_jobs_waiting" with labels eventually equals 0
      """
      class="bulk_cpu"
      """
    Then node "node-1" eventually observes a stable leader
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION restarted_reading_summaries_subscription TO reading_summaries;
      """
    Then node "node-1" eventually accepts http traffic for host "saturated-restore-{{test_id}}.example.com" path "/readings"
      """
      {"value":30}
      """
    And within "30s" the relay subscription receives a payload
      """
      "samples":2,"total":50
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
