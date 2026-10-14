Feature: State snapshots written while a node stops
  A window or deduplicator branch writes what it published last as its task stops, and goes on
  writing it after that task was ended at its stop grace. A node keeps its databases until every such
  write has finished, so a restart finds both the databases released and the state written.

  The window is unbranched, so its one branch task is the task the stop reaches: it publishes its
  rows as it stops, and only the write of that publication is left waiting.

  Scenario Outline: A window snapshot that outlasts its processor's stop grace is written before a node of a <cluster_size> node cluster releases its database
    Given runtime replication is configured with replica count 0 and snapshot interval "10m"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA reading ( value I64 );
      CREATE SCHEMA reading_summary ( samples I64, total I64 );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT ( value integer );
      CREATE CODEC reading_codec FROM WIRE JSON SCHEMA reading_wire TO SCHEMA reading;
      CREATE RELAY readings SCHEMA reading UNBRANCHED;
      CREATE RELAY reading_summaries SCHEMA reading_summary UNBRANCHED;
      CREATE VHOST edge stop-snapshot-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/readings' TYPE HTTP;
      CREATE INGESTOR reading_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_codec
        TO readings INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR sliding_readings FROM readings
        WIDTH 2 MESSAGES STEP 1 MESSAGES UNBRANCHED
        TO reading_summaries SET samples = COUNT(input.value), total = SUM(input.value)
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION reading_summaries_subscription TO reading_summaries;
      START;
      """
    Then node "node-1" eventually accepts http traffic for host "stop-snapshot-{{test_id}}.example.com" path "/readings"
      """
      {"value":10}
      """
    When http payload is posted to node "node-1" with host "stop-snapshot-{{test_id}}.example.com" path "/readings"
      """
      {"value":20}
      """
    Then within "30s" the relay subscription receives a payload
      """
      "samples":2,"total":30
      """
    When bulk execution on every node is occupied for "15s"
    And the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION restarted_reading_summaries_subscription TO reading_summaries;
      """
    Then node "node-1" eventually accepts http traffic for host "stop-snapshot-{{test_id}}.example.com" path "/readings"
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
