Feature: Cordon node

  Scenario: A cordoned node is excluded from new scheduling decisions
    Given Kafka is running
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      raft.cordoned_nodes: node-2
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};

      CREATE SCHEMA notification (
        user_id I64
      );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '{{kafka_addr}}',
          'auto.offset.reset' = 'earliest'
        };

      CREATE INGESTOR kafka_notifications
        FROM KAFKA kafka_main TOPIC notifications_{{test_id}} OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      DESCRIBE INGESTOR kafka_notifications;
      """
    Then the last command output does not contain
      """
      owner: node-2
      """

  Scenario: An uncordoned node is visible as schedulable again
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      UNCORDON NODE node-2;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      raft.cordoned_nodes: (none)
      """
