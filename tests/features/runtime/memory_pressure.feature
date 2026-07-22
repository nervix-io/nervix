Feature: Memory pressure

  Scenario Outline: Memory high watermark pauses scheduled ingestors
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And memory pressure is configured with high watermark "1B" and low watermark "0B"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_memory_pressure_source SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_memory_pressure_source;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT memory_pressure_ingress
        ON edge
        PATH '/memory-pressure'
        TYPE HTTP;
        CREATE INGESTOR memory_pressure_source
        FROM ENDPOINT memory_pressure_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_memory_pressure_source
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    Then within "5s" node "node-1" eventually reports describe ingestor "memory_pressure_source" as "status: stopped"
    And within "5s" node "node-1" eventually reports describe ingestor "memory_pressure_source" as "memory-backpressure: active"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
