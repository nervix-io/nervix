Feature: Ingestor quiesce engagement during dispatch

  Scenario Outline: Entity gate engagement during ingestor dispatch stops new intake on a <nodes> node cluster
    Given Kafka is running
    And the production sticky scheduler is configured
    And a <nodes> node nervix cluster is started
    And Kafka topic "quiesce_dispatch_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA quiesce_event ( id I64 );
      CREATE WIRE JSON SCHEMA quiesce_event_wire MODE STRICT ( id integer );
      CREATE CODEC quiesce_event_codec
        FROM WIRE JSON SCHEMA quiesce_event_wire
        TO SCHEMA quiesce_event;
      CREATE CODEC quiesce_event_codec_alt
        FROM WIRE JSON SCHEMA quiesce_event_wire
        TO SCHEMA quiesce_event;
      CREATE RELAY quiesce_output SCHEMA quiesce_event UNBRANCHED;
      CREATE CLIENT quiesce_kafka TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR quiesce_source
        FROM KAFKA quiesce_kafka TOPIC quiesce_dispatch_{{test_id}}
          OFFSET BY CONSUMER GROUP nervix_cucumber_quiesce_dispatch_{{test_id}}
          MODE NO_ACK PARALLEL
        ON QUIESCE SUSPEND DECODE USING quiesce_event_codec
        TO quiesce_output INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "ingestor" "quiesce_source" is saved as placeholder "source_owner"
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION quiesce_seen TO quiesce_output;
      """
    Given ingestor "quiesce_source" pauses inside dispatch
    When Kafka message is published to topic "quiesce_dispatch_{{test_id}}"
      """
      {"id":1}
      """
    Then ingestor "quiesce_source" reaches the dispatch pause
    Given the entity gate for domain "{{domain}}" pauses after engagement
    When these NSPL commands begin executing in the background
      """
      <operation>
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    When Kafka message is published to topic "quiesce_dispatch_{{test_id}}"
      """
      {"id":2}
      """
    And ingestor "quiesce_source" leaves the dispatch pause
    Then within "20s" the relay subscription receives a payload
      """
      "id":1
      """
    And the relay subscription does not receive a payload within "2s"
    When the entity gate pause for domain "{{domain}}" is released
    Then the background NSPL execution succeeds

    Examples:
      | nodes | operation                                                               |
      | 1     | ALTER INGESTOR quiesce_source SET DECODE USING quiesce_event_codec_alt; |
      | 3     | DRAIN NODE {{source_owner}};                                            |
