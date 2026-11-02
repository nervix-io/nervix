@ingestor_instances @ingestor_instance_layout
Feature: Kafka domain offsets retain their intake unit layout
  A domain offset belongs to the stable unit that committed it. Changing the number of units
  requires an explicitly recreated source rather than interpreting another unit's offset table.

  Scenario Outline: A domain-offset source refuses an in-place unit count change on <nodes> nodes
    Given the production sticky scheduler is configured
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE KAFKA TOPIC INGESTOR spread_source
        FROM broker TOPIC 'events' OFFSET BY DOMAIN INSTANCES 3
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "Kafka domain-offset instance layout cannot change in place"
      """
      ALTER KAFKA TOPIC INGESTOR spread_source
        SET FROM broker TOPIC 'events' OFFSET BY DOMAIN INSTANCES 4
        MODE NO ACK ON QUIESCE SUSPEND;
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status lists 3 instances of ingestor "spread_source" on <owners> distinct owners

    Examples:
      | nodes | owners |
      | 1     | 1      |
      | 3     | 3      |
