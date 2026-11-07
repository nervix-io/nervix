@ingestor_instances
Feature: Ingestor instance counts fit the execution-unit representation
  Nervix rejects an impossible unit-map size before placement materializes it.

  Scenario: An unaddressable instance count is refused before placement
    Given a 1 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( id integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      """
    When these Plexus statements fail with "cannot be represented as execution units on this target"
      """
      CREATE KAFKA TOPIC INGESTOR oversized_ingestor
        FROM broker TOPIC 'events' OFFSET BY DOMAIN INSTANCES 18446744073709551615
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
