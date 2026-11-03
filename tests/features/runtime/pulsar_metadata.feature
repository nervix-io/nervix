@pulsar_message_metadata @deloxide_stress
Feature: Pulsar message identity and times
  Scenario Outline: A Pulsar ingestor projects its message topic
    Given Pulsar is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar subscription "metadata_{{test_id}}" of topic "metadata_{{test_id}}" is provisioned
    When these Plexus commands are executed
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE SCHEMA observed ( seq I64, topic STRING OPTIONAL );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA observed UNBRANCHED;
      CREATE PULSAR CLIENT source CONFIG { 'addr' = '{{pulsar_addr}}' };
      CREATE PULSAR TOPIC INGESTOR records FROM source TOPIC 'metadata_{{test_id}}'
        SUBSCRIPTION 'metadata_{{test_id}}' PROVISIONED MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL SET topic = metadata.topic
        UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      START;
      """
    And Pulsar message is published to topic "metadata_{{test_id}}"
      """
      {"seq":1}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      "seq":1 | "topic":"persistent://public/default/metadata_{{test_id}}"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
