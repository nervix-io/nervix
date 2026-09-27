Feature: Gossip subscription interest
  Scenario: Published interest starts, reopens, and stops remote subscription fan-out
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports interconnect to "node-3" as "connected"
    When these NSPL commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge subscription-interest-{{test_id}}.example.com;
      CREATE ENDPOINT event_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_ingestor
        FROM ENDPOINT event_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=relay name=events owner=node-1
      """
    When these NSPL commands are executed on node "node-3"
      """
      CREATE SUBSCRIPTION remote_events TO events;
      """
    And http payload is posted to node "node-1" with host "subscription-interest-{{test_id}}.example.com" path "/events"
      """
      {"id":1}
      """
    Then within "5s" the relay subscription receives a payload
      """
      {"id":1}
      """
    When these NSPL commands are executed on the active session
      """
      DELETE SUBSCRIPTION remote_events;
      CREATE SUBSCRIPTION remote_events TO events;
      """
    And http payload is posted to node "node-1" with host "subscription-interest-{{test_id}}.example.com" path "/events"
      """
      {"id":2}
      """
    Then within "5s" the relay subscription receives a payload
      """
      {"id":2}
      """
    When these NSPL commands are executed on the active session
      """
      DELETE SUBSCRIPTION remote_events;
      """
    And http payload is posted to node "node-1" with host "subscription-interest-{{test_id}}.example.com" path "/events"
      """
      {"id":3}
      """
    Then the relay subscription does not receive a payload within "2s"
