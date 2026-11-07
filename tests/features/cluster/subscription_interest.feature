Feature: Gossip subscription interest
  Scenario: Published interest starts, reopens, and stops remote subscription fan-out
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then node "node-1" eventually observes a stable leader
    And node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports interconnect to "node-3" as "connected"
    When these Plexus commands are executed through the client on node "node-1"
      """
      CORDON NODE node-2;
      CORDON NODE node-3;
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( id integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge subscription-interest-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT event_endpoint ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR event_ingestor
        FROM event_endpoint MODE NO ACK
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
    When these Plexus commands are executed on node "node-3"
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
    When these Plexus commands are executed on the active session
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
    When these Plexus commands are executed on the active session
      """
      DELETE SUBSCRIPTION remote_events;
      """
    And http payload is posted to node "node-1" with host "subscription-interest-{{test_id}}.example.com" path "/events"
      """
      {"id":3}
      """
    Then the relay subscription does not receive a payload within "2s"

  Scenario: A subscription a live node refuses to confirm names that node and its refusal
    Given a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( id I64 );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      START;
      """
    Then node "node-1" eventually reports interconnect to "node-2" as "connected"
    And node "node-1" eventually reports interconnect to "node-3" as "connected"
    When node "node-2" refuses the next subscription interest visibility check it answers
    And these Plexus commands are attempted on node "node-1"
      """
      CREATE SUBSCRIPTION refused_events TO events;
      """
    Then node "node-2" refused a subscription interest visibility check
    And the last command error message contains "did not become visible on every live node" exactly once
    And the last command error message contains "subscription interest visibility failed on node 'node-2'" exactly once
    And the last command error message contains "is not served by the node that answered" exactly once
