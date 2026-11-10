Feature: Local engine and client capabilities

  Scenario Outline: A lowered VM inline limit admits small batches through the data workers
    Given node-1 starts with vm-inline-row-limit = 0
    And node-2 starts with vm-inline-row-limit = 0
    And node-3 starts with vm-inline-row-limit = 0
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge engine-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR event_intake FROM intake
        MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events SET value = input.value + 1 UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observed TO events;
      START;
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE CLUSTER;
      """
    Then the last command output contains
      """
      vm-inline-row-limit=0
      """
    And the last command output contains
      """
      wasm-parallel-compilation=false
      """
    And the data execution count on node "node-1" is saved
    When http payload is posted to node "node-1" with host "engine-{{test_id}}.example.com" path "/events"
      """
      {"value":41}
      """
    Then within "30s" the relay subscription receives a payload
      """
      "value":42
      """
    And the data execution count on node "node-1" eventually increases

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: The CLI REPL uses explicit local connection capabilities
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI REPL starts on node "{{leader}}" with explicit connection capabilities
    And the CLI REPL runs "LIST DOMAINS;"
    Then the CLI REPL eventually displays "{{domain}}"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
