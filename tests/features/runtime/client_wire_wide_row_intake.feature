@client_wire_wide_row_intake @slots_2
Feature: Establishing a wide subscription row after a refused input handoff

  Scenario: A native client receives its wide row after the first input handoff cannot finish
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    When node "node-3" is stopped
    Given inbound interconnect traffic to stopped node "node-3" is paced at 262144 bytes per second
    When node "node-3" is started
    Then node "node-3" eventually reports raft voters "node-1,node-2,node-3"
    Given a repeated text placeholder "wide_note" of 3000000 bytes is prepared
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA wide_record (sequence I64, note STRING);
      CREATE WIRE JSON SCHEMA wide_record_json MODE STRICT (sequence integer, note string);
      CREATE WIRE JSON CODEC wide_record_codec FROM WIRE SCHEMA wide_record_json TO SCHEMA wide_record;
      CREATE RELAY wide_records SCHEMA wide_record UNBRANCHED;
      CREATE VHOST edge wide-rows-{{test_id}}.example.com;
      CREATE ENDPOINT wide_ingress ON edge PATH '/records' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR wide_source
        FROM wide_ingress MODE ACK MAX 1 ACK TIMEOUT 30s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING wide_record_codec
        TO wide_records SET sequence = message.sequence, note = message.note UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      RELOCATE RELAY wide_records ONTO NODE node-3 IGNORE PREFERENCES;
      SHOW CLUSTER STATUS;
      """
    Then the last command output contains
      """
      - domain={{domain}} kind=relay name=wide_records owner=node-3
      """
    Given client "subscriber" is connected to node "node-1"
    When client "subscriber" executes these Plexus commands
      """
      CREATE SUBSCRIPTION wide_seen TO wide_records;
      """
    And http payload is acknowledged within "30s" by node "node-1" with host "wide-rows-{{test_id}}.example.com" path "/records" after releasing paced node "node-3" on server error "failed to forward output to relay 'wide_records'"
      """
      {"sequence":1,"note":"{{wide_note}}"}
      """
    Then the last server error contains
      """
      failed to forward output to relay 'wide_records'
      """
    And within "30s" client "subscriber" receives a subscription payload
      """
      {"note":"{{wide_note}}","sequence":1}
      """
    And client "subscriber" subscription "wide_seen" is active
