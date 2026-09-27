Feature: Integrated client wire qualification
  @client_wire15
  Scenario: Subscription restoration and typed transaction inspection survive the same leader loss
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA wire_record (tenant STRING, sequence I64);
      CREATE WIRE JSON SCHEMA wire_record_json MODE STRICT (tenant string, sequence integer);
      CREATE CODEC wire_record_codec FROM WIRE JSON SCHEMA wire_record_json TO SCHEMA wire_record;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m;
      CREATE RELAY wire_records SCHEMA wire_record BRANCHED BY by_tenant;
      CREATE VHOST edge qualified-{{test_id}}.example.com;
      CREATE ENDPOINT wire_ingress ON edge PATH '/records' TYPE HTTP;
      CREATE INGESTOR wire_source
        FROM ENDPOINT wire_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING wire_record_codec
        TO wire_records INHERIT ALL
        BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    Given client "subscriber" is connected to node "{{old_leader}}" with cluster seeds
    And client "owner" is connected to node "{{old_leader}}" with cluster seeds
    When client "subscriber" executes these NSPL commands
      """
      CREATE SUBSCRIPTION wire_seen TO wire_records;
      """
    And client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE SCHEMA queued_record (value STRING);
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    When client "owner" executes these NSPL commands
      """
      DESCRIBE TRANSACTION OPERATION 1 FORMAT JSON;
      """
    Then the last inspection reports
      """
      transaction: {{transaction_id}}
      state: OPEN
      accepted operations: 1
      applied operations: 0
      report operations: 1
      """
    When http payload is posted to node "{{old_leader}}" with host "qualified-{{test_id}}.example.com" path "/records"
      """
      {"tenant":"acme","sequence":1}
      """
    Then within "30s" client "subscriber" receives a subscription payload
      """
      key={"tenant":"acme"} payload={"sequence":1,"tenant":"acme"}
      """
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{new_leader}}" eventually reports leader "{{new_leader}}"
    When node "{{old_leader}}" is stopped
    Then within "30s" client "subscriber" observes subscription "wire_seen" interrupted
    When client "subscriber" executes these NSPL commands
      """
      DESCRIBE DOMAIN;
      """
    Then client "subscriber" subscription "wire_seen" is active
    When client "owner" executes these NSPL commands
      """
      DESCRIBE TRANSACTION '{{transaction_id}}' OPERATION 1 FORMAT JSON;
      """
    Then the last inspection reports
      """
      transaction: {{transaction_id}}
      state: OPEN
      accepted operations: 1
      applied operations: 0
      report operations: 1
      """
    When within "30s" client "subscriber" receives a subscription payload from repeated http posts to node "{{new_leader}}" with host "qualified-{{test_id}}.example.com" path "/records"
      """
      {"tenant":"beta","sequence":2}
      """
    Then the last relay subscription payload contains
      """
      key={"tenant":"beta"}
      payload={"sequence":2,"tenant":"beta"}
      """
    When client "owner" attempts to commit its transaction
    Then client "owner" transaction state is "COMMITTED"
    When client "owner" executes these NSPL commands
      """
      DESCRIBE TRANSACTION '{{transaction_id}}' OPERATION 1 FORMAT JSON;
      """
    Then the last inspection reports
      """
      transaction: {{transaction_id}}
      state: COMMITTED
      accepted operations: 1
      applied operations: 1
      report operations: 1
      """
