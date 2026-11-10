@partition_state_inspection
Feature: A moving processor key has one inspection partition
  Scenario Outline: A <kind> lists the holder before installation and the claimant before forget as <before> partitions become <after> on <cluster_size> nodes
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (tenant STRING, seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS <before>;
      CREATE RELAY incoming SCHEMA event BRANCHED BY by_tenant;
      CREATE RELAY outgoing SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge inspect-move-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR events FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO incoming INHERIT ALL BRANCHED BY by_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      <definition>
      START;
      """
    And http payload is posted to node "node-1" with host "inspect-move-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":1}
      """
    And http payload is posted to node "node-1" with host "inspect-move-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"eta","seq":10}
      """
    Given client "reader" is connected to node "node-1"
    Then within "30s" client "reader" sees <kind> "remembered" hold 2 rows
    And the last processor state lists the moving branch under partition <holder>
    Given the <kind> key claim is paused before install and before forget
    When these Plexus commands are executed on the leader node
      """
      ALTER BRANCH by_tenant SET PARTITIONS <after>;
      """
    And http payload is posted to node "node-1" with host "inspect-move-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","seq":2}
      """
    Then the <kind> key claim reaches the install pause
    And within "30s" client "reader" sees <kind> "remembered" hold 2 rows
    And the last processor state lists the moving branch under partition <holder>
    When the <kind> key claim's install pause is released
    Then the <kind> key claim reaches the forget pause
    And within "30s" client "reader" sees <kind> "remembered" hold 3 rows
    And the last processor state lists the moving branch under partition <claimant>
    And the last processor state preserves both tenants' input values
    And client "reader" reads <kind> "remembered" state one row per page with unchanged complete counts
    When the <kind> key claim's forget pause is released
    Then within "30s" client "reader" sees <kind> "remembered" hold 3 rows
    And the last processor state lists the moving branch under partition <claimant>
    And the last processor state preserves both tenants' input values

    Examples:
      | cluster_size | kind         | before | after | holder | claimant | definition                                                                                                                                                                                                                         |
      | 1            | deduplicator | 4      | 8     | 1      | 5        | CREATE DEDUPLICATOR remembered FROM incoming DEDUPLICATE ON input.seq MAX TIME 1h BRANCHED BY by_tenant TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;                                                              |
      | 3            | deduplicator | 4      | 8     | 1      | 5        | CREATE DEDUPLICATOR remembered FROM incoming DEDUPLICATE ON input.seq MAX TIME 1h BRANCHED BY by_tenant TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;                                                              |
      | 1            | window       | 4      | 8     | 1      | 5        | CREATE WINDOW PROCESSOR remembered FROM incoming WIDTH 10 MESSAGES 1h DURATION STEP 10 MESSAGES MAX STATE SIZE 1MiB BRANCHED BY by_tenant TO outgoing SET tenant = FIRST(input.tenant), seq = SUM(input.seq) ON MESSAGE ERROR LOG; |
      | 3            | window       | 4      | 8     | 1      | 5        | CREATE WINDOW PROCESSOR remembered FROM incoming WIDTH 10 MESSAGES 1h DURATION STEP 10 MESSAGES MAX STATE SIZE 1MiB BRANCHED BY by_tenant TO outgoing SET tenant = FIRST(input.tenant), seq = SUM(input.seq) ON MESSAGE ERROR LOG; |
      | 1            | deduplicator | 8      | 4     | 5      | 1        | CREATE DEDUPLICATOR remembered FROM incoming DEDUPLICATE ON input.seq MAX TIME 1h BRANCHED BY by_tenant TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;                                                              |
      | 3            | deduplicator | 8      | 4     | 5      | 1        | CREATE DEDUPLICATOR remembered FROM incoming DEDUPLICATE ON input.seq MAX TIME 1h BRANCHED BY by_tenant TO outgoing INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;                                                              |
      | 1            | window       | 8      | 4     | 5      | 1        | CREATE WINDOW PROCESSOR remembered FROM incoming WIDTH 10 MESSAGES 1h DURATION STEP 10 MESSAGES MAX STATE SIZE 1MiB BRANCHED BY by_tenant TO outgoing SET tenant = FIRST(input.tenant), seq = SUM(input.seq) ON MESSAGE ERROR LOG; |
      | 3            | window       | 8      | 4     | 5      | 1        | CREATE WINDOW PROCESSOR remembered FROM incoming WIDTH 10 MESSAGES 1h DURATION STEP 10 MESSAGES MAX STATE SIZE 1MiB BRANCHED BY by_tenant TO outgoing SET tenant = FIRST(input.tenant), seq = SUM(input.seq) ON MESSAGE ERROR LOG; |
