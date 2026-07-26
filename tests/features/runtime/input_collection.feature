Feature: Runtime node input collection

  Scenario Outline: Junction collects relay batches until its input timer expires
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        tenant STRING,
        sequence I64
      );
      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        tenant string,
        sequence integer
      );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY incoming SCHEMA notification BRANCHED BY by_tenant;
      CREATE RELAY collected SCHEMA notification BRANCHED BY by_tenant;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT input_collection_ingress
      ON edge
      PATH '/input-collection'
      TYPE HTTP;
      CREATE INGESTOR input_collection_source
      FROM ENDPOINT input_collection_ingress MODE NO_ACK SEQUENTIAL
      DECODE USING notification_codec
      TO incoming
        INHERIT ALL
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE JUNCTION input_collector
      FROM incoming
      COLLECT FOR 3s MAX BATCH SIZE 10MiB
      BRANCHED BY by_tenant
      TO collected
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION collected_subscription TO collected;
      START;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/input-collection"
      """
      {"tenant":"alpha","sequence":1}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/input-collection"
      """
      {"tenant":"beta","sequence":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/input-collection"
      """
      {"tenant":"alpha","sequence":2}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/input-collection"
      """
      {"tenant":"beta","sequence":2}
      """
    Then within "5s" node "node-1" eventually reports describe ingestor "input_collection_source" as "batches_total sent relay=incoming physical_node=node-1 total=4"
    And the relay subscription does not receive a payload within "300ms"
    Then within "6s" the relay subscription receives payloads
      """
      {"sequence":1,"tenant":"alpha"}
      {"sequence":2,"tenant":"alpha"}
      {"sequence":1,"tenant":"beta"}
      {"sequence":2,"tenant":"beta"}
      """
    And node "node-1" observability metric "nervix_batches_total" with labels eventually equals 2
      """
      target_kind="JUNCTION"
      target="input_collector"
      direction="sent"
      relay="collected"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Junction releases a collected batch at its input size boundary
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        sequence I64
      );
      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        sequence integer
      );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE RELAY incoming SCHEMA notification UNBRANCHED;
      CREATE RELAY collected SCHEMA notification UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT input_collection_ingress
      ON edge
      PATH '/input-collection'
      TYPE HTTP;
      CREATE INGESTOR input_collection_source
      FROM ENDPOINT input_collection_ingress MODE NO_ACK SEQUENTIAL
      DECODE USING notification_codec
      TO incoming
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE JUNCTION input_collector
      FROM incoming
      COLLECT FOR 1h MAX BATCH SIZE 1B
      UNBRANCHED
      TO collected
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION collected_subscription TO collected;
      START;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/input-collection"
      """
      {"sequence":1}
      """
    Then within "5s" the relay subscription receives payloads
      """
      {"sequence":1}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
