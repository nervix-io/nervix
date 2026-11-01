Feature: Relay partition failover
  Every partition of a relay is its own placement unit with its own owner. When the cluster node
  owning some partitions fails, only those partitions move: the owners of the other partitions keep
  running, with their concrete branch presence intact, and records keep flowing through every
  partition once the failed ones have new owners. With PARTITIONS 4 the keys eta, beta, lambda and
  acme land in partitions 0, 1, 2 and 3.

  Scenario: Failover of one partition owner moves only its partitions and leaves the other partitions running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA notification ( tenant STRING, seq I64 );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        tenant string,
        seq integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE SCHEMA tenant_branch ( tenant STRING );

      CREATE BRANCH by_tenant
        SCHEMA tenant_branch TTL 5m PARTITIONS 4;

      CREATE RELAY notifications
        SCHEMA notification
        BRANCHED BY by_tenant;

      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/ingest' TYPE HTTP;

      CREATE HTTP ENDPOINT INGESTOR http_notifications
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      START;
      """
    Then the current leader node is saved as placeholder "leader"
    When these Plexus commands are executed on node "{{leader}}"
      """
      CREATE SUBSCRIPTION failover_subscription TO notifications;
      """
    And http payload is posted to node "{{leader}}" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"eta","seq":1}
      """
    And http payload is posted to node "{{leader}}" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"beta","seq":1}
      """
    And http payload is posted to node "{{leader}}" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"lambda","seq":1}
      """
    And http payload is posted to node "{{leader}}" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","seq":1}
      """
    Then within "30s" the relay subscription receives payloads keeping the order of each key
      """
      "tenant":"eta" | "seq":1
      "tenant":"beta" | "seq":1
      "tenant":"lambda" | "seq":1
      "tenant":"acme" | "seq":1
      """
    When these Plexus commands are executed through the client on node "{{leader}}"
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status places the partitions of relay "notifications" on at least 2 distinct owners
    And the owner of every partition of relay "notifications" in the last cluster status is saved with placeholder prefix "owner_before"
    And a node other than placeholder "leader" that owns a partition of relay "notifications" in the last cluster status is saved as placeholder "failed_owner"
    When node "{{failed_owner}}" is stopped
    Then within "90s" node "{{leader}}" eventually reports no partition of relay "notifications" owned by node "{{failed_owner}}"
    And within "30s" node "{{leader}}" eventually reports relay "notifications" keys absent in the partitions node "{{failed_owner}}" owned under placeholder prefix "owner_before" and present in every other partition
      """
      (tenant = 'eta')
      (tenant = 'beta')
      (tenant = 'lambda')
      (tenant = 'acme')
      """
    When http payload is posted to node "{{leader}}" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"eta","seq":2}
      """
    And http payload is posted to node "{{leader}}" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"beta","seq":2}
      """
    And http payload is posted to node "{{leader}}" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"lambda","seq":2}
      """
    And http payload is posted to node "{{leader}}" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","seq":2}
      """
    Then within "30s" the relay subscription receives payloads keeping the order of each key
      """
      "tenant":"eta" | "seq":2
      "tenant":"beta" | "seq":2
      "tenant":"lambda" | "seq":2
      "tenant":"acme" | "seq":2
      """
