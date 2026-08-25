Feature: Reingestor output flushing

  Scenario Outline: Reingestor output waits for its route flush cadence
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event (
        tenant STRING,
        seq I64
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (
        tenant string,
        seq integer
      );
      CREATE CODEC event_codec
        FROM WIRE JSON SCHEMA event_wire
        TO SCHEMA event;
      CREATE SCHEMA tenant_seq_branch (
        tenant STRING,
        seq I64
      );
      CREATE SCHEMA tenant_branch (
        tenant STRING
      );
      CREATE BRANCH by_ingress SCHEMA tenant_seq_branch TTL 5m;
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY incoming SCHEMA event BRANCHED BY by_ingress;
      CREATE RELAY repartitioned SCHEMA event BRANCHED BY by_tenant;
      CREATE VHOST edge reingestor-flush-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO incoming
          INHERIT ALL
          BRANCHED BY by_ingress
          SET tenant = message.tenant, seq = message.seq
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE REINGESTOR repartition FROM incoming
        TO repartitioned
          INHERIT ALL
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH EACH 500ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION repartitioned_subscription TO repartitioned;
      START;
      """
    When http payload is posted to host "reingestor-flush-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"acme","seq":1}
      """
    Then the relay subscription does not receive a payload within "100ms"
    And within "5s" the relay subscription receives payloads
      """
      "seq":1,"tenant":"acme"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
