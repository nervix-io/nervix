Feature: Materialized state inspection respects schema sensitivity
  Scenario Outline: Every node masks sensitive materialized fields and branch keys in every partition
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event (
        tenant STRING SENSITIVE,
        zone STRING,
        secret STRING SENSITIVE,
        seq I64
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (
        tenant string, zone string, secret string, seq integer
      );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE SCHEMA tenant_key ( tenant STRING SENSITIVE, zone STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS <partitions>;
      CREATE RELAY events SCHEMA event BRANCHED BY by_tenant
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE VHOST edge materialized-sensitive-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT event_ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR event_ingestor
        FROM event_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL
        BRANCHED BY by_tenant SET tenant = message.tenant, zone = message.zone
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION events_subscription TO events;
      START;
      """
    When http payload is posted to node "node-1" with host "materialized-sensitive-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha-private","zone":"east","secret":"alpha-first","seq":1}
      """
    And http payload is posted to node "node-1" with host "materialized-sensitive-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta-private","zone":"west","secret":"beta-first","seq":2}
      """
    And http payload is posted to node "node-1" with host "materialized-sensitive-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha-private","zone":"east","secret":"alpha-final","seq":3}
      """
    And http payload is posted to node "node-1" with host "materialized-sensitive-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta-private","zone":"west","secret":"beta-final","seq":4}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "seq":1
      "seq":2
      "seq":3
      "seq":4
      """
    And within "30s" every node reports the same materialized records for relay "events" with these rows
      """
      {"branch":{"tenant":"<redacted>","zone":"east"},"fields":{"secret":"<redacted>","seq":3,"tenant":"<redacted>","zone":"east"}}
      {"branch":{"tenant":"<redacted>","zone":"west"},"fields":{"secret":"<redacted>","seq":4,"tenant":"<redacted>","zone":"west"}}
      """

    Examples:
      | cluster_size | partitions | redacted |
      | 1            | 1          | <masked> |
      | 3            | 1          | <masked> |
      | 1            | 4          | <masked> |
      | 3            | 4          | <masked> |
