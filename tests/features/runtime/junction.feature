Feature: Relay junction
  Scenario Outline: Junction routes multiple aligned relays into multiple destinations
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        user_id I64,
        source STRING,
        raw STRING
      );

      CREATE SCHEMA notification_projection (
        user_id I64,
        source STRING,
        lane STRING
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        source string,
        raw string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE RELAY ss1 SCHEMA notification PARAMETERIZED BY user_id_branch;
      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE RELAY ss2 SCHEMA notification PARAMETERIZED BY user_id_branch;
      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE RELAY ss10 SCHEMA notification_projection PARAMETERIZED BY user_id_branch;
      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE RELAY ss20 SCHEMA notification_projection PARAMETERIZED BY user_id_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress_one
        ON edge
        PATH '/ingest-a'
        TYPE HTTP;

      CREATE ENDPOINT ingress_two
        ON edge
        PATH '/ingest-b'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE IF NOT EXISTS BRANCH by_source_one PARAMETERIZED BY user_id_branch VALUES { user_id = ss1.user_id } TTL 5m; CREATE INGESTOR source_one
        TO ss1
        DECODE USING notification_codec
        BRANCHED BY by_source_one
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress_one MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE IF NOT EXISTS BRANCH by_source_two PARAMETERIZED BY user_id_branch VALUES { user_id = ss2.user_id } TTL 5m; CREATE INGESTOR source_two
        TO ss2
        DECODE USING notification_codec
        BRANCHED BY by_source_two
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress_two MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE JUNCTION join_streams
        FROM ss1 WHERE ss1.source != "drop-left",
             ss2 WHERE ss2.source != "drop-right"
        FILTER WHERE ss1.user_id > 0
        TO ss10 SET ss10.lane = "left" UNSET ss10.raw WHERE ss1.source = "left"
        TO ss20 SET ss20.lane = "right" UNSET ss20.raw WHERE ss1.source = "right"
        BRANCHED BY by_source_one
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO ss10;
      SUBSCRIBE SESSION TO ss20;
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest-a"
      """
      {"user_id":11,"source":"left","raw":"left-raw"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest-b"
      """
      {"user_id":22,"source":"right","raw":"right-raw"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest-a"
      """
      {"user_id":33,"source":"drop-left","raw":"dropped"}
      """
    Then within "5s" the relay subscription receives payloads
      """
      "lane":"left","source":"left","user_id":11
      "lane":"right","source":"right","user_id":22
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Junction preserves branch keys for interleaved concrete branches
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        tenant STRING,
        source STRING
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        tenant string,
        source string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY ss1 SCHEMA notification PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY ss2 SCHEMA notification PARAMETERIZED BY tenant_branch;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY ss10 SCHEMA notification PARAMETERIZED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress_one
        ON edge
        PATH '/branch-a'
        TYPE HTTP;

      CREATE ENDPOINT ingress_two
        ON edge
        PATH '/branch-b'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE IF NOT EXISTS BRANCH by_source_one PARAMETERIZED BY tenant_branch VALUES { tenant = ss1.tenant } TTL 5m; CREATE INGESTOR source_one
        TO ss1
        DECODE USING notification_codec
        BRANCHED BY by_source_one
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress_one MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE IF NOT EXISTS BRANCH by_source_two PARAMETERIZED BY tenant_branch VALUES { tenant = ss2.tenant } TTL 5m; CREATE INGESTOR source_two
        TO ss2
        DECODE USING notification_codec
        BRANCHED BY by_source_two
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress_two MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE JUNCTION join_streams
        FROM ss1, ss2
        TO ss10
        BRANCHED BY by_source_one
        <flush_policy> ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO ss10;
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/branch-a"
      """
      {"tenant":"acme","source":"left-acme"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/branch-b"
      """
      {"tenant":"beta","source":"right-beta"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/branch-a"
      """
      {"tenant":"beta","source":"left-beta"}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/branch-b"
      """
      {"tenant":"acme","source":"right-acme"}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "source":"left-acme"
      key={"tenant":"acme"} | "source":"right-acme"
      key={"tenant":"beta"} | "source":"left-beta"
      key={"tenant":"beta"} | "source":"right-beta"
      """

    Examples:
      | cluster_size | replica_count | flush_policy                         |
      | 1            | 0             | FLUSH EACH 100ms MAX BATCH SIZE 1MiB |
      | 3            | 0             | FLUSH EACH 100ms MAX BATCH SIZE 1MiB |
      | 1            | 0             | FLUSH IMMEDIATE                      |
      | 3            | 0             | FLUSH IMMEDIATE                      |

  Scenario Outline: Junction filter-map reads materialized relay state
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        user_id I64,
        source STRING
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        source string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE RELAY state_notifications
        SCHEMA notification PARAMETERIZED BY user_id_branch
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE RELAY ss1 SCHEMA notification PARAMETERIZED BY user_id_branch;
      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE RELAY ss2 SCHEMA notification PARAMETERIZED BY user_id_branch;
      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE RELAY ss10 SCHEMA notification PARAMETERIZED BY user_id_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT state_ingress
        ON edge
        PATH '/state'
        TYPE HTTP;

      CREATE ENDPOINT ingress_one
        ON edge
        PATH '/ingest-a'
        TYPE HTTP;

      CREATE ENDPOINT ingress_two
        ON edge
        PATH '/ingest-b'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE IF NOT EXISTS BRANCH by_state_source PARAMETERIZED BY user_id_branch VALUES { user_id = state_notifications.user_id } TTL 5m; CREATE INGESTOR state_source
        TO state_notifications
        DECODE USING notification_codec
        BRANCHED BY by_state_source
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT state_ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE IF NOT EXISTS BRANCH by_source_one PARAMETERIZED BY user_id_branch VALUES { user_id = ss1.user_id } TTL 5m; CREATE INGESTOR source_one
        TO ss1
        DECODE USING notification_codec
        BRANCHED BY by_source_one
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress_one MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE IF NOT EXISTS BRANCH by_source_two PARAMETERIZED BY user_id_branch VALUES { user_id = ss2.user_id } TTL 5m; CREATE INGESTOR source_two
        TO ss2
        DECODE USING notification_codec
        BRANCHED BY by_source_two
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT ingress_two MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE JUNCTION join_streams
        FROM ss1, ss2
        TO ss10 SET ss10.source = state_notifications.source BRANCHED BY by_state_source
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO ss10;
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/state"
      """
      {"user_id":11,"source":"state"}
      """
    Then within "5s" node "node-1" eventually reports materialized state for relay "state_notifications" containing
      """
      key={"user_id":11} payload={"source":"state","user_id":11}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest-b"
      """
      {"user_id":11,"source":"right"}
      """
    Then within "5s" the relay subscription receives a payload
      """
      {"source":"state","user_id":11}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Junction creation rejects mismatched schemas
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail with "requires equal internal schemas"
      """
      CREATE SCHEMA notification (
        user_id I64,
        source STRING
      );

      CREATE SCHEMA wide_notification (
        user_id I64,
        source STRING,
        extra STRING
      );

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE RELAY ss1 SCHEMA notification PARAMETERIZED BY user_id_branch;
      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE RELAY ss2 SCHEMA wide_notification PARAMETERIZED BY user_id_branch;
      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
      CREATE RELAY ss10 SCHEMA notification PARAMETERIZED BY user_id_branch;

      CREATE IF NOT EXISTS BRANCH by_join_streams PARAMETERIZED BY user_id_branch VALUES { user_id = ss1.user_id } TTL 5m;

      CREATE JUNCTION join_streams
        FROM ss1, ss2
        TO ss10 BRANCHED BY by_join_streams
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
