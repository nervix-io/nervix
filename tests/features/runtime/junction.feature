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
        CREATE IF NOT EXISTS BRANCH by_source_one SCHEMA user_id_branch TTL 5m;
        CREATE RELAY ss1 SCHEMA notification BRANCHED BY by_source_one;
        CREATE RELAY ss2 SCHEMA notification BRANCHED BY by_source_one;
        CREATE RELAY ss10 SCHEMA notification_projection BRANCHED BY by_source_one;
        CREATE RELAY ss20 SCHEMA notification_projection BRANCHED BY by_source_one;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT ingress_one
        ON edge
        PATH '/ingest-a'
        TYPE HTTP;
        CREATE ENDPOINT ingress_two
        ON edge
        PATH '/ingest-b'
        TYPE HTTP;
      CREATE INGESTOR source_one
        FROM ENDPOINT ingress_one MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO ss1
          INHERIT ALL
          BRANCHED BY by_source_one
          SET user_id = message.user_id
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR source_two
        FROM ENDPOINT ingress_two MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO ss2
          INHERIT ALL
          BRANCHED BY by_source_one
          SET user_id = message.user_id
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION join_streams
        FROM ss1 WHERE input.source != "drop-left",
             ss2 WHERE input.source != "drop-right"
        FILTER WHERE input.user_id > 0
        BRANCHED BY by_source_one
        TO ss10
          INHERIT ALL EXCEPT raw
          SET lane = "left"
          WHERE output.source = "left"
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        TO ss20
          INHERIT ALL EXCEPT raw
          SET lane = "right"
          WHERE output.source = "right"
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG;
        CREATE SUBSCRIPTION ss10_subscription TO ss10;
        CREATE SUBSCRIPTION ss20_subscription TO ss20;
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
        CREATE IF NOT EXISTS BRANCH by_source_one SCHEMA tenant_branch TTL 5m;
        CREATE RELAY ss1 SCHEMA notification BRANCHED BY by_source_one;
        CREATE RELAY ss2 SCHEMA notification BRANCHED BY by_source_one;
        CREATE RELAY ss10 SCHEMA notification BRANCHED BY by_source_one;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT ingress_one
        ON edge
        PATH '/branch-a'
        TYPE HTTP;
        CREATE ENDPOINT ingress_two
        ON edge
        PATH '/branch-b'
        TYPE HTTP;
        CREATE INGESTOR source_one
        FROM ENDPOINT ingress_one MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO ss1
        INHERIT ALL
        BRANCHED BY by_source_one
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE INGESTOR source_two
        FROM ENDPOINT ingress_two MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO ss2
        INHERIT ALL
        BRANCHED BY by_source_one
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE JUNCTION join_streams FROM ss1, ss2
        BRANCHED BY by_source_one
        TO ss10
        INHERIT ALL
        <flush_policy>
        ON MESSAGE ERROR LOG;
        CREATE SUBSCRIPTION ss10_subscription TO ss10;
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
        CREATE IF NOT EXISTS BRANCH by_state_source SCHEMA user_id_branch TTL 5m;
        CREATE RELAY state_notifications
        SCHEMA notification BRANCHED BY by_state_source
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
        CREATE RELAY ss1 SCHEMA notification BRANCHED BY by_state_source;
        CREATE RELAY ss2 SCHEMA notification BRANCHED BY by_state_source;
        CREATE RELAY ss10 SCHEMA notification BRANCHED BY by_state_source;
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
        CREATE INGESTOR state_source
        FROM ENDPOINT state_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO state_notifications
        INHERIT ALL
        BRANCHED BY by_state_source
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE INGESTOR source_one
        FROM ENDPOINT ingress_one MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO ss1
        INHERIT ALL
        BRANCHED BY by_state_source
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE INGESTOR source_two
        FROM ENDPOINT ingress_two MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO ss2
        INHERIT ALL
        BRANCHED BY by_state_source
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE JUNCTION join_streams
        FROM ss1, ss2
        BRANCHED BY by_state_source
        USING MATERIALIZED STATE state_notifications REQUIRED WAIT
        TO ss10
        INHERIT ALL
        SET source = relay_state.state_notifications.source
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
        CREATE SUBSCRIPTION ss10_subscription TO ss10;
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
        CREATE IF NOT EXISTS BRANCH by_join_streams SCHEMA user_id_branch TTL 5m;
        CREATE RELAY ss1 SCHEMA notification BRANCHED BY by_join_streams;
        CREATE RELAY ss2 SCHEMA wide_notification BRANCHED BY by_join_streams;
        CREATE RELAY ss10 SCHEMA notification BRANCHED BY by_join_streams;
        CREATE JUNCTION join_streams FROM ss1, ss2
        BRANCHED BY by_join_streams
        TO ss10
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
