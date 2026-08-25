Feature: Altering schemas on a running domain
  @alter_running_domain
  Scenario Outline: ALTER drains old-schema work before resuming on the new graph
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA running_event (
        seq I64
      );

      CREATE WIRE JSON SCHEMA running_event_wire MODE STRICT (
        seq integer
      );

      CREATE CODEC running_event_codec
        FROM WIRE JSON SCHEMA running_event_wire
        TO SCHEMA running_event;

      CREATE RELAY running_events SCHEMA running_event UNBRANCHED;

      CREATE CLIENT running_event_sink
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };

      CREATE VHOST edge http-{{test_id}}-alter.example.com;
      CREATE ENDPOINT running_event_ingress ON edge PATH '/alter-running' TYPE HTTP;

      CREATE INGESTOR running_event_source
        FROM ENDPOINT running_event_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING running_event_codec
        TO running_events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE EMITTER running_event_out
        FROM running_events
        TO ZEROMQ running_event_sink MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING running_event_codec
        INHERIT ALL
        FLUSH EACH 30s MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      START;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-alter.example.com" path "/alter-running"
      """
      {"seq":1}
      """
    When this NSPL command request is executed on the leader node
      """
      BEGIN;
      ALTER WIRE JSON SCHEMA running_event_wire
        ADD FIELD note string OPTIONAL;
      ALTER SCHEMA running_event
        ADD FIELD note STRING OPTIONAL;
      DROP EMITTER running_event_out;
      CREATE EMITTER running_event_out
        FROM running_events
        TO ZEROMQ running_event_sink MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING running_event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      COMMIT;
      """
    Then the last command output contains
      """
      altered schema 'running_event'
      """
    And the observed broker receives a payload
      """
      "seq":1
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-alter.example.com" path "/alter-running"
      """
      {"seq":2,"note":"new graph"}
      """
    Then the observed broker receives a payload
      """
      "seq":2,"note":"new graph"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @alter_drain_timeout
  Scenario Outline: ALTER drain timeout leaves the old graph running
    Given schema change drain timeout is configured as "250ms"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA timeout_event ( seq I64 );
      CREATE WIRE JSON SCHEMA timeout_event_wire MODE STRICT ( seq integer );
      CREATE CODEC timeout_event_codec
        FROM WIRE JSON SCHEMA timeout_event_wire
        TO SCHEMA timeout_event;
      CREATE RELAY timeout_events SCHEMA timeout_event UNBRANCHED;

      CREATE CLIENT timeout_sink
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };
      CREATE VHOST timeout_edge http-{{test_id}}-alter-timeout.example.com;
      CREATE ENDPOINT timeout_ingress
        ON timeout_edge PATH '/alter-timeout' TYPE HTTP;
      CREATE INGESTOR timeout_source
        FROM ENDPOINT timeout_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING timeout_event_codec
        TO timeout_events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER timeout_out
        FROM timeout_events
        TO ZEROMQ timeout_sink MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING timeout_event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And emitter "timeout_out" enters stall mode
    And http payload is posted to node "node-1" with host "http-{{test_id}}-alter-timeout.example.com" path "/alter-timeout"
      """
      {"seq":1}
      """
    When these NSPL commands fail with "timed out draining domain"
      """
      BEGIN;
      ALTER WIRE JSON SCHEMA timeout_event_wire
        ADD FIELD note string OPTIONAL;
      ALTER SCHEMA timeout_event
        ADD FIELD note STRING OPTIONAL;
      COMMIT;
      """
    And emitter "timeout_out" leaves stall mode
    Then the observed broker receives a payload
      """
      "seq":1
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE SCHEMA timeout_event;
      """
    Then the last command output does not contain
      """
      note
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-alter-timeout.example.com" path "/alter-timeout"
      """
      {"seq":2}
      """
    Then the observed broker receives a payload
      """
      "seq":2
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @alter_state_invalidation
  Scenario Outline: ALTER clears affected materialized branches and preserves independent branches
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA mutable_event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA mutable_event_wire MODE STRICT (
        tenant string,
        seq integer
      );
      CREATE CODEC mutable_event_codec
        FROM WIRE JSON SCHEMA mutable_event_wire
        TO SCHEMA mutable_event;

      CREATE SCHEMA stable_event ( tenant STRING, seq I64 );
      CREATE WIRE JSON SCHEMA stable_event_wire MODE STRICT (
        tenant string,
        seq integer
      );
      CREATE CODEC stable_event_codec
        FROM WIRE JSON SCHEMA stable_event_wire
        TO SCHEMA stable_event;

      CREATE SCHEMA tenant_branch_schema ( tenant STRING );
      CREATE BRANCH tenant_branch SCHEMA tenant_branch_schema TTL 5m;

      CREATE RELAY mutable_state
        SCHEMA mutable_event
        BRANCHED BY tenant_branch
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY stable_state
        SCHEMA stable_event
        BRANCHED BY tenant_branch
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;

      CREATE VHOST state_edge http-{{test_id}}-alter-state.example.com;
      CREATE ENDPOINT mutable_ingress
        ON state_edge PATH '/mutable' TYPE HTTP;
      CREATE ENDPOINT stable_ingress
        ON state_edge PATH '/stable' TYPE HTTP;

      CREATE INGESTOR mutable_source
        FROM ENDPOINT mutable_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING mutable_event_codec
        TO mutable_state
        INHERIT ALL
        BRANCHED BY tenant_branch
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR stable_source
        FROM ENDPOINT stable_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING stable_event_codec
        TO stable_state
        INHERIT ALL
        BRANCHED BY tenant_branch
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-alter-state.example.com" path "/mutable"
      """
      {"tenant":"acme","seq":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-alter-state.example.com" path "/stable"
      """
      {"tenant":"beta","seq":20}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-alter-state.example.com" path "/mutable"
      """
      {"tenant":"beta","seq":2}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-alter-state.example.com" path "/stable"
      """
      {"tenant":"acme","seq":10}
      """
    Then within "5s" node "node-1" eventually reports materialized state for relay "mutable_state" containing
      """
      key={"tenant":"acme"} payload={"seq":1,"tenant":"acme"}
      """
    And within "5s" node "node-1" eventually reports materialized state for relay "mutable_state" containing
      """
      key={"tenant":"beta"} payload={"seq":2,"tenant":"beta"}
      """
    And within "5s" node "node-1" eventually reports materialized state for relay "stable_state" containing
      """
      key={"tenant":"acme"} payload={"seq":10,"tenant":"acme"}
      """
    And within "5s" node "node-1" eventually reports materialized state for relay "stable_state" containing
      """
      key={"tenant":"beta"} payload={"seq":20,"tenant":"beta"}
      """
    When this NSPL command request is executed on the leader node
      """
      BEGIN;
      ALTER WIRE JSON SCHEMA mutable_event_wire
        ADD FIELD note string OPTIONAL;
      ALTER SCHEMA mutable_event
        ADD FIELD note STRING OPTIONAL;
      COMMIT;
      """
    Then the last command output contains
      """
      altered schema 'mutable_event'
      """
    And within "5s" node "node-1" eventually reports materialized state for relay "mutable_state" containing
      """
      relay 'mutable_state' materialized state is empty
      """
    And within "5s" node "node-1" eventually reports materialized state for relay "stable_state" containing
      """
      key={"tenant":"acme"} payload={"seq":10,"tenant":"acme"}
      """
    And within "5s" node "node-1" eventually reports materialized state for relay "stable_state" containing
      """
      key={"tenant":"beta"} payload={"seq":20,"tenant":"beta"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @alter_publish_failure_rollback
  Scenario Outline: A failed schedule publication rolls the committed models back
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA rollback_event ( seq I64 );

      CREATE WIRE JSON SCHEMA rollback_event_wire MODE STRICT ( seq integer );

      CREATE CODEC rollback_event_codec
        FROM WIRE JSON SCHEMA rollback_event_wire
        TO SCHEMA rollback_event;

      CREATE RELAY rollback_events SCHEMA rollback_event UNBRANCHED CAPACITY 3;

      CREATE CLIENT rollback_sink
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };

      CREATE VHOST rollback_edge http-{{test_id}}-rollback.example.com;
      CREATE ENDPOINT rollback_ingress ON rollback_edge PATH '/alter-rollback' TYPE HTTP;

      CREATE INGESTOR rollback_source
        FROM ENDPOINT rollback_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING rollback_event_codec
        TO rollback_events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE EMITTER rollback_out
        FROM rollback_events
        TO ZEROMQ rollback_sink MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING rollback_event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-rollback.example.com" path "/alter-rollback"
      """
      {"seq":1}
      """
    Then the observed broker receives a payload
      """
      "seq":1
      """
    When the next schedule publication for domain "{{domain}}" fails
    And these NSPL commands fail with "failed to publish schedule for domain"
      """
      ALTER RELAY rollback_events SET CAPACITY 7;
      """
    And these NSPL commands are executed on the leader node
      """
      SHOW CREATE RELAY rollback_events;
      """
    Then the last command output contains
      """
      CAPACITY 3
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-rollback.example.com" path "/alter-rollback"
      """
      {"seq":2}
      """
    Then the observed broker receives a payload
      """
      "seq":2
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
