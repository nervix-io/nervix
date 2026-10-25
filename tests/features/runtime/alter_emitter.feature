Feature: Altering emitters
  @alter_emitter_show_create_roundtrip
  Scenario Outline: ALTER EMITTER applies ordered operations and SHOW CREATE renders the result
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE ZEROMQ CLIENT emitter_a CONFIG {
        'addr' = 'tcp://127.0.0.1:63001',
        'bind' = 'false'
      };
      CREATE ZEROMQ CLIENT emitter_b CONFIG {
        'addr' = 'tcp://127.0.0.1:63002',
        'bind' = 'false'
      };
      CREATE ZEROMQ PUSH EMITTER event_emitter FROM outgoing
        TO emitter_a MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING event_codec INHERIT ALL
        FLUSH EACH 30s MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      ALTER ZEROMQ PUSH EMITTER event_emitter
        SET TO emitter_b MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING event_codec,
        SET COLLECT FOR 10ms MAX BATCH SIZE 512KiB,
        SET DETACHED,
        SET FLUSH IMMEDIATE;
      SHOW CREATE EMITTER event_emitter;
      """
    Then the last command output contains
      """
      CREATE DETACHED ZEROMQ PUSH EMITTER event_emitter
        FROM outgoing COLLECT FOR 10ms MAX BATCH SIZE 512KiB
        TO emitter_b
          MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @dynamic_emitter_flush_policy_change
  Scenario Outline: A dynamic emitter flush change preserves and releases buffered output
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-alter-emitter-flush.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR event_source
        FROM event_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO outgoing INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT emitter CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE ZEROMQ PUSH EMITTER event_emitter FROM outgoing
        TO emitter MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING event_codec INHERIT ALL
        FLUSH EACH 30s MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-alter-emitter-flush.example.com" path "/events"
      """
      {"seq":1}
      """
    Then the observed broker does not receive a payload within "300ms"
    When these Plexus commands are executed on the leader node
      """
      ALTER ZEROMQ PUSH EMITTER event_emitter SET FLUSH IMMEDIATE;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    And the observed broker receives a payload
      """
      "seq":1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @entity_pause_emitter_swap
  Scenario Outline: An emitter client and mode swap gates only its source relay and resumes flow
    Given entity gate deadline is configured as "5s"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-alter-emitter-swap.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR event_source
        FROM event_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO outgoing INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT emitter_a CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE ZEROMQ CLIENT emitter_b CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE ZEROMQ PUSH EMITTER event_emitter FROM outgoing
        TO emitter_a MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-alter-emitter-swap.example.com" path "/events"
      """
      {"seq":1}
      """
    Then the observed broker receives a payload
      """
      "seq":1
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER ZEROMQ PUSH EMITTER event_emitter SET CLIENT emitter_b, SET DETACHED;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-alter-emitter-swap.example.com" path "/events"
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

  @domain_pause_emitter_source_swap
  Scenario Outline: An emitter source and client swap replaces its relay edge and resumes flow
    Given entity gate deadline is configured as "5s"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE RELAY replacement SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-alter-emitter-swap.example.com;
      CREATE ENDPOINT event_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR event_source
        FROM event_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        TO replacement INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT emitter_a CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE ZEROMQ CLIENT emitter_b CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE ZEROMQ PUSH EMITTER event_emitter FROM outgoing
        TO emitter_a MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-alter-emitter-swap.example.com" path "/events"
      """
      {"seq":1}
      """
    Then the observed broker receives a payload
      """
      "seq":1
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER ZEROMQ PUSH EMITTER event_emitter
        ADD FROM replacement,
        DROP FROM outgoing,
        SET CLIENT emitter_b,
        SET DETACHED;
      """
    Then the last command output contains
      """
      quiesce level: DOMAIN_PAUSE
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}-alter-emitter-swap.example.com" path "/events"
      """
      {"seq":2}
      """
    Then the observed broker receives a payload
      """
      "seq":2
      """
    And the observed broker does not receive a payload within "300ms"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
