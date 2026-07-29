Feature: Emitter relay inputs

  Scenario Outline: One emitter consumes same-schema relays from different branches
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event (
        seq I64,
        tenant STRING,
        source STRING
      );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (
        seq integer,
        tenant string,
        source string
      );
      CREATE CODEC event_codec
        FROM WIRE JSON SCHEMA event_wire
        TO SCHEMA event;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_source_a SCHEMA tenant_branch TTL 5m;
      CREATE BRANCH by_source_b SCHEMA tenant_branch TTL 5m;
      CREATE RELAY source_a_events SCHEMA event BRANCHED BY by_source_a;
      CREATE RELAY source_b_events SCHEMA event BRANCHED BY by_source_b;
      CREATE VHOST edge emitter-inputs-{{test_id}}.example.com;
      CREATE ENDPOINT source_a_ingress ON edge PATH '/a' TYPE HTTP;
      CREATE ENDPOINT source_b_ingress ON edge PATH '/b' TYPE HTTP;
      CREATE INGESTOR source_a
        FROM ENDPOINT source_a_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING event_codec
        TO source_a_events
          INHERIT ALL
          BRANCHED BY by_source_a
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR source_b
        FROM ENDPOINT source_b_ingress MODE NO_ACK SEQUENTIAL
        DECODE USING event_codec
        TO source_b_events
          INHERIT ALL
          BRANCHED BY by_source_b
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT sink
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };
      CREATE EMITTER combined_sink
        FROM source_a_events WHERE input.source = 'a',
             source_b_events WHERE input.source = 'b'
        COLLECT FOR 20ms MAX BATCH SIZE 1MiB
        ENCODE USING event_codec
        TO ZEROMQ sink
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to host "emitter-inputs-{{test_id}}.example.com" path "/a"
      """
      {"seq":1,"tenant":"acme","source":"a"}
      """
    And http payload is posted to host "emitter-inputs-{{test_id}}.example.com" path "/b"
      """
      {"seq":2,"tenant":"acme","source":"b"}
      """
    And http payload is posted to host "emitter-inputs-{{test_id}}.example.com" path "/a"
      """
      {"seq":3,"tenant":"beta","source":"a"}
      """
    And http payload is posted to host "emitter-inputs-{{test_id}}.example.com" path "/b"
      """
      {"seq":4,"tenant":"beta","source":"b"}
      """
    Then within "5s" the observed broker receives payloads
      """
      "seq":1
      "seq":2
      "seq":3
      "seq":4
      """
    When http payload is posted to host "emitter-inputs-{{test_id}}.example.com" path "/a"
      """
      {"seq":99,"tenant":"acme","source":"b"}
      """
    Then the observed broker does not receive a payload within "300ms"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
