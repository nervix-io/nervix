Feature: Typed error qualification through public streams
  Scenario Outline: Parse diagnostics locate the unexpected token in the client source
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And client "diagnostics" is connected to the leader node
    When client "diagnostics" submits this NSPL command request
      """
      CREATE SCHEMA broken (id BOGUS);
      """
    Then the last client request failed with one diagnostic spanning bytes 25 to 30

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Validation identifies the node and route of a message error branch mismatch
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail
      """
      CREATE SCHEMA calculation (tenant STRING, id STRING);
      CREATE SCHEMA calculation_error (source_id STRING);
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE BRANCH by_other_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY calculations SCHEMA calculation BRANCHED BY by_tenant;
      CREATE RELAY calculation_results SCHEMA calculation BRANCHED BY by_tenant;
      CREATE RELAY calculation_errors SCHEMA calculation_error BRANCHED BY by_other_tenant;
      CREATE JUNCTION calculate
        FROM calculations
        BRANCHED BY by_tenant
        TO calculation_results
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO calculation_errors
          SET source_id = input.id;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Direct VALUES cannot expose a sensitive field without explicit leakage
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail
      """
      CREATE SCHEMA secret_event (id STRING, secret STRING SENSITIVE);
      CREATE RELAY secret_events SCHEMA secret_event UNBRANCHED;
      CREATE CLIENT database TYPE POSTGRES
        POOL SIZE MIN 1 MAX 4
        CONFIG { 'addr' = '127.0.0.1:5432' };
      CREATE EMITTER direct_values FROM secret_events
        TO POSTGRES database INSERT TO TABLE existing_events
        VALUES { "id" = input.id, "secret" = input.secret }
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        BATCH MAX MESSAGES 64 MAX SIZE 1MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE EMITTER direct_values_allowed FROM secret_events
        TO POSTGRES database INSERT TO TABLE existing_events
        VALUES { "id" = input.id, "secret" = leak_sensitive(input.secret) }
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        BATCH MAX MESSAGES 64 MAX SIZE 1MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Processor message errors retain their concrete branch and omit sensitive input
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA calculation (
        tenant STRING,
        id STRING,
        numerator I64,
        denominator I64,
        secret STRING SENSITIVE
      );
      CREATE SCHEMA calculation_result (id STRING, result I64);
      CREATE SCHEMA calculation_error (
        source_id STRING,
        error_code STRING,
        operation STRING,
        affected_fields <affected_fields_type>
      );
      CREATE WIRE JSON SCHEMA calculation_wire MODE STRICT (
        tenant string,
        id string,
        numerator integer,
        denominator integer,
        secret string
      );
      CREATE CODEC calculation_codec
        FROM WIRE JSON SCHEMA calculation_wire
        TO SCHEMA calculation;
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY calculations SCHEMA calculation BRANCHED BY by_tenant;
      CREATE RELAY calculation_results SCHEMA calculation_result BRANCHED BY by_tenant;
      CREATE RELAY calculation_errors SCHEMA calculation_error BRANCHED BY by_tenant;
      CREATE VHOST edge typed-errors-{{test_id}}.example.com;
      CREATE ENDPOINT calculation_ingress ON edge PATH '/calculations' TYPE HTTP;
      CREATE INGESTOR calculation_source
        FROM ENDPOINT calculation_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING calculation_codec
        TO calculations
          INHERIT ALL
          BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION calculate
        FROM calculations
        BRANCHED BY by_tenant
        TO calculation_results
          SET id = input.id, result = input.numerator / input.denominator
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO calculation_errors
          SET source_id = input.id,
              error_code = error.code,
              operation = error.operation,
              affected_fields = error.fields;
      CREATE SUBSCRIPTION calculation_errors_subscription TO calculation_errors;
      START;
      """
    When http payload is posted to node "node-1" with host "typed-errors-{{test_id}}.example.com" path "/calculations"
      """
      {"tenant":"alpha","id":"alpha-1","numerator":10,"denominator":0,"secret":"alpha-private-value"}
      """
    Then within "30s" the relay subscription receives a payload
      """
      "source_id":"alpha-1"
      """
    And the last relay subscription payload contains
      """
      "error_code":"evaluation","operation":"set"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"alpha"}'
    And the last relay subscription payload does not contain "alpha-private-value"
    When http payload is posted to node "node-1" with host "typed-errors-{{test_id}}.example.com" path "/calculations"
      """
      {"tenant":"beta","id":"beta-1","numerator":20,"denominator":0,"secret":"beta-private-value"}
      """
    Then within "30s" the relay subscription receives a payload
      """
      "source_id":"beta-1"
      """
    And the last relay subscription payload contains
      """
      "error_code":"evaluation","operation":"set"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"beta"}'
    And the last relay subscription payload does not contain "beta-private-value"
    When http payload is posted to node "node-1" with host "typed-errors-{{test_id}}.example.com" path "/calculations"
      """
      {"tenant":"alpha","id":"alpha-2","numerator":30,"denominator":0,"secret":"another-private-value"}
      """
    Then within "30s" the relay subscription receives a payload
      """
      "source_id":"alpha-2"
      """
    And the last relay subscription payload contains
      """
      "error_code":"evaluation","operation":"set"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"alpha"}'
    And the last relay subscription payload does not contain "another-private-value"

    Examples:
      | cluster_size | affected_fields_type |
      | 1            | VEC<STRING>          |
      | 3            | VEC<STRING>          |
