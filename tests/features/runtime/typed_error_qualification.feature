Feature: Typed error qualification through public streams
  Scenario Outline: VM compile failures retain the owning model and specific function cause
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands fail with "model 'transform' in domain '{{domain}}' is invalid: FILTER-MAP compile failed: unknown function 'missing_transform' with arity 1"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA function_input (value STRING);
      CREATE SCHEMA function_output (result STRING);
      CREATE RELAY function_inputs SCHEMA function_input UNBRANCHED;
      CREATE RELAY function_outputs SCHEMA function_output UNBRANCHED;
      CREATE JUNCTION transform
        FROM function_inputs
        UNBRANCHED
        TO function_outputs
          SET result = missing_transform(input.value)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

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
    Then the last client request failed with one diagnostic spanning bytes 26 to 31
    And the last client request diagnostic underlines "BOGUS"
    And the last client request diagnostic message contains "found BOGUS"
    And the last command error contains
      """
      parse error
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Parse diagnostics locate the unexpected token in a later statement of the client source
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And client "diagnostics" is connected to the leader node
    When client "diagnostics" submits this NSPL command request
      """
      CREATE SCHEMA accepted (id STRING);
      CREATE SCHEMA broken (id BOGUS);
      """
    Then the last client request failed with one diagnostic spanning bytes 62 to 67
    And the last client request diagnostic underlines "BOGUS"
    And the last client request diagnostic message contains "found BOGUS"
    And the last command error contains
      """
      parse error
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Lex diagnostics locate the rejected character in the client source
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And client "diagnostics" is connected to the leader node
    When client "diagnostics" submits this NSPL command request
      """
      CREATE SCHEMA accepted (id STRING);
      CREATE SCHEMA broken (id STRING @);
      """
    Then the last client request failed with one diagnostic spanning bytes 69 to 70
    And the last client request diagnostic underlines "@"
    And the last command error contains
      """
      lex error
      """

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

  Scenario Outline: A rejected alteration names the model and the refused operation and changes nothing
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_event (id STRING);
      CREATE RELAY orders SCHEMA order_event UNBRANCHED;
      CREATE RELAY accepted_orders SCHEMA order_event UNBRANCHED;
      CREATE JUNCTION route_orders
        FROM orders
        UNBRANCHED
        TO accepted_orders
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    When these NSPL commands fail with "model 'route_orders' in domain '{{domain}}' is invalid: input relay `accepted_orders` is not configured"
      """
      ALTER JUNCTION route_orders DROP FROM accepted_orders;
      """
    And these NSPL commands fail with "model 'route_orders' in domain '{{domain}}' is invalid: a processor must retain at least one input"
      """
      ALTER JUNCTION route_orders SET DETACHED, DROP FROM orders;
      """
    And these NSPL commands fail with "model 'order_event' in domain '{{domain}}' is invalid: field `missing` does not exist"
      """
      ALTER SCHEMA order_event DROP FIELD missing;
      """
    And these NSPL commands are executed on the leader node
      """
      SHOW CREATE JUNCTION route_orders;
      """
    Then the last command output contains
      """
      CREATE ATTACHED JUNCTION route_orders
        FROM orders
        UNBRANCHED
        TO accepted_orders
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
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
