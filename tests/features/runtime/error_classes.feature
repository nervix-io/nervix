Feature: Error classes, the producing node and the outside system's code
  Every error a node records carries one of ten classes, the kind and name of the graph node that
  produced it and, when an outside system refused the data with a code, that code. An error route
  reads them as error.class, error.node, error.node_kind and error.external_code, a runtime notice
  carries the class and the node as typed fields beside its text, and
  nervix_message_errors_total counts every error by domain, node kind, node, class and operation.

  @error_classes @error_route_classes
  Scenario Outline: An error route reads the class and the producing node of each failed row on interleaved branches
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA reading (
        tenant STRING,
        divisor I64,
        ratio I64 OPTIONAL
      );
      CREATE SCHEMA scoring_error (
        tenant STRING,
        divisor I64,
        error_class STRING,
        error_node STRING,
        error_node_kind STRING,
        external_code STRING OPTIONAL,
        operation STRING
      );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT (
        tenant string,
        divisor integer,
        ratio integer OPTIONAL
      );
      CREATE WIRE JSON CODEC reading_codec
        FROM WIRE SCHEMA reading_wire
        TO SCHEMA reading;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY readings SCHEMA reading BRANCHED BY by_tenant;
      CREATE RELAY ratios SCHEMA reading BRANCHED BY by_tenant;
      CREATE RELAY scoring_errors SCHEMA scoring_error BRANCHED BY by_tenant;
      CREATE VHOST edge error-classes-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/readings';
      CREATE HTTP ENDPOINT INGESTOR reading_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_codec
        TO readings
        INHERIT ALL
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION scorer
        FROM readings
        BRANCHED BY by_tenant
        TO ratios
          INHERIT ALL
          SET ratio = 10 / input.divisor
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO scoring_errors
            SET tenant = input.tenant,
                divisor = input.divisor,
                error_class = error.class,
                error_node = error.node,
                error_node_kind = error.node_kind,
                external_code = error.external_code,
                operation = error.operation;
      CREATE SUBSCRIPTION ratios_subscription TO ratios;
      CREATE SUBSCRIPTION scoring_errors_subscription TO scoring_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "error-classes-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"alpha","divisor":0}
      """
    And http payload is posted to node "node-1" with host "error-classes-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","divisor":2}
      """
    And http payload is posted to node "node-1" with host "error-classes-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"beta","divisor":0}
      """
    And http payload is posted to node "node-1" with host "error-classes-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"alpha","divisor":5}
      """
    Then within "10s" the relay subscription receives exactly one payload for each fragment set
      """
      "divisor":2 | "ratio":5 | "tenant":"beta"
      "divisor":5 | "ratio":2 | "tenant":"alpha"
      "divisor":0 | "error_class":"evaluation" | "error_node":"scorer" | "error_node_kind":"junction" | "operation":"set" | "tenant":"alpha"
      "divisor":0 | "error_class":"evaluation" | "error_node":"scorer" | "error_node_kind":"junction" | "operation":"set" | "tenant":"beta"
      """
    And the relay subscription does not receive a payload within "1s"
    And cluster observability metric "nervix_message_errors_total" with labels eventually totals 2 across the nodes that count it
      """
      domain="{{domain}}"
      node_kind="junction"
      node="scorer"
      class="evaluation"
      operation="set"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @error_classes @error_notice_classes
  Scenario Outline: A logged error reaches the session as a runtime notice with its class and producing node
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA reading (
        tenant STRING,
        divisor I64,
        ratio I64 OPTIONAL
      );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT (
        tenant string,
        divisor integer,
        ratio integer OPTIONAL
      );
      CREATE WIRE JSON CODEC reading_codec
        FROM WIRE SCHEMA reading_wire
        TO SCHEMA reading;
      CREATE RELAY readings SCHEMA reading UNBRANCHED;
      CREATE RELAY ratios SCHEMA reading UNBRANCHED;
      CREATE VHOST edge error-notices-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/readings';
      CREATE HTTP ENDPOINT INGESTOR reading_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_codec
        TO readings
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION scorer
        FROM readings
        UNBRANCHED
        TO ratios
          INHERIT ALL
          SET ratio = 10 / input.divisor
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION ratios_subscription TO ratios;
      START;
      """
    And http payload is posted to node "node-1" with host "error-notices-{{test_id}}.example.com" path "/readings"
      """
      {"tenant":"alpha","divisor":0}
      """
    Then within "10s" the active session observes a server error containing
      """
      junction 'scorer' message error in domain '{{domain}}'
      """
    And the last server error reports the class "evaluation" of the junction "scorer"
    And cluster observability metric "nervix_message_errors_total" with labels eventually totals 1 across the nodes that count it
      """
      domain="{{domain}}"
      node_kind="junction"
      node="scorer"
      class="evaluation"
      operation="set"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
