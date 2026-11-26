Feature: An emitter's error route reads the source row its route built the record from
  An emitter whose route builds its records in another schema than the rows it reads, a codec
  record or a client consumer emitter's TO SCHEMA record, hands every row error it raises after its
  route ran the original source row as `input`, the record the route built as the all-optional
  `partial_output`, and the materialized state the row was admitted with. An ON MESSAGE ERROR SEND
  TO route written against the emitter's input therefore builds its error record, which reaches its
  relay in the branch of its source row, and the source learns the row was handled.

  @emitter_error_route_input
  Scenario Outline: Codec emitters whose publish fails for good route every row with its source row, built record and branch state
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA order (order_id STRING, tenant STRING, amount I64);
      CREATE JAQ JSON CODEC orders_codec TO SCHEMA order
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_setting (tenant STRING, label STRING);
      CREATE JAQ JSON CODEC settings_codec TO SCHEMA tenant_setting
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA invoice (reference STRING, cents I64);
      CREATE WIRE JSON SCHEMA invoice_wire MODE STRICT (reference string, cents integer);
      CREATE WIRE JSON CODEC invoice_codec FROM WIRE SCHEMA invoice_wire TO SCHEMA invoice;
      CREATE SCHEMA failed_order (
        node STRING,
        order_id STRING,
        amount I64,
        label STRING,
        reference STRING OPTIONAL,
        cents I64 OPTIONAL,
        error_code STRING,
        operation STRING
      );
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE PARAMETRIZER tenants SCHEMA tenant_key TTL 5m;
      CREATE RELAY orders SCHEMA order BRANCHED BY tenants;
      CREATE RELAY settings SCHEMA tenant_setting BRANCHED BY tenants
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY failed_orders SCHEMA failed_order BRANCHED BY tenants;
      CREATE VHOST edge error-route-input-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT orders_endpoint ON edge PATH '/orders';
      CREATE HTTP ENDPOINT settings_endpoint ON edge PATH '/settings';
      CREATE HTTP ENDPOINT INGESTOR orders_in
        FROM orders_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING orders_codec
        TIMESTAMP NOW
        TO orders INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR settings_in
        FROM settings_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING settings_codec
        TIMESTAMP NOW
        TO settings INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT push_out CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE ZEROMQ PUSH EMITTER zeromq_invoices FROM orders
        USING MATERIALIZED STATE settings REQUIRED WAIT
        TO push_out MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 500ms
          ENCODE USING invoice_codec
        SET reference = concat('inv-', input.order_id), cents = input.amount * 100
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_orders
          SET node = error.node,
              order_id = input.order_id,
              amount = input.amount,
              label = relay_state.settings.label,
              reference = partial_output.reference,
              cents = partial_output.cents,
              error_code = error.class,
              operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE HTTP REQUEST EMITTER http_invoices FROM orders
        USING MATERIALIZED STATE settings REQUIRED WAIT
        TO api
          METHOD 'POST'
          PATH concat('/invoices/', input.order_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 500ms
          ENCODE USING invoice_codec
        SET reference = concat('inv-', input.order_id), cents = input.amount * 100
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_orders
          SET node = error.node,
              order_id = input.order_id,
              amount = input.amount,
              label = relay_state.settings.label,
              reference = partial_output.reference,
              cents = partial_output.cents,
              error_code = error.class,
              operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION failed_orders_subscription TO failed_orders;
      START;
      """
    And http payload is posted to host "error-route-input-{{test_id}}.example.com" path "/settings"
      """
      [{"tenant":"north","label":"north-ready"},{"tenant":"south","label":"south-ready"}]
      """
    # Each emitter's publish now fails for good, so every row it holds follows its error route. The
    # route reads fields only the source row has, the record the emitter's route built and the state
    # of the row's own branch.
    And emitter "zeromq_invoices" enters fault mode
    And emitter "http_invoices" enters fault mode
    And http payload is posted to host "error-route-input-{{test_id}}.example.com" path "/orders"
      """
      [{"order_id":"n1","tenant":"north","amount":7},{"order_id":"s1","tenant":"south","amount":8},{"order_id":"n2","tenant":"north","amount":9}]
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      "node":"zeromq_invoices" | "order_id":"n1" | "amount":7 | "label":"north-ready" | "reference":"inv-n1" | "cents":700 | "error_code":"refused" | "operation":"publish" | key={"tenant":"north"}
      "node":"zeromq_invoices" | "order_id":"s1" | "amount":8 | "label":"south-ready" | "reference":"inv-s1" | "cents":800 | "error_code":"refused" | "operation":"publish" | key={"tenant":"south"}
      "node":"zeromq_invoices" | "order_id":"n2" | "amount":9 | "label":"north-ready" | "reference":"inv-n2" | "cents":900 | "error_code":"refused" | "operation":"publish" | key={"tenant":"north"}
      "node":"http_invoices" | "order_id":"n1" | "amount":7 | "label":"north-ready" | "reference":"inv-n1" | "cents":700 | "error_code":"refused" | "operation":"publish" | key={"tenant":"north"}
      "node":"http_invoices" | "order_id":"s1" | "amount":8 | "label":"south-ready" | "reference":"inv-s1" | "cents":800 | "error_code":"refused" | "operation":"publish" | key={"tenant":"south"}
      "node":"http_invoices" | "order_id":"n2" | "amount":9 | "label":"north-ready" | "reference":"inv-n2" | "cents":900 | "error_code":"refused" | "operation":"publish" | key={"tenant":"north"}
      """
    And HTTP receiver "api" has captured exactly 0 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_error_route_input @emitter_give_up
  Scenario Outline: A codec emitter that gives its rows up routes every row with its source row and the record its route built
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA order (order_id STRING, tenant STRING, amount I64);
      CREATE JAQ JSON CODEC orders_codec TO SCHEMA order
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA syslog_event (facility U8, severity U8, message STRING);
      CREATE SYSLOG CODEC syslog_codec TO SCHEMA syslog_event;
      CREATE SCHEMA failed_order (
        order_id STRING,
        amount I64,
        message STRING OPTIONAL,
        error_code STRING,
        operation STRING
      );
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE PARAMETRIZER tenants SCHEMA tenant_key TTL 5m;
      CREATE RELAY orders SCHEMA order BRANCHED BY tenants;
      CREATE RELAY failed_orders SCHEMA failed_order BRANCHED BY tenants;
      CREATE VHOST edge error-route-give-up-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT orders_endpoint ON edge PATH '/orders';
      CREATE HTTP ENDPOINT INGESTOR orders_in
        FROM orders_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING orders_codec
        TIMESTAMP NOW
        TO orders INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SYSLOG CLIENT syslog_down CONFIG {
        'protocol' = 'tcp', 'addr' = '127.0.0.1:1', 'connect_timeout_ms' = '1000'
      };
      CREATE SYSLOG COLLECTOR EMITTER syslog_stranded FROM orders
        TO syslog_down
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 500ms GIVE UP AFTER 1s
          ENCODE USING syslog_codec
        SET facility = 1 AS U8,
            severity = 3 AS U8,
            message = concat(input.tenant, ':', input.order_id)
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_orders
          SET order_id = input.order_id,
              amount = input.amount,
              message = partial_output.message,
              error_code = error.class,
              operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION failed_orders_subscription TO failed_orders;
      START;
      """
    # The destination refuses every connection, so each attempt fails until GIVE UP AFTER passes and
    # the emitter gives every row it holds up into its error route.
    And http payload is posted to host "error-route-give-up-{{test_id}}.example.com" path "/orders"
      """
      [{"order_id":"n1","tenant":"north","amount":7},{"order_id":"s1","tenant":"south","amount":8},{"order_id":"n2","tenant":"north","amount":9}]
      """
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      "order_id":"n1" | "amount":7 | "message":"north:n1" | "error_code":"unavailable" | "operation":"publish" | key={"tenant":"north"}
      "order_id":"s1" | "amount":8 | "message":"south:s1" | "error_code":"unavailable" | "operation":"publish" | key={"tenant":"south"}
      "order_id":"n2" | "amount":9 | "message":"north:n2" | "error_code":"unavailable" | "operation":"publish" | key={"tenant":"north"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_error_route_input
  Scenario Outline: A codec emitter whose destination refuses records routes each with its source row and built record, and delivers the rest
    Given the "SQS" emission target is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the "SQS" emission target "invoices_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA order (order_id STRING, tenant STRING, region STRING, amount I64);
      CREATE JAQ JSON CODEC orders_codec TO SCHEMA order
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA invoice (reference STRING, cents I64);
      CREATE WIRE JSON SCHEMA invoice_wire MODE STRICT (reference string, cents integer);
      CREATE WIRE JSON CODEC invoice_codec FROM WIRE SCHEMA invoice_wire TO SCHEMA invoice;
      CREATE SCHEMA failed_order (
        order_id STRING,
        amount I64,
        region STRING,
        reference STRING OPTIONAL,
        cents I64 OPTIONAL,
        error_code STRING,
        operation STRING,
        error_message STRING
      );
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE PARAMETRIZER tenants SCHEMA tenant_key TTL 5m;
      CREATE RELAY orders SCHEMA order BRANCHED BY tenants;
      CREATE RELAY failed_orders SCHEMA failed_order BRANCHED BY tenants;
      CREATE VHOST edge error-route-refusal-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT orders_endpoint ON edge PATH '/orders';
      CREATE HTTP ENDPOINT INGESTOR orders_in
        FROM orders_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING orders_codec
        TIMESTAMP NOW
        TO orders INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SQS CLIENT sqs_out CONFIG {
        'endpoint' = '{{sqs_endpoint}}',
        'region' = 'us-east-1',
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret'
      };
      CREATE SQS QUEUE EMITTER sqs_invoices FROM orders
        TO sqs_out QUEUE 'invoices_{{test_id}}'
          SEND SINGLE MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          ENCODE USING invoice_codec
        SET reference = concat('inv-', input.order_id), cents = input.amount * 100
        INVOKE write_header('region', input.region)
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_orders
          SET order_id = input.order_id,
              amount = input.amount,
              region = input.region,
              reference = partial_output.reference,
              cents = partial_output.cents,
              error_code = error.class,
              operation = error.operation,
              error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION failed_orders_subscription TO failed_orders;
      START;
      """
    # SQS refuses an empty message attribute, so the record whose region is empty is refused alone
    # and the records around it, of both branches, still leave.
    And http payload is posted to host "error-route-refusal-{{test_id}}.example.com" path "/orders"
      """
      [{"order_id":"n1","tenant":"north","region":"","amount":7},{"order_id":"s1","tenant":"south","region":"eu","amount":8},{"order_id":"n2","tenant":"north","region":"eu","amount":9},{"order_id":"s2","tenant":"south","region":"","amount":10}]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "order_id":"n1" | "amount":7 | "region":"" | "reference":"inv-n1" | "cents":700 | "error_code":"refused" | "operation":"publish" | attribute 'region' has an empty value | key={"tenant":"north"}
      "order_id":"s2" | "amount":10 | "region":"" | "reference":"inv-s2" | "cents":1000 | "error_code":"refused" | "operation":"publish" | attribute 'region' has an empty value | key={"tenant":"south"}
      """
    And within "30s" the observed broker receives exactly these messages
      | payload                            | key | headers   | group |
      | {"reference":"inv-s1","cents":800} |     | region=eu |       |
      | {"reference":"inv-n2","cents":900} |     | region=eu |       |

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @emitter_error_route_input @client_emitter
  Scenario Outline: A client consumer emitter whose application rejects its batches routes every row with its source row and the record its TO SCHEMA route built
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA incoming (id STRING, amount I64, tenant STRING);
      CREATE SCHEMA outgoing (id STRING, cents I64);
      CREATE SCHEMA failed_order (
        id STRING,
        amount I64,
        tenant STRING,
        cents I64 OPTIONAL,
        error_code STRING,
        operation STRING
      );
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE PARAMETRIZER tenants SCHEMA tenant_key TTL 5m;
      CREATE RELAY orders SCHEMA incoming BRANCHED BY tenants;
      CREATE RELAY failed_orders SCHEMA failed_order BRANCHED BY tenants;
      CREATE CLIENT PRODUCER INGESTOR orders_in
        FROM SCHEMA incoming
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO orders INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT CONSUMER EMITTER app_output FROM orders
        TO SCHEMA outgoing
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        INHERIT id
        SET cents = input.amount * 100
        BATCH MAX MESSAGES 16 MAX SIZE 1MiB
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO failed_orders
          SET id = input.id,
              amount = input.amount,
              tenant = input.tenant,
              cents = partial_output.cents,
              error_code = error.class,
              operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION failed_orders_subscription TO failed_orders;
      START;
      """
    And client "app" is connected to the leader node
    When client "app" opens consumer "output" on emitter "app_output" expecting fields "id STRING, cents I64"
    And client "app" opens producer "input" on ingestor "orders_in" expecting fields "id STRING, amount I64, tenant STRING"
    # The emitter sends each branch's rows in an output batch of their own, and the application
    # rejects both, so every row follows the error route with the fields only its source row has.
    And producer "input" submits batch "interleaved" with rows
      | id  | amount | tenant |
      | o-1 | 1      | acme   |
      | o-2 | 2      | globex |
      | o-3 | 3      | acme   |
    And consumer "output" reads output batch "first"
    And output batch "first" is rejected because "application refused"
    And consumer "output" reads output batch "second"
    And output batch "second" is rejected because "application refused"
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "id":"o-1" | "amount":1 | "tenant":"acme" | "cents":100 | "error_code":"refused" | "operation":"publish" | key={"tenant":"acme"}
      "id":"o-2" | "amount":2 | "tenant":"globex" | "cents":200 | "error_code":"refused" | "operation":"publish" | key={"tenant":"globex"}
      "id":"o-3" | "amount":3 | "tenant":"acme" | "cents":300 | "error_code":"refused" | "operation":"publish" | key={"tenant":"acme"}
      """
    And batch "interleaved" completes
    When consumer "output" closes

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
