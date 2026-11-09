Feature: Web console visual window processor and correlator creation

  @visual_window_branches
  Scenario Outline: A visually created window aggregates each interleaved branch with aggregate and scalar output
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_window_event (tenant STRING, latency I64);
      CREATE SCHEMA visual_window_summary (tenant STRING, samples I64, total I64, spread I64);
      CREATE SCHEMA visual_window_key (tenant STRING);
      CREATE BRANCH visual_window_tenant SCHEMA visual_window_key TTL 5m;
      CREATE JAQ JSON CODEC visual_window_codec TO SCHEMA visual_window_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY visual_window_input SCHEMA visual_window_event BRANCHED BY visual_window_tenant;
      CREATE RELAY visual_window_output SCHEMA visual_window_summary BRANCHED BY visual_window_tenant;
      CREATE VHOST visual_window_host window-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT visual_window_endpoint ON visual_window_host PATH '/window';
      CREATE HTTP ENDPOINT INGESTOR visual_window_ingestor FROM visual_window_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_window_codec
        TO visual_window_input INHERIT ALL BRANCHED BY visual_window_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='window-processor']" is clicked
    And selector ".create-name" is filled with "visual_window_stage"
    And selector ".create-processor-branching [data-branch='named']" is clicked
    And selector ".create-processor-branch .create-choice-search" is filled with "visual_window_tenant"
    Then selector ".create-processor-branch [data-value='visual_window_tenant']" exists
    When selector ".create-processor-branch [data-value='visual_window_tenant']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_window_input"
    Then selector ".create-processor-input [data-value='visual_window_input']" exists
    When selector ".create-processor-input [data-value='visual_window_input']" is clicked
    And selector ".create-window-width-messages" is filled with "3"
    And selector ".create-window-step-messages" is filled with "3"
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_window_output"
    Then selector ".create-processor-route-relay [data-value='visual_window_output']" exists
    # A window route constructs its output from aggregates alone: it offers no inheritance or flush.
    And selector ".create-processor-inherit" does not exist
    And selector ".create-processor-flush" does not exist
    When selector ".create-processor-route-relay [data-value='visual_window_output']" is clicked
    # Each window input field is inserted as the argument of the aggregate typed beside it.
    And selector ".create-processor-output-fields .create-choice-search" is filled with "tenant"
    Then selector ".create-processor-output-fields [data-value='tenant']" exists
    When selector ".create-processor-output-fields [data-value='tenant']" is clicked
    And selector ".create-window-aggregate" is filled with "FIRST"
    And selector ".create-window-input-fields .create-choice-search" is filled with "tenant"
    Then selector ".create-window-input-fields [data-value='tenant']" exists
    When selector ".create-window-input-fields [data-value='tenant']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "samples"
    Then selector ".create-processor-output-fields [data-value='samples']" exists
    When selector ".create-processor-output-fields [data-value='samples']" is clicked
    And selector ".create-window-aggregate" is filled with "COUNT"
    And selector ".create-window-input-fields .create-choice-search" is filled with "latency"
    Then selector ".create-window-input-fields [data-value='latency']" exists
    When selector ".create-window-input-fields [data-value='latency']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "total"
    Then selector ".create-processor-output-fields [data-value='total']" exists
    When selector ".create-processor-output-fields [data-value='total']" is clicked
    And selector ".create-window-aggregate" is filled with "SUM"
    Then selector ".create-window-input-fields [data-value='latency']" exists
    When selector ".create-window-input-fields [data-value='latency']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "spread"
    Then selector ".create-processor-output-fields [data-value='spread']" exists
    When selector ".create-processor-output-fields [data-value='spread']" is clicked
    And selector ".create-window-aggregate" is filled with "MAX"
    Then selector ".create-window-input-fields [data-value='latency']" exists
    When selector ".create-window-input-fields [data-value='latency']" is clicked
    Then selector ".create-processor-output-assignments .create-processor-assignment-row:last-of-type .create-processor-assignment-expression" has value "max(input.latency)"
    When selector ".create-processor-output-assignments .create-processor-assignment-row:last-of-type .create-processor-assignment-expression" is filled with "MAX(input.latency) -"
    And selector ".create-window-aggregate" is filled with "MIN"
    Then selector ".create-window-input-fields [data-value='latency']" exists
    When selector ".create-window-input-fields [data-value='latency']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE WINDOW PROCESSOR visual_window_stage"
    And selector ".create-preview" contains "WIDTH 3 MESSAGES STEP 3 MESSAGES BRANCHED BY visual_window_tenant"
    And selector ".create-preview" contains "SET tenant = first(input.tenant), samples = count(input.latency), total = sum(input.latency), spread = max(input.latency) - min(input.latency)"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION visual_window_subscription TO visual_window_output;
      START;
      """
    # One unfolded input batch interleaves both branches; each branch fills its own window.
    And http payload is posted to host "window-{{test_id}}.example.com" path "/window"
      """
      [
        {"tenant":"a","latency":10},
        {"tenant":"b","latency":100},
        {"tenant":"a","latency":30},
        {"tenant":"b","latency":400},
        {"tenant":"a","latency":20},
        {"tenant":"b","latency":200}
      ]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"a" | "samples":3 | "total":60 | "spread":20
      "tenant":"b" | "samples":3 | "total":700 | "spread":300
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_correlator_branches
  Scenario Outline: A visually created correlator matches each interleaved branch and sends an unmatched order to its timeout relay
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_order_event (tenant STRING, order_id STRING, item STRING);
      CREATE SCHEMA visual_payment_event (tenant STRING, order_id STRING, amount I64);
      CREATE SCHEMA visual_paid_order (tenant STRING, order_id STRING, item STRING, amount I64);
      CREATE SCHEMA visual_correlate_key (tenant STRING);
      CREATE BRANCH visual_correlate_tenant SCHEMA visual_correlate_key TTL 5m;
      CREATE JAQ JSON CODEC visual_order_codec TO SCHEMA visual_order_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE JAQ JSON CODEC visual_payment_codec TO SCHEMA visual_payment_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY visual_orders SCHEMA visual_order_event BRANCHED BY visual_correlate_tenant;
      CREATE RELAY visual_payments SCHEMA visual_payment_event BRANCHED BY visual_correlate_tenant;
      CREATE RELAY visual_paid_orders SCHEMA visual_paid_order BRANCHED BY visual_correlate_tenant;
      CREATE RELAY visual_unpaid_orders SCHEMA visual_order_event BRANCHED BY visual_correlate_tenant;
      CREATE VHOST visual_correlate_host correlate-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT visual_orders_endpoint ON visual_correlate_host PATH '/orders';
      CREATE HTTP ENDPOINT visual_payments_endpoint ON visual_correlate_host PATH '/payments';
      CREATE HTTP ENDPOINT INGESTOR visual_orders_ingestor FROM visual_orders_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_order_codec
        TO visual_orders INHERIT ALL BRANCHED BY visual_correlate_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR visual_payments_ingestor FROM visual_payments_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_payment_codec
        TO visual_payments INHERIT ALL BRANCHED BY visual_correlate_tenant SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='correlator']" is clicked
    And selector ".create-name" is filled with "visual_correlate_stage"
    # A correlator filters each side on its own and has no node-wide filter.
    Then selector ".create-processor-filter" does not exist
    When selector ".create-processor-branching [data-branch='named']" is clicked
    And selector ".create-processor-branch .create-choice-search" is filled with "visual_correlate_tenant"
    Then selector ".create-processor-branch [data-value='visual_correlate_tenant']" exists
    When selector ".create-processor-branch [data-value='visual_correlate_tenant']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_orders"
    Then selector ".create-processor-input [data-value='visual_orders']" exists
    When selector ".create-processor-input [data-value='visual_orders']" is clicked
    And selector ".create-processor-right-input .create-choice-search" is filled with "visual_payments"
    Then selector ".create-processor-right-input [data-value='visual_payments']" exists
    When selector ".create-processor-right-input [data-value='visual_payments']" is clicked
    # Each side's fields enter the correlation in their own explicit scope.
    And selector ".create-correlate-left-fields .create-choice-search" is filled with "order_id"
    Then selector ".create-correlate-left-fields [data-value='order_id']" exists
    When selector ".create-correlate-left-fields [data-value='order_id']" is clicked
    Then selector ".create-correlate-where" has value "left.order_id"
    When selector ".create-correlate-where" is filled with "left.order_id ="
    And selector ".create-correlate-right-fields .create-choice-search" is filled with "order_id"
    Then selector ".create-correlate-right-fields [data-value='order_id']" exists
    When selector ".create-correlate-right-fields [data-value='order_id']" is clicked
    Then selector ".create-correlate-where" has value "left.order_id = right.order_id"
    When selector ".create-correlator-match [data-match='earliest']" is clicked
    And selector ".create-correlator-max-time" is filled with "10s"
    And selector ".create-correlator-left-timeout [data-timeout='send']" is clicked
    And selector ".create-correlator-left-timeout-relay .create-choice-search" is filled with "visual_"
    Then selector ".create-correlator-left-timeout-relay [data-value='visual_unpaid_orders']" exists
    And selector ".create-correlator-left-timeout-relay [data-value='visual_paid_orders']" does not exist
    When selector ".create-correlator-left-timeout-relay [data-value='visual_unpaid_orders']" is clicked
    And selector ".create-correlator-right-timeout [data-timeout='drop']" is clicked
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_paid_orders"
    Then selector ".create-processor-route-relay [data-value='visual_paid_orders']" exists
    # A correlator route constructs its output with SET alone, so it offers no inheritance.
    And selector ".create-processor-inherit" does not exist
    When selector ".create-processor-route-relay [data-value='visual_paid_orders']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "tenant"
    Then selector ".create-processor-output-fields [data-value='tenant']" exists
    When selector ".create-processor-output-fields [data-value='tenant']" is clicked
    And selector ".create-route-left-fields .create-choice-search" is filled with "tenant"
    Then selector ".create-route-left-fields [data-value='tenant']" exists
    When selector ".create-route-left-fields [data-value='tenant']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "order_id"
    Then selector ".create-processor-output-fields [data-value='order_id']" exists
    When selector ".create-processor-output-fields [data-value='order_id']" is clicked
    And selector ".create-route-left-fields .create-choice-search" is filled with "order_id"
    Then selector ".create-route-left-fields [data-value='order_id']" exists
    When selector ".create-route-left-fields [data-value='order_id']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "item"
    Then selector ".create-processor-output-fields [data-value='item']" exists
    When selector ".create-processor-output-fields [data-value='item']" is clicked
    And selector ".create-route-left-fields .create-choice-search" is filled with "item"
    Then selector ".create-route-left-fields [data-value='item']" exists
    When selector ".create-route-left-fields [data-value='item']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "amount"
    Then selector ".create-processor-output-fields [data-value='amount']" exists
    When selector ".create-processor-output-fields [data-value='amount']" is clicked
    And selector ".create-route-right-fields .create-choice-search" is filled with "amount"
    Then selector ".create-route-right-fields [data-value='amount']" exists
    When selector ".create-route-right-fields [data-value='amount']" is clicked
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE CORRELATOR visual_correlate_stage"
    And selector ".create-preview" contains "LEFT FROM visual_orders RIGHT FROM visual_payments ATTACHED CORRELATE WHERE left.order_id = right.order_id MATCH EARLIEST MAX TIME 10s ON CORRELATION TIMEOUT SEND TO visual_unpaid_orders, DROP BRANCHED BY visual_correlate_tenant"
    And selector ".create-preview" contains "SET tenant = left.tenant, order_id = left.order_id, item = left.item, amount = right.amount FLUSH IMMEDIATE"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION visual_paid_subscription TO visual_paid_orders;
      CREATE SUBSCRIPTION visual_unpaid_subscription TO visual_unpaid_orders;
      START;
      """
    # Both tenants place order o1; each payment matches only the order of its own branch.
    And http payload is posted to host "correlate-{{test_id}}.example.com" path "/orders"
      """
      [
        {"tenant":"a","order_id":"o1","item":"lamp"},
        {"tenant":"b","order_id":"o1","item":"desk"},
        {"tenant":"a","order_id":"o2","item":"rug"}
      ]
      """
    And http payload is posted to host "correlate-{{test_id}}.example.com" path "/payments"
      """
      [
        {"tenant":"b","order_id":"o1","amount":200},
        {"tenant":"a","order_id":"o1","amount":100}
      ]
      """
    # Order o2 of tenant a has no payment, so it reaches the timeout relay once MAX TIME passes.
    Then within "60s" the relay subscription receives exactly one payload for each fragment set
      """
      "tenant":"a" | "order_id":"o1" | "item":"lamp" | "amount":100
      "tenant":"b" | "order_id":"o1" | "item":"desk" | "amount":200
      "tenant":"a" | "order_id":"o2" | "item":"rug"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_window_validation
  Scenario: A window draft refuses missing bounds and SET routes, and reports aggregate scope and required-field refusals from validation
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_bounds_event (tenant STRING, latency I64);
      CREATE SCHEMA visual_bounds_summary (samples I64, total I64);
      CREATE RELAY visual_bounds_input SCHEMA visual_bounds_event UNBRANCHED;
      CREATE RELAY visual_bounds_output SCHEMA visual_bounds_summary UNBRANCHED;
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='window-processor']" is clicked
    And selector ".create-name" is filled with "visual_bounds_window"
    And selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_bounds_input"
    Then selector ".create-processor-input [data-value='visual_bounds_input']" exists
    When selector ".create-processor-input [data-value='visual_bounds_input']" is clicked
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_bounds_output"
    Then selector ".create-processor-route-relay [data-value='visual_bounds_output']" exists
    When selector ".create-processor-route-relay [data-value='visual_bounds_output']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "WIDTH"
    When selector ".create-window-width-duration" is filled with "10s"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "STEP"
    When selector ".create-window-step-duration" is filled with "5s"
    And selector ".create-window-state-limit" is clicked
    And selector ".create-window-state-size" is filled with "0B"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "MAX STATE SIZE"
    When selector ".create-window-state-size" is filled with "1MiB"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "SET assignment"
    # Live input outside an aggregate argument is refused by validation.
    When selector ".create-processor-output-fields .create-choice-search" is filled with "samples"
    Then selector ".create-processor-output-fields [data-value='samples']" exists
    When selector ".create-processor-output-fields [data-value='samples']" is clicked
    And selector ".create-processor-assignment-expression" is filled with "input.latency"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "visual_bounds_window"
    # A nested aggregate stays invalid.
    When selector ".create-processor-assignment-expression" is filled with "COUNT(SUM(input.latency))"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "aggregate functions must not be nested inside aggregate arguments"
    # A scalar combination of aggregates is valid, but every required output field must be set.
    When selector ".create-processor-assignment-expression" is filled with "COUNT(input.latency) + 1"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "total"
    When selector ".create-processor-output-fields .create-choice-search" is filled with "total"
    Then selector ".create-processor-output-fields [data-value='total']" exists
    When selector ".create-processor-output-fields [data-value='total']" is clicked
    And selector ".create-window-aggregate" is filled with "SUM"
    And selector ".create-window-input-fields .create-choice-search" is filled with "latency"
    Then selector ".create-window-input-fields [data-value='latency']" exists
    When selector ".create-window-input-fields [data-value='latency']" is clicked
    And selector ".create-processor-route-where" is filled with "output.samples > 1"
    Then selector ".create-preview" contains "WIDTH 10s DURATION STEP 5s DURATION MAX STATE SIZE 1048576B UNBRANCHED"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE WINDOW PROCESSOR visual_bounds_window;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "SET samples = count(input.latency) + 1"
    And selector ".terminal" contains "WHERE output.samples > 1"

  @visual_correlator_validation
  Scenario: A correlator draft requires both sides, its correlation, match, time and timeout policies, and reports a prohibited input scope
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_check_order (order_id STRING, item STRING);
      CREATE SCHEMA visual_check_payment (order_id STRING, amount I64);
      CREATE SCHEMA visual_check_paid (order_id STRING, amount I64);
      CREATE RELAY visual_check_orders SCHEMA visual_check_order UNBRANCHED;
      CREATE RELAY visual_check_payments SCHEMA visual_check_payment UNBRANCHED;
      CREATE RELAY visual_check_refunds SCHEMA visual_check_payment UNBRANCHED;
      CREATE RELAY visual_check_paid_orders SCHEMA visual_check_paid UNBRANCHED;
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='correlator']" is clicked
    And selector ".create-name" is filled with "visual_check_correlator"
    And selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_check_orders"
    Then selector ".create-processor-input [data-value='visual_check_orders']" exists
    When selector ".create-processor-input [data-value='visual_check_orders']" is clicked
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_check_paid_orders"
    Then selector ".create-processor-route-relay [data-value='visual_check_paid_orders']" exists
    When selector ".create-processor-route-relay [data-value='visual_check_paid_orders']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "RIGHT FROM input 1"
    # The first right relay may use another schema than the left side; later right relays share it.
    When selector ".create-processor-right-input .create-choice-search" is filled with "visual_check_payments"
    Then selector ".create-processor-right-input [data-value='visual_check_payments']" exists
    When selector ".create-processor-right-input [data-value='visual_check_payments']" is clicked
    And selector ".create-processor-right-add-input" is clicked
    And selector ".create-processor-right-input .create-choice-search" is filled with "visual_check_"
    Then selector ".create-processor-right-input [data-value='visual_check_refunds']" exists
    And selector ".create-processor-right-input [data-value='visual_check_orders']" does not exist
    When selector ".create-processor-right-input [data-value='visual_check_refunds']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "CORRELATE WHERE"
    When selector ".create-correlate-where" is filled with "left.order_id = right.order_id"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "MATCH EARLIEST or MATCH LATEST"
    When selector ".create-correlator-match [data-match='latest']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "MAX TIME"
    When selector ".create-correlator-max-time" is filled with "30s"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "left timeout"
    When selector ".create-correlator-left-timeout [data-timeout='drop']" is clicked
    And selector ".create-correlator-right-timeout [data-timeout='send']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "right timeout relay"
    When selector ".create-correlator-right-timeout-relay .create-choice-search" is filled with "visual_check_"
    Then selector ".create-correlator-right-timeout-relay [data-value='visual_check_refunds']" exists
    And selector ".create-correlator-right-timeout-relay [data-value='visual_check_orders']" does not exist
    When selector ".create-correlator-right-timeout-relay [data-value='visual_check_refunds']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "SET assignment"
    When selector ".create-processor-output-fields .create-choice-search" is filled with "order_id"
    Then selector ".create-processor-output-fields [data-value='order_id']" exists
    When selector ".create-processor-output-fields [data-value='order_id']" is clicked
    And selector ".create-processor-output-assignments .create-processor-assignment-row:last-of-type .create-processor-assignment-expression" is filled with "input.order_id"
    And selector ".create-processor-output-fields .create-choice-search" is filled with "amount"
    Then selector ".create-processor-output-fields [data-value='amount']" exists
    When selector ".create-processor-output-fields [data-value='amount']" is clicked
    And selector ".create-processor-output-assignments .create-processor-assignment-row:last-of-type .create-processor-assignment-expression" is filled with "right.amount"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "FLUSH IMMEDIATE or FLUSH EACH"
    When selector ".create-processor-flush [data-flush='each']" is clicked
    And selector ".create-processor-flush-interval" is filled with "100ms"
    And selector ".create-processor-flush-size" is filled with "1MiB"
    And selector ".create-submit" is clicked
    # A correlator exposes only its explicit left and right scopes.
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "visual_check_correlator"
    When selector ".create-processor-output-assignments .create-processor-assignment-row:first-of-type .create-processor-assignment-expression" is filled with "left.order_id"
    Then selector ".create-preview" contains "RIGHT FROM visual_check_payments, visual_check_refunds ATTACHED CORRELATE WHERE left.order_id = right.order_id MATCH LATEST MAX TIME 30s ON CORRELATION TIMEOUT DROP, SEND TO visual_check_refunds UNBRANCHED"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE CORRELATOR visual_check_correlator;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "ON CORRELATION TIMEOUT DROP, SEND TO visual_check_refunds"
    And selector ".terminal" contains "FLUSH EACH 100ms MAX BATCH SIZE 1MiB"

  @visual_window_transaction
  Scenario: A window draft selects relays staged in its transaction and is queued there until COMMIT
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_staged_event (tenant STRING, latency I64);
      CREATE SCHEMA visual_staged_summary (samples I64);
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".prompt-row" contains "{{domain}} tx"
    When selector ".prompt-row input" is filled with "CREATE RELAY visual_staged_events SCHEMA visual_staged_event UNBRANCHED;"
    And selector ".prompt-row input" is pressed with "Enter"
    And selector ".transaction-indicator" is clicked
    Then selector ".transaction-inspector .inspector-summary" contains "1 accepted"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is clicked
    And selector ".prompt-row input" is filled with "CREATE RELAY visual_staged_summaries SCHEMA visual_staged_summary UNBRANCHED;"
    And selector ".prompt-row input" is pressed with "Enter"
    And selector ".transaction-indicator" is clicked
    Then selector ".transaction-inspector .inspector-summary" contains "2 accepted"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='window-processor']" is clicked
    And selector ".create-name" is filled with "visual_staged_window"
    And selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_staged_events"
    Then selector ".create-processor-input [data-value='visual_staged_events']" exists
    When selector ".create-processor-input [data-value='visual_staged_events']" is clicked
    And selector ".create-window-width-messages" is filled with "10"
    And selector ".create-window-width-duration" is filled with "1m"
    And selector ".create-window-step-messages" is filled with "5"
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_staged_summaries"
    Then selector ".create-processor-route-relay [data-value='visual_staged_summaries']" exists
    When selector ".create-processor-route-relay [data-value='visual_staged_summaries']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "samples"
    Then selector ".create-processor-output-fields [data-value='samples']" exists
    When selector ".create-processor-output-fields [data-value='samples']" is clicked
    And selector ".create-window-aggregate" is filled with "COUNT"
    And selector ".create-window-input-fields .create-choice-search" is filled with "latency"
    Then selector ".create-window-input-fields [data-value='latency']" exists
    When selector ".create-window-input-fields [data-value='latency']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "WIDTH 10 MESSAGES 1m DURATION STEP 5 MESSAGES UNBRANCHED"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Queued in transaction"
    When selector ".create-close" is clicked
    And selector ".transaction-indicator" is clicked
    Then selector ".transaction-inspector .inspector-summary" contains "3 accepted"
    And selector ".transaction-inspector .inspector-summary" contains "COMPLETE"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is clicked
    And selector ".prompt-row input" is filled with "COMMIT;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" eventually disappears
    When selector ".prompt-row input" is filled with "SHOW CREATE WINDOW PROCESSOR visual_staged_window;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "SET samples = count(input.latency)"

  @visual_correlator_stale_scope
  Scenario: A correlator draft requires its left, right, timeout and output relays again after its domain changes
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the active domain is saved as placeholder "home"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{home}};
      CREATE UNPACED DOMAIN {{home}}_other;
      CREATE SCHEMA visual_stale_order (order_id STRING);
      CREATE SCHEMA visual_stale_payment (order_id STRING, amount I64);
      CREATE RELAY visual_stale_orders SCHEMA visual_stale_order UNBRANCHED;
      CREATE RELAY visual_stale_payments SCHEMA visual_stale_payment UNBRANCHED;
      CREATE RELAY visual_stale_unpaid SCHEMA visual_stale_order UNBRANCHED;
      CREATE RELAY visual_stale_paid SCHEMA visual_stale_payment UNBRANCHED;
      """
    Given the active domain is "{{home}}_other"
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA visual_stale_order (order_id STRING);
      CREATE SCHEMA visual_stale_payment (order_id STRING, amount I64);
      CREATE RELAY visual_stale_orders SCHEMA visual_stale_order UNBRANCHED;
      CREATE RELAY visual_stale_payments SCHEMA visual_stale_payment UNBRANCHED;
      CREATE RELAY visual_stale_unpaid SCHEMA visual_stale_order UNBRANCHED;
      CREATE RELAY visual_stale_paid SCHEMA visual_stale_payment UNBRANCHED;
      """
    Given the active domain is "{{home}}"
    When the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{home}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='correlator']" is clicked
    And selector ".create-name" is filled with "visual_stale_correlator"
    And selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_stale_orders"
    Then selector ".create-processor-input [data-value='visual_stale_orders']" exists
    When selector ".create-processor-input [data-value='visual_stale_orders']" is clicked
    And selector ".create-processor-right-input .create-choice-search" is filled with "visual_stale_payments"
    Then selector ".create-processor-right-input [data-value='visual_stale_payments']" exists
    When selector ".create-processor-right-input [data-value='visual_stale_payments']" is clicked
    And selector ".create-correlate-where" is filled with "left.order_id = right.order_id"
    And selector ".create-correlator-match [data-match='earliest']" is clicked
    And selector ".create-correlator-max-time" is filled with "10s"
    And selector ".create-correlator-left-timeout [data-timeout='send']" is clicked
    And selector ".create-correlator-left-timeout-relay .create-choice-search" is filled with "visual_stale_unpaid"
    Then selector ".create-correlator-left-timeout-relay [data-value='visual_stale_unpaid']" exists
    When selector ".create-correlator-left-timeout-relay [data-value='visual_stale_unpaid']" is clicked
    And selector ".create-correlator-right-timeout [data-timeout='drop']" is clicked
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_stale_paid"
    Then selector ".create-processor-route-relay [data-value='visual_stale_paid']" exists
    When selector ".create-processor-route-relay [data-value='visual_stale_paid']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "order_id"
    Then selector ".create-processor-output-fields [data-value='order_id']" exists
    When selector ".create-processor-output-fields [data-value='order_id']" is clicked
    And selector ".create-processor-output-assignments .create-processor-assignment-row:last-of-type .create-processor-assignment-expression" is filled with "left.order_id"
    And selector ".create-processor-output-fields .create-choice-search" is filled with "amount"
    Then selector ".create-processor-output-fields [data-value='amount']" exists
    When selector ".create-processor-output-fields [data-value='amount']" is clicked
    And selector ".create-processor-output-assignments .create-processor-assignment-row:last-of-type .create-processor-assignment-expression" is filled with "right.amount"
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE CORRELATOR visual_stale_correlator"
    When selector ".create-close" is clicked
    And selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{home}}_other']" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='correlator']" is clicked
    Then selector ".create-scope" contains "{{home}}"
    When selector ".create-scope-change" is clicked
    Then selector ".create-scope" contains "{{home}}_other"
    And selector ".create-name" has value "visual_stale_correlator"
    And selector ".create-correlate-where" has value "left.order_id = right.order_id"
    # Every relay the draft selected belongs to the previous domain, so each must be chosen again.
    When selector ".create-submit" is clicked
    Then selector ".create-validation" contains "LEFT FROM input 1: the relay belongs to a changed context"
    When selector ".create-processor-input .create-choice-search" is filled with "visual_stale_orders"
    Then selector ".create-processor-input [data-value='visual_stale_orders']" exists
    When selector ".create-processor-input [data-value='visual_stale_orders']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "RIGHT FROM input 1: the relay belongs to a changed context"
    When selector ".create-processor-right-input .create-choice-search" is filled with "visual_stale_payments"
    Then selector ".create-processor-right-input [data-value='visual_stale_payments']" exists
    When selector ".create-processor-right-input [data-value='visual_stale_payments']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "The left timeout relay belongs to a changed context"
    When selector ".create-correlator-left-timeout-relay .create-choice-search" is filled with "visual_stale_unpaid"
    Then selector ".create-correlator-left-timeout-relay [data-value='visual_stale_unpaid']" exists
    When selector ".create-correlator-left-timeout-relay [data-value='visual_stale_unpaid']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "output relay belongs to a changed context"
    And selector ".terminal" does not contain "visual_stale_correlator"

  @visual_correlator_reconnect
  Scenario: A selected correlator draft submits after the console follows a new leader
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_follow_order (order_id STRING);
      CREATE SCHEMA visual_follow_payment (order_id STRING, amount I64);
      CREATE RELAY visual_follow_orders SCHEMA visual_follow_order UNBRANCHED;
      CREATE RELAY visual_follow_payments SCHEMA visual_follow_payment UNBRANCHED;
      CREATE RELAY visual_follow_unpaid SCHEMA visual_follow_order UNBRANCHED;
      CREATE RELAY visual_follow_paid SCHEMA visual_follow_payment UNBRANCHED;
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    When leadership is transferred from node "{{new_leader}}" to node "{{old_leader}}"
    And the web console is opened on node "{{old_leader}}"
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='correlator']" is clicked
    And selector ".create-name" is filled with "visual_follow_correlator"
    And selector ".create-processor-branching [data-branch='unbranched']" is clicked
    And selector ".create-processor-input .create-choice-search" is filled with "visual_follow_orders"
    Then selector ".create-processor-input [data-value='visual_follow_orders']" exists
    When selector ".create-processor-input [data-value='visual_follow_orders']" is clicked
    And selector ".create-processor-right-input .create-choice-search" is filled with "visual_follow_payments"
    Then selector ".create-processor-right-input [data-value='visual_follow_payments']" exists
    When selector ".create-processor-right-input [data-value='visual_follow_payments']" is clicked
    And selector ".create-correlate-where" is filled with "left.order_id = right.order_id"
    And selector ".create-correlator-match [data-match='latest']" is clicked
    And selector ".create-correlator-max-time" is filled with "30s"
    And selector ".create-correlator-left-timeout [data-timeout='send']" is clicked
    And selector ".create-correlator-left-timeout-relay .create-choice-search" is filled with "visual_follow_unpaid"
    Then selector ".create-correlator-left-timeout-relay [data-value='visual_follow_unpaid']" exists
    When selector ".create-correlator-left-timeout-relay [data-value='visual_follow_unpaid']" is clicked
    And selector ".create-correlator-right-timeout [data-timeout='drop']" is clicked
    And selector ".create-processor-route-relay .create-choice-search" is filled with "visual_follow_paid"
    Then selector ".create-processor-route-relay [data-value='visual_follow_paid']" exists
    When selector ".create-processor-route-relay [data-value='visual_follow_paid']" is clicked
    And selector ".create-processor-output-fields .create-choice-search" is filled with "order_id"
    Then selector ".create-processor-output-fields [data-value='order_id']" exists
    When selector ".create-processor-output-fields [data-value='order_id']" is clicked
    And selector ".create-processor-output-assignments .create-processor-assignment-row:last-of-type .create-processor-assignment-expression" is filled with "left.order_id"
    And selector ".create-processor-output-fields .create-choice-search" is filled with "amount"
    Then selector ".create-processor-output-fields [data-value='amount']" exists
    When selector ".create-processor-output-fields [data-value='amount']" is clicked
    And selector ".create-processor-output-assignments .create-processor-assignment-row:last-of-type .create-processor-assignment-expression" is filled with "right.amount"
    And selector ".create-processor-flush [data-flush='immediate']" is clicked
    And selector ".create-processor-message-error [data-error='log']" is clicked
    And leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{old_leader}}" eventually reports leader "{{new_leader}}"
    And selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".create-preview" contains "CREATE CORRELATOR visual_follow_correlator"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And selector ".prompt-row input" is filled with "SHOW CREATE CORRELATOR visual_follow_correlator;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "LEFT FROM visual_follow_orders"
    And selector ".terminal" contains "RIGHT FROM visual_follow_payments"
    And selector ".terminal" contains "ON CORRELATION TIMEOUT SEND TO visual_follow_unpaid, DROP"
