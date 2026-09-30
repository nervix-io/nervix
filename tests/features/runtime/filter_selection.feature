Feature: Filter selections
  Scenario Outline: FILTER WHERE keeps sparse, dense and no rows of interleaved branches, with and without row errors
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA reading (
        id STRING,
        tenant STRING,
        amount I64,
        divisor I64,
        wanted BOOL
      );
      CREATE SCHEMA filter_failure (
        failed_id STRING,
        tenant STRING,
        error_message STRING
      );
      CREATE CODEC reading_batch_codec
        FROM JSON
        TO SCHEMA reading
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY readings SCHEMA reading BRANCHED BY by_tenant;
      CREATE RELAY selected_readings SCHEMA reading BRANCHED BY by_tenant;
      CREATE RELAY filter_failures SCHEMA filter_failure UNBRANCHED;
      CREATE VHOST edge filter-selection-{{test_id}}.example.com;
      CREATE ENDPOINT reading_ingress ON edge PATH '/readings' TYPE HTTP;
      CREATE INGESTOR reading_source
        FROM ENDPOINT reading_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_batch_codec
        FILTER WHERE input.amount / input.divisor > 0
        TO readings
          INHERIT ALL
          BRANCHED BY by_tenant SET tenant = message.tenant
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR SEND TO filter_failures
          SET failed_id = input.id,
              tenant = input.tenant,
              error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE JUNCTION select_wanted
        FROM readings
        FILTER WHERE input.wanted
        BRANCHED BY by_tenant
        TO selected_readings
          INHERIT ALL
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION selected_readings_subscription TO selected_readings;
      CREATE SUBSCRIPTION filter_failures_subscription TO filter_failures;
      START;
      """
    # Each request is one decoded batch, so each payload is one execution of the ingestor's filter.
    # Seven of these eight rows pass it, and the junction's branch-local filter drops one more.
    And http payload is posted to node "node-1" with host "filter-selection-{{test_id}}.example.com" path "/readings"
      """
      [{"id":"kept-dense-1","tenant":"north","amount":10,"divisor":2,"wanted":true},{"id":"kept-dense-2","tenant":"south","amount":9,"divisor":3,"wanted":true},{"id":"kept-dense-3","tenant":"north","amount":4,"divisor":1,"wanted":true},{"id":"gone-dense-4","tenant":"south","amount":-6,"divisor":2,"wanted":true},{"id":"kept-dense-5","tenant":"south","amount":7,"divisor":7,"wanted":true},{"id":"kept-dense-6","tenant":"north","amount":12,"divisor":5,"wanted":true},{"id":"gone-dense-7","tenant":"north","amount":5,"divisor":1,"wanted":false},{"id":"kept-dense-8","tenant":"south","amount":3,"divisor":1,"wanted":true}]
      """
    # Two rows pass the ingestor's filter, one in each branch, and the junction keeps only the north
    # one, so the south branch's filter selects no row.
    And http payload is posted to node "node-1" with host "filter-selection-{{test_id}}.example.com" path "/readings"
      """
      [{"id":"gone-sparse-1","tenant":"south","amount":0,"divisor":4,"wanted":true},{"id":"gone-sparse-2","tenant":"north","amount":1,"divisor":2,"wanted":true},{"id":"gone-sparse-3","tenant":"south","amount":-9,"divisor":3,"wanted":true},{"id":"kept-sparse-4","tenant":"north","amount":8,"divisor":2,"wanted":true},{"id":"gone-sparse-5","tenant":"south","amount":6,"divisor":2,"wanted":false},{"id":"gone-sparse-6","tenant":"north","amount":-3,"divisor":1,"wanted":true},{"id":"gone-sparse-7","tenant":"north","amount":2,"divisor":5,"wanted":true},{"id":"gone-sparse-8","tenant":"south","amount":-1,"divisor":1,"wanted":true}]
      """
    # No row passes the ingestor's filter.
    And http payload is posted to node "node-1" with host "filter-selection-{{test_id}}.example.com" path "/readings"
      """
      [{"id":"gone-none-1","tenant":"north","amount":-1,"divisor":1,"wanted":true},{"id":"gone-none-2","tenant":"south","amount":0,"divisor":9,"wanted":true},{"id":"gone-none-3","tenant":"north","amount":2,"divisor":3,"wanted":true},{"id":"gone-none-4","tenant":"south","amount":-5,"divisor":5,"wanted":true}]
      """
    # A zero divisor fails its row in each branch. The failed rows go to the error route and every
    # other row of the batch is still selected or dropped by its own value.
    And http payload is posted to node "node-1" with host "filter-selection-{{test_id}}.example.com" path "/readings"
      """
      [{"id":"kept-failing-1","tenant":"north","amount":6,"divisor":3,"wanted":true},{"id":"gone-failing-2","tenant":"south","amount":4,"divisor":0,"wanted":true},{"id":"gone-failing-3","tenant":"north","amount":-2,"divisor":1,"wanted":true},{"id":"kept-failing-4","tenant":"south","amount":10,"divisor":5,"wanted":true},{"id":"gone-failing-5","tenant":"north","amount":1,"divisor":0,"wanted":false},{"id":"gone-failing-6","tenant":"south","amount":3,"divisor":1,"wanted":false},{"id":"kept-failing-7","tenant":"north","amount":9,"divisor":1,"wanted":true}]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "id":"kept-dense-1" | "tenant":"north"
      "id":"kept-dense-2" | "tenant":"south"
      "id":"kept-dense-3" | "tenant":"north"
      "id":"kept-dense-5" | "tenant":"south"
      "id":"kept-dense-6" | "tenant":"north"
      "id":"kept-dense-8" | "tenant":"south"
      "id":"kept-sparse-4" | "tenant":"north"
      "id":"kept-failing-1" | "tenant":"north"
      "id":"kept-failing-4" | "tenant":"south"
      "id":"kept-failing-7" | "tenant":"north"
      "failed_id":"gone-failing-2" | "tenant":"south" | division_by_zero
      "failed_id":"gone-failing-5" | "tenant":"north" | division_by_zero
      """
    And the relay subscription does not receive a payload within "3s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
