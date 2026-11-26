Feature: DURATION values in user code
  A WASM guest reads and writes a DURATION as Arrow Duration(Nanosecond), a signed count of
  nanoseconds, and keeps one in its saved state like any other value. A Roto UDF takes a DURATION as a
  DurationColumn, computes with it directly and returns one; a constant is written with a unit
  function such as seconds(30) or millis(250).

  Scenario Outline: A WASM guest keeps a running total of DURATION values in its saved state across a restart and returns it in each branch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_wait_totals;
      UPLOAD RESOURCE wasm_wait_totals VERSION '{{wasm_processor}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA metric (value I32, tenant STRING);
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT (value integer, tenant string);
      CREATE WIRE JSON CODEC metric_codec FROM WIRE SCHEMA metric_wire TO SCHEMA metric;
      CREATE SCHEMA timed_metric (value I32, tenant STRING, wait DURATION);
      CREATE SCHEMA timed_total (value I32, tenant STRING, wait DURATION, total_wait DURATION);
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY timed_metrics SCHEMA timed_metric BRANCHED BY by_tenant;
      CREATE RELAY wait_totals SCHEMA timed_total BRANCHED BY by_tenant;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/metrics';
      CREATE HTTP ENDPOINT INGESTOR metric_ingestor
        FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING metric_codec
        TO timed_metrics
        INHERIT ALL
        SET wait = CASE message.value
            WHEN 1 AS I32 THEN 1s
            WHEN 2 AS I32 THEN 2s
            WHEN 11 AS I32 THEN 250ms
            WHEN 12 AS I32 THEN 500ms
            ELSE 0s
          END
        BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR total_waits FROM timed_metrics
        USING RESOURCE wasm_wait_totals VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        MAX TABLE ELEMENTS 4096
        BRANCHED BY by_tenant
        TO wait_totals
        SET value = value, tenant = tenant, wait = wait, total_wait = total_wait
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      CREATE SUBSCRIPTION wait_totals_subscription TO wait_totals;
      START;
      """
    # The reference guest keeps every second row of a branch, counted from its saved row ordinal, and
    # sums the wait of every row it accepts into a total it saves beside that ordinal.
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":1,"tenant":"acme"}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":11,"tenant":"beta"}
      """
    Then the relay subscription does not receive a payload within "1500ms"
    When the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    When these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION wait_totals_subscription TO wait_totals;
      """
    Then within "60s" repeatedly posting http payload to host "http-{{test_id}}.example.com" path "/metrics" yields a relay subscription payload
      """
      {"value":2,"tenant":"probe"}
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":12,"tenant":"beta"}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/metrics"
      """
      {"value":2,"tenant":"acme"}
      """
    # Each total holds the wait of the branch's first row, which the restored guest kept in its state.
    Then within "60s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "tenant":"acme" | "value":2 | "wait":"2s" | "total_wait":"3s"
      key={"tenant":"beta"} | "tenant":"beta" | "value":12 | "wait":"500ms" | "total_wait":"750ms"
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW WASM PROCESSOR total_waits STATE FOR BRANCH KEY { tenant = 'beta' };
      """
    Then the last command output contains
      """
      name=waits rows=1 fields=total_wait DURATION
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: A Roto UDF computes with DURATION values and returns them in each branch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE UDF padded
        WITH ROTO_0_13
        ARGS (wait DURATION)
        RETURNS DURATION
        CODE $roto$
          fn padded(wait: DurationColumn) -> DurationColumn {
              wait.add(wait).add_s(millis(250))
          }
        $roto$;
      CREATE UDF scaled
        WITH ROTO_0_13
        ARGS (wait DURATION)
        RETURNS DURATION
        CODE $roto$
          fn scaled(wait: DurationColumn) -> DurationColumn {
              wait.mul_s(3).div_s(2)
          }
        $roto$;
      CREATE UDF is_long
        WITH ROTO_0_13
        ARGS (wait DURATION)
        RETURNS BOOL
        CODE $roto$
          fn is_long(wait: DurationColumn) -> BoolColumn {
              wait.gt_s(seconds(1))
          }
        $roto$;
      CREATE UDF wait_nanos
        WITH ROTO_0_13
        ARGS (wait DURATION)
        RETURNS I64
        CODE $roto$
          fn wait_nanos(wait: DurationColumn) -> I64Column {
              wait.nanos()
          }
        $roto$;
      CREATE UDF kept_steps
        WITH ROTO_0_13
        ARGS (steps <duration_list>)
        RETURNS <duration_list>
        CODE $roto$
          fn kept_steps(steps: VecDurationColumn) -> VecDurationColumn {
              steps
          }
        $roto$;
      CREATE SCHEMA job (tenant STRING, id STRING, priority I64);
      CREATE SCHEMA job_wait (
        tenant STRING,
        id STRING,
        wait DURATION,
        padded DURATION,
        scaled DURATION,
        is_long BOOL,
        nanos I64,
        steps <duration_list>
      );
      CREATE JAQ JSON CODEC job_codec TO SCHEMA job WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE PARAMETRIZER by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY jobs SCHEMA job BRANCHED BY by_tenant;
      CREATE RELAY job_waits SCHEMA job_wait BRANCHED BY by_tenant;
      CREATE VHOST edge duration-udf-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/jobs';
      CREATE HTTP ENDPOINT INGESTOR job_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING job_codec
        TO jobs INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION time_jobs FROM jobs BRANCHED BY by_tenant
        TO job_waits
          SET tenant = input.tenant,
              id = input.id,
              wait = CASE WHEN input.priority > 5 THEN 1500ms ELSE 250ms END,
              padded = udf::padded(output.wait),
              scaled = udf::scaled(output.wait),
              is_long = udf::is_long(output.wait),
              nanos = udf::wait_nanos(output.wait),
              steps = udf::kept_steps(vec(output.wait, 2m))
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION job_waits_subscription TO job_waits;
      START;
      """
    And http payload is posted to node "node-1" with host "duration-udf-{{test_id}}.example.com" path "/jobs"
      """
      [{"tenant":"acme","id":"a1","priority":7},{"tenant":"beta","id":"b1","priority":2},{"tenant":"acme","id":"a2","priority":1},{"tenant":"beta","id":"b2","priority":9}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "id":"a1" | "wait":"1s 500ms" | "padded":"3s 250ms" | "scaled":"2s 250ms" | "is_long":true | "nanos":1500000000 | "steps":["1s 500ms","2m"]
      key={"tenant":"beta"} | "id":"b1" | "wait":"250ms" | "padded":"750ms" | "scaled":"375ms" | "is_long":false | "nanos":250000000 | "steps":["250ms","2m"]
      key={"tenant":"acme"} | "id":"a2" | "wait":"250ms" | "padded":"750ms" | "scaled":"375ms" | "is_long":false | "nanos":250000000 | "steps":["250ms","2m"]
      key={"tenant":"beta"} | "id":"b2" | "wait":"1s 500ms" | "padded":"3s 250ms" | "scaled":"2s 250ms" | "is_long":true | "nanos":1500000000 | "steps":["1s 500ms","2m"]
      """

    Examples:
      | cluster_size | replica_count | duration_list |
      | 1            | 0             | VEC<DURATION> |
      | 3            | 0             | VEC<DURATION> |
