Feature: Duration values
  A DURATION is a signed length of time counted in nanoseconds. A schema field holds one, an
  expression writes one as a literal such as 30s, 250ms or 1h 30m, and two durations compare. Text
  shows a duration in the literal's own form.

  Scenario Outline: A route sets DURATION fields from literals and compares them in each branch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA job (tenant STRING, id STRING, priority I64);
      CREATE SCHEMA job_plan (
        tenant STRING,
        id STRING,
        wait DURATION,
        retry DURATION OPTIONAL,
        steps <duration_list>,
        bounds <duration_pair>,
        step_count I64,
        first_step DURATION OPTIONAL,
        second_step DURATION OPTIONAL,
        longest_step DURATION OPTIONAL,
        later_steps <duration_list>,
        all_steps <duration_list>,
        quick BOOL,
        bounded BOOL,
        listed BOOL,
        has_step BOOL,
        changed BOOL,
        floor DURATION,
        clamped DURATION,
        unless_default DURATION OPTIONAL
      );
      CREATE SCHEMA quick_plan (tenant STRING, id STRING, quick_wait DURATION);
      CREATE JAQ JSON CODEC job_codec TO SCHEMA job WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY jobs SCHEMA job BRANCHED BY by_tenant;
      CREATE RELAY job_plans SCHEMA job_plan BRANCHED BY by_tenant;
      CREATE RELAY quick_plans SCHEMA quick_plan BRANCHED BY by_tenant;
      CREATE VHOST edge duration-values-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/jobs';
      CREATE HTTP ENDPOINT INGESTOR job_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING job_codec
        TO jobs INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION plan_jobs FROM jobs BRANCHED BY by_tenant
        TO job_plans
          SET tenant = input.tenant,
              id = input.id,
              wait = CASE WHEN input.priority > 5 THEN 30s ELSE 1h 30m END,
              retry = IF input.priority > 9 THEN 250ms ELSE NULL END,
              steps = vec(1s, 1.5s, 2m),
              bounds = [1d, 36h],
              step_count = count(output.steps),
              first_step = first(output.steps),
              second_step = nth(output.steps, 1),
              longest_step = max(output.steps),
              later_steps = slice(output.steps, 1, 2),
              all_steps = concat(output.steps, vec(1500ms)),
              quick = output.wait < 1m,
              bounded = coalesce(output.retry, 5s) BETWEEN 1s AND 10s,
              listed = output.wait IN (30s, 45s),
              has_step = contains(output.steps, 1500ms),
              changed = output.wait IS DISTINCT FROM 30s,
              floor = greatest(output.wait, 1m),
              clamped = clamp(output.wait, 1m, 1h),
              unless_default = nullif(output.wait, 30s)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        TO quick_plans
          SET tenant = input.tenant,
              id = input.id,
              quick_wait = CASE input.priority WHEN 2 THEN 1h 30m ELSE 30s END
          WHERE output.quick_wait <= 1m
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION job_plans_subscription TO job_plans;
      CREATE SUBSCRIPTION quick_plans_subscription TO quick_plans WHERE input.quick_wait BETWEEN 10s AND 1m;
      START;
      """
    And http payload is posted to node "node-1" with host "duration-values-{{test_id}}.example.com" path "/jobs"
      """
      [{"tenant":"acme","id":"a1","priority":7},{"tenant":"beta","id":"b1","priority":2},{"tenant":"acme","id":"a2","priority":10},{"tenant":"beta","id":"b2","priority":9}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} | "id":"a1" | "wait":"30s" | "steps":["1s","1s 500ms","2m"] | "bounds":["1d","1d 12h"] | "step_count":3 | "first_step":"1s" | "second_step":"1s 500ms" | "longest_step":"2m" | "later_steps":["1s 500ms","2m"] | "all_steps":["1s","1s 500ms","2m","1s 500ms"] | "quick":true | "bounded":true | "listed":true | "has_step":true | "changed":false | "floor":"1m" | "clamped":"1m"
      key={"tenant":"beta"} | "id":"b1" | "wait":"1h 30m" | "quick":false | "bounded":true | "listed":false | "changed":true | "floor":"1h 30m" | "clamped":"1h" | "unless_default":"1h 30m"
      key={"tenant":"acme"} | "id":"a2" | "wait":"30s" | "retry":"250ms" | "bounded":false | "listed":true
      key={"tenant":"beta"} | "id":"b2" | "wait":"30s" | "quick":true | "bounded":true
      key={"tenant":"acme"} | "id":"a1" | "quick_wait":"30s"
      key={"tenant":"acme"} | "id":"a2" | "quick_wait":"30s"
      key={"tenant":"beta"} | "id":"b2" | "quick_wait":"30s"
      """
    And the relay subscription does not receive a payload containing fragments within "1s"
      """
      "quick_wait":"1h 30m"
      """

    Examples:
      | cluster_size | replica_count | duration_list | duration_pair      |
      | 1            | 0             | VEC<DURATION> | ARRAY<DURATION, 2> |
      | 3            | 0             | VEC<DURATION> | ARRAY<DURATION, 2> |

  Scenario Outline: A DURATION branch key field keeps each length of time in its own branch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA sample (id STRING, priority I64);
      CREATE SCHEMA timed_sample (id STRING, window DURATION);
      CREATE SCHEMA window_count (window DURATION, samples I64, first_id STRING);
      CREATE JAQ JSON CODEC sample_codec TO SCHEMA sample WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA window_branch (window DURATION);
      CREATE BRANCH by_window SCHEMA window_branch TTL 5m;
      CREATE RELAY timed_samples SCHEMA timed_sample BRANCHED BY by_window;
      CREATE RELAY window_counts SCHEMA window_count BRANCHED BY by_window;
      CREATE VHOST edge duration-branches-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/samples';
      CREATE HTTP ENDPOINT INGESTOR sample_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING sample_codec
        TO timed_samples
          SET id = input.id,
              window = CASE WHEN input.priority > 5 THEN 5m ELSE 1h END
          BRANCHED BY by_window
          SET window = message.window
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR count_windows FROM timed_samples WIDTH 2 MESSAGES STEP 2 MESSAGES
        BRANCHED BY by_window TO window_counts
        SET window = FIRST(input.window),
            samples = COUNT(input.id),
            first_id = FIRST(input.id)
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION window_counts_subscription TO window_counts;
      START;
      """
    And http payload is posted to node "node-1" with host "duration-branches-{{test_id}}.example.com" path "/samples"
      """
      [{"id":"fast-1","priority":9},{"id":"slow-1","priority":1},{"id":"fast-2","priority":8},{"id":"slow-2","priority":2}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"window":"5m"} payload={"first_id":"fast-1","samples":2,"window":"5m"}
      key={"window":"1h"} payload={"first_id":"slow-1","samples":2,"window":"1h"}
      """
    And within "5s" node "node-1" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY timed_samples WHERE (window = 5m);
      """
    And within "5s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY timed_samples WHERE (window = 2h);
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: A deduplicator and a reorderer key their records by a DURATION in each branch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA request (tenant STRING, id STRING, priority I64);
      CREATE SCHEMA timed_request (tenant STRING, id STRING, wait DURATION);
      CREATE SCHEMA first_wait (tenant STRING, id STRING, wait DURATION);
      CREATE JAQ JSON CODEC request_codec TO SCHEMA request WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY timed_requests SCHEMA timed_request BRANCHED BY by_tenant;
      CREATE RELAY first_waits SCHEMA first_wait BRANCHED BY by_tenant;
      CREATE RELAY ordered_waits SCHEMA first_wait BRANCHED BY by_tenant;
      CREATE VHOST edge duration-keys-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/requests';
      CREATE HTTP ENDPOINT INGESTOR request_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING request_codec
        TO timed_requests
          SET tenant = input.tenant,
              id = input.id,
              wait = CASE WHEN input.priority > 5 THEN 30s ELSE 2m END
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE DEDUPLICATOR first_per_wait FROM timed_requests
        DEDUPLICATE ON input.wait
        MAX TIME 10m
        BRANCHED BY by_tenant
        TO first_waits INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE REORDERER order_by_wait FROM timed_requests
        COLLECT FOR 2s MAX BATCH SIZE 1MiB
        BY input.wait
        MAX TIME 10m
        BRANCHED BY by_tenant
        TO ordered_waits INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION first_waits_subscription TO first_waits;
      START;
      """
    And http payload is posted to node "node-1" with host "duration-keys-{{test_id}}.example.com" path "/requests"
      """
      [{"tenant":"acme","id":"a1","priority":9},{"tenant":"beta","id":"b1","priority":9},{"tenant":"acme","id":"a2","priority":1},{"tenant":"acme","id":"a3","priority":8},{"tenant":"beta","id":"b2","priority":9},{"tenant":"beta","id":"b3","priority":1}]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"acme"} | "id":"a1" | "wait":"30s"
      key={"tenant":"beta"} | "id":"b1" | "wait":"30s"
      key={"tenant":"acme"} | "id":"a2" | "wait":"2m"
      key={"tenant":"beta"} | "id":"b3" | "wait":"2m"
      """
    And the relay subscription does not receive a payload containing fragments within "1s"
      """
      "id":"a3"
      "id":"b2"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Window aggregates order, count and collect DURATION values in each branch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA attempt (tenant STRING, id STRING, priority I64);
      CREATE SCHEMA timed_attempt (tenant STRING, id STRING, wait DURATION);
      CREATE SCHEMA wait_summary (
        tenant STRING,
        shortest DURATION OPTIONAL,
        longest DURATION OPTIONAL,
        first_wait DURATION OPTIONAL,
        last_wait DURATION OPTIONAL,
        waits <duration_list>,
        quickest_id STRING OPTIONAL,
        slowest_id STRING OPTIONAL,
        distinct_waits I64,
        attempts I64
      );
      CREATE JAQ JSON CODEC attempt_codec TO SCHEMA attempt WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY timed_attempts SCHEMA timed_attempt BRANCHED BY by_tenant;
      CREATE RELAY wait_summaries SCHEMA wait_summary BRANCHED BY by_tenant;
      CREATE VHOST edge duration-windows-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/attempts';
      CREATE HTTP ENDPOINT INGESTOR attempt_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING attempt_codec
        TO timed_attempts
          SET tenant = input.tenant,
              id = input.id,
              wait = CASE input.priority WHEN 1 THEN 2m WHEN 2 THEN 750ms ELSE 45s END
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR summarize_waits FROM timed_attempts WIDTH 3 MESSAGES STEP 3 MESSAGES
        MAX STATE SIZE 1MiB BRANCHED BY by_tenant TO wait_summaries
        SET tenant = FIRST(input.tenant),
            shortest = MIN(input.wait),
            longest = MAX(input.wait),
            first_wait = FIRST(input.wait),
            last_wait = LAST(input.wait),
            waits = COLLECT_VEC(input.wait),
            quickest_id = ARG_MIN(input.id, input.wait),
            slowest_id = ARG_MAX(input.id, input.wait),
            distinct_waits = APPROX_COUNT_DISTINCT(input.wait, 10),
            attempts = COUNT(input.wait)
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION wait_summaries_subscription TO wait_summaries;
      START;
      """
    And http payload is posted to node "node-1" with host "duration-windows-{{test_id}}.example.com" path "/attempts"
      """
      [{"tenant":"acme","id":"a1","priority":3},{"tenant":"beta","id":"b1","priority":1},{"tenant":"acme","id":"a2","priority":1},{"tenant":"beta","id":"b2","priority":1},{"tenant":"acme","id":"a3","priority":2},{"tenant":"beta","id":"b3","priority":3}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} payload={"attempts":3,"distinct_waits":3,"first_wait":"45s","last_wait":"750ms","longest":"2m","quickest_id":"a3","shortest":"750ms","slowest_id":"a2","tenant":"acme","waits":["45s","2m","750ms"]}
      key={"tenant":"beta"} payload={"attempts":3,"distinct_waits":2,"first_wait":"2m","last_wait":"45s","longest":"2m","quickest_id":"b3","shortest":"45s","slowest_id":"b1","tenant":"beta","waits":["2m","2m","45s"]}
      """

    Examples:
      | cluster_size | replica_count | duration_list |
      | 1            | 0             | VEC<DURATION> |
      | 3            | 0             | VEC<DURATION> |

  Scenario Outline: A route reads a materialized DURATION field of its branch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA policy_update (tenant STRING, level I64);
      CREATE SCHEMA policy (tenant STRING, timeout DURATION);
      CREATE SCHEMA call (tenant STRING, id STRING, elapsed_ms I64);
      CREATE SCHEMA judged_call (tenant STRING, id STRING, timeout DURATION, strict BOOL);
      CREATE JAQ JSON CODEC policy_codec TO SCHEMA policy_update WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE JAQ JSON CODEC call_codec TO SCHEMA call WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY policies SCHEMA policy BRANCHED BY by_tenant WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY calls SCHEMA call BRANCHED BY by_tenant;
      CREATE RELAY judged_calls SCHEMA judged_call BRANCHED BY by_tenant;
      CREATE VHOST edge duration-state-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT policy_ingress ON edge PATH '/policies';
      CREATE HTTP ENDPOINT call_ingress ON edge PATH '/calls';
      CREATE HTTP ENDPOINT INGESTOR policy_ingestor FROM policy_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING policy_codec
        TO policies
          SET tenant = input.tenant,
              timeout = IF input.level > 1 THEN 1m 30s ELSE 10s END
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP ENDPOINT INGESTOR call_ingestor FROM call_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING call_codec
        TO calls INHERIT ALL BRANCHED BY by_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE JUNCTION judge_calls FROM calls BRANCHED BY by_tenant
        USING MATERIALIZED STATE policies REQUIRED WAIT
        TO judged_calls
          SET tenant = input.tenant,
              id = input.id,
              timeout = relay_state.policies.timeout,
              strict = relay_state.policies.timeout < 1m
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION judged_calls_subscription TO judged_calls;
      START;
      """
    And http payload is posted to node "node-1" with host "duration-state-{{test_id}}.example.com" path "/policies"
      """
      [{"tenant":"acme","level":2},{"tenant":"beta","level":0}]
      """
    Then within "10s" node "node-1" eventually reports materialized state for relay "policies" containing
      """
      {"branch":{"tenant":"acme"},"fields":{"tenant":"acme","timeout":"1m 30s"}}
      """
    When http payload is posted to node "node-1" with host "duration-state-{{test_id}}.example.com" path "/calls"
      """
      [{"tenant":"beta","id":"b1","elapsed_ms":5},{"tenant":"acme","id":"a1","elapsed_ms":7}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} payload={"id":"a1","strict":false,"tenant":"acme","timeout":"1m 30s"}
      key={"tenant":"beta"} payload={"id":"b1","strict":true,"tenant":"beta","timeout":"10s"}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: A codec or a UDF that would carry a DURATION is refused when it is applied
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA timed (id STRING, wait DURATION);
      CREATE SCHEMA timed_list (id STRING, waits <duration_list>);
      """
    When these Plexus commands fail with "<diagnostic>"
      """
      <statement>
      """

    Examples:
      | cluster_size | statement                                                                                                                                                                    | diagnostic                                                                          | duration_list |
      | 1            | CREATE WIRE JSON SCHEMA timed_wire MODE STRICT (id string, wait integer); CREATE WIRE JSON CODEC timed_codec FROM WIRE SCHEMA timed_wire TO SCHEMA timed;                    | codec 'timed_codec' in domain '{{domain}}' cannot carry DURATION field 'wait'       | VEC<DURATION> |
      | 3            | CREATE WIRE JSON SCHEMA timed_wire MODE STRICT (id string, wait string); CREATE WIRE JSON CODEC timed_codec FROM WIRE SCHEMA timed_wire TO SCHEMA timed;                     | codec 'timed_codec' in domain '{{domain}}' cannot carry DURATION field 'wait'       | VEC<DURATION> |
      | 1            | CREATE WIRE JSON SCHEMA timed_list_wire MODE STRICT (id string, waits array); CREATE WIRE JSON CODEC timed_list_codec FROM WIRE SCHEMA timed_list_wire TO SCHEMA timed_list; | codec 'timed_list_codec' in domain '{{domain}}' cannot carry DURATION field 'waits' | VEC<DURATION> |
      | 3            | CREATE WIRE CBOR SCHEMA timed_cbor MODE STRICT (id text, wait integer); CREATE WIRE CBOR CODEC timed_cbor_codec FROM WIRE SCHEMA timed_cbor TO SCHEMA timed;                 | codec 'timed_cbor_codec' in domain '{{domain}}' cannot carry DURATION field 'wait'  | VEC<DURATION> |
      | 1            | CREATE WIRE AVRO SCHEMA timed_avro MODE STRICT (id string, wait long); CREATE WIRE AVRO CODEC timed_avro_codec FROM WIRE SCHEMA timed_avro TO SCHEMA timed;                  | codec 'timed_avro_codec' in domain '{{domain}}' cannot carry DURATION field 'wait'  | VEC<DURATION> |
      | 3            | CREATE JAQ JSON CODEC timed_jaq TO SCHEMA timed WITH JAQ TRANSFORMATIONS ON INGESTION '.';                                                                                   | codec 'timed_jaq' in domain '{{domain}}' cannot carry DURATION field 'wait'         | VEC<DURATION> |
      | 1            | CREATE UDF wait_of WITH ROTO_0_13 ARGS (value DURATION) RETURNS I64 CODE $roto$ fn wait_of(value: I64Column) -> I64Column { value } $roto$;                                  | UDF 'wait_of' in domain '{{domain}}' cannot take DURATION argument 'value'          | VEC<DURATION> |
      | 3            | CREATE UDF wait_for WITH ROTO_0_13 ARGS (value I64) RETURNS DURATION CODE $roto$ fn wait_for(value: I64Column) -> I64Column { value } $roto$;                                | UDF 'wait_for' in domain '{{domain}}' cannot return a DURATION                      | VEC<DURATION> |

  Scenario Outline: A WASM processor whose guest would read a DURATION is refused when it is applied
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE guest;
      UPLOAD RESOURCE guest VERSION '{{wasm_processor}}';
      CREATE SCHEMA metric (value I32);
      CREATE SCHEMA timed_metric (value I32, wait DURATION);
      CREATE RELAY metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY timed_metrics SCHEMA timed_metric UNBRANCHED;
      """
    When these Plexus commands fail with "<diagnostic>"
      """
      CREATE WASM PROCESSOR filtering FROM <input> COLLECT FOR 10ms MAX BATCH SIZE 1MiB
        DETACHED USING RESOURCE guest VERSION 1 FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000 MAX MEMORY 64MiB
        UNBRANCHED TO <output> SET value = value
        ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;
      """

    Examples:
      | cluster_size | input         | output        | diagnostic                                                                                       |
      | 1            | timed_metrics | metrics       | WASM processor 'filtering' in domain '{{domain}}' cannot pass DURATION field 'wait' to its guest |
      | 3            | metrics       | timed_metrics | WASM processor 'filtering' in domain '{{domain}}' cannot pass DURATION field 'wait' to its guest |

  Scenario Outline: A database emitter declares a DURATION column value rejected when it is applied
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA timed (id STRING, wait DURATION);
      CREATE RELAY timed_events SCHEMA timed UNBRANCHED;
      CREATE POSTGRES CLIENT postgres_main POOL SIZE MIN 0 MAX 2 CONFIG { 'addr' = 'postgres://test:test@127.0.0.1:5432/test?sslmode=disable' };
      CREATE MYSQL CLIENT mysql_main POOL SIZE MIN 0 MAX 2 CONFIG { 'addr' = 'mysql://test:test@127.0.0.1:3306/test' };
      CREATE CLICKHOUSE CLIENT clickhouse_main CONFIG { 'addr' = 'http://127.0.0.1:8123', 'user' = 'default', 'database' = 'test', 'timeout_ms' = '1000' };
      """
    When these Plexus commands fail with "VALUES key 'wait' is a DURATION, which <phrase> declares rejected"
      """
      CREATE <phrase> EMITTER outgoing FROM timed_events TO <client> TABLE 'Orders'
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        VALUES { 'id' = input.id, 'wait' = input.wait }
        BATCH MAX MESSAGES 10 MAX SIZE 64KiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size | phrase           | client          |
      | 1            | POSTGRES TABLE   | postgres_main   |
      | 3            | POSTGRES TABLE   | postgres_main   |
      | 1            | MYSQL TABLE      | mysql_main      |
      | 1            | CLICKHOUSE TABLE | clickhouse_main |

  Scenario Outline: A DURATION literal names only units of a fixed length and converts to no other type
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA timed (id STRING, wait DURATION);
      CREATE SCHEMA plain (id STRING, count I64);
      CREATE RELAY timed_events SCHEMA timed UNBRANCHED;
      CREATE RELAY plain_events SCHEMA plain UNBRANCHED;
      """
    When these Plexus commands fail with "<diagnostic>"
      """
      CREATE JUNCTION check_wait FROM timed_events UNBRANCHED TO <target> SET id = input.id, <assignment> FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | target       | assignment                | diagnostic                                                   |
      | 1            | timed_events | wait = 1y                 | duration unit 'y' has no fixed length                        |
      | 3            | timed_events | wait = 2M                 | duration unit 'M' has no fixed length                        |
      | 1            | timed_events | wait = 1w                 | a DURATION literal takes the units ns, us, ms, s, m, h and d |
      | 3            | timed_events | wait = 106752d            | does not fit a DURATION                                      |
      | 1            | timed_events | wait = 30 AS DURATION     | converts to no other type                                    |
      | 3            | plain_events | count = input.wait AS I64 | converts to no other type                                    |
      | 1            | timed_events | wait = input.wait + 1s    | operator Add is not valid for Duration(Nanosecond)           |
