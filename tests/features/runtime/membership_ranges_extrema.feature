Feature: Membership, ranges, null-safe equality and scalar extrema
  Scenario Outline: Membership, ranges and null-safe equality classify every message, including nulls, empty sets, duplicate constants and large sets
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA shipment (
        id STRING,
        status STRING,
        region STRING OPTIONAL,
        home_region STRING OPTIONAL,
        priority I32,
        weight F64,
        low F64,
        high F64,
        reading STRING,
        code I64,
        shipped STRING
      );
      CREATE SCHEMA shipment_fact (
        id STRING,
        open BOOL,
        settled BOOL,
        known_region BOOL OPTIONAL,
        foreign_region BOOL OPTIONAL,
        membership_unknown BOOL,
        exclusion_unknown BOOL,
        in_nothing BOOL,
        outside_nothing BOOL,
        urgent BOOL,
        within_limits BOOL,
        outside_band BOOL,
        same_region BOOL,
        moved BOOL,
        zero_or_half BOOL,
        catalogued BOOL,
        launch_day BOOL
      );
      CREATE CODEC shipment_batch_codec
        FROM JSON
        TO SCHEMA shipment
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY shipments SCHEMA shipment UNBRANCHED;
      CREATE RELAY shipment_facts SCHEMA shipment_fact UNBRANCHED;
      CREATE VHOST edge membership-{{test_id}}.example.com;
      CREATE ENDPOINT shipment_ingress ON edge PATH '/shipments' TYPE HTTP;
      CREATE INGESTOR shipment_source
        FROM ENDPOINT shipment_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING shipment_batch_codec
        TO shipments
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION classify_shipments
        FROM shipments
        UNBRANCHED
        TO shipment_facts
          SET id = input.id,
              open = input.status IN ('open', 'held', 'open'),
              settled = input.status NOT IN ('open', 'held'),
              known_region = input.region IN ('eu', 'us'),
              foreign_region = input.region NOT IN ('eu', 'us'),
              membership_unknown = is_null(input.region IN ('eu', 'us')),
              exclusion_unknown = is_null(input.region NOT IN ('eu', 'us')),
              in_nothing = input.region IN (),
              outside_nothing = input.region NOT IN (),
              urgent = input.priority IN (1 AS I32, -1 AS I32, 2 AS I32),
              within_limits = input.weight BETWEEN input.low AND input.high,
              outside_band = input.weight NOT BETWEEN 10.0 AND 20.0,
              same_region = input.region IS NOT DISTINCT FROM input.home_region,
              moved = input.region IS DISTINCT FROM input.home_region,
              zero_or_half = input.reading AS F64 IN (0.0, 1.5),
              catalogued = input.code IN (
                1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009,
                1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019,
                1020, 1021, 1022, 1023, 1024, 1025, 1026, 1027, 1028, 1029,
                1030, 1031, 1032, 1033, 1034, 1035, 1036, 1037, 1038, 1039
              ),
              launch_day = input.shipped AS DATETIME IN (
                '2026-01-01T00:00:00Z' AS DATETIME,
                '2026-01-02T00:00:00Z' AS DATETIME
              )
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION shipment_facts_subscription TO shipment_facts;
      START;
      """
    And http payload is posted to node "node-1" with host "membership-{{test_id}}.example.com" path "/shipments"
      """
      [{"id":"open-eu","status":"open","region":"eu","home_region":"eu","priority":1,"weight":12.5,"low":10.0,"high":15.0,"reading":"-0.0","code":1003,"shipped":"2026-01-01T00:00:00Z"},{"id":"held-unknown","status":"held","region":null,"home_region":null,"priority":5,"weight":25.0,"low":30.0,"high":20.0,"reading":"nan","code":42,"shipped":"2026-03-01T00:00:00Z"},{"id":"closed-moved","status":"closed","region":"us","home_region":null,"priority":-1,"weight":20.0,"low":20.0,"high":20.0,"reading":"1.5","code":1039,"shipped":"2026-01-02T00:00:00Z"},{"id":"foreign","status":"void","region":"apac","home_region":"eu","priority":2,"weight":9.5,"low":10.0,"high":20.0,"reading":"0.5","code":999,"shipped":"2026-01-01T00:00:00.000000001Z"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"open-eu" | "open":true | "settled":false | "known_region":true | "foreign_region":false | "membership_unknown":false | "exclusion_unknown":false | "in_nothing":false | "outside_nothing":true | "urgent":true | "within_limits":true | "outside_band":false | "same_region":true | "moved":false | "zero_or_half":true | "catalogued":true | "launch_day":true
      "id":"held-unknown" | "open":true | "settled":false | "membership_unknown":true | "exclusion_unknown":true | "in_nothing":false | "outside_nothing":true | "urgent":false | "within_limits":false | "outside_band":true | "same_region":true | "moved":false | "zero_or_half":false | "catalogued":false | "launch_day":false
      "id":"closed-moved" | "open":false | "settled":true | "known_region":true | "foreign_region":false | "membership_unknown":false | "exclusion_unknown":false | "in_nothing":false | "outside_nothing":true | "urgent":true | "within_limits":true | "outside_band":false | "same_region":false | "moved":true | "zero_or_half":true | "catalogued":true | "launch_day":true
      "id":"foreign" | "open":false | "settled":true | "known_region":false | "foreign_region":true | "membership_unknown":false | "exclusion_unknown":false | "in_nothing":false | "outside_nothing":true | "urgent":true | "within_limits":false | "outside_band":true | "same_region":false | "moved":true | "zero_or_half":false | "catalogued":false | "launch_day":false
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Membership, range and null-safe predicates filter ingested messages, source inputs, routes and read-only session subscriptions
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA parcel (
        id STRING,
        status STRING,
        weight F64,
        carrier STRING OPTIONAL,
        preferred STRING OPTIONAL
      );
      CREATE CODEC parcel_batch_codec
        FROM JSON
        TO SCHEMA parcel
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY parcels SCHEMA parcel UNBRANCHED;
      CREATE RELAY routed_parcels SCHEMA parcel UNBRANCHED;
      CREATE VHOST edge parcel-filters-{{test_id}}.example.com;
      CREATE ENDPOINT parcel_ingress ON edge PATH '/parcels' TYPE HTTP;
      CREATE INGESTOR parcel_source
        FROM ENDPOINT parcel_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING parcel_batch_codec
        FILTER WHERE input.id NOT IN ('heartbeat', 'ping')
          AND input.id NOT BETWEEN 'probe-000' AND 'probe-999'
        TO parcels
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION route_parcels
        FROM parcels WHERE input.status NOT IN ('void', 'test')
        UNBRANCHED
        TO routed_parcels
          INHERIT ALL
          WHERE input.weight BETWEEN 1.0 AND 50.0
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION routed_parcels_subscription TO routed_parcels
        WHERE input.carrier IS DISTINCT FROM input.preferred
          AND input.status IN (
            'open', 'held', 'queued', 'sorted', 'loaded', 'staged', 'scanned', 'weighed',
            'labelled', 'sealed', 'cleared', 'boarded', 'departed', 'arrived', 'transferred',
            'delayed', 'rerouted', 'returned', 'relabelled', 'inspected', 'reserved', 'packed',
            'picked', 'waiting', 'queued', 'open'
          );
      START;
      """
    And http payload is posted to node "node-1" with host "parcel-filters-{{test_id}}.example.com" path "/parcels"
      """
      [{"id":"void-parcel","status":"void","weight":5.0,"carrier":"dhl","preferred":"ups"},{"id":"switched","status":"open","weight":5.0,"carrier":"dhl","preferred":"ups"},{"id":"too-heavy","status":"open","weight":60.0,"carrier":"dhl","preferred":"ups"},{"id":"kept-carrier","status":"open","weight":5.0,"carrier":"ups","preferred":"ups"},{"id":"test-parcel","status":"test","weight":5.0,"carrier":"dhl","preferred":"ups"},{"id":"unassigned","status":"held","weight":1.0,"carrier":null,"preferred":"ups"},{"id":"both-unassigned","status":"held","weight":50.0,"carrier":null,"preferred":null},{"id":"closed","status":"closed","weight":5.0,"carrier":"dhl","preferred":"ups"},{"id":"heartbeat","status":"open","weight":5.0,"carrier":"dhl","preferred":"ups"},{"id":"probe-042","status":"open","weight":5.0,"carrier":"dhl","preferred":"ups"},{"id":"boundary","status":"open","weight":50.0,"carrier":"fedex","preferred":null}]
      """
    Then within "30s" the relay subscription receives payloads in order
      """
      "id":"switched"
      "id":"unassigned"
      "id":"boundary"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Greatest, least and clamp skip nulls, order NaN above every value, keep sensitivity and fail only messages with invalid clamp bounds
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA meter (
        id STRING,
        first F64 OPTIONAL,
        second F64 OPTIONAL,
        third F64,
        reading STRING,
        low STRING,
        high STRING,
        label_a STRING,
        label_b STRING,
        seen_a STRING,
        seen_b STRING,
        active BOOL OPTIONAL,
        salary F64 SENSITIVE
      );
      CREATE SCHEMA meter_summary (
        id STRING,
        top F64,
        bottom F64,
        top_known F64 OPTIONAL,
        top_unknown BOOL,
        nan_top BOOL,
        nan_bottom BOOL,
        zero_tie F64,
        clamped F64,
        first_label STRING,
        latest DATETIME,
        any_active BOOL,
        capped F64 SENSITIVE
      );
      CREATE SCHEMA meter_error (
        input_id STRING,
        error_message STRING
      );
      CREATE CODEC meter_batch_codec
        FROM JSON
        TO SCHEMA meter
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY meters SCHEMA meter UNBRANCHED;
      CREATE RELAY meter_summaries SCHEMA meter_summary UNBRANCHED;
      CREATE RELAY meter_errors SCHEMA meter_error UNBRANCHED;
      CREATE VHOST edge extrema-{{test_id}}.example.com;
      CREATE ENDPOINT meter_ingress ON edge PATH '/meters' TYPE HTTP;
      CREATE INGESTOR meter_source
        FROM ENDPOINT meter_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING meter_batch_codec
        TO meters
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION summarize_meters
        FROM meters
        UNBRANCHED
        TO meter_summaries
          SET id = input.id,
              top = greatest(input.first, input.second, input.third),
              bottom = least(input.first, input.second, input.third),
              top_known = greatest(input.first, input.second),
              top_unknown = is_null(greatest(input.first, input.second)),
              nan_top = is_nan(greatest(input.third, input.reading AS F64)),
              nan_bottom = is_nan(least(input.third, input.reading AS F64)),
              zero_tie = greatest(-0.0, 0.0),
              clamped = clamp(input.third, input.low AS F64, input.high AS F64),
              first_label = least(input.label_a, input.label_b),
              latest = greatest(input.seen_a AS DATETIME, input.seen_b AS DATETIME),
              any_active = greatest(input.active, FALSE),
              capped = clamp(input.salary, 0.0, 100000.0)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO meter_errors
          SET input_id = input.id,
              error_message = error.message;
      CREATE SUBSCRIPTION meter_summaries_subscription TO meter_summaries;
      CREATE SUBSCRIPTION meter_errors_subscription TO meter_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "extrema-{{test_id}}.example.com" path "/meters"
      """
      [{"id":"mixed","first":3.5,"second":null,"third":-2.0,"reading":"1.0","low":"0","high":"10","label_a":"beta","label_b":"alpha","seen_a":"2026-01-01T00:00:00Z","seen_b":"2026-01-01T00:00:01Z","active":null,"salary":250000.0},{"id":"all-missing","first":null,"second":null,"third":7.25,"reading":"nan","low":"7.25","high":"7.25","label_a":"same","label_b":"same","seen_a":"2026-02-01T00:00:00Z","seen_b":"2026-01-31T00:00:00Z","active":true,"salary":10.0},{"id":"inverted","first":1.0,"second":2.0,"third":5.0,"reading":"2.0","low":"10","high":"0","label_a":"a","label_b":"b","seen_a":"2026-01-01T00:00:00Z","seen_b":"2026-01-01T00:00:00Z","active":false,"salary":1.0},{"id":"nan-bound","first":1.0,"second":2.0,"third":5.0,"reading":"2.0","low":"0","high":"nan","label_a":"a","label_b":"b","seen_a":"2026-01-01T00:00:00Z","seen_b":"2026-01-01T00:00:00Z","active":false,"salary":1.0}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"mixed" | "top":3.5 | "bottom":-2.0 | "top_known":3.5 | "top_unknown":false | "nan_top":false | "nan_bottom":false | "zero_tie":-0.0 | "clamped":0.0 | "first_label":"alpha" | "latest":"2026-01-01T00:00:01+00:00" | "any_active":false
      "id":"all-missing" | "top":7.25 | "bottom":7.25 | "top_unknown":true | "nan_top":true | "nan_bottom":false | "zero_tie":-0.0 | "clamped":7.25 | "first_label":"same" | "latest":"2026-02-01T00:00:00+00:00" | "any_active":true
      "input_id":"inverted" | invalid_argument: clamp lower bound is above its upper bound
      "input_id":"nan-bound" | invalid_argument: clamp bound is NaN
      """
    When http payload is posted to node "node-1" with host "extrema-{{test_id}}.example.com" path "/meters"
      """
      [{"id":"sensitive","first":1.0,"second":2.0,"third":3.0,"reading":"4.0","low":"0","high":"10","label_a":"a","label_b":"b","seen_a":"2026-01-01T00:00:00Z","seen_b":"2026-01-01T00:00:00Z","active":true,"salary":250000.0}]
      """
    Then within "30s" the relay subscription receives a payload
      """
      "id":"sensitive"
      """
    And the last relay subscription payload masks field "capped"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Membership and range tests choose the key of a LOOKUP_HASH_MAP call
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "tiers_dir" containing
      """
      {
        "tiers.jsonl": "{\"tier\":\"gold\",\"discount\":20.0}\n{\"tier\":\"domestic\",\"discount\":5.0}\n{\"tier\":\"standard\",\"discount\":0.0}\n"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE tiers_data;
      UPLOAD RESOURCE tiers_data VERSION '{{tiers_dir}}';
      """
    Then the last command output contains
      """
      uploaded resource version 1
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA order_line (
        id STRING,
        score I64,
        country STRING
      );
      CREATE SCHEMA priced_line (
        id STRING,
        discount F64 OPTIONAL
      );
      CREATE SCHEMA tier (
        tier STRING,
        discount F64
      );
      CREATE WIRE JSON SCHEMA tier_wire MODE STRICT (
        tier string,
        discount number
      );
      CREATE CODEC tier_codec
        FROM WIRE JSON SCHEMA tier_wire
        TO SCHEMA tier;
      CREATE CODEC order_line_batch_codec
        FROM JSON
        TO SCHEMA order_line
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY order_lines SCHEMA order_line UNBRANCHED;
      CREATE RELAY priced_lines SCHEMA priced_line UNBRANCHED;
      CREATE HASH MAP discounts_by_tier
        KEY tier
        FROM RESOURCE tiers_data VERSION 1
        PATH 'tiers.jsonl'
        DECODE USING tier_codec;
      CREATE VHOST edge tiers-{{test_id}}.example.com;
      CREATE ENDPOINT order_ingress ON edge PATH '/orders' TYPE HTTP;
      CREATE INGESTOR order_source
        FROM ENDPOINT order_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING order_line_batch_codec
        TO order_lines
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION price_order_lines
        FROM order_lines
        UNBRANCHED
        TO priced_lines
          SET id = input.id,
              discount = LOOKUP_HASH_MAP(
                'discounts_by_tier',
                CASE
                  WHEN input.score BETWEEN 90 AND 100 THEN 'gold'
                  WHEN input.country IN ('us', 'ca') THEN 'domestic'
                  ELSE 'standard'
                END,
                'discount'
              )
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION priced_lines_subscription TO priced_lines;
      START;
      """
    And http payload is posted to node "node-1" with host "tiers-{{test_id}}.example.com" path "/orders"
      """
      [{"id":"top-score","score":95,"country":"de"},{"id":"perfect-local","score":100,"country":"us"},{"id":"local","score":40,"country":"ca"},{"id":"abroad","score":89,"country":"fr"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"top-score" | "discount":20.0
      "id":"perfect-local" | "discount":20.0
      "id":"local" | "discount":5.0
      "id":"abroad" | "discount":0.0
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Membership and range tests select the tensor an inferencer input mapping sends to its model
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE inference;
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA banded_value (
        band STRING,
        value F32,
        fallback F32
      );
      CREATE SCHEMA inference_result (
        result F32
      );
      CREATE CODEC banded_value_batch_codec
        FROM JSON
        TO SCHEMA banded_value
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY banded_values SCHEMA banded_value UNBRANCHED;
      CREATE RELAY inference_results SCHEMA inference_result UNBRANCHED;
      CREATE VHOST edge infer-membership-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/values' TYPE HTTP;
      CREATE INGESTOR banded_value_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING banded_value_batch_codec
        TO banded_values
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INFERENCER select_values FROM banded_values
        USING RESOURCE inference VERSION 1
        FILE 'models/scalar_identity.onnx'
        INPUTS {
          "value" <tensor_type>[] = CASE
            WHEN input.band IN ('low', 'mid')
              AND input.value BETWEEN (0.0 AS F32) AND (10.0 AS F32) THEN input.value
            ELSE input.fallback
          END
        }
        OUTPUT SCHEMA { "result" <tensor_type>[] }
        UNBRANCHED
        TO inference_results
          SET result = result
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION inference_results_subscription TO inference_results;
      START;
      """
    And http payload is posted to node "node-1" with host "infer-membership-{{test_id}}.example.com" path "/values"
      """
      [{"band":"low","value":3.0,"fallback":-1.0},{"band":"high","value":4.0,"fallback":-2.0},{"band":"mid","value":42.0,"fallback":-3.0},{"band":"mid","value":10.0,"fallback":-4.0}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      {"result":3.0}
      {"result":-2.0}
      {"result":-3.0}
      {"result":10.0}
      """

    Examples:
      | cluster_size | replica_count | tensor_type       |
      | 1            | 0             | DENSE TENSOR<F32> |
      | 3            | 0             | DENSE TENSOR<F32> |

  Scenario Outline: Membership, ranges, null-safe equality and extrema reject operands outside their signatures and nondeterministic materialized-state defaults when the statement is applied
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA sample (
        id STRING,
        status STRING,
        region STRING OPTIONAL,
        priority I32,
        small U8,
        weight F64,
        active BOOL,
        salary F64 SENSITIVE
      );
      CREATE SCHEMA verdict (
        id STRING,
        flag BOOL,
        amount F64
      );
      CREATE RELAY samples SCHEMA sample UNBRANCHED;
      CREATE RELAY verdicts SCHEMA verdict UNBRANCHED;
      CREATE RELAY latest_verdicts SCHEMA verdict UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      """
    When these NSPL commands fail with "IN set element 1 has type Int64, but the operand has type Int32"
      """
      CREATE JUNCTION untyped_priorities
        FROM samples
        UNBRANCHED
        TO verdicts
          SET id = input.id,
              flag = input.priority IN (1, 2),
              amount = input.weight
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "IN set element 2 is NULL"
      """
      CREATE JUNCTION null_member
        FROM samples
        UNBRANCHED
        TO verdicts
          SET id = input.id,
              flag = input.status IN ('open', NULL),
              amount = input.weight
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "IN set element 2 is not a constant"
      """
      CREATE JUNCTION field_member
        FROM samples
        UNBRANCHED
        TO verdicts
          SET id = input.id,
              flag = input.status IN ('open', input.id),
              amount = input.weight
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "IN set element 2 cannot be evaluated: cannot cast value to UInt8"
      """
      CREATE JUNCTION oversized_member
        FROM samples
        UNBRANCHED
        TO verdicts
          SET id = input.id,
              flag = input.small IN (255 AS U8, 300 AS U8),
              amount = input.weight
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "BETWEEN is not valid for Boolean"
      """
      CREATE JUNCTION boolean_range
        FROM samples
        UNBRANCHED
        TO verdicts
          SET id = input.id,
              flag = input.active BETWEEN FALSE AND TRUE,
              amount = input.weight
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "BETWEEN requires the operand and both bounds to have one exact type, found Float64, Int64 and Int64"
      """
      CREATE JUNCTION untyped_range
        FROM samples
        UNBRANCHED
        TO verdicts
          SET id = input.id,
              flag = input.weight BETWEEN 1 AND 2,
              amount = input.weight
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "binary operator IsDistinctFrom requires matching operand types, found Utf8 and Int64"
      """
      CREATE JUNCTION mismatched_distinct
        FROM samples
        UNBRANCHED
        TO verdicts
          SET id = input.id,
              flag = input.region IS DISTINCT FROM 1,
              amount = input.weight
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "function 'greatest' requires matching operand types, found Float64 and Int32"
      """
      CREATE JUNCTION mismatched_extremum
        FROM samples
        UNBRANCHED
        TO verdicts
          SET id = input.id,
              flag = input.active,
              amount = greatest(input.weight, input.priority)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "function 'clamp' requires numeric, STRING or DATETIME input, found Boolean"
      """
      CREATE JUNCTION boolean_clamp
        FROM samples
        UNBRANCHED
        TO verdicts
          SET id = input.id,
              flag = clamp(input.active, FALSE, TRUE),
              amount = input.weight
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "SET field 'amount' would store sensitive data in a non-sensitive output field"
      """
      CREATE JUNCTION leaked_clamp
        FROM samples
        UNBRANCHED
        TO verdicts
          SET id = input.id,
              flag = input.active,
              amount = clamp(input.salary, 0.0, 1.0)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "SET field 'flag' would store sensitive data in a non-sensitive output field"
      """
      CREATE JUNCTION leaked_membership
        FROM samples
        UNBRANCHED
        TO verdicts
          SET id = input.id,
              flag = input.salary IN (1.0, 2.0),
              amount = input.weight
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "materialized-state DEFAULT for 'latest_verdicts' must use deterministic side-effect-free expressions"
      """
      CREATE JUNCTION generated_default_member
        FROM samples
        UNBRANCHED
        USING MATERIALIZED STATE latest_verdicts DEFAULT {
          id = 'none',
          flag = 'none' IN ('none', uuid_v4()),
          amount = 0.0
        }
        TO verdicts
          SET id = input.id,
              flag = relay_state.latest_verdicts.flag,
              amount = input.weight
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "materialized-state DEFAULT for 'latest_verdicts' must use deterministic side-effect-free expressions"
      """
      CREATE JUNCTION clock_default_bound
        FROM samples
        UNBRANCHED
        USING MATERIALIZED STATE latest_verdicts DEFAULT {
          id = 'none',
          flag = ('2026-01-01T00:00:00Z' AS DATETIME)
            BETWEEN ('2025-01-01T00:00:00Z' AS DATETIME) AND now(),
          amount = 0.0
        }
        TO verdicts
          SET id = input.id,
              flag = relay_state.latest_verdicts.flag,
              amount = input.weight
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
