Feature: Statistics over one list
  Scenario Outline: List statistics describe the values of arrays and vectors
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA series (
        id STRING,
        readings <readings_type> OPTIONAL,
        counts <counts_type>,
        labels <labels_type>,
        flags <flags_type>
      );
      CREATE SCHEMA series_summary (
        id STRING,
        var_pop F64 OPTIONAL,
        var_samp F64 OPTIONAL,
        stddev_pop F64 OPTIONAL,
        stddev_samp F64 OPTIONAL,
        median F64 OPTIONAL,
        lower_quartile F64 OPTIONAL,
        highest F64 OPTIONAL,
        newest_z F64 OPTIONAL,
        lowest_at I64 OPTIONAL,
        highest_at I64 OPTIONAL,
        lowest_identity BOOL OPTIONAL,
        median_identity BOOL OPTIONAL,
        count_var_samp F64 OPTIONAL,
        count_stddev_pop F64 OPTIONAL,
        count_median F64 OPTIONAL,
        count_quantile F64 OPTIONAL,
        count_highest_at I64 OPTIONAL,
        label_lowest_at I64 OPTIONAL,
        label_highest_at I64 OPTIONAL,
        flag_lowest_at I64 OPTIONAL,
        flag_highest_at I64 OPTIONAL
      );
      CREATE JAQ JSON CODEC series_codec TO SCHEMA series WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY series_in SCHEMA series UNBRANCHED;
      CREATE RELAY summaries SCHEMA series_summary UNBRANCHED;
      CREATE VHOST edge statistics-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT series_ingress ON edge PATH '/series';
      CREATE HTTP ENDPOINT INGESTOR series_ingestor
        FROM series_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING series_codec
        TO series_in
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION describe_series
        FROM series_in
        UNBRANCHED
        TO summaries
          SET id = input.id,
              var_pop = list_var_pop(input.readings),
              var_samp = list_var_samp(input.readings),
              stddev_pop = list_stddev_pop(input.readings),
              stddev_samp = list_stddev_samp(input.readings),
              median = list_median(input.readings),
              lower_quartile = list_quantile(input.readings, 25),
              highest = list_quantile(input.readings, 100),
              newest_z = CASE
                WHEN list_stddev_pop(input.readings) > 0.0
                THEN (last(input.readings) - mean(input.readings)) / list_stddev_pop(input.readings)
              END,
              lowest_at = list_argmin(input.readings),
              highest_at = list_argmax(input.readings),
              lowest_identity = nth(input.readings, list_argmin(input.readings)) = min(input.readings),
              median_identity = list_median(input.readings) = list_quantile(input.readings, 50),
              count_var_samp = list_var_samp(input.counts),
              count_stddev_pop = list_stddev_pop(input.counts),
              count_median = list_median(input.counts),
              count_quantile = list_quantile(input.counts, 62.5),
              count_highest_at = list_argmax(input.counts),
              label_lowest_at = list_argmin(input.labels),
              label_highest_at = list_argmax(input.labels),
              flag_lowest_at = list_argmin(input.flags),
              flag_highest_at = list_argmax(input.flags)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION summaries_subscription TO summaries;
      START;
      """
    And http payload is posted to node "node-1" with host "statistics-{{test_id}}.example.com" path "/series"
      """
      [{"id":"spread","readings":[5,2,9,4,4,7,4,13],"counts":[10,30,20,40],"labels":["beta","alpha","gamma","alpha"],"flags":[true,false,true,false]},{"id":"single","readings":[7.5],"counts":[1,1,1,1],"labels":["only"],"flags":[true]},{"id":"empty","readings":[],"counts":[0,0,0,0],"labels":[],"flags":[]},{"id":"absent","readings":null,"counts":[2,2,2,2],"labels":["x"],"flags":[false]}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"spread" | "var_pop":11.0 | "var_samp":12.571428571428571 | "stddev_pop":3.3166247903554 | "stddev_samp":3.5456210417116734 | "median":4.5 | "lower_quartile":4.0 | "highest":13.0 | "newest_z":2.1105794120443453 | "lowest_at":1 | "highest_at":7 | "lowest_identity":true | "median_identity":true | "count_var_samp":166.66666666666666 | "count_stddev_pop":11.180339887498949 | "count_median":25.0 | "count_quantile":28.75 | "count_highest_at":3 | "label_lowest_at":1 | "label_highest_at":2 | "flag_lowest_at":1 | "flag_highest_at":0
      {"count_highest_at":0,"count_median":1.0,"count_quantile":1.0,"count_stddev_pop":0.0,"count_var_samp":0.0,"flag_highest_at":0,"flag_lowest_at":0,"highest":7.5,"highest_at":0,"id":"single","label_highest_at":0,"label_lowest_at":0,"lower_quartile":7.5,"lowest_at":0,"lowest_identity":true,"median":7.5,"median_identity":true,"stddev_pop":0.0,"var_pop":0.0}
      {"count_highest_at":0,"count_median":0.0,"count_quantile":0.0,"count_stddev_pop":0.0,"count_var_samp":0.0,"id":"empty"}
      {"count_highest_at":0,"count_median":2.0,"count_quantile":2.0,"count_stddev_pop":0.0,"count_var_samp":0.0,"flag_highest_at":0,"flag_lowest_at":0,"id":"absent","label_highest_at":0,"label_lowest_at":0}
      """

    Examples:
      | cluster_size | replica_count | readings_type | counts_type   | labels_type | flags_type |
      | 1            | 0             | VEC<F64>      | ARRAY<I64, 4> | VEC<STRING> | VEC<BOOL>  |
      | 3            | 0             | VEC<F64>      | ARRAY<I64, 4> | VEC<STRING> | VEC<BOOL>  |

  Scenario Outline: A list statistic reports a non-finite element or result as that message's error
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA probe (id STRING, spread_reading STRING, middle_reading STRING);
      CREATE SCHEMA probe_result (id STRING, spread F64 OPTIONAL, middle F64 OPTIONAL);
      CREATE SCHEMA probe_error (input_id STRING, error_message STRING);
      CREATE JAQ JSON CODEC probe_codec TO SCHEMA probe WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY probes SCHEMA probe UNBRANCHED;
      CREATE RELAY probe_results SCHEMA probe_result UNBRANCHED;
      CREATE RELAY probe_errors SCHEMA probe_error UNBRANCHED;
      CREATE VHOST edge probes-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT probe_ingress ON edge PATH '/probes';
      CREATE HTTP ENDPOINT INGESTOR probe_ingestor
        FROM probe_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING probe_codec
        TO probes
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION probe_statistics
        FROM probes
        UNBRANCHED
        TO probe_results
          SET id = input.id,
              spread = list_stddev_samp(vec(input.spread_reading AS F64, 1.0)),
              middle = list_median(vec(1.0, input.middle_reading AS F64))
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO probe_errors
            SET input_id = input.id,
                error_message = error.message;
      CREATE SUBSCRIPTION probe_results_subscription TO probe_results;
      CREATE SUBSCRIPTION probe_errors_subscription TO probe_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "probes-{{test_id}}.example.com" path "/probes"
      """
      [{"id":"finite","spread_reading":"1.5","middle_reading":"2.5"},{"id":"nan-spread","spread_reading":"NaN","middle_reading":"2.5"},{"id":"infinite-middle","spread_reading":"1.5","middle_reading":"-inf"},{"id":"overflow","spread_reading":"1e308","middle_reading":"2.5"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      {"id":"finite","middle":1.75,"spread":0.3535533905932738}
      "input_id":"nan-spread" | "error_message":"junction 'probe_statistics' FILTER-MAP side error invalid_argument: list_stddev_samp input contains a non-finite element
      "input_id":"infinite-middle" | "error_message":"junction 'probe_statistics' FILTER-MAP side error invalid_argument: list_median input contains a non-finite element
      "input_id":"overflow" | "error_message":"junction 'probe_statistics' FILTER-MAP side error invalid_argument: list_stddev_samp produced a non-finite result
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: A window route applies list statistics inside aggregate arguments of each branch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA history_reading (tenant STRING, level F64, history <history_type>);
      CREATE SCHEMA history_summary (tenant STRING, level_spread F64 OPTIONAL, mean_history_spread F64 OPTIONAL, peak_history_median F64 OPTIONAL);
      CREATE JAQ JSON CODEC history_codec TO SCHEMA history_reading WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA history_branch_schema (tenant STRING);
      CREATE BRANCH history_branch SCHEMA history_branch_schema TTL 5m;
      CREATE RELAY history_readings SCHEMA history_reading BRANCHED BY history_branch;
      CREATE RELAY history_summaries SCHEMA history_summary BRANCHED BY history_branch;
      CREATE VHOST edge history-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/history';
      CREATE HTTP ENDPOINT INGESTOR history_ingestor FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING history_codec
        TO history_readings INHERIT ALL BRANCHED BY history_branch
        SET tenant = message.tenant
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR history_window FROM history_readings WIDTH 2 MESSAGES STEP 2 MESSAGES
        BRANCHED BY history_branch TO history_summaries
        SET tenant = FIRST(input.tenant),
            level_spread = STDDEV_POP(input.level),
            mean_history_spread = AVG(list_stddev_pop(input.history)),
            peak_history_median = MAX(list_median(input.history))
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION history_subscription TO history_summaries;
      START;
      """
    And http payload is posted to node "node-1" with host "history-{{test_id}}.example.com" path "/history"
      """
      [{"tenant":"acme","level":1,"history":[1,3]},{"tenant":"beta","level":10,"history":[10]},{"tenant":"acme","level":3,"history":[2,2,8]},{"tenant":"beta","level":10,"history":[4,6]}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      key={"tenant":"acme"} payload={"level_spread":1.0,"mean_history_spread":1.9142135623730951,"peak_history_median":2.0,"tenant":"acme"}
      key={"tenant":"beta"} payload={"level_spread":0.0,"mean_history_spread":0.5,"peak_history_median":10.0,"tenant":"beta"}
      """

    Examples:
      | cluster_size | replica_count | history_type |
      | 1            | 0             | VEC<F64>     |
      | 3            | 0             | VEC<F64>     |

  Scenario Outline: List statistics refuse elements and percentiles they cannot read when the statement is applied
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands fail with "function 'list_var_pop' requires ARRAY or VEC elements of a scalar type"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA nested_series (id STRING, windows <nested_type>);
      CREATE SCHEMA nested_summary (id STRING, spread F64 OPTIONAL);
      CREATE RELAY nested_series_in SCHEMA nested_series UNBRANCHED;
      CREATE RELAY nested_summaries SCHEMA nested_summary UNBRANCHED;
      CREATE JUNCTION describe_nested FROM nested_series_in UNBRANCHED TO nested_summaries
        SET id = input.id, spread = list_var_pop(input.windows)
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "function 'list_median' requires numeric input, found Utf8"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA label_series (id STRING, labels <labels_type>);
      CREATE SCHEMA label_summary (id STRING, middle F64 OPTIONAL);
      CREATE RELAY label_series_in SCHEMA label_series UNBRANCHED;
      CREATE RELAY label_summaries SCHEMA label_summary UNBRANCHED;
      CREATE JUNCTION describe_labels FROM label_series_in UNBRANCHED TO label_summaries
        SET id = input.id, middle = list_median(input.labels)
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "function 'list_quantile' requires a percentile from 0 to 100, found 101"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA reading_series (id STRING, readings <readings_type>);
      CREATE SCHEMA reading_summary (id STRING, tail F64 OPTIONAL);
      CREATE RELAY reading_series_in SCHEMA reading_series UNBRANCHED;
      CREATE RELAY reading_summaries SCHEMA reading_summary UNBRANCHED;
      CREATE JUNCTION describe_tail FROM reading_series_in UNBRANCHED TO reading_summaries
        SET id = input.id, tail = list_quantile(input.readings, 101)
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "function 'list_quantile' requires its percentile to be a numeric literal"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA chosen_series (id STRING, readings <readings_type>, percentile F64);
      CREATE SCHEMA chosen_summary (id STRING, chosen F64 OPTIONAL);
      CREATE RELAY chosen_series_in SCHEMA chosen_series UNBRANCHED;
      CREATE RELAY chosen_summaries SCHEMA chosen_summary UNBRANCHED;
      CREATE JUNCTION describe_chosen FROM chosen_series_in UNBRANCHED TO chosen_summaries
        SET id = input.id, chosen = list_quantile(input.readings, input.percentile)
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these Plexus commands fail with "function 'list_argmax' requires ordered scalar elements, found Binary"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA digest_series (id STRING, digests <digests_type>);
      CREATE SCHEMA digest_summary (id STRING, largest_at I64 OPTIONAL);
      CREATE RELAY digest_series_in SCHEMA digest_series UNBRANCHED;
      CREATE RELAY digest_summaries SCHEMA digest_summary UNBRANCHED;
      CREATE JUNCTION locate_digest FROM digest_series_in UNBRANCHED TO digest_summaries
        SET id = input.id, largest_at = list_argmax(input.digests)
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | nested_type        | labels_type | readings_type | digests_type |
      | 1            | VEC<ARRAY<F64, 2>> | VEC<STRING> | VEC<F64>      | VEC<BYTES>   |
      | 3            | VEC<ARRAY<F64, 2>> | VEC<STRING> | VEC<F64>      | VEC<BYTES>   |
