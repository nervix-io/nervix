Feature: Explicit list arithmetic over retained window values
  Scenario Outline: A window normalizes its collected sequence and a junction reads the result
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA source (value F64);
      CREATE SCHEMA window_result (values <vector_type>, centered <vector_type>, normalized <vector_type>, average F64, deviation F64);
      CREATE SCHEMA result (values <vector_type>, centered <vector_type>, normalized <vector_type>, magnitude <vector_type>, average F64, deviation F64);
      CREATE JAQ JSON CODEC source_codec TO SCHEMA source WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY sources SCHEMA source UNBRANCHED;
      CREATE RELAY windows SCHEMA window_result UNBRANCHED;
      CREATE RELAY results SCHEMA result UNBRANCHED;
      CREATE VHOST edge window-arithmetic-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT source_ingress ON edge PATH '/samples';
      CREATE HTTP ENDPOINT INGESTOR ingestor_node
        FROM source_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING source_codec
        TO sources INHERIT ALL UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE WINDOW PROCESSOR collect_samples FROM sources
        WIDTH 3 MESSAGES STEP 3 MESSAGES
        UNBRANCHED
        TO windows
          SET values = COLLECT_VEC(input.value),
              centered = list_sub_scalar(COLLECT_VEC(input.value), AVG(input.value)),
              normalized = list_div_scalar(list_sub_scalar(COLLECT_VEC(input.value), AVG(input.value)), STDDEV_POP(input.value)),
              average = AVG(input.value),
              deviation = STDDEV_POP(input.value)
          ON MESSAGE ERROR LOG;
      CREATE JUNCTION magnitudes FROM windows UNBRANCHED
        TO results INHERIT ALL
          SET magnitude = list_abs(input.centered)
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION results_subscription TO results;
      START;
      """
    And http payload is posted to node "node-1" with host "window-arithmetic-{{test_id}}.example.com" path "/samples"
      """
      [{"value":2.0},{"value":4.0},{"value":6.0}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "values":[2.0,4.0,6.0] | "centered":[-2.0,0.0,2.0] | "normalized":[-1.224744 | "magnitude":[2.0,0.0,2.0] | "average":4.0 | "deviation":1.632993
      """

    Examples:
      | cluster_size | vector_type |
      | 1            | VEC<F64>    |
      | 3            | VEC<F64>    |
