Feature: Exact decimal rounding in Plexus
  Scenario Outline: Decimal rounding at a power of two selects the nearest float toward zero
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA rounding_input (
        id STRING,
        wide STRING,
        wide_digits I16,
        wide_expected STRING,
        narrow STRING,
        narrow_digits I16,
        narrow_expected STRING
      );
      CREATE SCHEMA rounding_result (
        id STRING,
        wide_matches BOOL,
        narrow_matches BOOL,
        constant_matches BOOL
      );
      CREATE CODEC rounding_codec
        FROM JSON TO SCHEMA rounding_input
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY rounding_inputs SCHEMA rounding_input UNBRANCHED;
      CREATE RELAY rounding_results SCHEMA rounding_result UNBRANCHED;
      CREATE VHOST edge decimal-rounding-{{test_id}}.example.com;
      CREATE ENDPOINT rounding_ingress ON edge PATH '/round' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR rounding_source
        FROM rounding_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING rounding_codec
        TO rounding_inputs
          INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION round_values
        FROM rounding_inputs UNBRANCHED
        TO rounding_results
          SET id = input.id,
              wide_matches = round(input.wide AS F64, input.wide_digits) = (input.wide_expected AS F64),
              narrow_matches = round(input.narrow AS F32, input.narrow_digits) = (input.narrow_expected AS F32),
              constant_matches = round('5.0321474762477604e-234' AS F64, 249) = ('5.03214747624776e-234' AS F64)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION rounded_values TO rounding_results;
      START;
      """
    And http payload is posted to node "node-1" with host "decimal-rounding-{{test_id}}.example.com" path "/round"
      """
      [{"id":"fractional","wide":"5.0321474762477604e-234","wide_digits":249,"wide_expected":"5.03214747624776e-234","narrow":"9.860761315262648e-32","narrow_digits":38,"narrow_expected":"9.860760727515472e-32"},{"id":"negative-fractional","wide":"-5.0321474762477604e-234","wide_digits":249,"wide_expected":"-5.03214747624776e-234","narrow":"-9.860761315262648e-32","narrow_digits":38,"narrow_expected":"-9.860760727515472e-32"},{"id":"integer","wide":"6.189700196426902e26","wide_digits":-11,"wide_expected":"6.189700196426901e26","narrow":"1.5474250491067253e26","narrow_digits":-19,"narrow_expected":"1.547424956873005e26"},{"id":"negative-integer","wide":"-6.189700196426902e26","wide_digits":-11,"wide_expected":"-6.189700196426901e26","narrow":"-1.5474250491067253e26","narrow_digits":-19,"narrow_expected":"-1.547424956873005e26"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"fractional" | "wide_matches":true | "narrow_matches":true | "constant_matches":true
      "id":"negative-fractional" | "wide_matches":true | "narrow_matches":true | "constant_matches":true
      "id":"integer" | "wide_matches":true | "narrow_matches":true | "constant_matches":true
      "id":"negative-integer" | "wide_matches":true | "narrow_matches":true | "constant_matches":true
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
