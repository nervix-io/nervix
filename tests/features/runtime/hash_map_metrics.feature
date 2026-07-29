Feature: Hash map metrics

  Scenario Outline: DESCRIBE HASH MAP and Prometheus report scheduled node metrics
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "zip_codes_dir" containing
      """
      {
        "lookup.jsonl": "{\"zip\":\"60601\",\"city\":\"Chicago\"}\n"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE zip_codes;
      UPLOAD RESOURCE zip_codes VERSION '{{zip_codes_dir}}';
      """
    Then the last command output contains
      """
      uploaded resource version 1
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA zip_code_entry (
        zip STRING,
        city STRING
      );

      CREATE WIRE JSON SCHEMA zip_code_entry_wire MODE STRICT (
        zip string,
        city string
      );

      CREATE CODEC zip_code_entry_codec
        FROM WIRE JSON SCHEMA zip_code_entry_wire
        TO SCHEMA zip_code_entry;

      CREATE HASH MAP zip_codes_by_zip
        KEY zip
        FROM RESOURCE zip_codes
        PATH 'lookup.jsonl'
        DECODE USING zip_code_entry_codec;

      START;
      """
    When these NSPL commands are executed
      """
      LOOKUP zip_codes_by_zip KEY '60601';
      LOOKUP zip_codes_by_zip KEY '60601';
      """
    Then the last command output contains
      """
      "city":"Chicago"
      """
    When these NSPL commands are executed
      """
      DESCRIBE HASH MAP zip_codes_by_zip;
      """
    Then the last command output contains
      """
      hash map: zip_codes_by_zip
      """
    And the last command output owner is saved as placeholder "hash_map_metrics_owner"
    And the last command output contains
      """
      metrics:
      """
    And the last command output contains
      """
      messages_total received relay=- physical_node={{hash_map_metrics_owner}} total=2
      """
    And the last command output contains
      """
      wall_rate_per_sec=
      """
    And the last command output metric "messages_total" "received" relay "-" physical node "{{hash_map_metrics_owner}}" has values
      """
      total=2
      """
    And the last command output does not contain
      """
      batches_total received relay=-
      """
    And node "{{hash_map_metrics_owner}}" observability path "/metrics" eventually responds with 200 and contains 'target_kind="LOOKUP"'
    And node "{{hash_map_metrics_owner}}" observability path "/metrics" eventually responds with 200 and contains 'target="zip_codes_by_zip"'
    And node "{{hash_map_metrics_owner}}" observability path "/metrics" eventually responds with 200 and contains 'direction="received"'
    And node "{{hash_map_metrics_owner}}" observability metric "nervix_messages_total" with labels eventually equals 2
      """
      target_kind="LOOKUP"
      target="zip_codes_by_zip"
      direction="received"
      relay="-"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
