Feature: LOOKUP_HASH_MAP filter-map function
  Scenario Outline: Filter-map LOOKUP_HASH_MAP joins hash map matches, skips misses, and filters before lookup
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "titles_dir" containing
      """
      {
        "lookup.jsonl": "{\"normalized_title\":\"mr\",\"city_name\":\"Chicago\",\"region_name\":\"IL\"}\n{\"normalized_title\":\"dr\",\"city_name\":\"Austin\",\"region_name\":\"TX\"}\n"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE titles_data;
      UPLOAD RESOURCE titles_data VERSION '{{titles_dir}}';
      """
    Then the last command output contains
      """
      uploaded resource version 1
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification_in (
        id STRING,
        active BOOL,
        title STRING,
        legacy STRING
      );

      CREATE SCHEMA notification_out (
        id STRING,
        active BOOL,
        title_key STRING,
        city STRING OPTIONAL,
        region STRING OPTIONAL
      );

      CREATE SCHEMA title_lookup (
        normalized_title STRING,
        city_name STRING,
        region_name STRING
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        id string,
        active boolean,
        title string,
        legacy string
      );

      CREATE STRICT WIRE JSON SCHEMA title_lookup_wire (
        normalized_title string,
        city_name string,
        region_name string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification_in;

      CREATE CODEC title_lookup_codec
        FROM WIRE JSON SCHEMA title_lookup_wire
        TO SCHEMA title_lookup;

      CREATE RELAY incoming_logs SCHEMA notification_in UNPARAMETERIZED;
      CREATE RELAY enriched_logs SCHEMA notification_out UNPARAMETERIZED;

      CREATE HASH MAP titles_by_normalized
        KEY normalized_title
        FROM RESOURCE titles_data
        PATH 'lookup.jsonl'
        DECODE USING title_lookup_codec;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT ingress
        ON edge
        PATH '/enrich'
        TYPE HTTP;

      CREATE INGESTOR source_logs
        TO incoming_logs
        DECODE USING notification_codec
        UNPARAMETERIZED
        FLUSH IMMEDIATE
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE DEDUPLICATOR enrich_titles
        FROM incoming_logs
        FILTER WHERE incoming_logs.active
        TO enriched_logs
          SET enriched_logs.title_key = lower(incoming_logs.title),
              enriched_logs.city = LOOKUP_HASH_MAP("titles_by_normalized", lower(incoming_logs.title), "city_name"),
              enriched_logs.region = LOOKUP_HASH_MAP("titles_by_normalized", lower(incoming_logs.title), "region_name")
          UNSET incoming_logs.title, incoming_logs.legacy
          WHERE NOT is_null(LOOKUP_HASH_MAP("titles_by_normalized", lower(incoming_logs.title), "city_name"))
        UNPARAMETERIZED
        DEDUPLICATE ON incoming_logs.id
        MAX TIME 10m
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO enriched_logs;

      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/enrich"
      """
      {"id":"hit-1","active":true,"title":"MR","legacy":"old-hit"}
      """
    Then the relay subscription receives a payload
      """
      "id":"hit-1"
      """
    And the last relay subscription payload contains
      """
      title_key
      Chicago
      IL
      """
    And the last relay subscription payload does not contain "MR"
    And the last relay subscription payload does not contain "old-hit"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/enrich"
      """
      {"id":"hit-2","active":true,"title":"DR","legacy":"old-hit-2"}
      """
    Then the relay subscription receives a payload
      """
      "id":"hit-2"
      """
    And the last relay subscription payload contains
      """
      title_key
      Austin
      TX
      """
    And the last relay subscription payload does not contain "DR"
    And the last relay subscription payload does not contain "old-hit-2"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/enrich"
      """
      {"id":"miss-1","active":true,"title":"Unknown","legacy":"old-miss"}
      """
    Then the relay subscription does not receive a payload within "1s"
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/enrich"
      """
      {"id":"filtered-1","active":false,"title":"MR","legacy":"old-filtered"}
      """
    Then the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Filter-map LOOKUP_HASH_MAP validation rejects missing lookup fields
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "titles_dir" containing
      """
      {
        "lookup.jsonl": "{\"normalized_title\":\"mr\"}\n"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE titles_data;
      UPLOAD RESOURCE titles_data VERSION '{{titles_dir}}';

      CREATE SCHEMA notification_in (
        id STRING,
        title STRING
      );

      CREATE SCHEMA notification_out (
        id STRING,
        title_key STRING,
        city STRING OPTIONAL
      );

      CREATE SCHEMA title_lookup (
        normalized_title STRING
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        id string,
        title string
      );

      CREATE STRICT WIRE JSON SCHEMA title_lookup_wire (
        normalized_title string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification_in;

      CREATE CODEC title_lookup_codec
        FROM WIRE JSON SCHEMA title_lookup_wire
        TO SCHEMA title_lookup;

      CREATE RELAY incoming_logs SCHEMA notification_in UNPARAMETERIZED;
      CREATE RELAY enriched_logs SCHEMA notification_out UNPARAMETERIZED;

      CREATE HASH MAP titles_by_normalized
        KEY normalized_title
        FROM RESOURCE titles_data
        PATH 'lookup.jsonl'
        DECODE USING title_lookup_codec;
      """
    When these NSPL commands fail with "LOOKUP_HASH_MAP field 'missing_city' is missing from hash map 'titles_by_normalized' schema"
      """
      CREATE DEDUPLICATOR enrich_titles
        FROM incoming_logs
        TO enriched_logs
          SET enriched_logs.title_key = lower(incoming_logs.title),
              enriched_logs.city = LOOKUP_HASH_MAP("titles_by_normalized", lower(incoming_logs.title), "missing_city")
          UNSET incoming_logs.title
        UNPARAMETERIZED
        DEDUPLICATE ON incoming_logs.id
        MAX TIME 10m
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
