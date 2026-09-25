Feature: Practical string search and normalization
  Scenario Outline: Practical string search and normalization work across a cluster
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA source_text (id STRING, raw STRING, pattern STRING);
      CREATE SCHEMA searched_text (
        id STRING, raw STRING, pattern STRING,
        bytes I64, parts STRING, joined STRING,
        wildcard BOOL, insensitive BOOL, any_match BOOL, dynamic_match BOOL,
        captured STRING OPTIONAL, normalized STRING
      );
      CREATE SCHEMA search_error (source_id STRING, error_code STRING, error_message STRING);
      CREATE WIRE JSON SCHEMA source_wire MODE STRICT (id string, raw string, pattern string);
      CREATE CODEC source_codec FROM WIRE JSON SCHEMA source_wire TO SCHEMA source_text;
      CREATE RELAY source_texts SCHEMA source_text UNBRANCHED;
      CREATE RELAY searched_texts SCHEMA searched_text UNBRANCHED;
      CREATE RELAY search_errors SCHEMA search_error UNBRANCHED;
      CREATE VHOST edge text-search-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/text' TYPE HTTP;
      CREATE INGESTOR source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING source_codec
        TO source_texts INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION search FROM source_texts UNBRANCHED
        TO searched_texts INHERIT ALL
          SET bytes = octet_length(input.raw),
              parts = join(split(input.raw, ' '), '|'),
              joined = concat_ws('-', input.id, input.raw),
              wildcard = like(input.raw, input.pattern),
              insensitive = ilike(input.raw, input.pattern),
              any_match = contains_any(input.raw, vec('café', 'HELLO')),
              dynamic_match = contains_any(input.raw, vec(input.pattern, 'HELLO')),
              captured = CASE
                WHEN input.id = 'bad' THEN regexp_extract(input.raw, '(', 1)
                ELSE regexp_extract(input.raw, '([[:alpha:]]+)', 1)
              END,
              normalized = normalize_nfc(input.raw)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO search_errors
            SET source_id = input.id,
                error_code = error.code,
                error_message = error.message;
      CREATE SUBSCRIPTION searched_texts_subscription TO searched_texts;
      CREATE SUBSCRIPTION search_errors_subscription TO search_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "text-search-{{test_id}}.example.com" path "/text"
      """
      {"id":"accent","raw":"café HELLO","pattern":"CAF%"}
      """
    And http payload is posted to node "node-1" with host "text-search-{{test_id}}.example.com" path "/text"
      """
      {"id":"escape","raw":"a%b","pattern":"a\\%b"}
      """
    And http payload is posted to node "node-1" with host "text-search-{{test_id}}.example.com" path "/text"
      """
      {"id":"empty","raw":"","pattern":"%"}
      """
    And http payload is posted to node "node-1" with host "text-search-{{test_id}}.example.com" path "/text"
      """
      {"id":"combining","raw":"é","pattern":"_"}
      """
    And http payload is posted to node "node-1" with host "text-search-{{test_id}}.example.com" path "/text"
      """
      {"id":"bad","raw":"hello","pattern":"%"}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"accent" | "bytes":12 | "parts":"café|HELLO" | "joined":"accent-café HELLO" | "wildcard":false | "insensitive":true | "any_match":true | "dynamic_match":true | "captured":"cafe" | "normalized":"café HELLO"
      "id":"escape" | "bytes":3 | "parts":"a%b" | "wildcard":true | "dynamic_match":false | "captured":"a"
      "id":"empty" | "bytes":0 | "parts":"" | "joined":"empty-" | "wildcard":true | "any_match":false | "dynamic_match":false
      "id":"combining" | "bytes":3 | "wildcard":false | "normalized":"é"
      "source_id":"bad" | "error_code":"evaluation" | invalid regular expression
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
