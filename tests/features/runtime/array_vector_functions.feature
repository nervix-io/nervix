Feature: Typed array and vector expressions
  Scenario Outline: An ordinary route constructs a typed array from input columns
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA numbers (id STRING, first I64, second I64);
      CREATE SCHEMA result (id STRING, items <items_type>, vec_items <vec_type>, empty_items <vec_type>, item_count I64);
      CREATE CODEC numbers_codec FROM JSON TO SCHEMA numbers WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY numbers SCHEMA numbers UNBRANCHED;
      CREATE RELAY results SCHEMA result UNBRANCHED;
      CREATE VHOST edge arrays-{{test_id}}.example.com;
      CREATE ENDPOINT numbers_ingress ON edge PATH '/numbers' TYPE HTTP;
      CREATE INGESTOR numbers_source
        FROM ENDPOINT numbers_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING numbers_codec
        TO numbers
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION make_arrays
        FROM numbers
        UNBRANCHED
        TO results
          SET id = input.id,
              items = [input.first, input.second],
              vec_items = vec(input.first, input.second),
              empty_items = vec(),
              item_count = count([input.first, input.second])
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION results_subscription TO results;
      START;
      """
    And http payload is posted to node "node-1" with host "arrays-{{test_id}}.example.com" path "/numbers"
      """
      [{"id":"a","first":2,"second":3}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"a" | "items":[2,3] | "vec_items":[2,3] | "empty_items":[] | "item_count":2
      """

    Examples:
      | cluster_size | replica_count | items_type    | vec_type |
      | 1            | 0             | ARRAY<I64, 2> | VEC<I64> |
      | 3            | 0             | ARRAY<I64, 2> | VEC<I64> |

  Scenario Outline: Vector kernels transform ragged and empty child columns
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA source (id STRING, items <items_type> OPTIONAL, other <items_type> OPTIONAL, needle I64);
      CREATE SCHEMA result (
        id STRING,
        has_needle BOOL OPTIONAL,
        overlaps BOOL OPTIONAL,
        sliced <items_type> OPTIONAL,
        joined <items_type> OPTIONAL,
        smallest I64 OPTIONAL,
        largest I64 OPTIONAL,
        average F64 OPTIONAL,
        product I64 OPTIONAL,
        separation F64 OPTIONAL
      );
      CREATE SCHEMA vector_error (input_id STRING, error_message STRING);
      CREATE CODEC source_codec FROM JSON TO SCHEMA source WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY sources SCHEMA source UNBRANCHED;
      CREATE RELAY results SCHEMA result UNBRANCHED;
      CREATE RELAY vector_errors SCHEMA vector_error UNBRANCHED;
      CREATE VHOST edge vectors-{{test_id}}.example.com;
      CREATE ENDPOINT source_ingress ON edge PATH '/vectors' TYPE HTTP;
      CREATE INGESTOR source_node
        FROM ENDPOINT source_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING source_codec
        TO sources
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION vector_functions
        FROM sources
        UNBRANCHED
        TO results
          SET id = input.id,
              has_needle = contains(input.items, input.needle),
              overlaps = overlap(input.items, input.other),
              sliced = slice(input.items, 1, 2),
              joined = concat(input.items, input.other),
              smallest = min(input.items),
              largest = max(input.items),
              average = mean(input.items),
              product = dot(input.items, input.other),
              separation = distance(input.items, input.other)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO vector_errors
          SET input_id = input.id,
              error_message = error.message;
      CREATE SUBSCRIPTION results_subscription TO results;
      CREATE SUBSCRIPTION vector_errors_subscription TO vector_errors;
      START;
      """
    And http payload is posted to node "node-1" with host "vectors-{{test_id}}.example.com" path "/vectors"
      """
      [{"id":"ragged","items":[2,3],"other":[3,4],"needle":3},{"id":"empty","items":[],"other":[],"needle":3},{"id":"null","items":null,"other":[1],"needle":1},{"id":"mismatch","items":[1,2],"other":[3],"needle":1}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"ragged" | "has_needle":true | "overlaps":true | "sliced":[3] | "joined":[2,3,3,4] | "smallest":2 | "largest":3 | "average":2.5 | "product":18 | "separation":1.414
      "id":"empty" | "has_needle":false | "overlaps":false | "sliced":[] | "joined":[] | "product":0 | "separation":0
      "id":"null"
      "input_id":"mismatch" | invalid_argument: vector lengths differ: left has 2, right has 1
      """

    Examples:
      | cluster_size | replica_count | items_type |
      | 1            | 0             | VEC<I64>   |
      | 3            | 0             | VEC<I64>   |

  Scenario Outline: Fixed arrays retain their width through construction and concatenation
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA source (id STRING, first I64, second I64, secret I64 SENSITIVE);
      CREATE SCHEMA result (
        id STRING,
        pair <array_two>,
        doubled <array_four>,
        tail <vector_type> OPTIONAL,
        has_second BOOL OPTIONAL,
        least I64 OPTIONAL,
        greatest I64 OPTIONAL,
        average F64 OPTIONAL,
        product I64 OPTIONAL,
        distance_value F64 OPTIONAL,
        protected <array_two> SENSITIVE
      );
      CREATE SCHEMA unsafe_result (id STRING, exposed <array_two>);
      CREATE CODEC source_codec FROM JSON TO SCHEMA source WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY sources SCHEMA source UNBRANCHED;
      CREATE RELAY results SCHEMA result UNBRANCHED;
      CREATE RELAY unsafe_results SCHEMA unsafe_result UNBRANCHED;
      CREATE VHOST edge fixed-arrays-{{test_id}}.example.com;
      CREATE ENDPOINT source_ingress ON edge PATH '/fixed-arrays' TYPE HTTP;
      CREATE INGESTOR source_node
        FROM ENDPOINT source_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING source_codec
        TO sources
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And these NSPL commands fail with "SET field 'exposed' would store sensitive data in a non-sensitive output field"
      """
      CREATE JUNCTION unsafe_arrays FROM sources UNBRANCHED
        TO unsafe_results
          SET id = input.id, exposed = [input.secret, input.first]
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "requires exact matching element types"
      """
      CREATE JUNCTION mixed_arrays FROM sources UNBRANCHED
        TO unsafe_results
          SET id = input.id, exposed = [input.first, input.second AS I32]
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE JUNCTION fixed_arrays FROM sources UNBRANCHED
        TO results
          SET id = input.id,
              pair = [input.first, input.second],
              doubled = concat([input.first, input.second], [input.second, input.first]),
              tail = slice([input.first, input.second], 1, 2),
              has_second = contains([input.first, input.second], input.second),
              least = min([input.first, input.second]),
              greatest = max([input.first, input.second]),
              average = mean([input.first, input.second]),
              product = dot([input.first, input.second], [input.second, input.first]),
              distance_value = distance([input.first, input.second], [input.second, input.first]),
              protected = [input.secret, input.first]
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION results_subscription TO results;
      START;
      """
    And http payload is posted to node "node-1" with host "fixed-arrays-{{test_id}}.example.com" path "/fixed-arrays"
      """
      [{"id":"fixed","first":2,"second":3,"secret":11}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"fixed" | "pair":[2,3] | "doubled":[2,3,3,2] | "tail":[3] | "has_second":true | "least":2 | "greatest":3 | "average":2.5 | "product":12 | "distance_value":1.414
      """

    Examples:
      | cluster_size | replica_count | array_two     | array_four    | vector_type |
      | 1            | 0             | ARRAY<I64, 2> | ARRAY<I64, 4> | VEC<I64>    |
      | 3            | 0             | ARRAY<I64, 2> | ARRAY<I64, 4> | VEC<I64>    |
