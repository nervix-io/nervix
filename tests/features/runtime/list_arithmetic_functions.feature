Feature: Explicit element-wise list arithmetic
  Scenario Outline: List arithmetic preserves exact types through a junction and a subscription
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA source (id STRING, items <list_type> OPTIONAL, other <list_type> OPTIONAL, scale <element> OPTIONAL);
      CREATE SCHEMA result (
        id STRING,
        added <list_type> OPTIONAL,
        subtracted <list_type> OPTIONAL,
        multiplied <list_type> OPTIONAL,
        divided <list_type> OPTIONAL,
        shifted <list_type> OPTIONAL,
        reduced <list_type> OPTIONAL,
        scaled <list_type> OPTIONAL,
        normalized <list_type> OPTIONAL,
        absolute <list_type> OPTIONAL,
        selected <element> OPTIONAL
      );
      CREATE JAQ JSON CODEC source_codec TO SCHEMA source WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY sources SCHEMA source UNBRANCHED;
      CREATE RELAY results SCHEMA result UNBRANCHED;
      CREATE VHOST edge arithmetic-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT source_ingress ON edge PATH '/arithmetic';
      CREATE HTTP ENDPOINT INGESTOR ingestor_node
        FROM source_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING source_codec
        TO sources INHERIT ALL UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION arithmetic FROM sources UNBRANCHED
        TO results
          SET id = input.id,
              added = list_add(input.items, input.other),
              subtracted = list_sub(input.items, input.other),
              multiplied = list_mul(input.items, input.other),
              divided = list_div(input.items, input.other),
              shifted = list_add_scalar(input.items, input.scale),
              reduced = list_sub_scalar(input.items, input.scale),
              scaled = list_mul_scalar(input.items, input.scale),
              normalized = list_div_scalar(input.items, input.scale),
              absolute = list_abs(input.items),
              selected = CASE WHEN input.id = 'unselected' THEN first(list_div_scalar(input.items, 0 AS <element>)) ELSE first(input.items) END
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION results_subscription TO results;
      START;
      """
    And http payload is posted to node "node-1" with host "arithmetic-{{test_id}}.example.com" path "/arithmetic"
      """
      [{"id":"values","items":[6,-4],"other":[2,-2],"scale":2},{"id":"null","items":null,"other":[2,-2],"scale":2},{"id":"null_scalar","items":[6,-4],"other":[2,-2],"scale":null}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"values" | "added":<added> | "subtracted":<subtracted> | "multiplied":<multiplied> | "divided":<divided> | "shifted":<shifted> | "reduced":<reduced> | "scaled":<scaled> | "normalized":<normalized> | "absolute":<absolute> | "selected":<selected_value>
      "id":"null"
      "id":"null_scalar" | "added":<added> | "absolute":<absolute>
      """

    Examples:
      | cluster_size | list_type     | element | added      | subtracted | multiplied | divided   | shifted    | reduced    | scaled      | normalized | absolute  | selected_value |
      | 1            | VEC<I64>      | I64     | [8,-6]     | [4,-2]     | [12,8]     | [3,2]     | [8,-2]     | [4,-6]     | [12,-8]     | [3,-2]     | [6,4]     | 6              |
      | 3            | VEC<I64>      | I64     | [8,-6]     | [4,-2]     | [12,8]     | [3,2]     | [8,-2]     | [4,-6]     | [12,-8]     | [3,-2]     | [6,4]     | 6              |
      | 1            | ARRAY<F64, 2> | F64     | [8.0,-6.0] | [4.0,-2.0] | [12.0,8.0] | [3.0,2.0] | [8.0,-2.0] | [4.0,-6.0] | [12.0,-8.0] | [3.0,-2.0] | [6.0,4.0] | 6.0            |
      | 3            | ARRAY<F64, 2> | F64     | [8.0,-6.0] | [4.0,-2.0] | [12.0,8.0] | [3.0,2.0] | [8.0,-2.0] | [4.0,-6.0] | [12.0,-8.0] | [3.0,-2.0] | [6.0,4.0] | 6.0            |
      | 1            | ARRAY<I64, 2> | I64     | [8,-6]     | [4,-2]     | [12,8]     | [3,2]     | [8,-2]     | [4,-6]     | [12,-8]     | [3,-2]     | [6,4]     | 6              |
      | 1            | VEC<F64>      | F64     | [8.0,-6.0] | [4.0,-2.0] | [12.0,8.0] | [3.0,2.0] | [8.0,-2.0] | [4.0,-6.0] | [12.0,-8.0] | [3.0,-2.0] | [6.0,4.0] | 6.0            |
      | 3            | ARRAY<I64, 2> | I64     | [8,-6]     | [4,-2]     | [12,8]     | [3,2]     | [8,-2]     | [4,-6]     | [12,-8]     | [3,-2]     | [6,4]     | 6              |
      | 3            | VEC<F64>      | F64     | [8.0,-6.0] | [4.0,-2.0] | [12.0,8.0] | [3.0,2.0] | [8.0,-2.0] | [4.0,-6.0] | [12.0,-8.0] | [3.0,-2.0] | [6.0,4.0] | 6.0            |

  Scenario Outline: A failing element reports one message error while other lists continue
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA source (id STRING, items <list_type>, other <list_type>, scale <element>);
      CREATE SCHEMA result (id STRING, value <list_type> OPTIONAL);
      CREATE SCHEMA failure (input_id STRING, reason STRING);
      CREATE JAQ JSON CODEC source_codec TO SCHEMA source WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY sources SCHEMA source UNBRANCHED;
      CREATE RELAY results SCHEMA result UNBRANCHED;
      CREATE RELAY failures SCHEMA failure UNBRANCHED;
      CREATE VHOST edge arithmetic-errors-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT source_ingress ON edge PATH '/arithmetic';
      CREATE HTTP ENDPOINT INGESTOR ingestor_node
        FROM source_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING source_codec
        TO sources INHERIT ALL UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION arithmetic FROM sources UNBRANCHED
        TO results SET id = input.id, value = <expression>
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO failures SET input_id = input.id, reason = error.message;
      CREATE SUBSCRIPTION results_subscription TO results;
      CREATE SUBSCRIPTION failures_subscription TO failures;
      START;
      """
    And http payload is posted to node "node-1" with host "arithmetic-errors-{{test_id}}.example.com" path "/arithmetic"
      """
      [{"id":"bad",<bad_fields>},{"id":"good","items":[6,-4],"other":[2,-2],"scale":2},{"id":"empty","items":[],"other":[],"scale":0}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "input_id":"bad" | <reason>
      "id":"good" | "value":<expected>
      "id":"empty" | "value":[]
      """

    Examples:
      | cluster_size | list_type | element | expression                                | bad_fields                                                                   | reason                                                                  | expected    |
      | 1            | VEC<I64>  | I64     | list_add(input.items, input.other)        | "items":[9223372036854775807,9223372036854775807],"other":[1,2],"scale":2    | overflow: integer addition overflowed                                   | [8,-6]      |
      | 1            | VEC<I64>  | I64     | list_div(input.items, input.other)        | "items":[6,8],"other":[0,0],"scale":2                                        | division_by_zero: integer division by zero                              | [3,2]       |
      | 1            | VEC<I64>  | I64     | list_div_scalar(input.items, input.scale) | "items":[-9223372036854775808,-9223372036854775808],"other":[1,2],"scale":-1 | overflow: integer division overflowed                                   | [3,-2]      |
      | 1            | VEC<I64>  | I64     | list_abs(input.items)                     | "items":[-9223372036854775808,-9223372036854775808],"other":[1,2],"scale":2  | overflow: integer absolute value overflowed                             | [6,4]       |
      | 1            | VEC<I64>  | I64     | list_add(input.items, input.other)        | "items":[1,2],"other":[3],"scale":2                                          | invalid_argument: vector lengths differ: left has 2, right has 1        | [8,-6]      |
      | 1            | VEC<F64>  | F64     | list_mul_scalar(input.items, input.scale) | "items":[1e308,1e308],"other":[1,2],"scale":2                                | invalid_argument: floating-point operation produced a non-finite result | [12.0,-8.0] |
      | 3            | VEC<I64>  | I64     | list_add(input.items, input.other)        | "items":[9223372036854775807,9223372036854775807],"other":[1,2],"scale":2    | overflow: integer addition overflowed                                   | [8,-6]      |
      | 3            | VEC<I64>  | I64     | list_div(input.items, input.other)        | "items":[6,8],"other":[0,0],"scale":2                                        | division_by_zero: integer division by zero                              | [3,2]       |
      | 3            | VEC<I64>  | I64     | list_div_scalar(input.items, input.scale) | "items":[-9223372036854775808,-9223372036854775808],"other":[1,2],"scale":-1 | overflow: integer division overflowed                                   | [3,-2]      |
      | 3            | VEC<I64>  | I64     | list_abs(input.items)                     | "items":[-9223372036854775808,-9223372036854775808],"other":[1,2],"scale":2  | overflow: integer absolute value overflowed                             | [6,4]       |
      | 3            | VEC<I64>  | I64     | list_add(input.items, input.other)        | "items":[1,2],"other":[3],"scale":2                                          | invalid_argument: vector lengths differ: left has 2, right has 1        | [8,-6]      |
      | 3            | VEC<F64>  | F64     | list_mul_scalar(input.items, input.scale) | "items":[1e308,1e308],"other":[1,2],"scale":2                                | invalid_argument: floating-point operation produced a non-finite result | [12.0,-8.0] |

  Scenario Outline: Arithmetic preserves nested fixed shapes and mixed list containers
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA source (first I64, second I64);
      CREATE SCHEMA result (matrix <matrix_type>, combined <vector_type>);
      CREATE JAQ JSON CODEC source_codec TO SCHEMA source WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY sources SCHEMA source UNBRANCHED;
      CREATE RELAY results SCHEMA result UNBRANCHED;
      CREATE VHOST edge arithmetic-shapes-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT source_ingress ON edge PATH '/arithmetic';
      CREATE HTTP ENDPOINT INGESTOR ingestor_node
        FROM source_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING source_codec
        TO sources INHERIT ALL UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION arithmetic FROM sources UNBRANCHED
        TO results
          SET matrix = list_mul_scalar([[input.first, input.second], [input.second, input.first]], 2),
              combined = list_add(vec(input.first, input.second), [input.second, input.first])
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION results_subscription TO results;
      START;
      """
    And http payload is posted to node "node-1" with host "arithmetic-shapes-{{test_id}}.example.com" path "/arithmetic"
      """
      [{"first":2,"second":3}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "matrix":[[4,6],[6,4]] | "combined":[5,5]
      """

    Examples:
      | cluster_size | matrix_type             | vector_type |
      | 1            | ARRAY<ARRAY<I64, 2>, 2> | VEC<I64>    |
      | 3            | ARRAY<ARRAY<I64, 2>, 2> | VEC<I64>    |

  Scenario Outline: Arithmetic refuses implicit casts shapes broadcasts and sensitive leakage when applied
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA source (items <vector_type>, other <float_type>, fixed <array_type>, wide <wide_type>, nested <nested_type>, scale I64, secret_scale I64 SENSITIVE, secret <vector_type> SENSITIVE);
      CREATE SCHEMA result (value <vector_type>);
      CREATE RELAY sources SCHEMA source UNBRANCHED;
      CREATE RELAY results SCHEMA result UNBRANCHED;
      """
    When these Plexus commands fail with "<reason>"
      """
      CREATE JUNCTION invalid_arithmetic FROM sources UNBRANCHED
        TO results SET value = <expression>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size | vector_type | float_type | array_type    | wide_type     | nested_type   | expression                                       | reason                                                     |
      | 1            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | list_add(input.fixed, input.wide)                | requires equal ARRAY widths                                |
      | 1            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | list_add(input.items, input.other)               | requires exact matching element types                      |
      | 1            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | list_add_scalar(input.items, input.scale AS I32) | requires exact matching element types                      |
      | 1            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | list_add(input.items, input.scale)               | requires ARRAY or VEC input                                |
      | 1            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | list_add(input.nested, input.nested)             | requires numeric input                                     |
      | 1            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | abs(input.items)                                 | requires numeric input                                     |
      | 1            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | input.items + input.items                        | unsupported_binary                                         |
      | 1            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | list_abs(input.secret)                           | would store sensitive data in a non-sensitive output field |
      | 3            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | list_add(input.fixed, input.wide)                | requires equal ARRAY widths                                |
      | 3            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | list_add(input.items, input.other)               | requires exact matching element types                      |
      | 3            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | list_add_scalar(input.items, input.scale AS I32) | requires exact matching element types                      |
      | 3            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | list_add(input.items, input.scale)               | requires ARRAY or VEC input                                |
      | 3            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | list_add(input.nested, input.nested)             | requires numeric input                                     |
      | 3            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | abs(input.items)                                 | requires numeric input                                     |
      | 3            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | input.items + input.items                        | unsupported_binary                                         |
      | 3            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | list_abs(input.secret)                           | would store sensitive data in a non-sensitive output field |
      | 1            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | list_add(input.items, input.secret)              | would store sensitive data in a non-sensitive output field |
      | 1            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | list_add_scalar(input.items, input.secret_scale) | would store sensitive data in a non-sensitive output field |
      | 3            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | list_add(input.items, input.secret)              | would store sensitive data in a non-sensitive output field |
      | 3            | VEC<I64>    | VEC<F64>   | ARRAY<I64, 2> | ARRAY<I64, 3> | VEC<VEC<I64>> | list_add_scalar(input.items, input.secret_scale) | would store sensitive data in a non-sensitive output field |
