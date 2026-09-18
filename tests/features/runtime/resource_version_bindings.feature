Feature: Resource version bindings
  Scenario Outline: Every versioned resource binding requires an explicit VERSION clause
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail with "expected VERSION"
      """
      CREATE VHOST edge api.example.com WITH TLS tls_bundle;
      """
    And these NSPL commands fail with "expected VERSION"
      """
      CREATE CODEC notification_codec
        FROM PROTOBUF USING RESOURCE proto_bundle
        CONFIG {'file' = 'bindings.proto'}
        MESSAGE 'nervix.test.Notification'
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '{user_id: .user_id}';
      """
    And these NSPL commands fail with "expected VERSION"
      """
      CREATE SIGNALING PROTOCOL protobuf_subscribe
        FORMAT PROTOBUF USING RESOURCE proto_bundle
          CONFIG {'file' = 'bindings.proto'}
          SEND MESSAGE 'nervix.test.Subscribe'
          WAIT MESSAGE 'nervix.test.Ack'
        ON CONNECT
        SEND JAQ '{id: 1}'
        WAIT JAQ '.id == 1'
        TIMEOUT 5s;
      """
    And these NSPL commands fail with "expected VERSION"
      """
      CREATE INFERENCER score_model FROM features
        USING RESOURCE fraud_model
        FILE 'models/simple_score.onnx'
        INPUTS { "features" <tensor_type>[2] = input.vector }
        OUTPUT SCHEMA { "score" <tensor_type>[1] }
        UNBRANCHED
        TO scored
        SET score = score
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      """
    And these NSPL commands fail with "expected VERSION"
      """
      CREATE WASM PROCESSOR filter_even_rows FROM raw_metrics
        USING RESOURCE wasm_filter
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        UNBRANCHED
        TO filtered_metrics
        SET value = value
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      """
    And these NSPL commands fail with "expected VERSION"
      """
      CREATE HASH MAP lookup_by_id
        KEY id
        FROM RESOURCE lookup_bundle
        PATH 'lookup.jsonl'
        DECODE USING lookup_codec;
      """
    And these NSPL commands fail with "expected VERSION"
      """
      CREATE CLIENT mounted_http
        TYPE HTTP
        MOUNT tls_bundle
        CONFIG {'endpoint' = 'http://127.0.0.1:1'};
      """

    Examples:
      | cluster_size | tensor_type       |
      | 1            | DENSE TENSOR<F32> |
      | 3            | DENSE TENSOR<F32> |

  Scenario Outline: VERSION LATEST pins the highest completed version on every binding kind
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has TLS resource directory "tls_v1" for hosts "latest-{{test_id}}.example.com"
    And node "node-1" has TLS resource directory "tls_v2" for hosts "latest-{{test_id}}.example.com"
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "bindings.proto": "syntax = \"proto3\";\npackage nervix.test;\n\nmessage Notification {\n  uint32 user_id = 1;\n}\n\nmessage Subscribe {\n  uint32 id = 1;\n}\n\nmessage Ack {\n  uint32 id = 1;\n}\n"
      }
      """
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And node "node-1" has resource directory "lookup_dir" containing
      """
      {
        "lookup.jsonl": "{\"id\":\"one\",\"value\":1}\n"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE tls_bundle;
      UPLOAD RESOURCE tls_bundle VERSION '{{tls_v1}}';
      UPLOAD RESOURCE tls_bundle VERSION '{{tls_v2}}';
      CREATE RESOURCE proto_bundle;
      UPLOAD RESOURCE proto_bundle VERSION '{{proto_dir}}';
      UPLOAD RESOURCE proto_bundle VERSION '{{proto_dir}}';
      CREATE RESOURCE fraud_model;
      UPLOAD RESOURCE fraud_model VERSION '{{onnx_model}}';
      UPLOAD RESOURCE fraud_model VERSION '{{onnx_model}}';
      CREATE RESOURCE wasm_filter;
      UPLOAD RESOURCE wasm_filter VERSION '{{wasm_processor}}';
      UPLOAD RESOURCE wasm_filter VERSION '{{wasm_processor}}';
      CREATE RESOURCE lookup_bundle;
      UPLOAD RESOURCE lookup_bundle VERSION '{{lookup_dir}}';
      UPLOAD RESOURCE lookup_bundle VERSION '{{lookup_dir}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE VHOST secure latest-{{test_id}}.example.com WITH TLS tls_bundle VERSION LATEST;
      """
    Then the last command output contains
      """
      resolved VERSION LATEST of resource 'tls_bundle' to version 2 for vhost 'secure'
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE CODEC notification_codec
        FROM PROTOBUF USING RESOURCE proto_bundle VERSION LATEST
        CONFIG {'file' = 'bindings.proto', 'include' = '.'}
        MESSAGE 'nervix.test.Notification'
        TO SCHEMA notification
        WITH JAQ TRANSFORMATIONS ON INGESTION '{user_id: .user_id}';
      CREATE SIGNALING PROTOCOL protobuf_subscribe
        FORMAT PROTOBUF USING RESOURCE proto_bundle VERSION LATEST
          CONFIG {'file' = 'bindings.proto', 'include' = '.'}
          SEND MESSAGE 'nervix.test.Subscribe'
          WAIT MESSAGE 'nervix.test.Ack'
        ON CONNECT
        SEND JAQ '{id: 1}'
        WAIT JAQ '.id == 1'
        TIMEOUT 5s;
      CREATE SCHEMA features ( vector <vector_type> );
      CREATE SCHEMA scored ( score <score_type> );
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      CREATE INFERENCER score_model FROM features
        USING RESOURCE fraud_model VERSION LATEST
        FILE 'models/simple_score.onnx'
        INPUTS { "features" <tensor_type>[2] = input.vector }
        OUTPUT SCHEMA { "score" <tensor_type>[1] }
        UNBRANCHED
        TO scored
        SET score = score
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      CREATE SCHEMA metric ( value I32 );
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE RELAY filtered_metrics SCHEMA metric UNBRANCHED;
      CREATE WASM PROCESSOR filter_even_rows FROM raw_metrics
        USING RESOURCE wasm_filter VERSION LATEST
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        UNBRANCHED
        TO filtered_metrics
        SET value = value
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA lookup_entry ( id STRING, value I64 );
      CREATE WIRE JSON SCHEMA lookup_entry_wire MODE STRICT (
        id string,
        value integer
      );
      CREATE CODEC lookup_entry_codec
        FROM WIRE JSON SCHEMA lookup_entry_wire
        TO SCHEMA lookup_entry;
      CREATE HASH MAP lookup_by_id
        KEY id
        FROM RESOURCE lookup_bundle VERSION LATEST
        PATH 'lookup.jsonl'
        DECODE USING lookup_entry_codec;
      """
    Then the last command output contains
      """
      resolved VERSION LATEST of resource 'lookup_bundle' to version 2 for lookup 'lookup_by_id'
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE CLIENT mounted_http
        TYPE HTTP
        MOUNT tls_bundle VERSION LATEST
        CONFIG {'endpoint' = 'http://127.0.0.1:1'};
      """
    Then the last command output contains
      """
      resolved VERSION LATEST of resource 'tls_bundle' to version 2 for client 'mounted_http'
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE VHOST secure;
      """
    Then the last command output contains
      """
      CREATE VHOST secure latest-{{test_id}}.example.com WITH TLS tls_bundle VERSION 2;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE CODEC notification_codec;
      """
    Then the last command output contains
      """
      PROTOBUF USING RESOURCE proto_bundle VERSION 2
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE SIGNALING PROTOCOL protobuf_subscribe;
      """
    Then the last command output contains
      """
      PROTOBUF USING RESOURCE proto_bundle VERSION 2
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE INFERENCER score_model;
      """
    Then the last command output contains
      """
      USING RESOURCE fraud_model VERSION 2
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE WASM PROCESSOR filter_even_rows;
      """
    Then the last command output contains
      """
      USING RESOURCE wasm_filter VERSION 2
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE HASH MAP lookup_by_id;
      """
    Then the last command output contains
      """
      FROM RESOURCE lookup_bundle VERSION 2
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE CLIENT mounted_http;
      """
    Then the last command output contains
      """
      MOUNT tls_bundle VERSION 2
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE WASM PROCESSOR filter_even_rows;
      """
    Then the last command output contains
      """
      resource version: 2
      """
    When these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE tls_bundle TO VERSION 1 FOR VHOST secure, CLIENT mounted_http;
      """
    Then the last command output contains
      """
      - kind=vhost name=secure from=2 to=1
      """
    And the last command output contains
      """
      - kind=client name=mounted_http from=2 to=1
      """
    When these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE proto_bundle TO VERSION 1 FOR CODEC notification_codec, SIGNALING PROTOCOL protobuf_subscribe;
      """
    Then the last command output contains
      """
      - kind=codec name=notification_codec from=2 to=1
      """
    And the last command output contains
      """
      - kind=signaling_protocol name=protobuf_subscribe from=2 to=1
      """
    When these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE fraud_model TO VERSION 1 FOR INFERENCER score_model;
      """
    Then the last command output contains
      """
      - kind=inferencer name=score_model from=2 to=1
      """
    When these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE wasm_filter TO VERSION 1 FOR WASM PROCESSOR filter_even_rows;
      """
    Then the last command output contains
      """
      - kind=wasm_processor name=filter_even_rows from=2 to=1
      """
    When these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE lookup_bundle TO VERSION 1 FOR HASH MAP lookup_by_id;
      """
    Then the last command output contains
      """
      - kind=hash_map name=lookup_by_id from=2 to=1
      """

    Examples:
      | cluster_size | tensor_type       | vector_type   | score_type    |
      | 1            | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
      | 3            | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |

  Scenario Outline: Rebinding a hash map atomically moves its pinned resource version
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "lookup_v1" containing
      """
      {
        "lookup.jsonl": "{\"id\":\"one\",\"value\":1}\n"
      }
      """
    And node "node-1" has resource directory "lookup_v2" containing
      """
      {
        "lookup.jsonl": "{\"id\":\"one\",\"value\":2}\n"
      }
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE lookup_bundle;
      UPLOAD RESOURCE lookup_bundle VERSION '{{lookup_v1}}';
      CREATE SCHEMA lookup_entry (id STRING, value I64);
      CREATE WIRE JSON SCHEMA lookup_wire MODE STRICT (id string, value integer);
      CREATE CODEC lookup_codec FROM WIRE JSON SCHEMA lookup_wire TO SCHEMA lookup_entry;
      CREATE HASH MAP lookup_by_id
        KEY id
        FROM RESOURCE lookup_bundle VERSION 1
        PATH 'lookup.jsonl'
        DECODE USING lookup_codec;
      UPLOAD RESOURCE lookup_bundle VERSION '{{lookup_v2}}';
      """
    When these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE lookup_bundle TO VERSION LATEST;
      """
    Then the last command output contains
      """
      rebound 1 of 1 usage(s) of resource 'lookup_bundle' to version 2 (latest)
      """
    And the last command output contains
      """
      quiesce level: DYNAMIC
      """
    And the last command output contains
      """
      - kind=hash_map name=lookup_by_id from=1 to=2
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE HASH MAP lookup_by_id;
      """
    Then the last command output contains
      """
      FROM RESOURCE lookup_bundle VERSION 2
      """
    When these NSPL commands are executed on the leader node
      """
      LOOKUP lookup_by_id KEY 'one';
      """
    Then the last command output contains
      """
      "value":2
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE RESOURCE lookup_bundle;
      """
    Then the last command output contains
      """
      usages:
      - kind=hash_map name=lookup_by_id version=2
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE RESOURCE lookup_bundle VERSION 1;
      """
    Then the last command output contains
      """
      usages:
      - none
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE RESOURCE lookup_bundle VERSION 2;
      """
    Then the last command output contains
      """
      usages:
      - kind=hash_map name=lookup_by_id version=2
      """
    When these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE lookup_bundle TO VERSION 2;
      """
    Then the last command output contains
      """
      rebound 0 of 1 usage(s) of resource 'lookup_bundle' to version 2
      """
    And the last command output contains
      """
      - kind=hash_map name=lookup_by_id from=2 to=2 unchanged
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: REBIND VERSION LATEST in a transaction resolves when COMMIT applies it
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has TLS resource directory "tls_v1" for hosts "commit-{{test_id}}.example.com"
    And node "node-1" has TLS resource directory "tls_v2" for hosts "commit-{{test_id}}.example.com"
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE tls_bundle;
      UPLOAD RESOURCE tls_bundle VERSION '{{tls_v1}}';
      CREATE VHOST edge commit-{{test_id}}.example.com WITH TLS tls_bundle VERSION 1;
      """
    Given client "owner" is connected to the leader node
    And client "uploader" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      REBIND RESOURCE tls_bundle TO VERSION LATEST FOR VHOST edge;
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    And the last command output contains
      """
      rebound 0 of 1 usage(s) of resource 'tls_bundle' to version 1 (latest)
      """
    And the last command output contains
      """
      provisionally resolved VERSION LATEST of resource 'tls_bundle' to version 1 for vhost 'edge'
      """
    When client "uploader" uploads resource "tls_bundle" from "{{tls_v2}}" with identity "after-queue"
    And client "owner" fails to execute these NSPL commands
      """
      COMMIT;
      """
    Then the last command error contains
      """
      transaction preview is stale
      """
    And transaction "{{transaction_id}}" eventually has state "OPEN"
    When client "owner" executes these NSPL commands
      """
      COMMIT;
      """
    Then the last command output contains
      """
      resolved VERSION LATEST of resource 'tls_bundle' to version 2 for vhost 'edge'
      """
    And transaction "{{transaction_id}}" eventually has state "COMMITTED"
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE VHOST edge;
      """
    Then the last command output contains
      """
      CREATE VHOST edge commit-{{test_id}}.example.com WITH TLS tls_bundle VERSION 2;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: REBIND FOR selects an exact kind-qualified usage set atomically
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has TLS resource directory "tls_v1" for hosts "select-{{test_id}}.example.com"
    And node "node-1" has TLS resource directory "tls_v2" for hosts "select-{{test_id}}.example.com"
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE tls_bundle;
      UPLOAD RESOURCE tls_bundle VERSION '{{tls_v1}}';
      UPLOAD RESOURCE tls_bundle VERSION '{{tls_v2}}';
      CREATE RESOURCE other_bundle;
      UPLOAD RESOURCE other_bundle VERSION '{{tls_v1}}';
      CREATE VHOST edge select-{{test_id}}.example.com WITH TLS tls_bundle VERSION 1;
      CREATE CLIENT primary TYPE HTTP MOUNT tls_bundle VERSION 1 CONFIG {'endpoint' = 'http://127.0.0.1:1'};
      CREATE CLIENT secondary TYPE HTTP MOUNT tls_bundle VERSION 1 CONFIG {'endpoint' = 'http://127.0.0.1:1'};
      CREATE CLIENT spare TYPE HTTP MOUNT tls_bundle VERSION 1 CONFIG {'endpoint' = 'http://127.0.0.1:1'};
      CREATE CLIENT foreign TYPE HTTP MOUNT other_bundle VERSION 1 CONFIG {'endpoint' = 'http://127.0.0.1:1'};
      """
    When these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE tls_bundle TO VERSION LATEST FOR CLIENT primary;
      """
    Then the last command output contains
      """
      rebound 1 of 1 usage(s) of resource 'tls_bundle' to version 2 (latest)
      """
    And the last command output contains
      """
      - kind=client name=primary from=1 to=2
      """
    When these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE tls_bundle TO VERSION 2 FOR CLIENT secondary, VHOST edge, CLIENT primary;
      """
    Then the last command output contains
      """
      rebound 2 of 3 usage(s) of resource 'tls_bundle' to version 2
      """
    And the last command output contains
      """
      - kind=vhost name=edge from=1 to=2
      """
    And the last command output contains
      """
      - kind=client name=primary from=2 to=2 unchanged
      """
    And the last command output contains
      """
      - kind=client name=secondary from=1 to=2
      """
    When these NSPL commands fail with "CLIENT 'foreign' does not bind resource 'tls_bundle'"
      """
      REBIND RESOURCE tls_bundle TO VERSION 2 FOR CLIENT foreign;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE CLIENT spare;
      """
    Then the last command output contains
      """
      MOUNT tls_bundle VERSION 1
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE CLIENT foreign;
      """
    Then the last command output contains
      """
      MOUNT other_bundle VERSION 1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: REBIND rejects unavailable targets and treats an unused resource as a no-op
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "orphan_v1" containing
      """
      {
        "unused.txt": "present"
      }
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE orphan;
      UPLOAD RESOURCE orphan VERSION '{{orphan_v1}}';
      CREATE RESOURCE empty;
      """
    When these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE orphan TO VERSION 1;
      """
    Then the last command output contains
      """
      rebound 0 of 0 usage(s) of resource 'orphan' to version 1
      quiesce level: DYNAMIC
      """
    When these NSPL commands fail with "resource 'missing' does not exist"
      """
      REBIND RESOURCE missing TO VERSION 1;
      """
    And these NSPL commands fail with "resource 'orphan@2' does not exist in domain '{{domain}}'"
      """
      REBIND RESOURCE orphan TO VERSION 2;
      """
    And these NSPL commands fail with "resource 'empty' has no completed versions in domain '{{domain}}'"
      """
      REBIND RESOURCE empty TO VERSION LATEST;
      """
    And these NSPL commands fail with "VHOST 'missing' does not exist in domain '{{domain}}'"
      """
      REBIND RESOURCE orphan TO VERSION 1 FOR VHOST missing;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A failed rebound model validation leaves every selected usage unchanged
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "shared_v1"
    And resource directory "shared_v1" additionally contains
      """
      {
        "lookup.jsonl": "{\"id\":\"one\",\"value\":1}\n"
      }
      """
    And node "node-1" has ONNX fixture resource directory "shared_v2" with "models/simple_score.onnx" copied from fixture "matrix_identity.onnx"
    And resource directory "shared_v2" additionally contains
      """
      {
        "lookup.jsonl": "{\"id\":\"one\",\"value\":2}\n"
      }
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE shared_bundle;
      UPLOAD RESOURCE shared_bundle VERSION '{{shared_v1}}';
      CREATE SCHEMA features (vector <vector_type>);
      CREATE SCHEMA scored (score <score_type>);
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED;
      CREATE INFERENCER score_model FROM features
        USING RESOURCE shared_bundle VERSION 1
        FILE 'models/simple_score.onnx'
        INPUTS { "features" <tensor_type>[2] = input.vector }
        OUTPUT SCHEMA { "score" <tensor_type>[1] }
        UNBRANCHED
        TO scored
        SET score = score
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      CREATE SCHEMA lookup_entry (id STRING, value I64);
      CREATE WIRE JSON SCHEMA lookup_wire MODE STRICT (id string, value integer);
      CREATE CODEC lookup_codec FROM WIRE JSON SCHEMA lookup_wire TO SCHEMA lookup_entry;
      CREATE HASH MAP lookup_by_id
        KEY id
        FROM RESOURCE shared_bundle VERSION 1
        PATH 'lookup.jsonl'
        DECODE USING lookup_codec;
      UPLOAD RESOURCE shared_bundle VERSION '{{shared_v2}}';
      """
    When these NSPL commands fail with "invalid INFERENCER 'score_model':"
      """
      REBIND RESOURCE shared_bundle TO VERSION 2;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE INFERENCER score_model;
      """
    Then the last command output contains
      """
      USING RESOURCE shared_bundle VERSION 1
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE HASH MAP lookup_by_id;
      """
    Then the last command output contains
      """
      FROM RESOURCE shared_bundle VERSION 1
      """
    When these NSPL commands are executed on the leader node
      """
      LOOKUP lookup_by_id KEY 'one';
      """
    Then the last command output contains
      """
      "value":1
      """

    Examples:
      | cluster_size | tensor_type       | vector_type   | score_type    |
      | 1            | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
      | 3            | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |

  Scenario Outline: Pinned bindings keep their version through uploads, dynamic alterations and a cluster restart
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has TLS resource directory "tls_v1" for hosts "drift-{{test_id}}.example.com"
    And node "node-1" has TLS resource directory "tls_v2" for hosts "drift-{{test_id}}.example.com"
    And node "node-1" has ONNX fixture resource directory "onnx_v1"
    And node "node-1" has ONNX fixture resource directory "onnx_v2" with "models/simple_score.onnx" copied from fixture "alternate_score.onnx"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE tls_bundle;
      UPLOAD RESOURCE tls_bundle VERSION '{{tls_v1}}';
      CREATE RESOURCE fraud_model;
      UPLOAD RESOURCE fraud_model VERSION '{{onnx_v1}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA features ( vector <vector_type> );
      CREATE SCHEMA scored ( score <score_type> );
      CREATE WIRE JSON SCHEMA features_wire MODE STRICT ( vector array );
      CREATE CODEC features_codec FROM WIRE JSON SCHEMA features_wire TO SCHEMA features;
      CREATE RELAY features SCHEMA features UNBRANCHED;
      CREATE RELAY scored SCHEMA scored UNBRANCHED CAPACITY 16;
      CREATE VHOST edge drift-{{test_id}}.example.com WITH TLS tls_bundle VERSION LATEST;
      CREATE ENDPOINT ingress ON edge PATH '/features' TYPE HTTP;
      CREATE INGESTOR feature_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING features_codec
        TO features
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INFERENCER score_model FROM features
        USING RESOURCE fraud_model VERSION LATEST
        FILE 'models/simple_score.onnx'
        INPUTS { "features" <tensor_type>[2] = input.vector }
        OUTPUT SCHEMA { "score" <tensor_type>[1] }
        UNBRANCHED
        TO scored
        SET score = score
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION scored_subscription TO scored;
      START;
      """
    And https payload is posted to host "drift-{{test_id}}.example.com" path "/features" using CA from resource directory "tls_v1"
      """
      {"vector":[1.0,0.0]}
      """
    Then within "5s" the relay subscription receives payloads
      """
      {"score":[0.875]}
      """
    When these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE tls_bundle VERSION '{{tls_v2}}';
      UPLOAD RESOURCE fraud_model VERSION '{{onnx_v2}}';
      """
    And these NSPL commands are executed on the leader node
      """
      ALTER RELAY scored SET CAPACITY 32;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE VHOST edge;
      """
    Then the last command output contains
      """
      CREATE VHOST edge drift-{{test_id}}.example.com WITH TLS tls_bundle VERSION 1;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE INFERENCER score_model;
      """
    Then the last command output contains
      """
      USING RESOURCE fraud_model VERSION 1
      """
    When https payload is posted to host "drift-{{test_id}}.example.com" path "/features" using CA from resource directory "tls_v1"
      """
      {"vector":[1.0,0.0]}
      """
    Then within "5s" the relay subscription receives payloads
      """
      {"score":[0.875]}
      """
    When the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE VHOST edge;
      """
    Then the last command output contains
      """
      CREATE VHOST edge drift-{{test_id}}.example.com WITH TLS tls_bundle VERSION 1;
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE INFERENCER score_model;
      """
    Then the last command output contains
      """
      USING RESOURCE fraud_model VERSION 1
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION scored_subscription TO scored;
      """
    Then within "10s" repeatedly posting https payload to host "drift-{{test_id}}.example.com" path "/features" using CA from resource directory "tls_v1" yields a relay subscription payload
      """
      {"vector":[1.0,0.0]}
      """
    And the last relay subscription payload contains
      """
      {"score":[0.875]}
      """
    When these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE tls_bundle TO VERSION LATEST FOR VHOST edge;
      """
    Then the last command output contains
      """
      - kind=vhost name=edge from=1 to=2
      """
    And the last command output contains
      """
      quiesce level: DYNAMIC
      """
    When these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE fraud_model TO VERSION LATEST FOR INFERENCER score_model;
      """
    Then the last command output contains
      """
      - kind=inferencer name=score_model from=1 to=2
      """
    And the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    Then within "10s" repeatedly posting https payload to host "drift-{{test_id}}.example.com" path "/features" using CA from resource directory "tls_v2" yields a relay subscription payload
      """
      {"vector":[1.0,0.0]}
      """
    And the last relay subscription payload contains
      """
      {"score":[0.25]}
      """
    When these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE tls_bundle TO VERSION 1 FOR VHOST edge;
      REBIND RESOURCE fraud_model TO VERSION 1 FOR INFERENCER score_model;
      """
    Then the last command output contains
      """
      - kind=inferencer name=score_model from=2 to=1
      """
    Then within "10s" repeatedly posting https payload to host "drift-{{test_id}}.example.com" path "/features" using CA from resource directory "tls_v1" yields a relay subscription payload
      """
      {"vector":[1.0,0.0]}
      """
    And the last relay subscription payload contains
      """
      {"score":[0.875]}
      """

    Examples:
      | cluster_size | tensor_type       | vector_type   | score_type    |
      | 1            | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
      | 3            | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
