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
    And these NSPL commands are executed on the leader node
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
      DESCRIBE WASM PROCESSOR filter_even_rows;
      """
    Then the last command output contains
      """
      resource version: 2
      """

    Examples:
      | cluster_size | tensor_type       | vector_type   | score_type    |
      | 1            | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
      | 3            | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |

  Scenario Outline: VERSION LATEST in a transaction resolves when COMMIT applies it
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
      """
    Given client "owner" is connected to the leader node
    And client "uploader" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      CREATE VHOST edge commit-{{test_id}}.example.com WITH TLS tls_bundle VERSION LATEST;
      """
    Then the last command output contains
      """
      provisionally resolved VERSION LATEST of resource 'tls_bundle' to version 1 for vhost 'edge'
      """
    When client "uploader" uploads resource "tls_bundle" from "{{tls_v2}}" with identity "after-queue"
    And client "owner" executes these NSPL commands
      """
      COMMIT;
      """
    Then the last command output contains
      """
      resolved VERSION LATEST of resource 'tls_bundle' to version 2 for vhost 'edge'
      """
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

    Examples:
      | cluster_size | tensor_type       | vector_type   | score_type    |
      | 1            | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
      | 3            | DENSE TENSOR<F32> | ARRAY<F32, 2> | ARRAY<F32, 1> |
