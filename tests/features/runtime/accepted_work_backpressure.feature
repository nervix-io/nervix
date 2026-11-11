Feature: Accepted work waits for internal worker room

  @accepted_worker_room
  Scenario Outline: A NO ACK input survives a full worker class
    Given node-1 starts with vm-inline-row-limit = 0
    And node-2 starts with vm-inline-row-limit = 0
    And node-3 starts with vm-inline-row-limit = 0
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UDF add_one WITH ROTO_0_13 ARGS (value I64) RETURNS I64
        CODE $roto$ fn add_one(value: I64Column) -> I64Column { value.add_s(1) } $roto$;
      CREATE SCHEMA event ( id STRING, value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( id string, value integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY accepted SCHEMA event UNBRANCHED;
      CREATE RELAY delivered SCHEMA event UNBRANCHED;
      CREATE VHOST edge accepted-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR source FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO accepted INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR IGNORE;
      CREATE JUNCTION transform FROM accepted UNBRANCHED
        TO delivered SET id = input.id, value = <expression>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION output TO delivered;
      START;
      """
    Given processor "junction" "transform" pauses around its accepted input's first poll
    When http payload is posted to node "node-1" with host "accepted-{{test_id}}.example.com" path "/events"
      """
      {"id":"kept","value":41}
      """
    Then processor "junction" "transform" has accepted its paused input
    When accepted-work CPU class "<cpu_class>" is full on every node
    And processor "junction" "transform" first polls its paused input
    And accepted-work CPU classes are released on every node
    And processor "junction" "transform" resumes its paused input
    And http payload is posted to node "node-1" with host "accepted-{{test_id}}.example.com" path "/events"
      """
      {"id":"barrier","value":41}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "id":"kept" | "value":42
      "id":"barrier" | "value":42
      """
    And the active session observes no server error containing "transform" within "100ms"

    @accepted_data_room
    Examples:
      | cluster_size | cpu_class | expression      |
      | 1            | data      | input.value + 1 |
      | 3            | data      | input.value + 1 |

    @accepted_extension_room
    Examples:
      | cluster_size | cpu_class | expression                |
      | 1            | extension | udf::add_one(input.value) |
      | 3            | extension | udf::add_one(input.value) |

  @accepted_worker_stop
  Scenario Outline: STOP ends accepted input waiting behind an extension worker that never returns
    Given schema change drain timeout is configured as "2s"
    And node-1 starts with vm-inline-row-limit = 0
    And node-2 starts with vm-inline-row-limit = 0
    And node-3 starts with vm-inline-row-limit = 0
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UDF add_one WITH ROTO_0_13 ARGS (value I64) RETURNS I64
        CODE $roto$ fn add_one(value: I64Column) -> I64Column { value.add_s(1) } $roto$;
      CREATE SCHEMA event ( id STRING, value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( id string, value integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY accepted SCHEMA event UNBRANCHED;
      CREATE RELAY delivered SCHEMA event UNBRANCHED;
      CREATE VHOST edge accepted-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR source FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO accepted INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR IGNORE;
      CREATE JUNCTION transform FROM accepted UNBRANCHED
        TO delivered SET id = input.id, value = udf::add_one(input.value)
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION output TO delivered;
      START;
      """
    Given processor "junction" "transform" pauses around its accepted input's first poll
    When http payload is posted to node "node-1" with host "accepted-{{test_id}}.example.com" path "/events"
      """
      {"id":"stopped","value":41}
      """
    Then processor "junction" "transform" has accepted its paused input
    When accepted-work CPU class "extension" is full on every node
    And processor "junction" "transform" first polls its paused input
    And processor "junction" "transform" resumes its paused input
    Then within "30s" these Plexus commands complete on the leader node
      """
      STOP;
      """
    When accepted-work CPU classes are released on every node

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @accepted_worker_shutdown
  Scenario Outline: Shutdown ends accepted input waiting behind an extension worker that never returns
    Given graceful shutdown drain is enabled
    And drain timeout is configured as "2s"
    And shutdown timeout is configured as "20s"
    And schema change drain timeout is configured as "2s"
    And node-1 starts with vm-inline-row-limit = 0
    And node-2 starts with vm-inline-row-limit = 0
    And node-3 starts with vm-inline-row-limit = 0
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UDF add_one WITH ROTO_0_13 ARGS (value I64) RETURNS I64
        CODE $roto$ fn add_one(value: I64Column) -> I64Column { value.add_s(1) } $roto$;
      CREATE SCHEMA event ( id STRING, value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( id string, value integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY accepted SCHEMA event UNBRANCHED;
      CREATE RELAY delivered SCHEMA event UNBRANCHED;
      CREATE VHOST edge accepted-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR source FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO accepted INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR IGNORE;
      CREATE JUNCTION transform FROM accepted UNBRANCHED
        TO delivered SET id = input.id, value = udf::add_one(input.value)
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION output TO delivered;
      START;
      """
    Given processor "junction" "transform" pauses around its accepted input's first poll
    When http payload is posted to node "node-1" with host "accepted-{{test_id}}.example.com" path "/events"
      """
      {"id":"stopped","value":41}
      """
    Then processor "junction" "transform" has accepted its paused input
    When accepted-work CPU class "extension" is full on every node
    And processor "junction" "transform" first polls its paused input
    And processor "junction" "transform" resumes its paused input
    When all nodes are gracefully stopped
    Then the last cluster operation completes within "30s"
    And every node reports that its last shutdown finished before its deadline
    When accepted-work CPU classes are released on every node

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @accepted_worker_request_refusal
  Scenario Outline: An acknowledged HTTP request still receives immediate admission refusal
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( id STRING, value I64 );
      CREATE JAQ JSON CODEC event_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY accepted SCHEMA event UNBRANCHED;
      CREATE VHOST edge request-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR source FROM ingress
        MODE ACK MAX 8 ACK TIMEOUT 30s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO accepted INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    When accepted-work CPU class "extension" is full on every node
    Then within "10s" HTTP admission on node "node-1" host "request-{{test_id}}.example.com" path "/events" refuses with retry-after
      """
      {"id":"retry","value":41}
      """
    When accepted-work CPU classes are released on every node

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @accepted_stateful_pipeline
  Scenario Outline: Stateful processors keep interleaved branches through a full data class
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And node-1 starts with vm-inline-row-limit = 0
    And node-2 starts with vm-inline-row-limit = 0
    And node-3 starts with vm-inline-row-limit = 0
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (tenant STRING, id STRING, value I64);
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m;
      CREATE JAQ JSON CODEC event_codec TO SCHEMA event WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY accepted SCHEMA event BRANCHED BY tenants;
      CREATE RELAY unique_events SCHEMA event BRANCHED BY tenants;
      CREATE RELAY ordered_events SCHEMA event BRANCHED BY tenants;
      CREATE RELAY left_windows SCHEMA event BRANCHED BY tenants;
      CREATE RELAY right_windows SCHEMA event BRANCHED BY tenants;
      CREATE RELAY delivered SCHEMA event BRANCHED BY tenants;
      CREATE VHOST edge stateful-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR source FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO accepted INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR IGNORE;
      CREATE DEDUPLICATOR unique_events FROM accepted DEDUPLICATE ON input.id MAX TIME 5m
        BRANCHED BY tenants TO unique_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE REORDERER ordered_events FROM unique_events BY input.value MAX TIME 1ms
        BRANCHED BY tenants TO ordered_events INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE WINDOW PROCESSOR windows FROM ordered_events WIDTH 2 MESSAGES STEP 1 MESSAGES
        BRANCHED BY tenants
        TO left_windows SET tenant = LAST(input.tenant), id = LAST(input.id), value = SUM(input.value)
          ON MESSAGE ERROR LOG
        TO right_windows SET tenant = LAST(input.tenant), id = LAST(input.id), value = SUM(input.value)
          ON MESSAGE ERROR LOG;
      CREATE CORRELATOR pairs LEFT FROM left_windows RIGHT FROM right_windows
        CORRELATE WHERE left.id = right.id MATCH EARLIEST MAX TIME 5m
        ON CORRELATION TIMEOUT DROP, DROP BRANCHED BY tenants
        TO delivered SET tenant = left.tenant, id = left.id, value = right.value
          FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION output TO delivered;
      START;
      """
    When http payload is posted to node "node-1" with host "stateful-{{test_id}}.example.com" path "/events"
      """
      [{"tenant":"alpha","id":"a1","value":1},{"tenant":"beta","id":"b1","value":10},
       {"tenant":"alpha","id":"a2","value":2},{"tenant":"beta","id":"b2","value":20}]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "id":"a2" | "value":3
      key={"tenant":"beta"} | "id":"b2" | "value":30
      """
    Given processor "<kind>" "<processor>" pauses around its accepted input's first poll
    When accepted-work rows 3 through 1027 are posted to node "node-1" host "stateful-{{test_id}}.example.com" path "/events"
    Then processor "<kind>" "<processor>" has accepted its paused input
    When accepted-work CPU class "data" is full on every node
    And processor "<kind>" "<processor>" first polls its paused input
    And accepted-work CPU classes are released on every node
    And processor "<kind>" "<processor>" resumes its paused input
    And http payload is posted to node "node-1" with host "stateful-{{test_id}}.example.com" path "/events"
      """
      [{"tenant":"alpha","id":"a2","value":2},{"tenant":"beta","id":"b2","value":20},
       {"tenant":"alpha","id":"a1028","value":1028},{"tenant":"beta","id":"b1028","value":10280}]
      """
    Then within "120s" accepted-work window rows 3 through 1028 arrive exactly once
    And the relay subscription does not receive a payload within "100ms"
    And the active session observes no server error containing "{{domain}}" within "100ms"

    @accepted_stateful_keys
    Examples:
      | cluster_size | kind         | processor     |
      | 1            | deduplicator | unique_events |
      | 3            | deduplicator | unique_events |

    @accepted_stateful_order
    Examples:
      | cluster_size | kind      | processor      |
      | 1            | reorderer | ordered_events |
      | 3            | reorderer | ordered_events |

    @accepted_stateful_window
    Examples:
      | cluster_size | kind   | processor |
      | 1            | window | windows   |
      | 3            | window | windows   |

    @accepted_stateful_correlation
    Examples:
      | cluster_size | kind       | processor |
      | 1            | correlator | pairs     |
      | 3            | correlator | pairs     |

  @accepted_stateful_model
  Scenario Outline: A stateful model keeps each branch's installed session while extension room is full
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has ONNX fixture resource directory "onnx_model"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE inference;
      """
    When these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE inference VERSION '{{onnx_model}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA features (tenant STRING, vector <input_type>);
      CREATE SCHEMA scored (score <output_type>);
      CREATE WIRE JSON SCHEMA features_wire MODE STRICT (tenant string, vector array);
      CREATE WIRE JSON CODEC features_codec FROM WIRE SCHEMA features_wire TO SCHEMA features;
      CREATE SCHEMA tenant_key (tenant STRING);
      CREATE BRANCH tenants SCHEMA tenant_key TTL 5m;
      CREATE RELAY features SCHEMA features BRANCHED BY tenants;
      CREATE RELAY scored SCHEMA scored BRANCHED BY tenants;
      CREATE VHOST edge model-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/features';
      CREATE HTTP ENDPOINT INGESTOR source FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING features_codec
        TO features INHERIT ALL BRANCHED BY tenants SET tenant = message.tenant
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR IGNORE;
      CREATE INFERENCER running FROM features
        USING RESOURCE inference VERSION 1 FILE 'models/running_sum.onnx'
        INPUTS {'features' <tensor_type>[2] = input.vector}
        STATE {'state_in' FROM 'state_out' <tensor_type>[2] INITIAL ZEROS}
        OUTPUT SCHEMA {'score' <tensor_type>[1]}
        BRANCHED BY tenants TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION output TO scored;
      START;
      """
    When http payload is posted to node "node-1" with host "model-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"alpha","vector":[1.0,10.0]}
      """
    And http payload is posted to node "node-1" with host "model-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[100.0,1000.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "score":[11.0]
      key={"tenant":"beta"} | "score":[1100.0]
      """
    Given processor "inferencer" "running" pauses around its accepted input's first poll
    When http payload is posted to node "node-1" with host "model-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"alpha","vector":[2.0,20.0]}
      """
    Then processor "inferencer" "running" has accepted its paused input
    When accepted-work CPU class "extension" is full on every node
    And processor "inferencer" "running" first polls its paused input
    And accepted-work CPU classes are released on every node
    And processor "inferencer" "running" resumes its paused input
    And http payload is posted to node "node-1" with host "model-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"beta","vector":[200.0,2000.0]}
      """
    And http payload is posted to node "node-1" with host "model-{{test_id}}.example.com" path "/features"
      """
      {"tenant":"alpha","vector":[3.0,30.0]}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "score":[33.0]
      key={"tenant":"beta"} | "score":[3300.0]
      key={"tenant":"alpha"} | "score":[66.0]
      """
    And the active session observes no server error containing "running" within "100ms"

    Examples:
      | cluster_size | input_type    | output_type   | tensor_type       |
      | 1            | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |
      | 3            | ARRAY<F32, 2> | ARRAY<F32, 1> | DENSE TENSOR<F32> |

  @accepted_wasm_checkpoint
  Scenario Outline: A WASM guest keeps interleaved branch checkpoints through full data room
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And node-1 starts with vm-inline-row-limit = 0
    And node-2 starts with vm-inline-row-limit = 0
    And node-3 starts with vm-inline-row-limit = 0
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has state-counting WASM processor fixture resource directory "wasm_processor"
    And a branched state-counting WASM reset graph is running
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":1}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":1}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":2}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":2}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"even"
      key={"tenant":"beta"} | "tenant":"beta" | "note":"even"
      """
    Given processor "wasm" "counting_guest" pauses around its accepted input's first poll
    When http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":3}
      """
    Then processor "wasm" "counting_guest" has accepted its paused input
    When accepted-work CPU class "data" is full on every node
    And processor "wasm" "counting_guest" first polls its paused input
    And accepted-work CPU classes are released on every node
    And processor "wasm" "counting_guest" resumes its paused input
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":3}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"alpha","sequence":4}
      """
    And http payload is posted to host "wasm-reset-{{test_id}}.example.com" path "/events"
      """
      {"tenant":"beta","sequence":4}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      key={"tenant":"alpha"} | "tenant":"alpha" | "note":"even"
      key={"tenant":"beta"} | "tenant":"beta" | "note":"even"
      """
    And the active session observes no server error containing "counting_guest" within "100ms"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
