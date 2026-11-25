Feature: Local engine and client capabilities

  Scenario Outline: A lowered VM inline limit admits small batches through the data workers
    Given node-1 starts with vm-inline-row-limit = 0
    And node-2 starts with vm-inline-row-limit = 0
    And node-3 starts with vm-inline-row-limit = 0
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( value integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge engine-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR event_intake FROM intake
        MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events SET value = input.value + 1 UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observed TO events;
      START;
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE CLUSTER;
      """
    Then the last command output contains
      """
      vm-inline-row-limit=0
      """
    And the last command output contains
      """
      wasm-parallel-compilation=false
      """
    And the data execution count on node "node-1" is saved
    When http payload is posted to node "node-1" with host "engine-{{test_id}}.example.com" path "/events"
      """
      {"value":41}
      """
    Then within "30s" the relay subscription receives a payload
      """
      "value":42
      """
    And the data execution count on node "node-1" eventually increases

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # The UDF counts to a hundred million before it returns, which no node does within a
  # millisecond, so the watchdog expires for its batch whatever the load of the node that runs it.
  @udf_watchdog
  Scenario Outline: A UDF call that outlasts the node's watchdog fails its batch on a cluster of <cluster_size>
    Given node-1 starts with udf-watchdog = 1ms
    And node-2 starts with udf-watchdog = 1ms
    And node-3 starts with udf-watchdog = 1ms
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UDF slow_copy WITH ROTO_0_13 ARGS (value I64) RETURNS I64
        CODE $roto$
          fn slow_copy(value: I64Column) -> I64Column {
              let count = 0;
              while count < 100000000 {
                  count = count + 1;
              }
              value.add_s(count - 100000000)
          }
        $roto$;
      CREATE SCHEMA event ( id STRING, value I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( id string, value integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY accepted SCHEMA event UNBRANCHED;
      CREATE RELAY delivered SCHEMA event UNBRANCHED;
      CREATE VHOST edge watchdog-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR source FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO accepted INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION copy_slowly FROM accepted UNBRANCHED
        TO delivered SET id = input.id, value = udf::slow_copy(input.value)
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION output TO delivered;
      START;
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE CLUSTER;
      """
    Then the last command output contains
      """
      udf-watchdog=1ms
      """
    When http payload is posted to node "node-1" with host "watchdog-{{test_id}}.example.com" path "/events"
      """
      {"id":"slow","value":41}
      """
    Then within "30s" the active session observes a server error containing
      """
      injected function 'slow_copy' failed: watchdog expired after 1ms
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A node given a zero UDF watchdog refuses to start on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And node "node-<cluster_size>" is stopped
    And node-<cluster_size> starts with udf-watchdog = 0s
    When node "node-<cluster_size>" attempts to start
    Then node "node-<cluster_size>" refused to start because "udf-watchdog must be positive"
    Given node-<cluster_size> starts with udf-watchdog = 250ms
    When node "node-<cluster_size>" is started
    Then node "node-<cluster_size>" eventually observes a stable leader

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: The CLI REPL uses explicit local connection capabilities
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI REPL starts on node "{{leader}}" with explicit connection capabilities
    And the CLI REPL runs "LIST DOMAINS;"
    Then the CLI REPL eventually displays "{{domain}}"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A WASM stack above Wasmtime's default call stack runs guest calls
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And node-1 starts with wasm-stack-bytes = 4MiB
    And node-2 starts with wasm-stack-bytes = 4MiB
    And node-3 starts with wasm-stack-bytes = 4MiB
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_stack_guest;
      UPLOAD RESOURCE wasm_stack_guest VERSION '{{wasm_processor}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA wasm_input_event ( value I32 );
      CREATE SCHEMA wasm_output_event ( value I32, note STRING OPTIONAL, untouched STRING OPTIONAL );
      CREATE WIRE JSON SCHEMA wasm_input_wire MODE STRICT ( value integer );
      CREATE WIRE JSON CODEC wasm_input_codec FROM WIRE SCHEMA wasm_input_wire TO SCHEMA wasm_input_event;
      CREATE RELAY wasm_input_events SCHEMA wasm_input_event UNBRANCHED;
      CREATE RELAY wasm_output_events SCHEMA wasm_output_event UNBRANCHED;
      CREATE VHOST edge wasm-stack-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR wasm_input_ingestor FROM ingress
        MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING wasm_input_codec
        TO wasm_input_events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR stack_guest FROM wasm_input_events
        USING RESOURCE wasm_stack_guest VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        MAX TABLE ELEMENTS 4096
        UNBRANCHED
        TO wasm_output_events
        SET value = value, note = note, untouched = untouched, note = coalesce(note, "fallback")
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      CREATE SUBSCRIPTION wasm_output_events_subscription TO wasm_output_events;
      START;
      """
    # The reference guest keeps the rows at even positions, so the second row is the one it emits.
    When http payload is posted to host "wasm-stack-{{test_id}}.example.com" path "/events"
      """
      {"value":1}
      """
    And http payload is posted to host "wasm-stack-{{test_id}}.example.com" path "/events"
      """
      {"value":2}
      """
    Then within "30s" the relay subscription receives payloads
      """
      "note":"fallback","value":2
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE CLUSTER;
      """
    Then the last command output contains
      """
      wasm-stack-bytes=4194304
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An explicit WASM memory guard and growth reserve reach every node's engine
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And node-1 starts with wasm-memory-reservation-bytes = 0
    And node-1 starts with wasm-memory-guard-bytes = 64KiB
    And node-1 starts with wasm-memory-growth-reservation-bytes = 1MiB
    And node-2 starts with wasm-memory-reservation-bytes = 0
    And node-2 starts with wasm-memory-guard-bytes = 64KiB
    And node-2 starts with wasm-memory-growth-reservation-bytes = 1MiB
    And node-3 starts with wasm-memory-reservation-bytes = 0
    And node-3 starts with wasm-memory-guard-bytes = 64KiB
    And node-3 starts with wasm-memory-growth-reservation-bytes = 1MiB
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has WASM processor fixture resource directory "wasm_processor"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE RESOURCE wasm_reserve_guest;
      UPLOAD RESOURCE wasm_reserve_guest VERSION '{{wasm_processor}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA wasm_input_event ( value I32 );
      CREATE SCHEMA wasm_output_event ( value I32, note STRING OPTIONAL, untouched STRING OPTIONAL );
      CREATE WIRE JSON SCHEMA wasm_input_wire MODE STRICT ( value integer );
      CREATE WIRE JSON CODEC wasm_input_codec FROM WIRE SCHEMA wasm_input_wire TO SCHEMA wasm_input_event;
      CREATE RELAY wasm_input_events SCHEMA wasm_input_event UNBRANCHED;
      CREATE RELAY wasm_output_events SCHEMA wasm_output_event UNBRANCHED;
      CREATE VHOST edge wasm-reserve-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR wasm_input_ingestor FROM ingress
        MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING wasm_input_codec
        TO wasm_input_events INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE WASM PROCESSOR reserve_guest FROM wasm_input_events
        USING RESOURCE wasm_reserve_guest VERSION 1
        FILE 'processors/filter_even.wasm'
        MAX FUEL 1000000000
        MAX MEMORY 64MiB
        MAX TABLE ELEMENTS 4096
        UNBRANCHED
        TO wasm_output_events
        SET value = value, note = note, untouched = untouched, note = coalesce(note, "fallback")
        ON MESSAGE ERROR LOG
        ON GLOBAL ERROR LOG;
      CREATE SUBSCRIPTION wasm_output_events_subscription TO wasm_output_events;
      START;
      """
    # The reference guest keeps the rows at even positions, so the second row is the one it emits.
    When http payload is posted to host "wasm-reserve-{{test_id}}.example.com" path "/events"
      """
      {"value":1}
      """
    And http payload is posted to host "wasm-reserve-{{test_id}}.example.com" path "/events"
      """
      {"value":2}
      """
    Then within "30s" the relay subscription receives payloads
      """
      "note":"fallback","value":2
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE CLUSTER;
      """
    Then the last command output contains
      """
      wasm-memory-reservation-bytes=0 wasm-memory-guard-bytes=65536 wasm-memory-growth-reservation-bytes=1048576
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: The CLI refuses invalid local connection capabilities before it connects
    Given a 1 node nervix cluster is started
    When the CLI executes "LIST DOMAINS;" on node "node-1" with connection capabilities "<capabilities>"
    Then the CLI fails with "<refusal>"

    Examples:
      | capabilities                                            | refusal                                                                     |
      | --restoration-backoff 31s --restoration-max-backoff 30s | restoration-backoff must not exceed restoration-max-backoff                 |
      | --notice-bytes 0                                        | invalid value '0' for '--notice-bytes                                       |
      | --call-queue-capacity 0                                 | invalid value '0' for '--call-queue-capacity                                |
      | --header-list-bytes 0                                   | invalid value '0' for '--header-list-bytes                                  |
      | --reset-stream-duration 0s                              | reset_stream_duration must be between one millisecond and one day           |
      | --connection-attempt-delay 5ms                          | connection-attempt-delay 5ms must be between 10ms and 2s, RFC 8305's bounds |

  # The node's answer to a session opening carries its status, content type and session-limit
  # announcements, far more than 64 bytes of header list, and far less than the default 16 KiB.
  Scenario Outline: The CLI refuses a session whose opening headers reach its header list bound on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the CLI executes "LIST DOMAINS;" on node "node-<cluster_size>" with connection capabilities "--header-list-bytes 64"
    Then the CLI fails with "failed to start session exchange"
    When the CLI executes "LIST DOMAINS;" on node "node-<cluster_size>" with connection capabilities "--header-list-bytes 16384"
    Then the CLI output contains "{{domain}}"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
