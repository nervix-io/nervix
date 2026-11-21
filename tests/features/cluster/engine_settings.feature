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
        MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
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
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
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

  Scenario Outline: The CLI refuses invalid local connection capabilities before it connects
    Given a 1 node nervix cluster is started
    When the CLI executes "LIST DOMAINS;" on node "node-1" with connection capabilities "<capabilities>"
    Then the CLI fails with "<refusal>"

    Examples:
      | capabilities                                            | refusal                                                     |
      | --restoration-backoff 31s --restoration-max-backoff 30s | restoration-backoff must not exceed restoration-max-backoff |
      | --notice-bytes 0                                        | invalid value '0' for '--notice-bytes                       |
