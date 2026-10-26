@cluster_parameters
Feature: Committed cluster parameters

  Nervix commits one parameter record for the cluster. A parameter change is validated against
  the current record and graph, then applied on every node without a process restart.

  Scenario Outline: A committed parameter change applies on every node without restarting on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    When these Plexus statements are executed on the leader node
      """
      ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = 15s;
      """
    Then node "node-1" eventually reports status containing "node-unavailability-timeout committed=15s"
    And node "node-<cluster_size>" eventually reports status containing "node=node-<cluster_size> node-unavailability-timeout running=15s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: First bootstrap seeds the parameter record for every node on a cluster of <cluster_size>
    Given node-1 starts with node-unavailability-timeout = 17s
    And a <cluster_size> node nervix cluster is started
    Then node "node-1" eventually reports status containing "node-unavailability-timeout committed=17s"
    And node "node-<cluster_size>" eventually reports status containing "node=node-<cluster_size> node-unavailability-timeout running=17s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An existing cluster refuses parameter seed options at a later node start on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And node "node-<cluster_size>" is stopped
    And node-<cluster_size> starts with node-unavailability-timeout = 17s
    When node "node-<cluster_size>" attempts to start
    Then node "node-<cluster_size>" refused to start because "node-unavailability-timeout is a first-bootstrap seed; use ALTER CLUSTER SET"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A committed parameter survives a node restart on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = 15s;
      """
    When node "node-<cluster_size>" is stopped
    And node "node-<cluster_size>" is started
    Then node "node-<cluster_size>" eventually reports status containing "node=node-<cluster_size> node-unavailability-timeout running=15s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A node down during a parameter change adopts the committed record when it returns
    Given a 3 node nervix cluster is started
    And node "node-3" is stopped
    When these Plexus statements are executed through the client on node "node-1"
      """
      ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = 15s, STATE_SNAPSHOT_INTERVAL = 250ms;
      """
    Then node "node-1" eventually reports status containing "node-unavailability-timeout committed=15s"
    When node "node-3" is started
    Then node "node-3" eventually reports status containing "node=node-3 node-unavailability-timeout running=15s"
    And node "node-3" eventually reports status containing "node=node-3 state-snapshot-interval running=250ms"

  Scenario Outline: Lowering a cap refuses the existing model that exceeds it on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UDF keep_value
        WITH ROTO_0_13
        ARGS (value I64)
        RETURNS I64
        CODE $roto$
          fn keep_value(value: I64Column) -> I64Column { value }
        $roto$;
      """
    When these Plexus statements fail with "udf-code-bytes"
      """
      ALTER CLUSTER SET UDF_CODE_BYTES = 1B;
      """
    Then the last command error contains
      """
      keep_value
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A parameter change refuses a nonpositive snapshot interval on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    When these Plexus statements fail with "state-snapshot-interval must be greater than zero"
      """
      ALTER CLUSTER SET STATE_SNAPSHOT_INTERVAL = 0s;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A live producer byte cap changes new grants and retains admitted credit on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA input_schema (id I64);
      CREATE RELAY input_relay SCHEMA input_schema UNBRANCHED;
      CREATE CLIENT PRODUCER INGESTOR input_in
        FROM SCHEMA input_schema
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
          ON QUIESCE SUSPEND
        TIMESTAMP NOW
        TO input_relay INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And client "policy" is connected to the leader node
    Then client "policy" cannot open a producer on ingestor "input_in" expecting fields "id I64" with 1 batch and "48MiB" of credit because "invalid limits"
    When these Plexus statements are executed on the leader node
      """
      ALTER CLUSTER SET CLIENT_PRODUCER_SESSION_BYTES = 64MiB;
      """
    And client "policy" opens producer "retained" on ingestor "input_in" expecting fields "id I64" with 1 batch and "48MiB" of credit
    Then producer "retained" eventually reports admission "open"
    When these Plexus statements are executed on the leader node
      """
      ALTER CLUSTER SET CLIENT_PRODUCER_SESSION_BYTES = 32MiB;
      """
    Then producer "retained" eventually reports admission "open"
    And client "policy" cannot open a producer on ingestor "input_in" expecting fields "id I64" with 1 batch and "33MiB" of credit because "invalid limits"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A lower HTTP header count refuses the model that exceeds it on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event_schema (id I64);
      CREATE RELAY outgoing SCHEMA event_schema UNBRANCHED;
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = 'http://127.0.0.1:9', 'timeout_ms' = 60000
      };
      CREATE HTTP REQUEST EMITTER two_headers FROM outgoing
        TO api METHOD 'POST' PATH '/events'
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        INVOKE write_header('x-first', 'a'), write_header('x-second', 'b')
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "http-header-fields"
      """
      ALTER CLUSTER SET HTTP_HEADER_FIELDS = 1;
      """
    Then the last command error contains
      """
      two_headers
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
