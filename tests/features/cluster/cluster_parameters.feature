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
    When this Plexus command request with execution reference "parameter-change" is sent once to node "node-1"
      """
      ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = 15s, STATE_SNAPSHOT_INTERVAL = 250ms;
      """
    Then the last command request succeeded
    And node "node-1" eventually reports status containing "node-unavailability-timeout committed=15s"
    When node "node-3" is started with live parameter discovery paused
    Then node "node-3" eventually reports status containing "node.initialization=initializing"
    And node "node-3" eventually reports status containing "node=node-3 cluster parameters running=not installed"
    And node "node-3" observability path "/livez" eventually responds with 200 and "live"
    And node "node-3" observability path "/readyz" eventually responds with 503 and "waiting for live cluster parameters"
    When this Plexus command request with execution reference "parameter-change" is sent once to node "node-3"
      """
      ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = 15s, STATE_SNAPSHOT_INTERVAL = 250ms;
      """
    Then the last command request reports that its outcome is not yet authoritative
    When these Plexus commands fail through the client on node "node-3" with "waiting for a live cluster parameter publication"
      """
      SHOW TRANSACTIONS;
      """
    And live parameter discovery on node "node-3" is released
    Then node "node-3" eventually reports status containing "node=node-3 node-unavailability-timeout running=15s"
    And node "node-3" eventually reports status containing "node=node-3 state-snapshot-interval running=250ms"
    And node "node-3" eventually reports status containing "node.initialization=ready"
    And node "node-3" observability path "/readyz" eventually responds with 200 and "ready"

    When this Plexus command request with execution reference "parameter-change" is executed on the leader node
      """
      ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = 15s, STATE_SNAPSHOT_INTERVAL = 250ms;
      """
    Then the last command request succeeded

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

  Scenario Outline: A lower HTTP header <parameter> cap refuses known names with dynamic values on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event_schema (id STRING);
      CREATE RELAY outgoing SCHEMA event_schema UNBRANCHED;
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = 'http://127.0.0.1:9', 'timeout_ms' = 60000
      };
      CREATE HTTP REQUEST EMITTER two_headers FROM outgoing
        TO api METHOD 'POST' PATH '/events'
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
          WITHOUT BODY
        INVOKE write_header('x-first', input.id), write_header('x-second', input.id)
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "<refusal>"
      """
      ALTER CLUSTER SET <parameter> = <limit>;
      """
    Then the last command error contains
      """
      two_headers
      """
    And node "node-1" eventually reports status containing "<refusal> committed=<initial>"

    Examples:
      | cluster_size | parameter          | limit | refusal            | initial |
      | 1            | HTTP_HEADER_FIELDS | 1     | http-header-fields | 128     |
      | 3            | HTTP_HEADER_FIELDS | 1     | http-header-fields | 128     |
      | 1            | HTTP_HEADER_BYTES  | 10B   | http-header-bytes  | 32768B  |
      | 3            | HTTP_HEADER_BYTES  | 10B   | http-header-bytes  | 32768B  |

  Scenario Outline: A lower HTTP request header <parameter> cap protects stored and new ingestors on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event_schema (id STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (id string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event_schema;
      CREATE RELAY incoming SCHEMA event_schema UNBRANCHED;
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = 'http://127.0.0.1:9', 'timeout_ms' = 60000
      };
      CREATE HTTP REQUEST INGESTOR two_headers FROM api
        METHOD 'GET' HEADERS { 'x-first' = '1', 'x-second' = '2' }
        EVERY 1s MODE NO ACK ON QUIESCE SUSPEND
        DECODE USING event_codec TO incoming INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "<refusal>"
      """
      ALTER CLUSTER SET <parameter> = <limit>;
      """
    Then the last command error contains
      """
      two_headers
      """
    And node "node-1" eventually reports status containing "<refusal> committed=<initial>"
    When these Plexus statements are executed on the leader node
      """
      DROP INGESTOR two_headers;
      ALTER CLUSTER SET <parameter> = <limit>;
      """
    And these Plexus statements fail with "<refusal>"
      """
      CREATE HTTP REQUEST INGESTOR two_headers FROM api
        METHOD 'GET' HEADERS { 'x-first' = '1', 'x-second' = '2' }
        EVERY 1s MODE NO ACK ON QUIESCE SUSPEND
        DECODE USING event_codec TO incoming INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size | parameter          | limit | refusal            | initial |
      | 1            | HTTP_HEADER_FIELDS | 1     | http-header-fields | 128     |
      | 3            | HTTP_HEADER_FIELDS | 1     | http-header-fields | 128     |
      | 1            | HTTP_HEADER_BYTES  | 16B   | http-header-bytes  | 32768B  |
      | 3            | HTTP_HEADER_BYTES  | 16B   | http-header-bytes  | 32768B  |

  Scenario Outline: A parameter change reserves transport capacity in the HTTP header count on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    When these Plexus statements fail with "http-header-fields"
      """
      ALTER CLUSTER SET HTTP_HEADER_FIELDS = 16385;
      """
    Then the last command error contains
      """
      16384
      """
    And node "node-1" eventually reports status containing "http-header-fields committed=128"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A lower WebSocket upgrade header <parameter> cap protects stored and new ingestors on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event_schema (id STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (id string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event_schema;
      CREATE RELAY incoming SCHEMA event_schema UNBRANCHED;
      CREATE WEBSOCKETS CLIENT api CONFIG {
        'endpoint' = 'ws://127.0.0.1:9', 'connect_timeout_ms' = 60000
      };
      CREATE WEBSOCKETS CONNECTION INGESTOR two_headers FROM api
        HEADERS { 'x-first' = '1', 'x-second' = '2' }
        MODE NO ACK ON QUIESCE DROP
        DECODE USING event_codec TO incoming INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "<refusal>"
      """
      ALTER CLUSTER SET <parameter> = <limit>;
      """
    Then the last command error contains
      """
      two_headers
      """
    And node "node-1" eventually reports status containing "<refusal> committed=<initial>"
    When these Plexus statements are executed on the leader node
      """
      DROP INGESTOR two_headers;
      ALTER CLUSTER SET <parameter> = <limit>;
      """
    And these Plexus statements fail with "<refusal>"
      """
      CREATE WEBSOCKETS CONNECTION INGESTOR two_headers FROM api
        HEADERS { 'x-first' = '1', 'x-second' = '2' }
        MODE NO ACK ON QUIESCE DROP
        DECODE USING event_codec TO incoming INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size | parameter          | limit | refusal            | initial |
      | 1            | HTTP_HEADER_FIELDS | 1     | http-header-fields | 128     |
      | 3            | HTTP_HEADER_FIELDS | 1     | http-header-fields | 128     |
      | 1            | HTTP_HEADER_BYTES  | 16B   | http-header-bytes  | 32768B  |
      | 3            | HTTP_HEADER_BYTES  | 16B   | http-header-bytes  | 32768B  |
