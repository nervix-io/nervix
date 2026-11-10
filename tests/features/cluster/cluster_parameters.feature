@cluster_parameters
Feature: Committed cluster parameters

  Nervix commits one parameter record for the cluster. A parameter change is validated against
  the current record and graph, then applied on every node without a process restart.

  @multi_span_cluster_duration
  Scenario Outline: A multi-span duration commits and applies on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    When these Plexus statements are executed on the leader node
      """
      ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = 13m 49s;
      """
    Then node "node-1" eventually reports status containing "node-unavailability-timeout committed=13m 49s"
    And node "node-<cluster_size>" eventually reports status containing "node=node-<cluster_size> node-unavailability-timeout running=13m 49s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @cluster_parameter_preview_fence
  Scenario Outline: A parameter change invalidates an inspected transaction preview on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA committed_parameter_schema (id U32);
      """
    And client "owner" is connected to the leader node
    And client "administrator" is connected to the leader node
    When client "owner" executes these Plexus commands
      """
      BEGIN;
      CREATE SCHEMA parameter_preview (id U32);
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    When client "owner" executes these Plexus commands
      """
      DESCRIBE TRANSACTION OPERATION 1 FORMAT JSON;
      """
    Then the last inspection reports
      """
      transaction: {{transaction_id}}
      domain: {{domain}}
      state: OPEN
      accepted operations: 1
      applied operations: 0
      selected operation: 1
      report operations: 1
      """
    When client "administrator" executes these Plexus commands
      """
      ALTER CLUSTER SET UDF_CODE_BYTES = 1KiB;
      """
    And client "owner" attempts to commit its transaction
    Then client "owner" commit was refused because its expected preview is stale
    And client "owner" transaction state is "OPEN"
    And transaction "{{transaction_id}}" eventually has state "OPEN"
    When client "administrator" fails to execute these Plexus commands
      """
      SHOW CREATE SCHEMA parameter_preview;
      """
    Then the last command error contains
      """
      schema 'parameter_preview' does not exist in domain '{{domain}}'
      """
    When client "owner" executes these Plexus commands
      """
      DESCRIBE TRANSACTION OPERATION 1 FORMAT JSON;
      """
    Then the last inspection reports
      """
      transaction: {{transaction_id}}
      domain: {{domain}}
      state: OPEN
      accepted operations: 1
      applied operations: 0
      selected operation: 1
      report operations: 1
      """
    When client "owner" executes these Plexus commands
      """
      COMMIT;
      """
    Then client "owner" transaction state is "COMMITTED"
    And transaction "{{transaction_id}}" eventually has state "COMMITTED"
    When client "administrator" executes these Plexus commands
      """
      SHOW CREATE SCHEMA parameter_preview;
      """
    Then the last command output contains
      """
      CREATE SCHEMA parameter_preview (
        id U32
      );
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

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
    When these Plexus commands are executed on node "node-1"
      """
      DESCRIBE CLUSTER;
      """
    And node "node-3" is stopped
    # A refused finalization leaves the committed change recoverable under its original
    # reference. The returning node must install that same revision before recovery completes.
    Given the leader refuses the finalization of execution reference "parameter-change" as a leader whose lease lapsed
    When the active session begins this Plexus command request with execution reference "parameter-change" in the background
      """
      ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = 15s, STATE_SNAPSHOT_INTERVAL = 250ms;
      """
    Then the background command request reports an unknown outcome because the leader could not confirm its lease
    And the leader refused the finalization of execution reference "parameter-change" as a leader whose lease lapsed
    And node "node-1" eventually reports status containing "node-unavailability-timeout committed=15s revision=2"
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
    Then node "node-3" eventually reports status containing "node=node-3 node-unavailability-timeout running=15s revision=2"
    And node "node-3" eventually reports status containing "node=node-3 state-snapshot-interval running=250ms revision=2"
    And node "node-3" eventually reports status containing "node.initialization=ready"
    And node "node-3" observability path "/readyz" eventually responds with 200 and "ready"

    When this Plexus command request with execution reference "parameter-change" is executed on the leader node
      """
      ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = 15s, STATE_SNAPSHOT_INTERVAL = 250ms;
      """
    Then the last command request succeeded
    And node "node-1" eventually reports status containing "node-unavailability-timeout committed=15s revision=2"

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

  @cluster_parameter_completion
  Scenario Outline: Completing a parameter change offers only the parameters it has not assigned on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the active session collects completion pages for "ALTER CLUSTER SET domain_drain_timeout = 13m 49s, Replica_Count = 2, UDF_CODE_BYTES = 1KiB, " at byte 92 with page size 8
    Then the collected completion candidates are
      """
      CLIENT_BATCH_ROWS
      CLIENT_CONSUMER_SESSION_BYTES
      CLIENT_PRODUCER_SESSION_BYTES
      COMMAND_EXECUTION_CAPACITY
      COMMAND_RETRY_VALIDITY
      ENTITY_GATE_DEADLINE
      ENTITY_LABELS_MAX_BYTES
      ENTITY_LABELS_MAX_COUNT
      ENTITY_LABELS_MAX_KEY_BYTES
      ENTITY_LABELS_MAX_VALUE_BYTES
      HTTP_HEADER_BYTES
      HTTP_HEADER_FIELDS
      LICENSING_REQUEST_TIMEOUT
      LICENSING_RETRY_BACKOFF_MAX
      LICENSING_STREAM_IDLE_TIMEOUT
      MAX_BATCH_MESSAGES
      NODE_UNAVAILABILITY_TIMEOUT
      PASSWORD_ITERATIONS
      RESOURCE_MAX_ARCHIVE_BYTES
      RESOURCE_MAX_EXTRACTED_BYTES
      RESOURCE_MAX_FILE_COUNT
      STATE_SNAPSHOT_INTERVAL
      TLS_CIPHER_SUITES
      TLS_KEY_EXCHANGE_GROUPS
      TRANSACTION_IDLE_TIMEOUT
      TRANSACTION_MAX_OPEN
      TRANSACTION_MAX_SOURCE_BYTES
      TRANSACTION_MAX_STATEMENTS
      TRANSACTION_TOMBSTONE_RETENTION
      """
    When the active session collects completion pages for "ALTER CLUSTER SET HTTP_HEADER_BYTES = 8KiB, http_header_" at byte 56 with page size 8
    Then the collected completion candidates are
      """
      HTTP_HEADER_FIELDS
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @cluster_parameter_completion
  Scenario Outline: A parameter change refuses a repeated parameter where the repeat is written on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And client "administration" is connected to the leader node
    When client "administration" submits this Plexus command request
      """
      ALTER CLUSTER SET DOMAIN_DRAIN_TIMEOUT = 13m 49s, REPLICA_COUNT = 2, domain_drain_timeout = 100ms;
      """
    Then the last client request failed with one diagnostic spanning bytes 70 to 90
    And the last client request diagnostic underlines "domain_drain_timeout"
    And the last client request diagnostic message contains "ALTER CLUSTER SET assigns domain-drain-timeout more than once"

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
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event_schema;
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
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event_schema;
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

  @restore_parameter_fence
  Scenario Outline: A restore refuses models above a cap committed while runtime installation waits on a cluster of <cluster_size>
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event_schema (id STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (id string);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event_schema;
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
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "parameter-capture.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    Given restore step "apply models" of domain "{{domain}}_copy" on node "{{leader}}" pauses before it applies
    When restore "domain {{domain}} AS {{domain}}_copy" of backup archive "parameter-capture.nvxb" is streamed to node "{{leader}}" under execution reference "restore-parameter-capture" in the background
    Then the restore pauses at step "apply models" of domain "{{domain}}_copy" on node "{{leader}}"
    When runtime installation on node "{{leader}}" is paused
    And this Plexus command request begins executing in the background on the leader node
      """
      ALTER CLUSTER SET HTTP_HEADER_FIELDS = 1;
      """
    Then node "{{leader}}" reaches its runtime installation pause
    And node "{{leader}}" eventually reports status containing "http-header-fields committed=1 revision=2"
    And node "{{leader}}" eventually reports status containing "node={{leader}} http-header-fields running=128 revision=1"
    When the restore step pause on node "{{leader}}" is released
    Then node "{{leader}}" records the restore's lower header cap refusal before publishing ingestor "two_headers" in domain "{{domain}}_copy"
    And the background command request succeeds
    And the background restore stream's outcome is "failed" as "executed"
    And the restore stream's outcome message contains "http-header-fields"
    And the restore stream's outcome message contains "two_headers"
    Given the active domain is "{{domain}}_copy"
    When these Plexus commands fail with "ingestor 'two_headers' does not exist"
      """
      SHOW CREATE INGESTOR two_headers;
      """
    And these Plexus commands fail with "schema 'event_schema' does not exist"
      """
      SHOW CREATE SCHEMA event_schema;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
