Feature: Emitters name their connector and entity in the Plexus statement head

  @kinded_emitter_head
  Scenario Outline: SHOW CREATE EMITTER preserves the declared entity and external name
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE DETACHED KAFKA TOPIC EMITTER outgoing FROM events
        TO kafka_main TOPIC 'Orders' MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER outgoing;
      """
    Then the last command output contains
      """
      CREATE DETACHED KAFKA TOPIC EMITTER outgoing
        FROM events
        TO kafka_main TOPIC 'Orders'
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @kinded_emitter_head
  Scenario Outline: ALTER checks the emitter entity and replaces its complete destination body
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE CODEC replacement_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE KAFKA TOPIC EMITTER outgoing FROM events TO broker TOPIC 'Orders'
        MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And these Plexus commands fail with "ALTER NATS SUBJECT EMITTER targets an emitter declared as KAFKA TOPIC"
      """
      ALTER NATS SUBJECT EMITTER outgoing SET DETACHED;
      """
    And these Plexus commands are executed on the leader node
      """
      ALTER KAFKA TOPIC EMITTER outgoing SET TO broker TOPIC 'Orders.Audit'
        MODE ACK PARALLEL MAX 4 ACK TIMEOUT 2s RETRY POLICY BACKOFF 10ms MAX 1s
        ENCODE USING replacement_codec;
      SHOW CREATE EMITTER outgoing;
      """
    Then the last command output contains
      """
      TO broker TOPIC 'Orders.Audit'
          MODE ACK PARALLEL MAX 4 ACK TIMEOUT 2s RETRY POLICY BACKOFF 10ms MAX 1s
          ENCODE USING replacement_codec
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @kinded_emitter_head
  Scenario Outline: DESCRIBE exposes the declared connector and entity with the exact external name
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE KAFKA TOPIC EMITTER outgoing FROM events TO broker TOPIC 'Orders'
        MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      DESCRIBE EMITTER outgoing;
      """
    Then the last command output contains
      """
      to: KAFKA TOPIC broker TOPIC 'Orders'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @kinded_emitter_head
  Scenario Outline: OTEL entity heads retain their signal and payload through SHOW CREATE
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA telemetry ( event_ts DATETIME, body STRING, trace_id STRING, span_id STRING, value F64 );
      CREATE RELAY events SCHEMA telemetry UNBRANCHED;
      CREATE OTEL CLIENT collector CONFIG { 'endpoint' = 'http://127.0.0.1:4317', 'protocol' = 'grpc' };
      CREATE OTEL <entity> EMITTER outgoing FROM events TO collector <options>
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s VALUES { <values> }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER outgoing;
      """
    Then the last command output contains
      """
      CREATE ATTACHED OTEL <entity> EMITTER outgoing
      """

    Examples:
      | cluster_size | entity  | options                        | values                                                                                                                                  |
      | 1            | LOGS    |                                | 'time' = input.event_ts, 'body' = input.body                                                                                            |
      | 3            | LOGS    |                                | 'time' = input.event_ts, 'body' = input.body                                                                                            |
      | 1            | TRACES  |                                | 'trace_id' = input.trace_id, 'span_id' = input.span_id, 'name' = input.body, 'start_time' = input.event_ts, 'end_time' = input.event_ts |
      | 3            | TRACES  |                                | 'trace_id' = input.trace_id, 'span_id' = input.span_id, 'name' = input.body, 'start_time' = input.event_ts, 'end_time' = input.event_ts |
      | 1            | METRICS | METRIC 'Orders' UNIT '1' GAUGE | 'time' = input.event_ts, 'value' = input.value                                                                                          |
      | 3            | METRICS | METRIC 'Orders' UNIT '1' GAUGE | 'time' = input.event_ts, 'value' = input.value                                                                                          |

  @kinded_emitter_head
  Scenario Outline: Iceberg destinations resolve each declared storage client kind
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE <storage> CLIENT storage CONFIG { <config> };
      CREATE ICEBERG_REST CLIENT catalog CONFIG { 'uri' = 'http://127.0.0.1:8181', 'warehouse' = 'warehouse' };
      CREATE ICEBERG TABLE EMITTER outgoing FROM events TO storage TABLE 'Orders' NAMESPACE '{{domain}}'
        LOCATION '<location>' CATALOG catalog COMMIT EACH 1m MAX SIZE 64MiB
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s VALUES { 'seq' = input.seq }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER outgoing;
      """
    Then the last command output contains
      """
      TO storage TABLE 'Orders' NAMESPACE '{{domain}}' LOCATION '<location>' CATALOG catalog COMMIT EACH 1m MAX SIZE 64MiB
      """

    Examples:
      | cluster_size | storage    | config                                                                         | location                                      |
      | 1            | S3         | 'region' = 'us-east-1', 'access_key_id' = 'test', 'secret_access_key' = 'test' | s3://warehouse/Orders                         |
      | 3            | S3         | 'region' = 'us-east-1', 'access_key_id' = 'test', 'secret_access_key' = 'test' | s3://warehouse/Orders                         |
      | 1            | GCS        | 'no_auth' = true                                                               | gs://warehouse/Orders                         |
      | 3            | GCS        | 'no_auth' = true                                                               | gs://warehouse/Orders                         |
      | 1            | AZURE_BLOB | 'account_name' = 'devstoreaccount1', 'account_key' = 'test'                    | wasb://warehouse@account.blob.core.net/Orders |
      | 3            | AZURE_BLOB | 'account_name' = 'devstoreaccount1', 'account_key' = 'test'                    | wasb://warehouse@account.blob.core.net/Orders |

  @kinded_emitter_head
  Scenario Outline: Iceberg refuses a client outside its declared object stores
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE ICEBERG_REST CLIENT catalog CONFIG { 'uri' = 'http://127.0.0.1:8181', 'warehouse' = 'warehouse' };
      """
    And these Plexus commands fail with "ICEBERG TABLE emitter requires a S3 or GCS or AZURE_BLOB client, found KAFKA"
      """
      CREATE ICEBERG TABLE EMITTER outgoing FROM events TO broker TABLE 'Orders' NAMESPACE '{{domain}}'
        LOCATION 's3://warehouse/Orders' CATALOG catalog COMMIT EACH 1m MAX SIZE 64MiB
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s VALUES { 'seq' = input.seq }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @kinded_emitter_head
  Scenario Outline: Database entity locators preserve external case in SHOW CREATE
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA event ( seq I64 );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE <connector> CLIENT database POOL SIZE MIN 1 MAX 2 CONFIG { <config> };
      CREATE <connector> <entity> EMITTER outgoing FROM events TO database <locator> '<name>'
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s VALUES { 'seq' = input.seq }
        BATCH MAX MESSAGES 10 MAX SIZE 1MiB FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER outgoing;
      """
    Then the last command output contains
      """
      TO database <locator> '<name>'
      """

    Examples:
      | cluster_size | connector | entity     | locator    | name          | config                                                                      |
      | 1            | POSTGRES  | TABLE      | TABLE      | public.Orders | 'addr' = 'postgresql://nervix:nervix@127.0.0.1:5432/nervix?sslmode=disable' |
      | 3            | POSTGRES  | TABLE      | TABLE      | public.Orders | 'addr' = 'postgresql://nervix:nervix@127.0.0.1:5432/nervix?sslmode=disable' |
      | 1            | MONGODB   | COLLECTION | COLLECTION | Orders        | 'addr' = 'mongodb://127.0.0.1:27017', 'database' = 'nervix'                 |
      | 3            | MONGODB   | COLLECTION | COLLECTION | Orders        | 'addr' = 'mongodb://127.0.0.1:27017', 'database' = 'nervix'                 |
