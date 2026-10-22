Feature: Connector names head client definitions
  Plexus declares a client's connector before CLIENT. Its configuration, pool bounds and
  references keep their connector-specific meaning.

  Scenario Outline: Every connector client renders its declared head on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE <connector> CLIENT named_client <pool> CONFIG {<config>};
      CREATE IF NOT EXISTS <connector> CLIENT named_client <pool> CONFIG {<config>};
      SHOW CREATE CLIENT named_client;
      """
    Then the last command output contains
      """
      CREATE <connector> CLIENT named_client
      """

    Examples:
      | cluster_size | connector    | pool                  | config                                                  |
      | 1            | KAFKA        |                       |                                                         |
      | 3            | KAFKA        |                       |                                                         |
      | 1            | PULSAR       |                       |                                                         |
      | 3            | PULSAR       |                       |                                                         |
      | 1            | HTTP         |                       |                                                         |
      | 3            | HTTP         |                       |                                                         |
      | 1            | SENTRY       |                       |                                                         |
      | 3            | SENTRY       |                       |                                                         |
      | 1            | OTEL         |                       |                                                         |
      | 3            | OTEL         |                       |                                                         |
      | 1            | PROMETHEUS   |                       | 'addr' = 'http://127.0.0.1:9090', 'timeout_ms' = '5000' |
      | 3            | PROMETHEUS   |                       | 'addr' = 'http://127.0.0.1:9090', 'timeout_ms' = '5000' |
      | 1            | MQTT         |                       |                                                         |
      | 3            | MQTT         |                       |                                                         |
      | 1            | NATS         |                       |                                                         |
      | 3            | NATS         |                       |                                                         |
      | 1            | RABBITMQ     |                       |                                                         |
      | 3            | RABBITMQ     |                       |                                                         |
      | 1            | REDIS        | POOL SIZE MIN 0 MAX 1 |                                                         |
      | 3            | REDIS        | POOL SIZE MIN 0 MAX 1 |                                                         |
      | 1            | ZEROMQ       |                       |                                                         |
      | 3            | ZEROMQ       |                       |                                                         |
      | 1            | SQS          |                       |                                                         |
      | 3            | SQS          |                       |                                                         |
      | 1            | WEBSOCKETS   |                       |                                                         |
      | 3            | WEBSOCKETS   |                       |                                                         |
      | 1            | SYSLOG       |                       |                                                         |
      | 3            | SYSLOG       |                       |                                                         |
      | 1            | CLICKHOUSE   |                       |                                                         |
      | 3            | CLICKHOUSE   |                       |                                                         |
      | 1            | POSTGRES     | POOL SIZE MIN 0 MAX 1 |                                                         |
      | 3            | POSTGRES     | POOL SIZE MIN 0 MAX 1 |                                                         |
      | 1            | MYSQL        | POOL SIZE MIN 0 MAX 1 |                                                         |
      | 3            | MYSQL        | POOL SIZE MIN 0 MAX 1 |                                                         |
      | 1            | MONGODB      | POOL SIZE MIN 0 MAX 1 |                                                         |
      | 3            | MONGODB      | POOL SIZE MIN 0 MAX 1 |                                                         |
      | 1            | S3           |                       |                                                         |
      | 3            | S3           |                       |                                                         |
      | 1            | GCS          |                       |                                                         |
      | 3            | GCS          |                       |                                                         |
      | 1            | AZURE_BLOB   |                       |                                                         |
      | 3            | AZURE_BLOB   |                       |                                                         |
      | 1            | ICEBERG_REST |                       |                                                         |
      | 3            | ICEBERG_REST |                       |                                                         |

  Scenario Outline: A head-declared client is referenced by both graph directions on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA event (value I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value integer);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = 'http://127.0.0.1:19080',
        'method' = 'GET',
        'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST INGESTOR polling FROM api EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER publishing FROM events TO HTTP api METHOD 'POST' PATH '/events'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE INGESTOR polling;
      """
    Then the last command output contains
      """
      FROM api
      """
    When these Plexus statements are executed on the leader node
      """
      SHOW CREATE EMITTER publishing;
      """
    Then the last command output contains
      """
      TO HTTP api
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE KAFKA CLIENT wrong_client CONFIG {};
      """
    And these Plexus statements fail with "HTTP emitter requires a HTTP client, found KAFKA client 'wrong_client'"
      """
      CREATE EMITTER wrong_kind FROM events TO HTTP wrong_client METHOD 'POST' PATH '/events'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
