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
      | cluster_size | connector    | pool                  | config                                                                         |
      | 1            | KAFKA        |                       |                                                                                |
      | 3            | KAFKA        |                       |                                                                                |
      | 1            | PULSAR       |                       | 'addr' = 'pulsar://127.0.0.1:6650'                                             |
      | 3            | PULSAR       |                       | 'addr' = 'pulsar://127.0.0.1:6650'                                             |
      | 1            | HTTP         |                       | 'endpoint' = 'http://127.0.0.1:19080', 'timeout_ms' = 5000                     |
      | 3            | HTTP         |                       | 'endpoint' = 'http://127.0.0.1:19080', 'timeout_ms' = 5000                     |
      | 1            | SENTRY       |                       | 'dsn' = 'http://public@127.0.0.1:8000/1', 'timeout_ms' = 5000                  |
      | 3            | SENTRY       |                       | 'dsn' = 'http://public@127.0.0.1:8000/1', 'timeout_ms' = 5000                  |
      | 1            | OTEL         |                       | 'endpoint' = 'http://127.0.0.1:4317', 'protocol' = 'grpc', 'timeout_ms' = 5000 |
      | 3            | OTEL         |                       | 'endpoint' = 'http://127.0.0.1:4317', 'protocol' = 'grpc', 'timeout_ms' = 5000 |
      | 1            | PROMETHEUS   |                       | 'addr' = 'http://127.0.0.1:9090', 'timeout_ms' = '5000'                        |
      | 3            | PROMETHEUS   |                       | 'addr' = 'http://127.0.0.1:9090', 'timeout_ms' = '5000'                        |
      | 1            | MQTT         |                       | 'addr' = 'mqtt://127.0.0.1:1883'                                               |
      | 3            | MQTT         |                       | 'addr' = 'mqtt://127.0.0.1:1883'                                               |
      | 1            | NATS         |                       | 'addr' = 'nats://127.0.0.1:4222'                                               |
      | 3            | NATS         |                       | 'addr' = 'nats://127.0.0.1:4222'                                               |
      | 1            | RABBITMQ     |                       | 'addr' = 'amqp://127.0.0.1:5672/%2f'                                           |
      | 3            | RABBITMQ     |                       | 'addr' = 'amqp://127.0.0.1:5672/%2f'                                           |
      | 1            | REDIS        | POOL SIZE MIN 0 MAX 1 | 'addr' = 'redis://127.0.0.1:6379'                                              |
      | 3            | REDIS        | POOL SIZE MIN 0 MAX 1 | 'addr' = 'redis://127.0.0.1:6379'                                              |
      | 1            | ZEROMQ       |                       | 'addr' = 'tcp://127.0.0.1:5555'                                                |
      | 3            | ZEROMQ       |                       | 'addr' = 'tcp://127.0.0.1:5555'                                                |
      | 1            | SQS          |                       | 'endpoint' = 'http://127.0.0.1:4566'                                           |
      | 3            | SQS          |                       | 'endpoint' = 'http://127.0.0.1:4566'                                           |
      | 1            | WEBSOCKETS   |                       | 'endpoint' = 'ws://127.0.0.1:19080/events'                                     |
      | 3            | WEBSOCKETS   |                       | 'endpoint' = 'ws://127.0.0.1:19080/events'                                     |
      | 1            | SYSLOG       |                       | 'addr' = '127.0.0.1:5514', 'protocol' = 'udp'                                  |
      | 3            | SYSLOG       |                       | 'addr' = '127.0.0.1:5514', 'protocol' = 'udp'                                  |
      | 1            | CLICKHOUSE   |                       | 'addr' = 'http://127.0.0.1:8123', 'timeout_ms' = '5000'                        |
      | 3            | CLICKHOUSE   |                       | 'addr' = 'http://127.0.0.1:8123', 'timeout_ms' = '5000'                        |
      | 1            | POSTGRES     | POOL SIZE MIN 0 MAX 1 | 'addr' = 'postgres://127.0.0.1:5432/events?sslmode=disable'                    |
      | 3            | POSTGRES     | POOL SIZE MIN 0 MAX 1 | 'addr' = 'postgres://127.0.0.1:5432/events?sslmode=disable'                    |
      | 1            | MYSQL        | POOL SIZE MIN 0 MAX 1 | 'addr' = 'mysql://127.0.0.1:3306/events'                                       |
      | 3            | MYSQL        | POOL SIZE MIN 0 MAX 1 | 'addr' = 'mysql://127.0.0.1:3306/events'                                       |
      | 1            | MONGODB      | POOL SIZE MIN 0 MAX 1 | 'addr' = 'mongodb://127.0.0.1:27017/events'                                    |
      | 3            | MONGODB      | POOL SIZE MIN 0 MAX 1 | 'addr' = 'mongodb://127.0.0.1:27017/events'                                    |
      | 1            | S3           |                       |                                                                                |
      | 3            | S3           |                       |                                                                                |
      | 1            | GCS          |                       |                                                                                |
      | 3            | GCS          |                       |                                                                                |
      | 1            | AZURE_BLOB   |                       |                                                                                |
      | 3            | AZURE_BLOB   |                       |                                                                                |
      | 1            | ICEBERG_REST |                       | 'uri' = 'http://127.0.0.1:8181'                                                |
      | 3            | ICEBERG_REST |                       | 'uri' = 'http://127.0.0.1:8181'                                                |

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
        'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST INGESTOR polling FROM api METHOD 'GET' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE HTTP REQUEST EMITTER publishing FROM events TO api METHOD 'POST' PATH '/events'
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
      TO api METHOD 'POST' PATH '/events'
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE KAFKA CLIENT wrong_client CONFIG {};
      """
    And these Plexus statements fail with "HTTP REQUEST emitter requires a HTTP client, found KAFKA client 'wrong_client'"
      """
      CREATE HTTP REQUEST EMITTER wrong_kind FROM events TO wrong_client METHOD 'POST' PATH '/events'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
