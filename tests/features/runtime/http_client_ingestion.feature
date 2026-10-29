Feature: HTTP client ingestion
  Scenario Outline: HTTP polling declares its request and client transport contract on <cluster_size> nodes
    Given the HTTP mock server is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( user_id I64 );
      CREATE SCHEMA routed_event ( user_id I64, status U16 OPTIONAL, polled_at DATETIME OPTIONAL );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( user_id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA routed_event UNBRANCHED;
      """
    When these Plexus statements fail with "missing required client config key 'timeout_ms'"
      """
      CREATE HTTP CLIENT missing_timeout CONFIG { 'endpoint' = 'http://127.0.0.1:9/events' };
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE HTTP CLIENT source CONFIG {
        'endpoint' = '{{mock_http_addr}}/http/{{test_id}}', 'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST INGESTOR polled FROM source
        METHOD 'GET' HEADERS { 'accept' = 'application/json' }
        MAX BODY SIZE 1MiB EVERY 1s MODE NO ACK ON QUIESCE SUSPEND
        DECODE USING event_codec TO events INHERIT ALL
        SET status = metadata.status, polled_at = metadata.polled_at UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION event_subscription TO events;
      START;
      SHOW CREATE INGESTOR polled;
      """
    Then the last command output contains
      """
      METHOD 'GET'
      """
    And the last command output contains
      """
      HEADERS { 'accept' = 'application/json' }
      """
    And the last command output contains
      """
      MAX BODY SIZE 1MiB
      """
    And the relay subscription receives a payload
      """
      "status":200
      """
    And the relay subscription receives a payload
      """
      "polled_at":
      """
    When these Plexus statements fail with "BATCH"
      """
      CREATE HTTP REQUEST EMITTER batched FROM events TO source
        METHOD 'POST' PATH '/events' MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        WITHOUT BODY BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: HTTP polling resolves its endpoint with the node DNS fixture
    Given the HTTP mock server is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the HTTP mock endpoint is published under fixture DNS
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE SCHEMA user_id_branch (user_id I64);
      CREATE BRANCH by_http_notifications SCHEMA user_id_branch TTL 5m;
      CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
      CREATE HTTP CLIENT http_main CONFIG {
        'endpoint' = '{{mock_http_dns_addr}}/http/{{test_id}}',
        'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST INGESTOR http_notifications
      FROM http_main METHOD 'GET' EVERY 1s
      MODE NO ACK ON QUIESCE SUSPEND DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      BRANCHED BY by_http_notifications
      SET user_id = message.user_id
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then the DNS fixture eventually receives a question for the HTTP mock
    And the relay subscription receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: HTTP client ingestor polls a remote endpoint and delivers a JSON payload
    Given the HTTP mock server is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE HTTP CLIENT http_main
        CONFIG {
          'endpoint' = '{{mock_http_addr}}/http/{{test_id}}',
          'timeout_ms' = 5000
        };
        CREATE HTTP REQUEST INGESTOR http_notifications
        FROM http_main METHOD 'GET' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then the relay subscription receives a payload
      """
      {"user_id":42}
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: HTTP client ingestor reports transient source failures and recovers
    Given the HTTP mock server is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When ingestor "http_notifications" enters fault mode
    And these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE HTTP CLIENT http_main
        CONFIG {
          'endpoint' = '{{mock_http_addr}}/http/{{test_id}}',
          'timeout_ms' = 5000
        };
        CREATE HTTP REQUEST INGESTOR http_notifications
        FROM http_main METHOD 'GET' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "5s" DESCRIBE INGESTOR "http_notifications" on the leader node contains
      """
      transient error: ingestor fault injector failed source
      """
    When ingestor "http_notifications" leaves fault mode
    Then the relay subscription receives a payload
      """
      {"user_id":42}
      """
    And within "5s" DESCRIBE INGESTOR "http_notifications" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |
