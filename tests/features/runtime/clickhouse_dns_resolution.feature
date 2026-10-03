Feature: ClickHouse name resolution

  A ClickHouse emitter's client resolves the host of its `addr` through the node's resolver for each
  new connection, dials the answers in order, keeps the configured host as the request authority,
  and over HTTPS verifies the server certificate against that host. These scenarios point every node
  at a DNS fixture the scenario controls, and provision every table and topic explicitly.

  Scenario Outline: ClickHouse emitters insert through a host named by the node DNS fixture over <scheme>
    Given Kafka is running
    And ClickHouse is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the ClickHouse endpoints are published under fixture DNS name "clickhouse.nervix.test"
    And node "node-1" has dev TLS resource directory "dev_tls"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "clickhouse_dns_in_{{test_id}}" exists with 1 partitions
    And ClickHouse table "clickhouse_dns_{{test_id}}" exists
    When these NSPL commands are executed
      """
      CREATE RESOURCE dev_tls;
      """
    And these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE dev_tls VERSION "{{dev_tls}}";
      """
    And these NSPL commands are executed
      """
      CREATE SCHEMA notification (user_id I64, action STRING);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer, action string);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT kafka_ingress TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR kafka_notifications
        FROM KAFKA kafka_ingress TOPIC clickhouse_dns_in_{{test_id}}
          OFFSET BY CONSUMER GROUP clickhouse_dns_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT clickhouse_client TYPE CLICKHOUSE MOUNT dev_tls VERSION 1 CONFIG {
        <client_config>,
        'user' = 'default',
        'password' = 'nervix'
      };
      CREATE EMITTER to_clickhouse FROM notifications
        TO CLICKHOUSE clickhouse_client INSERT TO TABLE clickhouse_dns_{{test_id}}
        VALUES {
          "clickhouse_user_id" = input.user_id,
          "clickhouse_now" = NOW() AS STRING,
          "clickhouse_action" = LOWER(input.action)
        }
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        BATCH MAX MESSAGES 10 MAX SIZE 1MiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And Kafka message is published to topic "clickhouse_dns_in_{{test_id}}"
      """
      {"user_id":42,"action":"OPEN"}
      """
    Then the ClickHouse table eventually contains a row
      """
      {"clickhouse_user_id":42,"clickhouse_action":"open"}
      """
    And the DNS fixture eventually receives a question for "clickhouse.nervix.test"

    Examples:
      | scheme | cluster_size | client_config                                                                |
      | HTTP   | 1            | 'addr' = '{{clickhouse_dns_addr}}'                                           |
      | HTTP   | 3            | 'addr' = '{{clickhouse_dns_addr}}'                                           |
      | HTTPS  | 1            | 'addr' = '{{clickhouse_tls_dns_addr}}', 'tls_ca_file' = '{{dev_tls}}/ca.pem' |
      | HTTPS  | 3            | 'addr' = '{{clickhouse_tls_dns_addr}}', 'tls_ca_file' = '{{dev_tls}}/ca.pem' |

  Scenario Outline: An HTTPS ClickHouse client rejects a certificate that does not name the host it resolved
    Given Kafka is running
    And ClickHouse is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the ClickHouse endpoints are published under fixture DNS name "clickhouse.unlisted.test"
    And node "node-1" has dev TLS resource directory "dev_tls"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "clickhouse_dns_in_{{test_id}}" exists with 1 partitions
    And ClickHouse table "clickhouse_dns_{{test_id}}" exists
    When these NSPL commands are executed
      """
      CREATE RESOURCE dev_tls;
      """
    And these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE dev_tls VERSION "{{dev_tls}}";
      """
    And these NSPL commands are executed
      """
      CREATE SCHEMA notification (user_id I64, action STRING);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer, action string);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT kafka_ingress TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR kafka_notifications
        FROM KAFKA kafka_ingress TOPIC clickhouse_dns_in_{{test_id}}
          OFFSET BY CONSUMER GROUP clickhouse_dns_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT clickhouse_client TYPE CLICKHOUSE MOUNT dev_tls VERSION 1 CONFIG {
        'addr' = '{{clickhouse_tls_dns_addr}}',
        'tls_ca_file' = '{{dev_tls}}/ca.pem',
        'user' = 'default',
        'password' = 'nervix'
      };
      CREATE EMITTER to_clickhouse FROM notifications
        TO CLICKHOUSE clickhouse_client INSERT TO TABLE clickhouse_dns_{{test_id}}
        VALUES {
          "clickhouse_user_id" = input.user_id,
          "clickhouse_now" = NOW() AS STRING,
          "clickhouse_action" = LOWER(input.action)
        }
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        BATCH MAX MESSAGES 10 MAX SIZE 1MiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And Kafka message is published to topic "clickhouse_dns_in_{{test_id}}"
      """
      {"user_id":42,"action":"OPEN"}
      """
    Then within "30s" DESCRIBE EMITTER "to_clickhouse" on the leader node contains
      """
      certificate not valid for name "clickhouse.unlisted.test"
      """
    And the DNS fixture eventually receives a question for "clickhouse.unlisted.test"
    And within "2s" Kafka consumer group "clickhouse_dns_group_{{test_id}}" next offset for topic "clickhouse_dns_in_{{test_id}}" partition 0 is "below 1"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: ClickHouse inserts wait out <answer> for their host name, through a restart, and follow the next answer
    Given Kafka is running
    And ClickHouse is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And ClickHouse is forwarded as "clickhouse.nervix.test" from the fixture addresses "127.0.5.1,127.0.5.2"
    And the DNS fixture answers "clickhouse.nervix.test" with addresses "127.0.5.3,127.0.5.1"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "clickhouse_dns_in_{{test_id}}" exists with 1 partitions
    And ClickHouse table "clickhouse_dns_{{test_id}}" exists
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (user_id I64, action STRING);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer, action string);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT kafka_ingress TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR kafka_notifications
        FROM KAFKA kafka_ingress TOPIC clickhouse_dns_in_{{test_id}}
          OFFSET BY CONSUMER GROUP clickhouse_dns_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT clickhouse_client TYPE CLICKHOUSE CONFIG {
        'addr' = '{{clickhouse_forwarded_addr}}',
        'user' = 'default',
        'password' = 'nervix'
      };
      CREATE EMITTER to_clickhouse FROM notifications
        TO CLICKHOUSE clickhouse_client INSERT TO TABLE clickhouse_dns_{{test_id}}
        VALUES {
          "clickhouse_user_id" = input.user_id,
          "clickhouse_now" = NOW() AS STRING,
          "clickhouse_action" = LOWER(input.action)
        }
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        BATCH MAX MESSAGES 10 MAX SIZE 1MiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And Kafka message is published to topic "clickhouse_dns_in_{{test_id}}"
      """
      {"user_id":41,"action":"OPEN"}
      """
    Then the ClickHouse table eventually contains a row
      """
      {"clickhouse_user_id":41,"clickhouse_action":"open"}
      """
    And the TCP forwarder at "127.0.5.1" eventually accepts a connection
    And within "10s" Kafka consumer group "clickhouse_dns_group_{{test_id}}" next offset for topic "clickhouse_dns_in_{{test_id}}" partition 0 is "at least 1"
    When the DNS fixture answers "clickhouse.nervix.test" with "<answer>"
    And the TCP forwarder at "127.0.5.1" stops
    And Kafka message is published to topic "clickhouse_dns_in_{{test_id}}"
      """
      {"user_id":42,"action":"OPEN"}
      """
    Then within "30s" DESCRIBE EMITTER "to_clickhouse" on the leader node contains
      """
      ClickHouse insert request failed: resolving 'clickhouse.nervix.test' failed
      """
    And within "2s" Kafka consumer group "clickhouse_dns_group_{{test_id}}" next offset for topic "clickhouse_dns_in_{{test_id}}" partition 0 is "below 2"
    When the cluster is restarted
    # Restart can begin an ownership handoff while the emitting task retains its failed batch.
    # Observe the retry at the DNS boundary before allowing that batch to drain.
    Then the DNS fixture eventually receives another question for "clickhouse.nervix.test"
    And within "2s" Kafka consumer group "clickhouse_dns_group_{{test_id}}" next offset for topic "clickhouse_dns_in_{{test_id}}" partition 0 is "below 2"
    When the DNS fixture answers "clickhouse.nervix.test" with addresses "127.0.5.2"
    Then the ClickHouse table eventually contains a row
      """
      {"clickhouse_user_id":42,"clickhouse_action":"open"}
      """
    And the TCP forwarder at "127.0.5.2" eventually accepts a connection
    And within "10s" Kafka consumer group "clickhouse_dns_group_{{test_id}}" next offset for topic "clickhouse_dns_in_{{test_id}}" partition 0 is "at least 2"

    Examples:
      | cluster_size | answer         |
      | 1            | name not found |
      | 3            | name not found |
      | 1            | silence        |
      | 3            | silence        |
