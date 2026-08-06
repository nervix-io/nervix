Feature: SQS emission
  Scenario Outline: SQS emitter publishes JSON payloads from a relay
    Given MQTT is running
    And SQS is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "notifications_out_{{test_id}}" is observed
    When these NSPL commands are executed
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
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };
        CREATE INGESTOR mqtt_notifications
        FROM MQTT mqtt_ingress TOPIC notifications_in_{{test_id}} MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE CLIENT sqs_main
        TYPE SQS
        CONFIG {
          'endpoint' = '{{sqs_endpoint}}',
          'region' = 'us-east-1'
        };
        CREATE EMITTER sqs_notifications FROM notifications TO SQS sqs_main QUEUE notifications_out_{{test_id}} MODE <publishing_mode> RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    And emitter "sqs_notifications" enters stall mode
    And MQTT message is published to topic "notifications_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "5s" DESCRIBE EMITTER "sqs_notifications" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "sqs_notifications" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | replica_count | publishing_mode |
      | 1            | 0             | SINGLE          |
      | 3            | 0             | BATCH           |
      | 3            | 1             | BATCH           |

  Scenario Outline: SQS batch emission isolates a poison record
    Given MQTT is running
    And SQS is running
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "sqs_batch_poison_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        header_name STRING
      );
      CREATE SCHEMA emitter_error (
        error_code STRING,
        source_user_id I64
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        header_name string
      );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE RELAY emitter_errors SCHEMA emitter_error UNBRANCHED;
      CREATE CLIENT mqtt_ingress
      TYPE MQTT
      CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-sqs-poison-{{test_id}}'
      };
      CREATE INGESTOR mqtt_notifications
      FROM MQTT mqtt_ingress TOPIC sqs_poison_in_{{test_id}} MODE NO_ACK SEQUENTIAL
      DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE CLIENT sqs_main
      TYPE SQS
      CONFIG {
        'endpoint' = '{{sqs_endpoint}}',
        'region' = 'us-east-1'
      };
      CREATE EMITTER sqs_notifications
      FROM notifications
      TO SQS sqs_main QUEUE sqs_batch_poison_{{test_id}}
      MODE BATCH RETRY POLICY BACKOFF 100ms MAX 1s
      ENCODE USING notification_codec
      INHERIT ALL
      INVOKE write_header(input.header_name, "test")
      FLUSH EACH 2s MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR SEND TO emitter_errors
      SET error_code = error.code,
          source_user_id = input.user_id
      ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION emitter_errors_subscription TO emitter_errors;
      START;
      """
    When these MQTT messages are rapidly published to topic "sqs_poison_in_{{test_id}}"
      """
      {"user_id":1,"header_name":"tenant"}
      {"user_id":2,"header_name":"AWS.poison"}
      {"user_id":3,"header_name":"tenant"}
      """
    Then within "10s" the relay subscription receives a payload
      """
      "source_user_id":2
      """
    And the relay subscription does not receive a payload within "1s"
    And within "10s" the observed broker receives payloads
      """
      "user_id":1
      "user_id":3
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @sqs_fifo_group_ordering
  Scenario Outline: SQS FIFO preserves order independently for interleaved branch groups
    Given MQTT is running
    And SQS is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "fifo_orders_{{test_id}}.fifo" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA order_event (
        tenant STRING,
        sequence I64
      );
      CREATE WIRE JSON SCHEMA order_wire MODE STRICT (
        tenant string,
        sequence integer
      );
      CREATE CODEC order_codec
      FROM WIRE JSON SCHEMA order_wire
      TO SCHEMA order_event;
      CREATE SCHEMA tenant_branch ( tenant STRING );
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE RELAY orders SCHEMA order_event BRANCHED BY by_tenant;
      CREATE CLIENT mqtt_ingress
      TYPE MQTT
      CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-sqs-fifo-{{test_id}}'
      };
      CREATE INGESTOR mqtt_orders
      FROM MQTT mqtt_ingress TOPIC sqs_fifo_in_{{test_id}} MODE NO_ACK SEQUENTIAL
      DECODE USING order_codec
      TO orders
      INHERIT ALL
      BRANCHED BY by_tenant
      SET tenant = message.tenant
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE CLIENT sqs_main
      TYPE SQS
      CONFIG {
        'endpoint' = '{{sqs_endpoint}}',
        'region' = 'us-east-1'
      };
      CREATE EMITTER sqs_orders
      FROM orders
      TO SQS sqs_main QUEUE fifo_orders_{{test_id}}.fifo FIFO GROUP FROM BRANCH
      MODE BATCH RETRY POLICY BACKOFF 100ms MAX 1s
      ENCODE USING order_codec
      INHERIT ALL
      FLUSH EACH 1s MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    When these MQTT messages are rapidly published to topic "sqs_fifo_in_{{test_id}}"
      """
      {"tenant":"alpha","sequence":1}
      {"tenant":"beta","sequence":1}
      {"tenant":"alpha","sequence":2}
      {"tenant":"beta","sequence":2}
      {"tenant":"alpha","sequence":3}
      {"tenant":"beta","sequence":3}
      """
    Then within "10s" the observed broker receives JSON payloads preserving "tenant" group order
      """
      {"tenant":"alpha","sequence":1}
      {"tenant":"beta","sequence":1}
      {"tenant":"alpha","sequence":2}
      {"tenant":"beta","sequence":2}
      {"tenant":"alpha","sequence":3}
      {"tenant":"beta","sequence":3}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
