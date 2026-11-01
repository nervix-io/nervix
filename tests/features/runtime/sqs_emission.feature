Feature: SQS emission
  Scenario Outline: SQS QUEUE emitter publishes JSON payloads from a relay
    Given MQTT is running
    And SQS is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "notifications_out_{{test_id}}" is observed
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
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE MQTT CLIENT mqtt_ingress
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-ingress-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'notifications_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SQS CLIENT sqs_main
        CONFIG {
          'endpoint' = '{{sqs_endpoint}}',
          'region' = 'us-east-1',
          'access_key_id' = 'scenario-key',
          'secret_access_key' = 'scenario-secret'
        };
        CREATE SQS QUEUE EMITTER sqs_notifications FROM notifications TO sqs_main QUEUE 'notifications_out_{{test_id}}' SEND <publishing_mode> MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
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
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "sqs_batch_poison_{{test_id}}" is observed
    When these Plexus commands are executed
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
      CREATE MQTT CLIENT mqtt_ingress
      CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-sqs-poison-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_notifications
      FROM mqtt_ingress TOPIC 'sqs_poison_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE SQS CLIENT sqs_main
      CONFIG {
        'endpoint' = '{{sqs_endpoint}}',
        'region' = 'us-east-1',
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret'
      };
      CREATE SQS QUEUE EMITTER sqs_notifications
      FROM notifications
      TO sqs_main QUEUE 'sqs_batch_poison_{{test_id}}'
      SEND BATCH MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
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

  Scenario Outline: SQS emission rejects a body holding a noncharacter and delivers the others
    Given MQTT is running
    And SQS is running
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "sqs_noncharacter_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA note_event (
        user_id I64,
        note STRING
      );
      CREATE SCHEMA emitter_error (
        error_code STRING,
        source_user_id I64
      );
      CREATE WIRE JSON SCHEMA note_wire MODE STRICT (
        user_id integer,
        note string
      );
      CREATE CODEC note_codec
      FROM WIRE JSON SCHEMA note_wire
      TO SCHEMA note_event;
      CREATE RELAY notes SCHEMA note_event UNBRANCHED;
      CREATE RELAY emitter_errors SCHEMA emitter_error UNBRANCHED;
      CREATE MQTT CLIENT mqtt_ingress
      CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-sqs-noncharacter-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_notes
      FROM mqtt_ingress TOPIC 'sqs_noncharacter_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
      ON QUIESCE DROP DECODE USING note_codec
      TO notes
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE SQS CLIENT sqs_main
      CONFIG {
        'endpoint' = '{{sqs_endpoint}}',
        'region' = 'us-east-1',
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret'
      };
      CREATE SQS QUEUE EMITTER sqs_notes
      FROM notes
      TO sqs_main QUEUE 'sqs_noncharacter_{{test_id}}'
      SEND <publishing_mode> MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
      ENCODE USING note_codec
      INHERIT ALL
      FLUSH EACH 2s MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR SEND TO emitter_errors
      SET error_code = error.code,
          source_user_id = input.user_id
      ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION emitter_errors_subscription TO emitter_errors;
      START;
      """
    When these MQTT messages are rapidly published to topic "sqs_noncharacter_in_{{test_id}}"
      """
      {"user_id":1,"note":"replacement \ufffd and emoji \ud83d\ude00"}
      {"user_id":2,"note":"noncharacter \uffff"}
      {"user_id":3,"note":"private use \ue000"}
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
      | cluster_size | replica_count | publishing_mode |
      | 1            | 0             | SINGLE          |
      | 3            | 1             | BATCH           |

  @sqs_fifo_group_ordering @domain_execution_time
  Scenario Outline: SQS FIFO preserves order independently for interleaved branch groups
    Given MQTT is running
    And SQS is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    And SQS queue "fifo_orders_{{test_id}}.fifo" is observed
    When these Plexus commands are executed
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
      CREATE MQTT CLIENT mqtt_ingress
      CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-sqs-fifo-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_orders
      FROM mqtt_ingress TOPIC 'sqs_fifo_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
      ON QUIESCE DROP DECODE USING order_codec
      TIMESTAMP NOW
      TO orders
      INHERIT ALL
      BRANCHED BY by_tenant
      SET tenant = message.tenant
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE SQS CLIENT sqs_main
      CONFIG {
        'endpoint' = '{{sqs_endpoint}}',
        'region' = 'us-east-1',
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret'
      };
      CREATE SQS QUEUE EMITTER sqs_orders
      FROM orders
      TO sqs_main QUEUE 'fifo_orders_{{test_id}}.fifo'
      FIFO GROUP CASE
        WHEN now() < ('2001-01-01T00:00:00Z' AS DATETIME) THEN input.tenant
        ELSE ''
      END
      SEND BATCH MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
      ENCODE USING order_codec
      INHERIT ALL
      FLUSH EACH 1s MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 1.0;
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

  @sqs_fifo_group_ordering
  Scenario Outline: SQS FIFO groups follow the rows an emitter route keeps
    Given MQTT is running
    And SQS is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "fifo_kept_{{test_id}}.fifo" is observed
    When these Plexus commands are executed
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
      CREATE MQTT CLIENT mqtt_ingress
      CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-sqs-fifo-kept-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_orders
      FROM mqtt_ingress TOPIC 'sqs_fifo_kept_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
      ON QUIESCE DROP DECODE USING order_codec
      TIMESTAMP NOW
      TO orders
      INHERIT ALL
      BRANCHED BY by_tenant
      SET tenant = message.tenant
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE SQS CLIENT sqs_main
      CONFIG {
        'endpoint' = '{{sqs_endpoint}}',
        'region' = 'us-east-1',
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret'
      };
      CREATE SQS QUEUE EMITTER sqs_orders
      FROM orders
      TO sqs_main QUEUE 'fifo_kept_{{test_id}}.fifo'
      FIFO GROUP FROM BRANCH
      SEND BATCH MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
      ENCODE USING order_codec
      INHERIT ALL
      WHERE input.sequence != (2 AS I64)
      FLUSH EACH 1s MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    When these MQTT messages are rapidly published to topic "sqs_fifo_kept_in_{{test_id}}"
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
      {"tenant":"alpha","sequence":3}
      {"tenant":"beta","sequence":3}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @sqs_fifo_group_ordering
  Scenario Outline: SQS FIFO rejects only the records whose ordering group cannot be evaluated
    Given MQTT is running
    And SQS is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "fifo_failed_{{test_id}}.fifo" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA order_event (
        tenant STRING,
        sequence I64
      );
      CREATE SCHEMA rejected_order (
        tenant STRING,
        sequence I64,
        error_code STRING,
        error_message STRING,
        operation STRING
      );
      CREATE WIRE JSON SCHEMA order_wire MODE STRICT (
        tenant string,
        sequence integer
      );
      CREATE CODEC order_codec
      FROM WIRE JSON SCHEMA order_wire
      TO SCHEMA order_event;
      CREATE RELAY orders SCHEMA order_event UNBRANCHED;
      CREATE RELAY rejected_orders SCHEMA rejected_order UNBRANCHED;
      CREATE MQTT CLIENT mqtt_ingress
      CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-sqs-fifo-failed-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_orders
      FROM mqtt_ingress TOPIC 'sqs_fifo_failed_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
      ON QUIESCE DROP DECODE USING order_codec
      TIMESTAMP NOW
      TO orders
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE SQS CLIENT sqs_main
      CONFIG {
        'endpoint' = '{{sqs_endpoint}}',
        'region' = 'us-east-1',
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret'
      };
      CREATE SQS QUEUE EMITTER sqs_orders
      FROM orders
      TO sqs_main QUEUE 'fifo_failed_{{test_id}}.fifo'
      FIFO GROUP CASE
        WHEN input.sequence = (2 AS I64) THEN (input.sequence / (input.sequence - (2 AS I64))) AS STRING
        ELSE input.tenant
      END
      SEND BATCH MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
      ENCODE USING order_codec
      INHERIT ALL
      WHERE input.sequence != (3 AS I64)
      FLUSH EACH 1s MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR SEND TO rejected_orders
        SET tenant = input.tenant,
            sequence = input.sequence,
            error_code = error.code,
            error_message = error.message,
            operation = error.operation
      ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_orders_subscription TO rejected_orders;
      START;
      """
    When these MQTT messages are rapidly published to topic "sqs_fifo_failed_in_{{test_id}}"
      """
      {"tenant":"alpha","sequence":1}
      {"tenant":"beta","sequence":1}
      {"tenant":"alpha","sequence":2}
      {"tenant":"beta","sequence":2}
      {"tenant":"alpha","sequence":3}
      {"tenant":"beta","sequence":3}
      {"tenant":"alpha","sequence":4}
      {"tenant":"beta","sequence":4}
      """
    Then within "10s" the observed broker receives JSON payloads preserving "tenant" group order
      """
      {"tenant":"alpha","sequence":1}
      {"tenant":"beta","sequence":1}
      {"tenant":"alpha","sequence":4}
      {"tenant":"beta","sequence":4}
      """
    And within "10s" the relay subscription receives payloads containing all fragments
      """
      "tenant":"alpha" | "sequence":2 | "error_code":"external" | "operation":"publish" | ordering group expression failed with division_by_zero
      "tenant":"beta" | "sequence":2 | "error_code":"external" | "operation":"publish" | ordering group expression failed with division_by_zero
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: SQS QUEUE emitters send typed attributes and a message above 256 KiB
    Given MQTT is running
    And SQS is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "typed_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA reading (
        user_id I64,
        tenant STRING,
        note STRING
      );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT (
        user_id integer,
        tenant string,
        note string
      );
      CREATE CODEC reading_codec
      FROM WIRE JSON SCHEMA reading_wire
      TO SCHEMA reading;
      CREATE RELAY readings SCHEMA reading UNBRANCHED;
      CREATE MQTT CLIENT mqtt_ingress
      CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-sqs-typed-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_readings
      FROM mqtt_ingress TOPIC 'sqs_typed_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
      ON QUIESCE DROP DECODE USING reading_codec
      TO readings
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE SQS CLIENT sqs_main
      CONFIG {
        'endpoint' = '{{sqs_endpoint}}',
        'region' = 'us-east-1',
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret'
      };
      CREATE SQS QUEUE EMITTER typed_attributes
      FROM readings
      TO sqs_main QUEUE 'typed_out_{{test_id}}'
      SEND <send> MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
      ENCODE USING reading_codec
      INHERIT ALL EXCEPT note
      SET note = repeat('x', 614400)
      INVOKE write_header('tenant', input.tenant),
        write_header('user', input.user_id),
        write_header('digest', hex_decode('00ff10'))
      FLUSH EACH 100ms MAX BATCH SIZE 2MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    # The note makes the message 600 KiB, above the 256 KiB SQS once allowed and within its 1 MiB.
    And MQTT message is published to topic "sqs_typed_in_{{test_id}}"
      """
      {"user_id":42,"tenant":"acme","note":""}
      """
    Then the observed broker receives a payload
      """
      "user_id":42
      """
    And the last observed broker payload is at least 614400 bytes long
    And the last observed broker message has typed headers
      """
      tenant: String acme
      user: Number 42
      digest: Binary 00ff10
      """

    Examples:
      | cluster_size | send   |
      | 1            | SINGLE |
      | 3            | BATCH  |

  Scenario Outline: SQS FIFO emitters name each message's deduplication id
    Given MQTT is running
    And SQS is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS FIFO queue "dedup_out_{{test_id}}.fifo" without content-based deduplication is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA order_event (
        order_id STRING,
        attempt I64
      );
      CREATE WIRE JSON SCHEMA order_wire MODE STRICT (
        order_id string,
        attempt integer
      );
      CREATE CODEC order_codec
      FROM WIRE JSON SCHEMA order_wire
      TO SCHEMA order_event;
      CREATE RELAY orders SCHEMA order_event UNBRANCHED;
      CREATE MQTT CLIENT mqtt_ingress
      CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-sqs-dedup-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_orders
      FROM mqtt_ingress TOPIC 'sqs_dedup_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
      ON QUIESCE DROP DECODE USING order_codec
      TO orders
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE SQS CLIENT sqs_main
      CONFIG {
        'endpoint' = '{{sqs_endpoint}}',
        'region' = 'us-east-1',
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret'
      };
      CREATE SQS QUEUE EMITTER deduplicated
      FROM orders
      TO sqs_main QUEUE 'dedup_out_{{test_id}}.fifo'
      FIFO GROUP input.order_id
      SEND <send> MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
      ENCODE USING order_codec
      DEDUPLICATION ID input.order_id
      INHERIT ALL
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    # The queue does no content-based deduplication, so it accepts each message only because the
    # message names its deduplication id, and drops the second attempt of order o-1 as a repeat.
    And these MQTT messages are rapidly published to topic "sqs_dedup_in_{{test_id}}"
      """
      {"order_id":"o-1","attempt":1}
      {"order_id":"o-2","attempt":1}
      {"order_id":"o-1","attempt":2}
      """
    Then within "15s" the observed broker receives exactly these payloads
      """
      {"order_id":"o-1","attempt":1}
      {"order_id":"o-2","attempt":1}
      """

    Examples:
      | cluster_size | send   |
      | 1            | SINGLE |
      | 3            | BATCH  |

  Scenario Outline: SQS QUEUE emitters delay each message by their DELAY
    Given MQTT is running
    And SQS is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "delayed_out_{{test_id}}" is observed
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
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MQTT CLIENT mqtt_ingress
      CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-sqs-delay-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_notifications
      FROM mqtt_ingress TOPIC 'sqs_delay_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE SQS CLIENT sqs_main
      CONFIG {
        'endpoint' = '{{sqs_endpoint}}',
        'region' = 'us-east-1',
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret'
      };
      CREATE SQS QUEUE EMITTER delayed
      FROM notifications
      TO sqs_main QUEUE 'delayed_out_{{test_id}}'
      SEND <send> MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
      ENCODE USING notification_codec
      DELAY 5s
      INHERIT ALL
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    And MQTT message is published to topic "sqs_delay_in_{{test_id}}"
      """
      {"user_id":7}
      """
    Then the observed broker receives a payload no sooner than "5s" after it was published
      """
      {"user_id":7}
      """

    Examples:
      | cluster_size | send   |
      | 1            | SINGLE |
      | 3            | BATCH  |
