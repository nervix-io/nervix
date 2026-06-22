Feature: Reingestor repartitioning
  Scenario Outline: Reingestor reparameterizes an internal relay
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        tenant STRING,
        user_id I64
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        tenant string,
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification;
      CREATE RELAY tenant_notifications SCHEMA notification;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;

      CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 ); CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY tenant_user_id_branch VALUES { tenant = notifications.tenant, user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE REINGESTOR tenant_partition
        FROM notifications
        TO tenant_notifications
        PARAMETERIZED BY tenant_branch VALUES { tenant = tenant_notifications.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      SUBSCRIBE SESSION TO tenant_notifications;
      START;
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"tenant":"acme","user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "tenant":"acme","user_id":42
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  @reingestor_fan_in
  Scenario Outline: Branch collapse feeds subscriptions, reingestors, and emitters
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        tenant STRING,
        user_id I64
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        tenant string,
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 );
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE RELAY notifications SCHEMA notification PARAMETERIZED BY tenant_user_id_branch;
      CREATE RELAY tenant_notifications SCHEMA notification PARAMETERIZED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}-fan-in.example.com;
      CREATE ENDPOINT http_notifications_endpoint ON edge PATH '/ingest' TYPE HTTP;

      CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY tenant_user_id_branch VALUES {
          tenant = notifications.tenant,
          user_id = notifications.user_id
        } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE REINGESTOR tenant_partition
        FROM notifications
        TO tenant_notifications
        PARAMETERIZED BY tenant_branch VALUES { tenant = tenant_notifications.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      CREATE CLIENT zeromq_main
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };

      CREATE EMITTER source_notifications_out
        FROM notifications
        ENCODE USING notification_codec
        TO ZEROMQ zeromq_main ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;

      SUBSCRIBE SESSION TO notifications WHERE notifications.user_id = 1;
      SUBSCRIBE SESSION TO tenant_notifications WHERE tenant_notifications.user_id = 2;
      START;
      """
    And http payload is posted to host "http-{{test_id}}-fan-in.example.com" path "/ingest"
      """
      {"tenant":"acme","user_id":1}
      """
    And http payload is posted to host "http-{{test_id}}-fan-in.example.com" path "/ingest"
      """
      {"tenant":"beta","user_id":2}
      """
    And http payload is posted to host "http-{{test_id}}-fan-in.example.com" path "/ingest"
      """
      {"tenant":"acme","user_id":2}
      """
    Then the observed broker receives a payload
      """
      "user_id":
      """
    Then within "5s" the relay subscription receives payloads
      """
      "tenant":"acme","user_id":1
      "tenant":"beta","user_id":2
      "tenant":"acme","user_id":2
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @unparameterized_direct_fanout
  Scenario Outline: Unparameterized relay fan-out feeds subscriptions, reingestors, and emitters directly
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA notification (
        tenant STRING,
        user_id I64
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        tenant string,
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNPARAMETERIZED;
      CREATE RELAY copied_notifications SCHEMA notification UNPARAMETERIZED;

      CREATE VHOST edge http-{{test_id}}-direct-fanout.example.com;
      CREATE ENDPOINT http_notifications_endpoint ON edge PATH '/ingest' TYPE HTTP;

      CREATE INGESTOR http_notifications
        TO notifications
        DECODE USING notification_codec
        UNPARAMETERIZED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE REINGESTOR copy_notifications
        FROM notifications
        TO copied_notifications
        UNPARAMETERIZED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      CREATE CLIENT zeromq_main
        TYPE ZEROMQ
        CONFIG {
          'addr' = '{{zeromq_emit_addr}}',
          'bind' = 'false'
        };

      CREATE EMITTER source_notifications_out
        FROM notifications
        ENCODE USING notification_codec
        TO ZEROMQ zeromq_main ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;

      SUBSCRIBE SESSION TO copied_notifications;
      START;
      """
    And http payload is posted to host "http-{{test_id}}-direct-fanout.example.com" path "/ingest"
      """
      {"tenant":"acme","user_id":7}
      """
    Then the observed broker receives a payload
      """
      "tenant":"acme","user_id":7
      """
    Then the relay subscription receives a payload
      """
      "tenant":"acme","user_id":7
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Kafka ACK SEQUENTIAL replays on default attached reingestor branch failure
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        tenant STRING,
        user_id I64
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        tenant string,
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification;
      CREATE RELAY tenant_notifications SCHEMA notification;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 ); CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY tenant_user_id_branch VALUES { tenant = notifications.tenant, user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE REINGESTOR tenant_partition
        FROM notifications
        TO tenant_notifications
        PARAMETERIZED BY tenant_branch VALUES { tenant = tenant_notifications.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      CREATE EMITTER kafka_forward
        FROM tenant_notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    And emitter "kafka_forward" enters fault mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"tenant":"acme","user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "tenant":"acme","user_id":42
      """
    And within "2s" the relay subscription receives payloads
      """
      "tenant":"acme","user_id":42
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Kafka ACK SEQUENTIAL ignores detached reingestor branch failures
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        tenant STRING,
        user_id I64
      );

      CREATE JSON WIRE SCHEMA notification_wire (
        tenant string,
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification;
      CREATE RELAY tenant_notifications SCHEMA notification;

      CREATE CLIENT kafka_main
        TYPE KAFKA
        CONFIG {
          'bootstrap.servers' = '127.0.0.1:9092'
        };

      CREATE IF NOT EXISTS SCHEMA tenant_user_id_branch ( tenant STRING, user_id I64 ); CREATE INGESTOR kafka_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY tenant_user_id_branch VALUES { tenant = notifications.tenant, user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM KAFKA kafka_main
        TOPIC notifications_{{test_id}}
        OFFSET BY CONSUMER GROUP nervix_cucumber_{{test_id}}
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING ); CREATE DETACHED REINGESTOR tenant_partition
        FROM notifications
        TO tenant_notifications
        PARAMETERIZED BY tenant_branch VALUES { tenant = tenant_notifications.tenant } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;

      CREATE EMITTER kafka_forward
        FROM tenant_notifications
        ENCODE USING notification_codec
        TO KAFKA kafka_main TOPIC notifications_out_{{test_id}} ON MESSAGE ERROR LOG ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;

      SUBSCRIBE SESSION TO notifications;
      START;
      """
    And emitter "kafka_forward" enters fault mode
    And Kafka message is published to topic "notifications_{{test_id}}"
      """
      {"tenant":"acme","user_id":43}
      """
    Then the relay subscription receives a payload
      """
      "tenant":"acme","user_id":43
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
