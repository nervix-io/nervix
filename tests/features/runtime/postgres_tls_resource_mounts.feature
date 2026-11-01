Feature: Postgres TLS resource mounts
  Scenario Outline: Postgres emitter inserts over TLS with a mounted resource directory
    Given MQTT is running
    And Postgres is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has dev TLS resource directory "dev_tls"
    And Postgres TLS table "tls_notifications_pg_out_{{test_id}}" exists
    When these Plexus commands are executed
      """
      CREATE RESOURCE dev_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE dev_tls VERSION "{{dev_tls}}";
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
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
          'client_id' = 'nervix-cucumber-postgres-tls-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'postgres_tls_notifications_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE POSTGRES CLIENT postgres_client
        POOL SIZE MIN 2 MAX 8
        MOUNT dev_tls VERSION 1
        CONFIG {
          'addr' = '{{postgres_tls_addr}}',
          'tls_ca_file' = '{{dev_tls}}/ca.pem'
        };
        CREATE POSTGRES TABLE EMITTER to_pg FROM notifications TO postgres_client TABLE 'tls_notifications_pg_out_{{test_id}}' MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "postgres_user_id" = input.user_id, "postgres_now" = NOW() AS STRING, "postgres_action" = LOWER(input.action) } BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "10s" repeatedly publishing MQTT message to topic "postgres_tls_notifications_in_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42,"action":"OPEN"}
      """
    And the Postgres table eventually contains a row
      """
      {"postgres_user_id":42,"postgres_action":"open"}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
