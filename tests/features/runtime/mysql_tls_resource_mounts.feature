Feature: MySQL TLS resource mounts
  Scenario Outline: MySQL emitter inserts over TLS with a mounted resource directory
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has dev TLS resource directory "dev_tls"
    And MySQL TLS table "tls_notifications_mysql_out_{{test_id}}" exists
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
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        action string
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification;

      CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = 'mqtt://127.0.0.1:1883',
          'client_id' = 'nervix-cucumber-mysql-tls-{{test_id}}'
        };

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_ingress
        TOPIC mysql_tls_notifications_in_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE CLIENT mysql_client
        TYPE MYSQL
        MOUNT dev_tls
        CONFIG {
          'addr' = 'mysql://nervix:nervix@127.0.0.1:3307/nervix?require_ssl=true',
          'tls_ca_file' = '{{dev_tls}}/ca.pem'
        };

      CREATE EMITTER to_mysql
        FROM notifications
        TO MYSQL mysql_client INSERT TO TABLE tls_notifications_mysql_out_{{test_id}}
        VALUES {
          "mysql_user_id" = notifications.user_id,
          "mysql_now" = NOW() AS STRING,
          "mysql_action" = LOWER(notifications.action)
        }
        WITH MAX BATCH 2
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
      SUBSCRIBE SESSION TO notifications;
      START;
      """
    Then within "10s" repeatedly publishing MQTT message to topic "mysql_tls_notifications_in_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42,"action":"OPEN"}
      """
    And the MySQL table eventually contains a row
      """
      {"mysql_user_id":42,"mysql_action":"open"}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
