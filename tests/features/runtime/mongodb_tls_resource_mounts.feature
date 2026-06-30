Feature: MongoDB TLS resource mounts
  Scenario Outline: MongoDB emitter inserts over TLS with a mounted resource directory
    Given runtime replication is configured with replica count <replicas> and snapshot interval "100ms"
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has dev TLS resource directory "dev_tls"
    And MongoDB TLS collection "tls_notifications_mongodb_out_{{test_id}}" exists
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
          'client_id' = 'nervix-cucumber-mongodb-tls-{{test_id}}'
        };

      CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 ); CREATE IF NOT EXISTS BRANCH by_mqtt_notifications PARAMETERIZED BY user_id_branch VALUES { user_id = notifications.user_id } TTL 5m; CREATE INGESTOR mqtt_notifications
        TO notifications
        DECODE USING notification_codec
        BRANCHED BY by_mqtt_notifications
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        FROM MQTT mqtt_ingress
        TOPIC mongodb_tls_notifications_in_{{test_id}}
        MODE NO_ACK SEQUENTIAL ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;

      CREATE CLIENT mongodb_client
        TYPE MONGODB
        MOUNT dev_tls
        CONFIG {
          'addr' = 'mongodb://root:nervix@127.0.0.1:27018/nervix?authSource=admin&tls=true',
          'database' = 'nervix',
          'tls_ca_file' = '{{dev_tls}}/ca.pem'
        };

      CREATE EMITTER to_mongodb
        FROM notifications
        TO MONGODB mongodb_client INSERT TO COLLECTION tls_notifications_mongodb_out_{{test_id}}
        VALUES {
          "mongodb_user_id" = notifications.user_id,
          "mongodb_now" = NOW() AS STRING,
          "mongodb_action" = LOWER(notifications.action)
        }
        WITH MAX BATCH 2
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
      SUBSCRIBE SESSION TO notifications;
      START;
      """
    Then within "10s" repeatedly publishing MQTT message to topic "mongodb_tls_notifications_in_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42,"action":"OPEN"}
      """
    And the MongoDB collection eventually contains a document
      """
      {"mongodb_user_id":42,"mongodb_action":"open"}
      """

    Examples:
      | nodes | replicas |
      | 1     | 0        |
      | 3     | 0        |
