Feature: MongoDB TLS resource mounts
  @mongodb_client_identity @deloxide_stress
  Scenario Outline: MongoDB emitter inserts over mutual TLS with a mounted client identity
    Given MQTT is running
    And MongoDB mutual TLS is running
    Given runtime replication is configured with replica count <replicas> and snapshot interval "100ms"
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has dev TLS resource directory "dev_tls"
    And MongoDB mutual TLS collection "tls_notifications_mongodb_out_{{test_id}}" exists
    And MongoDB mutual TLS refuses a client without an identity
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
          'client_id' = 'nervix-cucumber-mongodb-tls-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'mongodb_tls_notifications_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE MONGODB CLIENT mongodb_client
        POOL SIZE MIN 2 MAX 8
        MOUNT dev_tls VERSION 1
        CONFIG {
          'addr' = '{{mongodb_mtls_addr}}',
          'database' = 'nervix',
          'tls_ca_file' = '{{dev_tls}}/ca.pem',
          'tls_cert_file' = '{{dev_tls}}/node.pem',
          'tls_key_file' = '{{dev_tls}}/node-key.pem'
        };
        CREATE MONGODB COLLECTION EMITTER to_mongodb FROM notifications TO mongodb_client COLLECTION 'tls_notifications_mongodb_out_{{test_id}}' MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "mongodb_user_id" = input.user_id, "mongodb_now" = NOW() AS STRING, "mongodb_action" = LOWER(input.action) } BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW CREATE CLIENT mongodb_client;
      """
    Then the last command output contains
      """
      'tls_cert_file'
      """
    And the last command output contains
      """
      'tls_key_file'
      """
    And within "10s" repeatedly publishing MQTT message to topic "mongodb_tls_notifications_in_{{test_id}}" yields a relay subscription payload
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
