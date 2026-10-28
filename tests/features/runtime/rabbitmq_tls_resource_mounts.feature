Feature: RabbitMQ TLS resource mounts

  The test broker's AMQP TLS listener requires a client certificate signed by the scenario CA. An
  `amqps` client reads its CA, certificate and private key from a mounted resource directory and
  presents that identity in the TLS handshake; a client that names no identity is refused by the
  broker.

  Scenario Outline: RabbitMQ ingestor presents its mounted client certificate to a broker that requires one
    Given RabbitMQ is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has dev TLS resource directory "dev_tls"
    And RabbitMQ queue "notifications_{{test_id}}" exists
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
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_rabbit_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_rabbit_notifications;
        CREATE RABBITMQ CLIENT rabbit_tls
        MOUNT dev_tls VERSION 1
        CONFIG {
          'addr' = '{{rabbitmq_tls_addr}}',
          'tls_ca_file' = '{{dev_tls}}/ca.pem',
          'tls_cert_file' = '{{dev_tls}}/node.pem',
          'tls_key_file' = '{{dev_tls}}/node-key.pem'
        };
        CREATE RABBITMQ QUEUE INGESTOR rabbit_notifications
        FROM rabbit_tls QUEUE 'notifications_{{test_id}}' INSTANCES 1 MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_rabbit_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then RabbitMQ queue "notifications_{{test_id}}" eventually has 1 consumers
    When RabbitMQ message is published to queue "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: A broker that requires a client certificate refuses a RabbitMQ ingestor that presents none
    Given RabbitMQ is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has dev TLS resource directory "dev_tls"
    And RabbitMQ queue "refused_{{test_id}}" exists
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
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE RABBITMQ CLIENT rabbit_tls MOUNT dev_tls VERSION 1 CONFIG {
        'addr' = '{{rabbitmq_tls_addr}}',
        'tls_ca_file' = '{{dev_tls}}/ca.pem'
      };
      CREATE RABBITMQ QUEUE INGESTOR rabbit_refused
      FROM rabbit_tls QUEUE 'refused_{{test_id}}' MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
      ON QUIESCE SUSPEND DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "rabbit_refused" on the leader node contains
      """
      RabbitMQ host '127.0.0.1' refused the TLS session with alert CertificateRequired
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
