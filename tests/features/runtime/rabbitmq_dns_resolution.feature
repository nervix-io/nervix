Feature: RabbitMQ name resolution

  A RabbitMQ source or sink resolves the host of its client's `addr` through the node's resolver
  each time it opens a connection, dials the answers in order, and verifies an `amqps` broker's
  certificate against that host. These scenarios point every node at a DNS fixture the scenario
  controls, and provision every queue explicitly.

  Scenario Outline: RabbitMQ sources and sinks reach a broker named by the node DNS fixture over <scheme>
    Given RabbitMQ is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the RabbitMQ endpoints are published under fixture DNS name "rabbitmq.nervix.test"
    And node "node-1" has dev TLS resource directory "dev_tls"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "dns_in_{{test_id}}" exists
    And RabbitMQ queue "dns_out_{{test_id}}" is observed
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
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT rabbit_main TYPE RABBITMQ MOUNT dev_tls VERSION 1 CONFIG {
        'addr' = '{{<address>}}',
        'tls_ca_file' = '{{dev_tls}}/ca.pem'
      };
      CREATE INGESTOR rabbit_in
      FROM RABBITMQ rabbit_main QUEUE dns_in_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
      ON QUIESCE SUSPEND DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE ATTACHED EMITTER rabbit_out FROM notifications
      TO RABBITMQ rabbit_main QUEUE dns_out_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 200ms MAX 1s
      ENCODE USING notification_codec
      INHERIT ALL
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then RabbitMQ queue "dns_in_{{test_id}}" eventually has 1 consumers
    When RabbitMQ message is published to queue "dns_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """
    And the DNS fixture eventually receives a question for "rabbitmq.nervix.test"

    Examples:
      | scheme | address               | cluster_size |
      | AMQP   | rabbitmq_dns_addr     | 1            |
      | AMQP   | rabbitmq_dns_addr     | 3            |
      | AMQPS  | rabbitmq_tls_dns_addr | 1            |
      | AMQPS  | rabbitmq_tls_dns_addr | 3            |

  Scenario Outline: An AMQPS client rejects a broker certificate that does not name the host it resolved
    Given RabbitMQ is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the RabbitMQ endpoints are published under fixture DNS name "rabbitmq.unlisted.test"
    And node "node-1" has dev TLS resource directory "dev_tls"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "dns_in_{{test_id}}" exists
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
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT rabbit_main TYPE RABBITMQ MOUNT dev_tls VERSION 1 CONFIG {
        'addr' = '{{rabbitmq_tls_dns_addr}}',
        'tls_ca_file' = '{{dev_tls}}/ca.pem'
      };
      CREATE INGESTOR rabbit_in
      FROM RABBITMQ rabbit_main QUEUE dns_in_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
      ON QUIESCE SUSPEND DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "rabbit_in" on the leader node contains
      """
      the TLS handshake with RabbitMQ host 'rabbitmq.unlisted.test' failed
      """
    And the DNS fixture eventually receives a question for "rabbitmq.unlisted.test"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: RabbitMQ sources and sinks reconnect once their broker name resolves again after <answer>
    Given RabbitMQ is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And RabbitMQ is forwarded as "rabbitmq.nervix.test" from the fixture addresses "127.0.5.1,127.0.5.2"
    And the DNS fixture answers "rabbitmq.nervix.test" with addresses "127.0.5.1"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "dns_in_{{test_id}}" exists
    And RabbitMQ queue "dns_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT rabbit_main TYPE RABBITMQ CONFIG {
        'addr' = '{{rabbitmq_forwarded_addr}}'
      };
      CREATE INGESTOR rabbit_in
      FROM RABBITMQ rabbit_main QUEUE dns_in_{{test_id}} INSTANCES 2 MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
      ON QUIESCE SUSPEND DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE ATTACHED EMITTER rabbit_out FROM notifications
      TO RABBITMQ rabbit_main QUEUE dns_out_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 200ms MAX 1s
      ENCODE USING notification_codec
      INHERIT ALL
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then RabbitMQ queue "dns_in_{{test_id}}" eventually has 2 consumers
    When the DNS fixture answers "rabbitmq.nervix.test" with "<answer>"
    And the TCP forwarder at "127.0.5.1" stops
    Then RabbitMQ queue "dns_in_{{test_id}}" eventually has 0 consumers
    And within "30s" DESCRIBE INGESTOR "rabbit_in" on the leader node contains
      """
      resolving 'rabbitmq.nervix.test' failed
      """
    When RabbitMQ message is published to queue "dns_in_{{test_id}}"
      """
      {"user_id":43}
      """
    And the DNS fixture answers "rabbitmq.nervix.test" with addresses "127.0.5.2"
    Then RabbitMQ queue "dns_in_{{test_id}}" eventually has 2 consumers
    And the observed broker receives a payload
      """
      {"user_id":43}
      """
    And the TCP forwarder at "127.0.5.2" eventually accepts a connection

    Examples:
      | cluster_size | answer         |
      | 1            | name not found |
      | 3            | name not found |
      | 1            | silence        |
      | 3            | silence        |

  Scenario Outline: RabbitMQ publisher confirms hold the input offset while the broker name does not resolve
    Given Kafka is running
    And RabbitMQ is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And RabbitMQ is forwarded as "rabbitmq.nervix.test" from the fixture addresses "127.0.5.1,127.0.5.2"
    And the DNS fixture answers "rabbitmq.nervix.test" with addresses "127.0.5.1"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "rabbitmq_dns_boundary_in_{{test_id}}" exists with 1 partitions
    And RabbitMQ queue "rabbitmq_dns_boundary_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT kafka_ingress TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR kafka_notifications
        FROM KAFKA kafka_ingress TOPIC rabbitmq_dns_boundary_in_{{test_id}}
          OFFSET BY CONSUMER GROUP rabbitmq_dns_boundary_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
            RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT rabbitmq_sink TYPE RABBITMQ CONFIG {
        'addr' = '{{rabbitmq_forwarded_addr}}'
      };
      CREATE ATTACHED EMITTER rabbitmq_boundary FROM notifications
        TO RABBITMQ rabbitmq_sink QUEUE rabbitmq_dns_boundary_out_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 300ms
            RETRY POLICY BACKOFF 100ms MAX 200ms
          ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "rabbitmq_dns_boundary_group_{{test_id}}" eventually has 1 consumers
    And the TCP forwarder at "127.0.5.1" eventually accepts a connection
    When the DNS fixture answers "rabbitmq.nervix.test" with "name not found"
    And the TCP forwarder at "127.0.5.1" stops
    And Kafka message is published to topic "rabbitmq_dns_boundary_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "30s" DESCRIBE EMITTER "rabbitmq_boundary" on the leader node contains
      """
      resolving RabbitMQ host 'rabbitmq.nervix.test' failed: the name does not exist
      """
    And within "2s" Kafka consumer group "rabbitmq_dns_boundary_group_{{test_id}}" next offset for topic "rabbitmq_dns_boundary_in_{{test_id}}" partition 0 is "below 1"
    When the DNS fixture answers "rabbitmq.nervix.test" with addresses "127.0.5.2"
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """
    And within "10s" Kafka consumer group "rabbitmq_dns_boundary_group_{{test_id}}" next offset for topic "rabbitmq_dns_boundary_in_{{test_id}}" partition 0 is "at least 1"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: RabbitMQ connections dial the answer that connects, follow a changed answer and resolve again after a restart
    Given RabbitMQ is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And RabbitMQ is forwarded as "rabbitmq.nervix.test" from the fixture addresses "127.0.5.1,127.0.5.2"
    And the DNS fixture answers "rabbitmq.nervix.test" with addresses "127.0.5.3,127.0.5.1"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And RabbitMQ queue "dns_in_{{test_id}}" exists
    And RabbitMQ queue "dns_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT rabbit_main TYPE RABBITMQ CONFIG {
        'addr' = '{{rabbitmq_forwarded_addr}}'
      };
      CREATE INGESTOR rabbit_in
      FROM RABBITMQ rabbit_main QUEUE dns_in_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
      ON QUIESCE SUSPEND DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE ATTACHED EMITTER rabbit_out FROM notifications
      TO RABBITMQ rabbit_main QUEUE dns_out_{{test_id}} MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 200ms MAX 1s
      ENCODE USING notification_codec
      INHERIT ALL
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then RabbitMQ queue "dns_in_{{test_id}}" eventually has 1 consumers
    And the TCP forwarder at "127.0.5.1" eventually accepts a connection
    When the DNS fixture answers "rabbitmq.nervix.test" with addresses "127.0.5.2"
    And the TCP forwarder at "127.0.5.1" stops
    And RabbitMQ message is published to queue "dns_in_{{test_id}}"
      """
      {"user_id":44}
      """
    Then the observed broker receives a payload
      """
      {"user_id":44}
      """
    And the TCP forwarder at "127.0.5.2" eventually accepts a connection
    And RabbitMQ queue "dns_in_{{test_id}}" eventually has 1 consumers
    When the cluster is restarted
    Then RabbitMQ queue "dns_in_{{test_id}}" eventually has 1 consumers
    Given RabbitMQ queue "dns_out_{{test_id}}" is observed
    When RabbitMQ message is published to queue "dns_in_{{test_id}}"
      """
      {"user_id":45}
      """
    Then the observed broker receives a payload
      """
      {"user_id":45}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
