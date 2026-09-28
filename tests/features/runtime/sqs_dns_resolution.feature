Feature: SQS name resolution

  An SQS source or sink resolves the host of its client's `endpoint` through the node's resolver for
  each new connection, dials the answers in order, signs every request for the configured host, and
  over HTTPS verifies the service certificate against that host. These scenarios point every node at
  a DNS fixture the scenario controls, and provision every queue and topic explicitly.

  Scenario Outline: SQS sources and sinks reach a service named by the node DNS fixture over <scheme>
    Given SQS is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the SQS endpoints are published under fixture DNS name "sqs.nervix.test"
    And node "node-1" has dev TLS resource directory "dev_tls"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "dns_in_{{test_id}}" exists
    And SQS queue "dns_out_{{test_id}}" is observed
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
      CREATE CLIENT sqs_main TYPE SQS MOUNT dev_tls VERSION 1 CONFIG {
        <client_config>,
        'region' = 'us-east-1'
      };
      CREATE INGESTOR sqs_in
        FROM SQS sqs_main QUEUE dns_in_{{test_id}} INSTANCES 1
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER sqs_out FROM notifications
        TO SQS sqs_main QUEUE dns_out_{{test_id}} MODE <mode> RETRY POLICY BACKOFF 200ms MAX 1s
          ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And SQS message is published to queue "dns_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """
    And the DNS fixture eventually receives a question for "sqs.nervix.test"

    Examples:
      | scheme | cluster_size | mode   | client_config                                                                 |
      | HTTP   | 1            | SINGLE | 'endpoint' = '{{sqs_dns_endpoint}}'                                           |
      | HTTP   | 3            | BATCH  | 'endpoint' = '{{sqs_dns_endpoint}}'                                           |
      | HTTPS  | 1            | BATCH  | 'endpoint' = '{{sqs_tls_dns_endpoint}}', 'tls_ca_file' = '{{dev_tls}}/ca.pem' |
      | HTTPS  | 3            | SINGLE | 'endpoint' = '{{sqs_tls_dns_endpoint}}', 'tls_ca_file' = '{{dev_tls}}/ca.pem' |

  Scenario Outline: An HTTPS SQS client rejects a certificate that does not name the host it resolved
    Given SQS is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the SQS endpoints are published under fixture DNS name "sqs.unlisted.test"
    And node "node-1" has dev TLS resource directory "dev_tls"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "dns_out_{{test_id}}" exists
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
      CREATE CLIENT sqs_main TYPE SQS MOUNT dev_tls VERSION 1 CONFIG {
        'endpoint' = '{{sqs_tls_dns_endpoint}}',
        'tls_ca_file' = '{{dev_tls}}/ca.pem',
        'region' = 'us-east-1'
      };
      CREATE EMITTER sqs_out FROM notifications
        TO SQS sqs_main QUEUE dns_out_{{test_id}} MODE SINGLE RETRY POLICY BACKOFF 200ms MAX 1s
          ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE EMITTER "sqs_out" on the leader node contains
      """
      certificate not valid for name "sqs.unlisted.test"
      """
    And the DNS fixture eventually receives a question for "sqs.unlisted.test"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: SQS sends hold the input offset while the service name does not resolve
    Given Kafka is running
    And SQS is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And SQS is forwarded as "sqs.nervix.test" from the fixture addresses "127.0.5.1,127.0.5.2"
    And the DNS fixture answers "sqs.nervix.test" with addresses "127.0.5.3,127.0.5.1"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "sqs_dns_in_{{test_id}}" exists with 1 partitions
    And SQS queue "dns_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT kafka_ingress TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR kafka_notifications
        FROM KAFKA kafka_ingress TOPIC sqs_dns_in_{{test_id}}
          OFFSET BY CONSUMER GROUP sqs_dns_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT sqs_main TYPE SQS CONFIG {
        'endpoint' = '{{sqs_forwarded_endpoint}}',
        'region' = 'us-east-1'
      };
      CREATE EMITTER sqs_out FROM notifications
        TO SQS sqs_main QUEUE dns_out_{{test_id}} MODE SINGLE RETRY POLICY BACKOFF 100ms MAX 1s
          ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And Kafka message is published to topic "sqs_dns_in_{{test_id}}"
      """
      {"user_id":41}
      """
    Then the observed broker receives a payload
      """
      {"user_id":41}
      """
    And the TCP forwarder at "127.0.5.1" eventually accepts a connection
    And within "10s" Kafka consumer group "sqs_dns_group_{{test_id}}" next offset for topic "sqs_dns_in_{{test_id}}" partition 0 is "at least 1"
    When the DNS fixture answers "sqs.nervix.test" with "name not found"
    And the TCP forwarder at "127.0.5.1" stops
    And Kafka message is published to topic "sqs_dns_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "30s" DESCRIBE EMITTER "sqs_out" on the leader node contains
      """
      resolving 'sqs.nervix.test' failed: the name does not exist
      """
    And within "2s" Kafka consumer group "sqs_dns_group_{{test_id}}" next offset for topic "sqs_dns_in_{{test_id}}" partition 0 is "below 2"
    When the DNS fixture answers "sqs.nervix.test" with addresses "127.0.5.2"
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """
    And the TCP forwarder at "127.0.5.2" eventually accepts a connection
    And within "10s" Kafka consumer group "sqs_dns_group_{{test_id}}" next offset for topic "sqs_dns_in_{{test_id}}" partition 0 is "at least 2"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: SQS sources resume once their service name resolves again after <answer>
    Given SQS is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And SQS is forwarded as "sqs.nervix.test" from the fixture addresses "127.0.5.1,127.0.5.2"
    And the DNS fixture answers "sqs.nervix.test" with addresses "127.0.5.1"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And SQS queue "dns_in_{{test_id}}" exists
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT sqs_main TYPE SQS CONFIG {
        'endpoint' = '{{sqs_forwarded_endpoint}}',
        'region' = 'us-east-1'
      };
      CREATE INGESTOR sqs_in
        FROM SQS sqs_main QUEUE dns_in_{{test_id}} INSTANCES 1
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And SQS message is published to queue "dns_in_{{test_id}}"
      """
      {"user_id":41}
      """
    Then the relay subscription receives a payload
      """
      "user_id":41
      """
    And the TCP forwarder at "127.0.5.1" eventually accepts a connection
    When the DNS fixture answers "sqs.nervix.test" with "<answer>"
    And the TCP forwarder at "127.0.5.1" stops
    Then within "30s" DESCRIBE INGESTOR "sqs_in" on the leader node contains
      """
      resolving 'sqs.nervix.test' failed
      """
    When SQS message is published to queue "dns_in_{{test_id}}"
      """
      {"user_id":42}
      """
    And the DNS fixture answers "sqs.nervix.test" with addresses "127.0.5.2"
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And the TCP forwarder at "127.0.5.2" eventually accepts a connection

    Examples:
      | cluster_size | answer         |
      | 1            | name not found |
      | 3            | name not found |
      | 1            | silence        |
      | 3            | silence        |
