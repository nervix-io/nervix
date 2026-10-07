Feature: MQTT name resolution

  An MQTT source or sink resolves the host of its client's `addr` through the node's resolver every
  time its client connects to the broker, first and after every lost connection, dials the answers
  in order, and verifies an `mqtts` broker's certificate against that host. These scenarios point
  every node at a DNS fixture the scenario controls.

  Scenario Outline: MQTT sources and sinks reach a broker named by the node DNS fixture over <scheme>
    Given MQTT is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the MQTT endpoints are published under fixture DNS name "mqtt.nervix.test"
    And node "node-1" has dev TLS resource directory "dev_tls"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MQTT topic "dns_out_{{test_id}}" is observed
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
      CREATE CLIENT mqtt_in TYPE MQTT MOUNT dev_tls VERSION 1 CONFIG {
        'addr' = '{{<address>}}',
        'client_id' = 'nervix-dns-in-{{test_id}}',
        'tls_ca_file' = '{{dev_tls}}/ca.pem'
      };
      CREATE CLIENT mqtt_out TYPE MQTT MOUNT dev_tls VERSION 1 CONFIG {
        'addr' = '{{<address>}}',
        'client_id' = 'nervix-dns-out-{{test_id}}',
        'tls_ca_file' = '{{dev_tls}}/ca.pem'
      };
      CREATE INGESTOR mqtt_in
      FROM MQTT mqtt_in TOPIC dns_in_{{test_id}} SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
      ON QUIESCE SUSPEND DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE ATTACHED EMITTER mqtt_out FROM notifications
      TO MQTT mqtt_out TOPIC dns_out_{{test_id}} MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 200ms MAX 1s
      ENCODE USING notification_codec
      INHERIT ALL
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "mqtt_in" on the leader node contains
      """
      ready: true
      """
    When MQTT QoS 1 message is published to topic "dns_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """
    And the DNS fixture eventually receives a question for "mqtt.nervix.test"

    Examples:
      | scheme | address           | cluster_size |
      | MQTT   | mqtt_dns_addr     | 1            |
      | MQTT   | mqtt_dns_addr     | 3            |
      | MQTTS  | mqtt_tls_dns_addr | 1            |
      | MQTTS  | mqtt_tls_dns_addr | 3            |

  Scenario Outline: An MQTTS client rejects a broker certificate that does not name the host it resolved
    Given MQTT is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the MQTT endpoints are published under fixture DNS name "mqtt.unlisted.test"
    And node "node-1" has dev TLS resource directory "dev_tls"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
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
      CREATE CLIENT mqtt_in TYPE MQTT MOUNT dev_tls VERSION 1 CONFIG {
        'addr' = '{{mqtt_tls_dns_addr}}',
        'client_id' = 'nervix-dns-in-{{test_id}}',
        'tls_ca_file' = '{{dev_tls}}/ca.pem'
      };
      CREATE CLIENT mqtt_out TYPE MQTT MOUNT dev_tls VERSION 1 CONFIG {
        'addr' = '{{mqtt_tls_dns_addr}}',
        'client_id' = 'nervix-dns-out-{{test_id}}',
        'tls_ca_file' = '{{dev_tls}}/ca.pem'
      };
      CREATE INGESTOR mqtt_in
      FROM MQTT mqtt_in TOPIC dns_in_{{test_id}} SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
      ON QUIESCE SUSPEND DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE EMITTER mqtt_out FROM notifications
      TO MQTT mqtt_out TOPIC dns_out_{{test_id}} MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 200ms MAX 1s
      ENCODE USING notification_codec
      INHERIT ALL
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "mqtt_in" on the leader node contains
      """
      certificate not valid for name "mqtt.unlisted.test"
      """
    And within "30s" DESCRIBE EMITTER "mqtt_out" on the leader node contains
      """
      certificate not valid for name "mqtt.unlisted.test"
      """
    And the DNS fixture eventually receives a question for "mqtt.unlisted.test"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: MQTT sources and sinks reconnect once their broker name resolves again after <answer>
    Given MQTT is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And MQTT is forwarded as "mqtt.nervix.test" from the fixture addresses "127.0.5.1,127.0.5.2"
    And the DNS fixture answers "mqtt.nervix.test" with addresses "127.0.5.1"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MQTT topic "dns_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT mqtt_in TYPE MQTT CONFIG {
        'addr' = '{{mqtt_forwarded_addr}}',
        'client_id' = 'nervix-dns-in-{{test_id}}'
      };
      CREATE CLIENT mqtt_out TYPE MQTT CONFIG {
        'addr' = '{{mqtt_forwarded_addr}}',
        'client_id' = 'nervix-dns-out-{{test_id}}'
      };
      CREATE INGESTOR mqtt_in
      FROM MQTT mqtt_in TOPIC dns_in_{{test_id}} SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
      ON QUIESCE SUSPEND DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE ATTACHED EMITTER mqtt_out FROM notifications
      TO MQTT mqtt_out TOPIC dns_out_{{test_id}} MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 200ms MAX 1s
      ENCODE USING notification_codec
      INHERIT ALL
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "mqtt_in" on the leader node contains
      """
      ready: true
      """
    And the TCP forwarder at "127.0.5.1" eventually accepts a connection
    When the DNS fixture answers "mqtt.nervix.test" with "<answer>"
    And the TCP forwarder at "127.0.5.1" stops
    Then within "30s" DESCRIBE INGESTOR "mqtt_in" on the leader node contains
      """
      resolving 'mqtt.nervix.test' failed
      """
    And within "30s" DESCRIBE EMITTER "mqtt_out" on the leader node contains
      """
      resolving MQTT host 'mqtt.nervix.test' failed
      """
    When the DNS fixture answers "mqtt.nervix.test" with addresses "127.0.5.2"
    Then within "30s" DESCRIBE INGESTOR "mqtt_in" on the leader node contains
      """
      ready: true
      """
    When MQTT QoS 1 message is published to topic "dns_in_{{test_id}}"
      """
      {"user_id":43}
      """
    Then the observed broker receives a payload
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

  Scenario Outline: MQTT QoS 1 publishes hold the input offset while the broker name does not resolve
    Given Kafka is running
    And MQTT is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And MQTT is forwarded as "mqtt.nervix.test" from the fixture addresses "127.0.5.1,127.0.5.2"
    And the DNS fixture answers "mqtt.nervix.test" with addresses "127.0.5.1"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "mqtt_dns_boundary_in_{{test_id}}" exists with 1 partitions
    And MQTT topic "mqtt_dns_boundary_out_{{test_id}}" is observed
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
        FROM KAFKA kafka_ingress TOPIC mqtt_dns_boundary_in_{{test_id}}
          OFFSET BY CONSUMER GROUP mqtt_dns_boundary_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s
            RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT mqtt_sink TYPE MQTT CONFIG {
        'addr' = '{{mqtt_forwarded_addr}}',
        'client_id' = 'nervix-mqtt-dns-boundary-{{test_id}}'
      };
      CREATE ATTACHED EMITTER mqtt_boundary FROM notifications
        TO MQTT mqtt_sink TOPIC mqtt_dns_boundary_out_{{test_id}}
          MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 300ms
            RETRY POLICY BACKOFF 100ms MAX 200ms
          ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "mqtt_dns_boundary_group_{{test_id}}" eventually has 1 consumers
    And the TCP forwarder at "127.0.5.1" eventually accepts a connection
    When the DNS fixture answers "mqtt.nervix.test" with "name not found"
    And the TCP forwarder at "127.0.5.1" stops
    And Kafka message is published to topic "mqtt_dns_boundary_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "30s" DESCRIBE EMITTER "mqtt_boundary" on the leader node contains
      """
      resolving MQTT host 'mqtt.nervix.test' failed: the name does not exist
      """
    And within "2s" Kafka consumer group "mqtt_dns_boundary_group_{{test_id}}" next offset for topic "mqtt_dns_boundary_in_{{test_id}}" partition 0 is "below 1"
    When the DNS fixture answers "mqtt.nervix.test" with addresses "127.0.5.2"
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """
    And within "10s" Kafka consumer group "mqtt_dns_boundary_group_{{test_id}}" next offset for topic "mqtt_dns_boundary_in_{{test_id}}" partition 0 is "at least 1"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: MQTT connections dial the answer that connects, follow a changed answer and resolve again after a restart
    Given MQTT is running
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And MQTT is forwarded as "mqtt.nervix.test" from the fixture addresses "127.0.5.1,127.0.5.2"
    And the DNS fixture answers "mqtt.nervix.test" with addresses "127.0.5.3,127.0.5.1"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MQTT topic "dns_out_{{test_id}}" is observed
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT mqtt_in TYPE MQTT CONFIG {
        'addr' = '{{mqtt_forwarded_addr}}',
        'client_id' = 'nervix-dns-in-{{test_id}}'
      };
      CREATE CLIENT mqtt_out TYPE MQTT CONFIG {
        'addr' = '{{mqtt_forwarded_addr}}',
        'client_id' = 'nervix-dns-out-{{test_id}}'
      };
      CREATE INGESTOR mqtt_in
      FROM MQTT mqtt_in TOPIC dns_in_{{test_id}} SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
      ON QUIESCE SUSPEND DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE ATTACHED EMITTER mqtt_out FROM notifications
      TO MQTT mqtt_out TOPIC dns_out_{{test_id}} MODE QOS 0 RETRY POLICY BACKOFF 200ms MAX 1s
      ENCODE USING notification_codec
      INHERIT ALL
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "mqtt_in" on the leader node contains
      """
      ready: true
      """
    And the TCP forwarder at "127.0.5.1" eventually accepts a connection
    When the DNS fixture answers "mqtt.nervix.test" with addresses "127.0.5.2"
    And the TCP forwarder at "127.0.5.1" stops
    Then the TCP forwarder at "127.0.5.2" eventually accepts a connection
    And within "30s" DESCRIBE INGESTOR "mqtt_in" on the leader node contains
      """
      ready: true
      """
    When MQTT QoS 1 message is published to topic "dns_in_{{test_id}}"
      """
      {"user_id":44}
      """
    Then the observed broker receives a payload
      """
      {"user_id":44}
      """
    When the cluster is restarted
    Then within "60s" DESCRIBE INGESTOR "mqtt_in" on the leader node contains
      """
      ready: true
      """
    When MQTT QoS 1 message is published to topic "dns_in_{{test_id}}"
      """
      {"user_id":45}
      """
    Then within "30s" the observed broker receives payloads
      """
      {"user_id":45}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
