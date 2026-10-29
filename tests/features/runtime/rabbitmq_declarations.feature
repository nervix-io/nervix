Feature: RabbitMQ queue declarations
  RabbitMQ queue locators preserve external spelling and follow the AMQP short-string byte limit,
  and a RabbitMQ client's declared configuration is checked before it is committed. These checks
  inspect stopped graph definitions without connecting to a broker.

  Scenario Outline: RabbitMQ queue ingestion preserves its declared policy and external locator
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value integer);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RABBITMQ CLIENT broker CONFIG {'addr' = 'amqp://127.0.0.1:1'};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE RABBITMQ QUEUE INGESTOR incoming
        FROM broker QUEUE 'Orders.Éurope-01' <instances>
        MODE <mode>
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      SHOW CREATE INGESTOR incoming;
      """
    Then the last command output contains
      """
      FROM broker QUEUE 'Orders.Éurope-01'
      """
    And the last command output contains
      """
      MODE <mode>
      """

    And the last command output contains
      """
      ON QUIESCE SUSPEND
      """

    Examples:
      | cluster_size | instances   | mode                                                                                    |
      | 1            |             | ACK SEQUENTIAL ACK TIMEOUT 7s RETRY POLICY BACKOFF 200ms MAX 3s                         |
      | 3            |             | ACK SEQUENTIAL ACK TIMEOUT 7s RETRY POLICY BACKOFF 200ms MAX 3s                         |
      | 1            | INSTANCES 2 | ACK SEQUENTIAL ACK TIMEOUT 7s RETRY POLICY BACKOFF 200ms MAX 3s                         |
      | 3            | INSTANCES 2 | ACK SEQUENTIAL ACK TIMEOUT 7s RETRY POLICY BACKOFF 200ms MAX 3s                         |
      | 1            |             | NO ACK                                                                                  |
      | 3            |             | NO ACK                                                                                  |
      | 1            |             | ACK PARALLEL MAX 2 BATCH TIMEOUT 200ms ACK TIMEOUT 7s RETRY POLICY BACKOFF 200ms MAX 3s |
      | 3            |             | ACK PARALLEL MAX 2 BATCH TIMEOUT 200ms ACK TIMEOUT 7s RETRY POLICY BACKOFF 200ms MAX 3s |

  Scenario Outline: RabbitMQ queue emission preserves its declared publishing and batching policy
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value integer);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE RABBITMQ CLIENT broker CONFIG {'addr' = 'amqp://127.0.0.1:1'};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE RABBITMQ QUEUE EMITTER outgoing FROM events
        TO broker QUEUE 'Orders.Éurope-01'
        MODE <mode> RETRY POLICY BACKOFF 200ms MAX 3s
        ENCODE USING event_codec
        INHERIT ALL
        <batch>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER outgoing;
      """
    Then the last command output contains
      """
      TO broker QUEUE 'Orders.Éurope-01'
      """
    And the last command output contains
      """
      MODE <mode> RETRY POLICY BACKOFF 200ms MAX 3s
      """
    And the last command output contains
      """
      ENCODE USING event_codec
      """
    And the last command output contains
      """
      <batch_evidence>
      """

    Examples:
      | cluster_size | mode                              | batch                                | batch_evidence                       |
      | 1            | NO ACK                            |                                      | FLUSH IMMEDIATE                      |
      | 3            | NO ACK                            |                                      | FLUSH IMMEDIATE                      |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 7s     |                                      | FLUSH IMMEDIATE                      |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 7s     |                                      | FLUSH IMMEDIATE                      |
      | 1            | ACK PARALLEL MAX 2 ACK TIMEOUT 7s | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | BATCH MAX MESSAGES 10 MAX SIZE 64KiB |
      | 3            | ACK PARALLEL MAX 2 ACK TIMEOUT 7s | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | BATCH MAX MESSAGES 10 MAX SIZE 64KiB |


  Scenario Outline: RabbitMQ queue ingestion refuses a locator outside the protocol byte contract
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "<diagnostic>"
      """
      CREATE RABBITMQ QUEUE INGESTOR incoming FROM broker QUEUE '<queue>'
        MODE ACK SEQUENTIAL ACK TIMEOUT 7s RETRY POLICY BACKOFF 200ms MAX 3s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size | queue                                                                                                                                                                                                                                                            | diagnostic        |
      | 1            |                                                                                                                                                                                                                                                                  | must not be empty |
      | 3            |                                                                                                                                                                                                                                                                  | must not be empty |
      | 1            | QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ | is 256 bytes long |
      | 3            | QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ | is 256 bytes long |
      | 1            | éééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééé                                                                                                                                 | is 256 bytes long |
      | 3            | éééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééé                                                                                                                                 | is 256 bytes long |

  Scenario Outline: RabbitMQ queue emission refuses a locator outside the protocol byte contract
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "<diagnostic>"
      """
      CREATE RABBITMQ QUEUE EMITTER outgoing FROM events TO broker QUEUE '<queue>'
        MODE NO ACK RETRY POLICY BACKOFF 200ms MAX 3s ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size | queue                                                                                                                                                                                                                                                            | diagnostic        |
      | 1            |                                                                                                                                                                                                                                                                  | must not be empty |
      | 3            |                                                                                                                                                                                                                                                                  | must not be empty |
      | 1            | QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ | is 256 bytes long |
      | 3            | QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ | is 256 bytes long |
      | 1            | éééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééé                                                                                                                                 | is 256 bytes long |
      | 3            | éééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééééé                                                                                                                                 | is 256 bytes long |

  Scenario Outline: A RabbitMQ client refuses an address that does not name an AMQP broker before commit
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "invalid client config key 'addr'"
      """
      CREATE RABBITMQ CLIENT broker CONFIG { 'addr' = 'amqp:/%2f' };
      """
    And these Plexus statements fail with "invalid client config key 'addr'"
      """
      CREATE RABBITMQ CLIENT broker CONFIG { 'addr' = 'amqp://127.0.0.1:0/%2f' };
      """
    And these Plexus statements fail with "invalid client config key 'addr'"
      """
      CREATE RABBITMQ CLIENT broker CONFIG { 'addr' = 'http://127.0.0.1:5672/%2f' };
      """
    And these Plexus statements fail with "invalid client config key 'addr'"
      """
      CREATE RABBITMQ CLIENT broker CONFIG { 'addr' = 'amqp://127.0.0.1:5672/%2f?heartbeat=soon' };
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A RabbitMQ client refuses an unpaired client identity and an undeclared key before commit
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "client config key 'tls_cert_file' requires 'tls_key_file'"
      """
      CREATE RABBITMQ CLIENT broker CONFIG {
        'addr' = 'amqps://127.0.0.1:5671/%2f',
        'tls_cert_file' = 'client.pem'
      };
      """
    And these Plexus statements fail with "client config key 'tls_key_file' requires 'tls_cert_file'"
      """
      CREATE RABBITMQ CLIENT broker CONFIG {
        'addr' = 'amqps://127.0.0.1:5671/%2f',
        'tls_key_file' = 'client-key.pem'
      };
      """
    And these Plexus statements fail with "unknown client config key 'tls_verify'"
      """
      CREATE RABBITMQ CLIENT broker CONFIG {
        'addr' = 'amqps://127.0.0.1:5671/%2f',
        'tls_verify' = 'false'
      };
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A RabbitMQ client refuses TLS files with an amqp address before commit
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has TLS resource directory "dev_tls" for hosts "broker.test"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE dev_tls;
      """
    When these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE dev_tls VERSION "{{dev_tls}}";
      """
    When these Plexus statements fail with "<diagnostic>"
      """
      CREATE RABBITMQ CLIENT broker MOUNT dev_tls VERSION 1 CONFIG {
        'addr' = 'amqp://127.0.0.1:5672/%2f', <files>
      };
      """

    Examples:
      | cluster_size | files                                                                           | diagnostic                                                     |
      | 1            | 'tls_ca_file' = '{{dev_tls}}/ca.crt'                                            | client config key 'tls_ca_file' requires 'addr' to use amqps   |
      | 3            | 'tls_ca_file' = '{{dev_tls}}/ca.crt'                                            | client config key 'tls_ca_file' requires 'addr' to use amqps   |
      | 1            | 'tls_cert_file' = '{{dev_tls}}/tls.crt', 'tls_key_file' = '{{dev_tls}}/tls.key' | client config key 'tls_cert_file' requires 'addr' to use amqps |
      | 3            | 'tls_cert_file' = '{{dev_tls}}/tls.crt', 'tls_key_file' = '{{dev_tls}}/tls.key' | client config key 'tls_cert_file' requires 'addr' to use amqps |
      | 1            | 'tls_cert_file' = '{{dev_tls}}/tls.crt'                                         | client config key 'tls_cert_file' requires 'tls_key_file'      |
      | 3            | 'tls_cert_file' = '{{dev_tls}}/tls.crt'                                         | client config key 'tls_cert_file' requires 'tls_key_file'      |
      | 1            | 'tls_key_file' = '{{dev_tls}}/tls.key'                                          | client config key 'tls_key_file' requires 'tls_cert_file'      |
      | 3            | 'tls_key_file' = '{{dev_tls}}/tls.key'                                          | client config key 'tls_key_file' requires 'tls_cert_file'      |

  Scenario Outline: A RabbitMQ client accepts TLS files with an amqps address before commit
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has TLS resource directory "dev_tls" for hosts "broker.test"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE dev_tls;
      """
    When these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE dev_tls VERSION "{{dev_tls}}";
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE RABBITMQ CLIENT broker MOUNT dev_tls VERSION 1 CONFIG {
        'addr' = 'amqps://127.0.0.1:5671/%2f', <files>
      };
      SHOW CREATE CLIENT broker;
      """
    Then the last command output contains
      """
      CREATE RABBITMQ CLIENT broker
      """

    Examples:
      | cluster_size | files                                                                           |
      | 1            | 'tls_ca_file' = '{{dev_tls}}/ca.crt'                                            |
      | 3            | 'tls_ca_file' = '{{dev_tls}}/ca.crt'                                            |
      | 1            | 'tls_cert_file' = '{{dev_tls}}/tls.crt', 'tls_key_file' = '{{dev_tls}}/tls.key' |
      | 3            | 'tls_cert_file' = '{{dev_tls}}/tls.crt', 'tls_key_file' = '{{dev_tls}}/tls.key' |
