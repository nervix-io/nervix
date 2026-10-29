Feature: SQS queue declarations
  SQS QUEUE uses one literal, case-preserving queue rule in both directions. Its delivery, quiesce
  and publishing modes, its request shape, the envelope clauses and header values it carries, and
  the configuration an SQS client honours, apply at CREATE without contacting the service.

  Scenario Outline: SQS queue declarations preserve queue literals in both directions
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (tenant STRING, value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = 'http://127.0.0.1:1',
        'region' = 'eu-west-1',
        'access_key_id' = 'declaration-key',
        'secret_access_key' = 'declaration-secret',
        'timeout_ms' = '5000'
      };
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SQS QUEUE INGESTOR intake
        FROM sqs_main QUEUE '<queue>' INSTANCES 2
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL
        SET tenant = coalesce(read_header('tenant'), 'absent')
        UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE INGESTOR intake;
      """
    Then the last command output contains
      """
      QUEUE '<queue>' INSTANCES 2 MODE ACK SEQUENTIAL ACK TIMEOUT 5s
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SQS QUEUE EMITTER publishing FROM events
        TO sqs_main QUEUE '<queue>' <fifo_group>
        SEND <send> MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING event_codec <envelope> INHERIT ALL
        INVOKE write_header('tenant', input.tenant), write_header('size', length(input.value))
        <batch>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER publishing;
      """
    Then the last command output contains
      """
      QUEUE '<queue>' <options>
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING event_codec
          <envelope>
        INHERIT ALL
      """
    And the last command output contains
      """
      <shown>
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE EMITTER publishing;
      """
    Then the last command output contains
      """
      options: <options>
      mode: ACK RETRY POLICY BACKOFF 250ms MAX 30s
      quiesce: none
      envelope: <envelope>
      """

    Examples:
      | cluster_size | queue            | fifo_group              | send   | envelope                     | batch                               | shown                                     | options                            |
      | 1            | Orders-EU_1      |                         | SINGLE | DELAY 5s                     |                                     | write_header('size', length(input.value)) | SEND SINGLE                        |
      | 3            | Orders-EU_1      |                         | SINGLE | DELAY 900s                   |                                     | write_header('tenant', input.tenant)      | SEND SINGLE                        |
      | 1            | Orders-EU_1.fifo | FIFO GROUP input.tenant | BATCH  | DEDUPLICATION ID input.value | BATCH MAX MESSAGES 10 MAX SIZE 1MiB | FIFO GROUP input.tenant                   | FIFO GROUP input.tenant SEND BATCH |
      | 3            | Orders-EU_1.fifo | FIFO GROUP input.tenant | BATCH  | DEDUPLICATION ID input.value | BATCH MAX MESSAGES 10 MAX SIZE 1MiB | BATCH MAX MESSAGES 10 MAX SIZE 1MiB       | FIFO GROUP input.tenant SEND BATCH |

  Scenario Outline: SQS queue ingestors refuse delivery and quiesce modes the queue does not declare
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (tenant STRING, value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SQS CLIENT sqs_main CONFIG { 'endpoint' = 'http://127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE SQS QUEUE INGESTOR refused
        FROM sqs_main QUEUE 'orders' MODE <mode>
        ON QUIESCE <quiesce> DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | mode                                                               | quiesce                                      | expected                            |
      | 1            | NO ACK                                                             | SUSPEND                                      | expected ACK, found NO              |
      | 3            | NO ACK                                                             | SUSPEND                                      | expected ACK, found NO              |
      | 1            | ACK PARALLEL MAX 11 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | SUSPEND                                      | expected SEQUENTIAL, found PARALLEL |
      | 3            | ACK PARALLEL MAX 11 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | SUSPEND                                      | expected SEQUENTIAL, found PARALLEL |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s      | DROP                                         | expected SUSPEND, found DROP        |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s      | DROP                                         | expected SUSPEND, found DROP        |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s      | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | expected SUSPEND, found BUFFER      |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s      | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | expected SUSPEND, found BUFFER      |

  Scenario Outline: SQS queue emitters require a request shape and refuse shapes and publishing modes the queue does not declare
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (tenant STRING, value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SQS CLIENT sqs_main CONFIG { 'endpoint' = 'http://127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE SQS QUEUE EMITTER refused FROM events TO sqs_main QUEUE 'orders'
        <publishing> ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | publishing                                                                    | expected                                 |
      | 1            | MODE ACK RETRY POLICY BACKOFF 1s MAX 30s                                      | expected FIFO GROUP \| SEND, found MODE  |
      | 3            | MODE ACK RETRY POLICY BACKOFF 1s MAX 30s                                      | expected FIFO GROUP \| SEND, found MODE  |
      | 1            | SEND PARALLEL MODE ACK RETRY POLICY BACKOFF 1s MAX 30s                        | expected SINGLE \| BATCH, found PARALLEL |
      | 3            | SEND PARALLEL MODE ACK RETRY POLICY BACKOFF 1s MAX 30s                        | expected SINGLE \| BATCH, found PARALLEL |
      | 1            | SEND SINGLE MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s                       | expected ACK, found NO                   |
      | 3            | SEND SINGLE MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s                       | expected ACK, found NO                   |
      | 1            | SEND BATCH MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | expected RETRY POLICY, found SEQUENTIAL  |
      | 3            | SEND BATCH MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | expected RETRY POLICY, found SEQUENTIAL  |

  Scenario Outline: SQS queue emitters refuse envelope clauses and header values SQS cannot carry
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (tenant STRING, value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SQS CLIENT sqs_main CONFIG { 'endpoint' = 'http://127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE SQS QUEUE EMITTER refused FROM events TO sqs_main QUEUE '<queue>' <fifo_group>
        SEND SINGLE MODE ACK RETRY POLICY BACKOFF 1s MAX 30s
        ENCODE USING event_codec <envelope> INHERIT ALL <invoke>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | queue       | fifo_group              | envelope                     | invoke                                             | expected                                                                                        |
      | 1            | orders.fifo | FIFO GROUP input.tenant | DELAY 5s                     |                                                    | SQS FIFO queue 'orders.fifo' takes no per-message DELAY                                         |
      | 3            | orders.fifo | FIFO GROUP input.tenant | DELAY 5s                     |                                                    | SQS FIFO queue 'orders.fifo' takes no per-message DELAY                                         |
      | 1            | orders      |                         | DEDUPLICATION ID input.value |                                                    | SQS DEDUPLICATION ID requires a queue name ending in .fifo, found 'orders'                      |
      | 3            | orders      |                         | DEDUPLICATION ID input.value |                                                    | SQS DEDUPLICATION ID requires a queue name ending in .fifo, found 'orders'                      |
      | 1            | orders      |                         | DELAY 1500ms                 |                                                    | SQS DELAY must be whole seconds from 1s to 900s, found '1500ms'                                 |
      | 3            | orders      |                         | DELAY 16m                    |                                                    | SQS DELAY must be whole seconds from 1s to 900s, found '16m'                                    |
      | 1            | orders      |                         | DELAY 0s                     |                                                    | DELAY must be greater than zero                                                                 |
      | 3            | orders      |                         | DELAY 0s                     |                                                    | DELAY must be greater than zero                                                                 |
      | 1            | orders      |                         |                              | INVOKE write_header('flag', input.tenant = 'acme') | write_header value must be STRING, U8, I8, U16, I16, U32, I32, U64, I64 or BYTES, found Boolean |
      | 3            | orders      |                         |                              | INVOKE write_header('ratio', 0.5)                  | write_header value must be STRING, U8, I8, U16, I16, U32, I32, U64, I64 or BYTES, found Float64 |

  Scenario Outline: SQS queue names outside the SQS rule are refused in both directions
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (tenant STRING, value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SQS CLIENT sqs_main CONFIG { 'endpoint' = 'http://127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE SQS QUEUE INGESTOR refused_intake
        FROM sqs_main QUEUE '<queue>'
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      invalid sqs_queue '<queue>': <reason>
      """
    When these Plexus commands fail
      """
      CREATE SQS QUEUE EMITTER refused_publishing FROM events
        TO sqs_main QUEUE '<queue>'
        SEND SINGLE MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      invalid sqs_queue '<queue>': <reason>
      """

    Examples:
      | cluster_size | queue                                                                             | reason                                             |
      | 1            | orders.v2                                                                         | holds '.', which it may not hold                   |
      | 3            | orders.v2                                                                         | holds '.', which it may not hold                   |
      | 1            | Orders EU                                                                         | holds ' ', which it may not hold                   |
      | 3            | Orders EU                                                                         | holds ' ', which it may not hold                   |
      | 1            | .fifo                                                                             | holds nothing before its '.fifo' suffix            |
      | 3            | .fifo                                                                             | holds nothing before its '.fifo' suffix            |
      | 1            | orders-012345678901234567890123456789012345678901234567890123456789012345678.fifo | is 81 bytes long, longer than the 80 bytes allowed |
      | 3            | orders-012345678901234567890123456789012345678901234567890123456789012345678.fifo | is 81 bytes long, longer than the 80 bytes allowed |

  Scenario Outline: SQS clients refuse configuration an SQS client does not honour
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail
      """
      CREATE SQS CLIENT refused_client CONFIG {
        <config>
      };
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | config                                                    | expected                                                                                  |
      | 1            | 'region' = 'eu-west-1'                                    | missing required client config key 'endpoint'                                             |
      | 3            | 'region' = 'eu-west-1'                                    | missing required client config key 'endpoint'                                             |
      | 1            | 'endpoint' = 'ftp://127.0.0.1:1'                          | invalid client config key 'endpoint': expected URL                                        |
      | 3            | 'endpoint' = 'ftp://127.0.0.1:1'                          | invalid client config key 'endpoint': expected URL                                        |
      | 1            | 'endpoint' = 'http://127.0.0.1:1', 'timeout_ms' = 'soon'  | invalid client config key 'timeout_ms': expected positive, schedulable whole milliseconds |
      | 3            | 'endpoint' = 'http://127.0.0.1:1', 'timeout_ms' = 'soon'  | invalid client config key 'timeout_ms': expected positive, schedulable whole milliseconds |
      | 1            | 'endpoint' = 'http://127.0.0.1:1', 'regoin' = 'eu-west-1' | unknown client config key 'regoin'                                                        |
      | 3            | 'endpoint' = 'http://127.0.0.1:1', 'regoin' = 'eu-west-1' | unknown client config key 'regoin'                                                        |

  Scenario Outline: SQS clients refuse a client identity that SQS connections never present
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE sqs_tls;
      """
    And node "node-1" has resource directory "sqs_tls_fixture" containing
      """
      {"ca.pem":"trust material is inspected by the connector when it initializes\n","client.pem":"an identity no SQS connection presents\n","client-key.pem":"an identity no SQS connection presents\n"}
      """
    When these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE sqs_tls VERSION "{{sqs_tls_fixture}}";
      """
    And these Plexus commands fail
      """
      CREATE SQS CLIENT identity_client
      MOUNT sqs_tls VERSION 1
      CONFIG {
        'endpoint' = 'https://127.0.0.1:1',
        'tls_ca_file' = '{{ sqs_tls }}/ca.pem',
        'tls_cert_file' = '{{ sqs_tls }}/client.pem',
        'tls_key_file' = '{{ sqs_tls }}/client-key.pem'
      };
      """
    Then the last command error contains
      """
      unknown client config key 'tls_cert_file'
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SQS CLIENT trusting_client
      MOUNT sqs_tls VERSION 1
      CONFIG {
        'endpoint' = 'https://127.0.0.1:1',
        'tls_ca_file' = '{{ sqs_tls }}/ca.pem'
      };
      SHOW CREATE CLIENT trusting_client;
      """
    Then the last command output contains
      """
      'tls_ca_file' = '{{ sqs_tls }}/ca.pem'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
