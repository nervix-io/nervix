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
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = 'http://127.0.0.1:1',
        'region' = 'eu-west-1',
        'access_key_id' = 'declaration-key',
        'secret_access_key' = 'declaration-secret'
      };
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
      | cluster_size | mode                                                                                   | quiesce                                      | expected                                      |
      | 1            | NO ACK                                                                                 | SUSPEND                                      | expected ACK, found NO                        |
      | 3            | NO ACK                                                                                 | SUSPEND                                      | expected ACK, found NO                        |
      | 1            | ACK PARALLEL MAX 11 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | SUSPEND                                      | SQS ACK PARALLEL MAX must be between 1 and 10 |
      | 3            | ACK PARALLEL MAX 11 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | SUSPEND                                      | SQS ACK PARALLEL MAX must be between 1 and 10 |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s                          | DROP                                         | expected SUSPEND, found DROP                  |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s                          | DROP                                         | expected SUSPEND, found DROP                  |
      | 1            | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s                          | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | expected SUSPEND, found BUFFER                |
      | 3            | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s                          | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | expected SUSPEND, found BUFFER                |

  Scenario Outline: SQS queue ingestors accept ACK PARALLEL up to the service's receive maximum
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
        'secret_access_key' = 'declaration-secret'
      };
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SQS QUEUE INGESTOR parallel
        FROM sqs_main QUEUE 'orders' INSTANCES 2
        MODE <mode>
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      SHOW CREATE INGESTOR parallel;
      """
    Then the last command output contains
      """
      QUEUE 'orders' INSTANCES 2 MODE <mode>
      """

    Examples:
      | cluster_size | mode                                                                                   |
      | 1            | ACK PARALLEL MAX 1 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s  |
      | 3            | ACK PARALLEL MAX 1 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s  |
      | 1            | ACK PARALLEL MAX 10 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
      | 3            | ACK PARALLEL MAX 10 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |

  Scenario Outline: SQS queue emitters require a request shape and refuse shapes and publishing modes the queue does not declare
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
        'secret_access_key' = 'declaration-secret'
      };
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
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = 'http://127.0.0.1:1',
        'region' = 'eu-west-1',
        'access_key_id' = 'declaration-key',
        'secret_access_key' = 'declaration-secret'
      };
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
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = 'http://127.0.0.1:1',
        'region' = 'eu-west-1',
        'access_key_id' = 'declaration-key',
        'secret_access_key' = 'declaration-secret'
      };
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
      | cluster_size | config                                                                                                                                                             | expected                                                                                  |
      | 1            | 'region' = 'eu-west-1', 'access_key_id' = 'declaration-key', 'secret_access_key' = 'declaration-secret'                                                            | missing required client config key 'endpoint'                                             |
      | 3            | 'region' = 'eu-west-1', 'access_key_id' = 'declaration-key', 'secret_access_key' = 'declaration-secret'                                                            | missing required client config key 'endpoint'                                             |
      | 1            | 'endpoint' = 'http://127.0.0.1:1', 'access_key_id' = 'declaration-key', 'secret_access_key' = 'declaration-secret'                                                 | missing required client config key 'region'                                               |
      | 3            | 'endpoint' = 'http://127.0.0.1:1', 'access_key_id' = 'declaration-key', 'secret_access_key' = 'declaration-secret'                                                 | missing required client config key 'region'                                               |
      | 1            | 'endpoint' = 'http://127.0.0.1:1', 'region' = 'eu-west-1', 'secret_access_key' = 'declaration-secret'                                                              | missing required client config key 'access_key_id'                                        |
      | 3            | 'endpoint' = 'http://127.0.0.1:1', 'region' = 'eu-west-1', 'secret_access_key' = 'declaration-secret'                                                              | missing required client config key 'access_key_id'                                        |
      | 1            | 'endpoint' = 'http://127.0.0.1:1', 'region' = 'eu-west-1', 'access_key_id' = 'declaration-key'                                                                     | missing required client config key 'secret_access_key'                                    |
      | 3            | 'endpoint' = 'http://127.0.0.1:1', 'region' = 'eu-west-1', 'access_key_id' = 'declaration-key'                                                                     | missing required client config key 'secret_access_key'                                    |
      | 1            | 'endpoint' = 'ftp://127.0.0.1:1', 'region' = 'eu-west-1', 'access_key_id' = 'declaration-key', 'secret_access_key' = 'declaration-secret'                          | invalid client config key 'endpoint': expected URL                                        |
      | 3            | 'endpoint' = 'ftp://127.0.0.1:1', 'region' = 'eu-west-1', 'access_key_id' = 'declaration-key', 'secret_access_key' = 'declaration-secret'                          | invalid client config key 'endpoint': expected URL                                        |
      | 1            | 'endpoint' = 'http://127.0.0.1:1', 'region' = 'eu-west-1', 'access_key_id' = 'declaration-key', 'secret_access_key' = 'declaration-secret', 'timeout_ms' = 'soon'  | invalid client config key 'timeout_ms': expected positive, schedulable whole milliseconds |
      | 3            | 'endpoint' = 'http://127.0.0.1:1', 'region' = 'eu-west-1', 'access_key_id' = 'declaration-key', 'secret_access_key' = 'declaration-secret', 'timeout_ms' = 'soon'  | invalid client config key 'timeout_ms': expected positive, schedulable whole milliseconds |
      | 1            | 'endpoint' = 'http://127.0.0.1:1', 'region' = 'eu-west-1', 'access_key_id' = 'declaration-key', 'secret_access_key' = 'declaration-secret', 'regoin' = 'eu-west-1' | unknown client config key 'regoin'                                                        |
      | 3            | 'endpoint' = 'http://127.0.0.1:1', 'region' = 'eu-west-1', 'access_key_id' = 'declaration-key', 'secret_access_key' = 'declaration-secret', 'regoin' = 'eu-west-1' | unknown client config key 'regoin'                                                        |

  Scenario Outline: An SQS client carries a session token as a confidential key
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SQS CLIENT session_client CONFIG {
        'endpoint' = 'http://127.0.0.1:1',
        'region' = 'eu-west-1',
        'access_key_id' = 'declaration-key',
        'secret_access_key' = 'declaration-secret',
        'session_token' = 'declaration-session-token'
      };
      SHOW CREATE CLIENT session_client;
      """
    Then the last command output contains
      """
      'session_token' = '********'
      """
    And the last command output does not contain
      """
      declaration-session-token
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: SQS clients refuse a client identity that SQS connections never present
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE sqs_tls;
      """
    And node "node-1" has TLS resource directory "sqs_tls_fixture" for hosts "sqs.nervix.test"
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
        'region' = 'eu-west-1',
        'access_key_id' = 'declaration-key',
        'secret_access_key' = 'declaration-secret',
        'tls_ca_file' = '{{ sqs_tls }}/ca.crt',
        'tls_cert_file' = '{{ sqs_tls }}/tls.crt',
        'tls_key_file' = '{{ sqs_tls }}/tls.key'
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
        'region' = 'eu-west-1',
        'access_key_id' = 'declaration-key',
        'secret_access_key' = 'declaration-secret',
        'tls_ca_file' = '{{ sqs_tls }}/ca.crt'
      };
      SHOW CREATE CLIENT trusting_client;
      """
    Then the last command output contains
      """
      'tls_ca_file' = '{{ sqs_tls }}/ca.crt'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: SQS clients refuse a CA file that holds no certificate they could trust
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE sqs_trust;
      """
    And node "node-1" has resource directory "sqs_trust_fixture" containing
      """
      {"unencoded.pem":"-----BEGIN CERTIFICATE-----\n!this is not base64!\n-----END CERTIFICATE-----\n","undecodable.pem":"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n","empty.pem":"no certificate is written here\n"}
      """
    When these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE sqs_trust VERSION "{{sqs_trust_fixture}}";
      """
    And these Plexus commands fail
      """
      CREATE SQS CLIENT untrusting_client
      MOUNT sqs_trust VERSION 1
      CONFIG {
        'endpoint' = 'https://127.0.0.1:1',
        'region' = 'eu-west-1',
        'access_key_id' = 'declaration-key',
        'secret_access_key' = 'declaration-secret',
        'tls_ca_file' = '{{ sqs_trust }}/<file>'
      };
      """
    Then the last command error contains
      """
      invalid client config key 'tls_ca_file'
      """

    Examples:
      | cluster_size | file            |
      | 1            | unencoded.pem   |
      | 3            | unencoded.pem   |
      | 1            | undecodable.pem |
      | 3            | undecodable.pem |
      | 1            | empty.pem       |
      | 3            | empty.pem       |
