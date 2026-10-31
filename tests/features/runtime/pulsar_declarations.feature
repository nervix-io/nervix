Feature: Pulsar topic declarations
  PULSAR TOPIC uses one topic rule in both directions: a local name that the client's namespace
  qualifies, or a fully qualified persistent or non-persistent name sent as written. Its
  subscription, delivery, quiesce and publishing modes, and the configuration a Pulsar client
  honours, apply at CREATE without contacting the broker.

  Scenario Outline: Pulsar topic declarations preserve topic literals and subscription ownership in both directions
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (tenant STRING, value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE PULSAR CLIENT pulsar_main CONFIG { 'addr' = 'pulsar://127.0.0.1:1' };
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE PULSAR TOPIC INGESTOR intake
        FROM pulsar_main TOPIC '<topic>' SUBSCRIPTION 'Readers (eu)' <ownership> INSTANCES 2
        MODE <ingest_mode>
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL
        SET tenant = coalesce(read_header('tenant'), 'absent')
        UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE INGESTOR intake;
      """
    Then the last command output contains
      """
      TOPIC '<topic>' SUBSCRIPTION 'Readers (eu)' <ownership> INSTANCES 2 MODE <ingest_mode> ON QUIESCE SUSPEND
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE PULSAR TOPIC EMITTER publishing FROM events
        TO pulsar_main TOPIC '<topic>'
        MODE <emit_mode>
        ENCODE USING event_codec INHERIT ALL
        INVOKE write_header('tenant', input.tenant)
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER publishing;
      """
    Then the last command output contains
      """
      TO pulsar_main TOPIC '<topic>'
      """
    And the last command output contains
      """
      MODE <emit_mode>
      """
    And the last command output contains
      """
      INVOKE write_header('tenant', input.tenant)
      """

    Examples:
      | cluster_size | topic                                  | ownership         | ingest_mode                                                                           | emit_mode                                                            |
      | 1            | Orders-EU_1                            | OWNED BY INGESTOR | NO ACK                                                                                | NO ACK RETRY POLICY BACKOFF 250ms MAX 30s                            |
      | 3            | Orders-EU_1                            | OWNED BY INGESTOR | NO ACK                                                                                | NO ACK RETRY POLICY BACKOFF 250ms MAX 30s                            |
      | 1            | persistent://tenant-a/orders/Placed    | PROVISIONED       | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s                         | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s     |
      | 3            | persistent://tenant-a/orders/Placed    | PROVISIONED       | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s                         | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s     |
      | 1            | non-persistent://public/default/orders | OWNED BY INGESTOR | ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | ACK PARALLEL MAX 4 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s |
      | 3            | non-persistent://public/default/orders | OWNED BY INGESTOR | ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | ACK PARALLEL MAX 4 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s |

  Scenario Outline: Pulsar topic ingestors refuse a subscription without its ownership and quiesce modes the topic does not declare
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (tenant STRING, value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE PULSAR CLIENT pulsar_main CONFIG { 'addr' = 'pulsar://127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE PULSAR TOPIC INGESTOR refused
        FROM pulsar_main TOPIC 'orders' <subscription> MODE NO ACK
        ON QUIESCE <quiesce> DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | subscription                             | quiesce                                      | expected                                          |
      | 1            |                                          | SUSPEND                                      | expected SUBSCRIPTION, found MODE                 |
      | 3            |                                          | SUSPEND                                      | expected SUBSCRIPTION, found MODE                 |
      | 1            | SUBSCRIPTION 'readers'                   | SUSPEND                                      | PROVISIONED, found MODE                           |
      | 3            | SUBSCRIPTION 'readers'                   | SUSPEND                                      | PROVISIONED, found MODE                           |
      | 1            | SUBSCRIPTION '' OWNED BY INGESTOR        | SUSPEND                                      | invalid pulsar_subscription '': must not be empty |
      | 3            | SUBSCRIPTION '' OWNED BY INGESTOR        | SUSPEND                                      | invalid pulsar_subscription '': must not be empty |
      | 1            | SUBSCRIPTION 'readers' OWNED BY INGESTOR | DROP                                         | expected SUSPEND, found DROP                      |
      | 3            | SUBSCRIPTION 'readers' OWNED BY INGESTOR | DROP                                         | expected SUSPEND, found DROP                      |
      | 1            | SUBSCRIPTION 'readers' PROVISIONED       | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | expected SUSPEND, found BUFFER                    |
      | 3            | SUBSCRIPTION 'readers' PROVISIONED       | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | expected SUSPEND, found BUFFER                    |

  Scenario Outline: Pulsar topic names outside the topic rule are refused in both directions
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (tenant STRING, value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (tenant string, value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE PULSAR CLIENT pulsar_main CONFIG { 'addr' = 'pulsar://127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE PULSAR TOPIC INGESTOR refused_intake
        FROM pulsar_main TOPIC '<topic>' SUBSCRIPTION 'readers' OWNED BY INGESTOR MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      invalid pulsar_topic '<topic>': <reason>
      """
    When these Plexus commands fail
      """
      CREATE PULSAR TOPIC EMITTER refused_publishing FROM events
        TO pulsar_main TOPIC '<topic>' MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s
        ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      invalid pulsar_topic '<topic>': <reason>
      """

    Examples:
      | cluster_size | topic                         | reason                                                                               |
      | 1            | public/default/orders         | holds '/' without naming its domain, one of persistent, non-persistent               |
      | 3            | public/default/orders         | holds '/' without naming its domain, one of persistent, non-persistent               |
      | 1            | persistent://public/orders    | names 2 levels after its domain, where a qualified name names tenant/namespace/topic |
      | 3            | persistent://public/orders    | names 2 levels after its domain, where a qualified name names tenant/namespace/topic |
      | 1            | persistent://public//orders   | holds an empty level separated by '/'                                                |
      | 3            | persistent://public//orders   | holds an empty level separated by '/'                                                |
      | 1            | kafka://public/default/orders | names the domain 'kafka', which is not one of persistent, non-persistent             |
      | 3            | kafka://public/default/orders | names the domain 'kafka', which is not one of persistent, non-persistent             |
      | 1            | Orders EU                     | holds ' ', which it may not hold                                                     |
      | 3            | Orders EU                     | holds ' ', which it may not hold                                                     |

  Scenario Outline: Pulsar client inspection reports the namespace that qualifies short topic names
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE PULSAR CLIENT plain CONFIG { 'addr' = 'pulsar://127.0.0.1:6650' };
      CREATE PULSAR CLIENT tenanted CONFIG {
        'addr' = 'pulsar://127.0.0.1:6650', 'namespace' = 'tenant-a/orders'
      };
      CREATE PULSAR CLIENT secured CONFIG {
        'addr' = 'pulsar+ssl://127.0.0.1:6651',
        'tls_allow_insecure_connection' = 'false',
        'tls_hostname_verification_enabled' = 'true'
      };
      """
    When these Plexus commands are executed
      """
      DESCRIBE CLIENT plain;
      """
    Then the last command output contains
      """
      namespace: public/default (default)
      """
    When these Plexus commands are executed
      """
      DESCRIBE CLIENT tenanted;
      """
    Then the last command output contains
      """
      namespace: tenant-a/orders
      """
    And the last command output does not contain
      """
      (default)
      """
    When these Plexus commands are executed
      """
      DESCRIBE CLIENT secured;
      """
    Then the last command output contains
      """
      tls_allow_insecure_connection: false
      tls_hostname_verification_enabled: true
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Pulsar clients refuse TLS settings on a plaintext pulsar address
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail
      """
      CREATE PULSAR CLIENT refused CONFIG {
        'addr' = 'pulsar://127.0.0.1:6650', '<key>' = '<value>'
      };
      """
    Then the last command error contains
      """
      client config key '<key>' requires 'addr' to use pulsar+ssl
      """

    Examples:
      | cluster_size | key                               | value                      |
      | 1            | tls_allow_insecure_connection     | true                       |
      | 3            | tls_allow_insecure_connection     | true                       |
      | 1            | tls_hostname_verification_enabled | false                      |
      | 3            | tls_hostname_verification_enabled | false                      |
      | 1            | tls_ca_file                       | /tmp/missing-pulsar-ca.pem |
      | 3            | tls_ca_file                       | /tmp/missing-pulsar-ca.pem |

  Scenario Outline: Pulsar clients refuse a namespace that is not a tenant and a namespace
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail
      """
      CREATE PULSAR CLIENT refused CONFIG {
        'addr' = 'pulsar://127.0.0.1:6650', 'namespace' = '<namespace>'
      };
      """
    Then the last command error contains
      """
      invalid client config key 'namespace': expected tenant/namespace
      """

    Examples:
      | cluster_size | namespace          |
      | 1            | tenant-a           |
      | 3            | tenant-a           |
      | 1            | tenant-a/orders/eu |
      | 3            | tenant-a/orders/eu |
      | 1            | tenant-a/          |
      | 3            | tenant-a/          |
      | 1            | tenant a/orders    |
      | 3            | tenant a/orders    |

  Scenario Outline: Pulsar clients refuse configuration no Pulsar connection reads
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail
      """
      CREATE PULSAR CLIENT refused CONFIG { <config> };
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | config                                                                                                              | expected                                       |
      | 1            | 'addr' = 'pulsar+ssl://127.0.0.1:6651', 'tls_cert_file' = '/tmp/client.pem', 'tls_key_file' = '/tmp/client-key.pem' | unknown client config key 'tls_cert_file'      |
      | 3            | 'addr' = 'pulsar+ssl://127.0.0.1:6651', 'tls_cert_file' = '/tmp/client.pem', 'tls_key_file' = '/tmp/client-key.pem' | unknown client config key 'tls_cert_file'      |
      | 1            | 'addr' = 'pulsar://127.0.0.1:6650', 'namespcae' = 'public/default'                                                  | unknown client config key 'namespcae'          |
      | 3            | 'addr' = 'pulsar://127.0.0.1:6650', 'namespcae' = 'public/default'                                                  | unknown client config key 'namespcae'          |
      | 1            | 'addr' = 'http://127.0.0.1:6650'                                                                                    | invalid client config key 'addr': expected URL |
      | 3            | 'addr' = 'http://127.0.0.1:6650'                                                                                    | invalid client config key 'addr': expected URL |
      | 1            | 'namespace' = 'public/default'                                                                                      | missing required client config key 'addr'      |
      | 3            | 'namespace' = 'public/default'                                                                                      | missing required client config key 'addr'      |
