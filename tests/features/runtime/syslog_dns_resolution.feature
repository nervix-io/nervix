Feature: Syslog name resolution

  Scenario Outline: Syslog UDP emitter reaches an endpoint named by the node DNS fixture
    Given Syslog UDP emission endpoint "{{syslog_emit_addr}}" is observed
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the observed Syslog UDP endpoint is published under fixture DNS
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA syslog_event (
        facility U8,
        severity U8,
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY syslog_events SCHEMA syslog_event UNBRANCHED;
      CREATE CLIENT syslog_listener TYPE SYSLOG CONFIG {
        'protocol' = 'udp',
        'addr' = '{{syslog_ingest_addr}}'
      };
      CREATE CLIENT syslog_forwarder TYPE SYSLOG CONFIG {
        'protocol' = 'udp',
        'addr' = '{{syslog_dns_addr}}'
      };
      CREATE INGESTOR syslog_intake
        FROM SYSLOG syslog_listener MODE NO_ACK SEQUENTIAL
        ON QUIESCE SUSPEND DECODE USING syslog_codec
        TO syslog_events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER syslog_forward
        FROM syslog_events
        TO SYSLOG syslog_forwarder MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s
        ENCODE USING syslog_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    When Syslog UDP message is published to "{{syslog_ingest_addr}}"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-1 orders 123 ID47 - dns delivered
      """
    Then the observed Syslog UDP endpoint receives a payload
      """
      dns delivered
      """
    And the DNS fixture eventually receives a question for "syslog.nervix.test"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Syslog output waits for name recovery before delivery
    Given Syslog UDP emission endpoint "{{syslog_emit_addr}}" is observed
    And the Syslog endpoint "{{syslog_emit_addr}}" is forwarded as "syslog.nervix.test" from fixture addresses "127.0.5.2"
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the DNS fixture answers "syslog.nervix.test" with "<dns_answer>"
    And these NSPL commands are executed
      """
      CREATE SCHEMA syslog_event (facility U8, severity U8, message STRING);
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY input_events SCHEMA syslog_event UNBRANCHED;
      CREATE RELAY delivered_events SCHEMA syslog_event UNBRANCHED;
      CREATE CLIENT syslog_input TYPE SYSLOG CONFIG {
        'protocol' = 'udp', 'addr' = '{{syslog_ingest_addr}}'
      };
      CREATE CLIENT syslog_output TYPE SYSLOG CONFIG {
        'protocol' = 'tcp', 'addr' = '{{syslog_emit_addr}}'
      };
      CREATE CLIENT syslog_forwarder TYPE SYSLOG CONFIG {
        'protocol' = 'tcp', 'addr' = '{{syslog_forwarded_addr}}'
      };
      CREATE INGESTOR syslog_input_ingestor
        FROM SYSLOG syslog_input MODE NO_ACK SEQUENTIAL ON QUIESCE SUSPEND
        DECODE USING syslog_codec TO input_events
        INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR syslog_output_ingestor
        FROM SYSLOG syslog_output MODE NO_ACK SEQUENTIAL ON QUIESCE SUSPEND
        DECODE USING syslog_codec TO delivered_events
        INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER syslog_forward FROM input_events
        TO SYSLOG syslog_forwarder MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s
        ENCODE USING syslog_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION delivered_subscription TO delivered_events;
      START;
      """
    Then within "35s" DESCRIBE EMITTER "syslog_forward" on the leader node contains
      """
      resolving 'syslog.nervix.test' failed
      """
    When Syslog UDP message is published to "{{syslog_ingest_addr}}"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-1 orders 123 ID47 - dns recovered
      """
    And the DNS fixture answers "syslog.nervix.test" with addresses "127.0.5.2"
    Then the relay subscription receives a payload
      """
      "message":"dns recovered"
      """
    And the TCP forwarder at "127.0.5.2" eventually accepts a connection

    Examples:
      | cluster_size | dns_answer     |
      | 1            | name not found |
      | 3            | name not found |
      | 1            | silence        |
      | 3            | silence        |

  Scenario Outline: Syslog TCP emitter reaches a fixture-named listener with octet framing
    Given Syslog UDP emission endpoint "{{syslog_emit_addr}}" is observed
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the observed Syslog UDP endpoint is published under fixture DNS
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA syslog_event (facility U8, severity U8, message STRING);
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY input_events SCHEMA syslog_event UNBRANCHED;
      CREATE RELAY delivered_events SCHEMA syslog_event UNBRANCHED;
      CREATE CLIENT syslog_input TYPE SYSLOG CONFIG {
        'protocol' = 'udp', 'addr' = '{{syslog_ingest_addr}}'
      };
      CREATE CLIENT syslog_output TYPE SYSLOG CONFIG {
        'protocol' = 'tcp', 'addr' = '{{syslog_emit_addr}}'
      };
      CREATE CLIENT syslog_forwarder TYPE SYSLOG CONFIG {
        'protocol' = 'tcp', 'addr' = '{{syslog_dns_addr}}', 'framing' = 'octet-counting'
      };
      CREATE INGESTOR syslog_input_ingestor
        FROM SYSLOG syslog_input MODE NO_ACK SEQUENTIAL ON QUIESCE SUSPEND
        DECODE USING syslog_codec TO input_events
        INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR syslog_output_ingestor
        FROM SYSLOG syslog_output MODE NO_ACK SEQUENTIAL ON QUIESCE SUSPEND
        DECODE USING syslog_codec TO delivered_events
        INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER syslog_forward FROM input_events
        TO SYSLOG syslog_forwarder MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s
        ENCODE USING syslog_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION delivered_subscription TO delivered_events;
      START;
      """
    When Syslog UDP message is published to "{{syslog_ingest_addr}}"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-1 orders 123 ID47 - tcp via dns
      """
    Then the relay subscription receives a payload
      """
      "message":"tcp via dns"
      """
    And the DNS fixture eventually receives a question for "syslog.nervix.test"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Syslog TLS emitter verifies the fixture hostname and mutual TLS identity
    Given Syslog UDP emission endpoint "{{syslog_emit_addr}}" is observed
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    And the observed Syslog UDP endpoint is published under fixture DNS
    And the DNS fixture answers "syslog.nervix.test" with addresses "127.0.5.9,127.0.0.1"
    And node "node-1" has TLS resource directory "syslog_tls_dir" for hosts "syslog.nervix.test"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE RESOURCE syslog_tls;
      """
    And these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE syslog_tls VERSION "{{syslog_tls_dir}}";
      """
    And these NSPL commands are executed
      """
      CREATE SCHEMA syslog_event (facility U8, severity U8, message STRING);
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY input_events SCHEMA syslog_event UNBRANCHED;
      CREATE RELAY delivered_events SCHEMA syslog_event UNBRANCHED;
      CREATE CLIENT syslog_input TYPE SYSLOG CONFIG {
        'protocol' = 'udp', 'addr' = '{{syslog_ingest_addr}}'
      };
      CREATE CLIENT syslog_output TYPE SYSLOG MOUNT syslog_tls VERSION 1 CONFIG {
        'protocol' = 'tls', 'addr' = '{{syslog_emit_addr}}',
        'tls_cert_file' = '{{syslog_tls}}/tls.crt',
        'tls_key_file' = '{{syslog_tls}}/tls.key',
        'tls_ca_file' = '{{syslog_tls}}/ca.crt'
      };
      CREATE CLIENT syslog_forwarder TYPE SYSLOG MOUNT syslog_tls VERSION 1 CONFIG {
        'protocol' = 'tls', 'addr' = '{{syslog_dns_addr}}',
        'tls_cert_file' = '{{syslog_tls}}/tls.crt',
        'tls_key_file' = '{{syslog_tls}}/tls.key',
        'tls_ca_file' = '{{syslog_tls}}/ca.crt'
      };
      CREATE INGESTOR syslog_input_ingestor
        FROM SYSLOG syslog_input MODE NO_ACK SEQUENTIAL ON QUIESCE SUSPEND
        DECODE USING syslog_codec TO input_events
        INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR syslog_output_ingestor
        FROM SYSLOG syslog_output MODE NO_ACK SEQUENTIAL ON QUIESCE SUSPEND
        DECODE USING syslog_codec TO delivered_events
        INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER syslog_forward FROM input_events
        TO SYSLOG syslog_forwarder MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s
        ENCODE USING syslog_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION delivered_subscription TO delivered_events;
      START;
      """
    When Syslog UDP message is published to "{{syslog_ingest_addr}}"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-1 orders 123 ID47 - tls via dns
      """
    Then the relay subscription receives a payload
      """
      "message":"tls via dns"
      """
    And the DNS fixture eventually receives a question for "syslog.nervix.test"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
