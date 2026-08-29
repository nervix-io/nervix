Feature: Syslog support
  Scenario Outline: Syslog UDP intake decodes and forwards RFC 5424 messages
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And Syslog UDP emission endpoint "{{syslog_emit_addr}}" is observed
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA syslog_event (
        facility U8,
        severity U8,
        timestamp DATETIME OPTIONAL,
        hostname STRING OPTIONAL,
        app_name STRING OPTIONAL,
        proc_id STRING OPTIONAL,
        msg_id STRING OPTIONAL,
        structured_data STRING OPTIONAL,
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY syslog_events SCHEMA syslog_event UNBRANCHED;
      CREATE CLIENT syslog_listener
        TYPE SYSLOG
        CONFIG {
          'protocol' = 'udp',
          'addr' = '{{syslog_ingest_addr}}'
        };
      CREATE CLIENT syslog_forwarder
        TYPE SYSLOG
        CONFIG {
          'protocol' = 'udp',
          'addr' = '{{syslog_emit_addr}}'
        };
      CREATE INGESTOR syslog_intake
        FROM SYSLOG syslog_listener MODE NO_ACK SEQUENTIAL
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER syslog_forward
        FROM syslog_events
        TO SYSLOG syslog_forwarder
          MODE NO_ACK RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING syslog_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Then within "5s" DESCRIBE INGESTOR "syslog_intake" on the leader node contains
      """
      status: running
      """
    And within "5s" DESCRIBE EMITTER "syslog_forward" on the leader node contains
      """
      transient error: -
      reconnect backoff: -
      reconnect wait: -
      from: syslog_events
      codec: syslog_codec
      sink: SYSLOG client=syslog_forwarder
      """
    When Syslog UDP message is published to "{{syslog_ingest_addr}}"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-1 orders 123 ID47 [exampleSDID@32473 iut="3"] order accepted
      """
    Then the relay subscription receives a payload
      """
      "facility":4
      """
    And the last relay subscription payload contains
      """
      "severity":2
      "hostname":"edge-1"
      "app_name":"orders"
      "proc_id":"123"
      "msg_id":"ID47"
      "structured_data":"[exampleSDID@32473 iut=\"3\"]"
      "message":"order accepted"
      """
    And the observed Syslog UDP endpoint receives a payload
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-1 orders 123 ID47 [exampleSDID@32473 iut="3"] order accepted
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario: Invalid Syslog transport configuration fails locally owned entity startup
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a 1 node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA syslog_message (
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA syslog_message UNBRANCHED;
      CREATE CLIENT invalid_syslog_listener
        TYPE SYSLOG
        CONFIG {
          'protocol' = 'udp',
          'addr' = '{{syslog_ingest_addr}}',
          'framing' = 'octet-counting'
        };
      CREATE INGESTOR invalid_syslog_intake
        FROM SYSLOG invalid_syslog_listener MODE NO_ACK SEQUENTIAL
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "framing"
      """
      START;
      """

  Scenario Outline: Syslog TLS intake uses RFC 5425 framing and mounted mutual TLS identity
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has TLS resource directory "syslog_tls_dir" for hosts "127.0.0.1"
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
      CREATE SCHEMA syslog_message (
        message STRING
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA syslog_message UNBRANCHED;
      CREATE CLIENT syslog_listener
        TYPE SYSLOG
        MOUNT syslog_tls
        CONFIG {
          'protocol' = 'tls',
          'addr' = '{{syslog_ingest_addr}}',
          'tls_cert_file' = '{{ syslog_tls }}/tls.crt',
          'tls_key_file' = '{{ syslog_tls }}/tls.key',
          'tls_ca_file' = '{{ syslog_tls }}/ca.crt'
        };
      CREATE INGESTOR syslog_intake
        FROM SYSLOG syslog_listener MODE NO_ACK SEQUENTIAL
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Then within "5s" DESCRIBE INGESTOR "syslog_intake" on the leader node contains
      """
      status: running
      """
    When Syslog TLS message is published to "{{syslog_ingest_addr}}" using identity and CA from resource directory "syslog_tls_dir"
      """
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-3 secure 789 ID49 - tls framed
      """
    Then the relay subscription receives a payload
      """
      "message":"tls framed"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: Syslog TCP intake accepts mixed RFC 6587 framing and exposes peer metadata
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA syslog_message (
        message STRING
      );
      CREATE SCHEMA received_syslog_message (
        message STRING,
        peer_addr STRING OPTIONAL
      );
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_message;
      CREATE RELAY syslog_events SCHEMA received_syslog_message UNBRANCHED;
      CREATE CLIENT syslog_listener
        TYPE SYSLOG
        CONFIG {
          'protocol' = 'tcp',
          'addr' = '{{syslog_ingest_addr}}',
          'max_message_size' = '4096'
        };
      CREATE INGESTOR syslog_intake
        FROM SYSLOG syslog_listener MODE NO_ACK SEQUENTIAL
        ON QUIESCE SUSPEND
        DECODE USING syslog_codec
        TO syslog_events
          INHERIT message
          SET peer_addr = metadata.peer_addr
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION syslog_events_subscription TO syslog_events;
      START;
      """
    Then within "5s" DESCRIBE INGESTOR "syslog_intake" on the leader node contains
      """
      status: running
      """
    When Syslog TCP messages are published with mixed framing to "{{syslog_ingest_addr}}"
      """
      {{syslog_pri}}Oct 11 22:14:15 edge-2 worker: octet counted
      {{syslog_pri}}1 2003-10-11T22:14:15.003Z edge-2 worker 456 ID48 - line framed
      """
    Then the relay subscription receives a payload
      """
      "message":"octet counted"
      """
    And the last relay subscription payload contains
      """
      "peer_addr":"127.0.0.1:
      """
    Then the relay subscription receives a payload
      """
      "message":"line framed"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |
