Feature: Syslog listener and collector declarations
  SYSLOG LISTENER runs on every node and SYSLOG COLLECTOR sends through one client. Both sides
  deliver without acknowledgements and carry no transport headers, and what each declares applies
  at CREATE without opening a socket. A collector writes only a payload that fits one frame
  within the frame limits the Syslog vocabulary declares.

  Scenario Outline: A Syslog listener refuses a client key only collectors read when it binds the client
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (host STRING, message STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (host string, message string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_main CONFIG {
        'protocol' = 'tcp', 'addr' = '127.0.0.1:5514', 'timeout_ms' = '1000'
      };
      """
    When these Plexus commands fail
      """
      CREATE SYSLOG LISTENER INGESTOR refused
        FROM syslog_main MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      SYSLOG LISTENER ingestor 'refused' in domain '{{domain}}' cannot use client 'syslog_main': client config key 'timeout_ms' is read only by emitters
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A Syslog collector refuses a client key only listeners read when it binds the client
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (host STRING, message STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (host string, message string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_main CONFIG {
        'protocol' = 'tcp', 'addr' = '127.0.0.1:5514', <setting>
      };
      """
    When these Plexus commands fail
      """
      CREATE SYSLOG COLLECTOR EMITTER refused FROM events TO syslog_main
        MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      SYSLOG COLLECTOR emitter 'refused' in domain '{{domain}}' cannot use client 'syslog_main': client config key '<key>' is read only by ingestors
      """

    Examples:
      | cluster_size | setting                    | key              |
      | 1            | 'max_message_size' = '512' | max_message_size |
      | 3            | 'max_message_size' = '512' | max_message_size |
      | 1            | 'idle_timeout_ms' = '1000' | idle_timeout_ms  |
      | 3            | 'idle_timeout_ms' = '1000' | idle_timeout_ms  |

  Scenario Outline: An alteration that binds a Syslog client keeps to the keys its side reads
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (host STRING, message STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (host string, message string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SYSLOG CLIENT listening CONFIG {
        'protocol' = 'tcp', 'addr' = '127.0.0.1:5514', 'max_message_size' = '512'
      };
      CREATE SYSLOG CLIENT collecting CONFIG {
        'protocol' = 'tcp', 'addr' = '127.0.0.1:5515', 'timeout_ms' = '1000'
      };
      CREATE SYSLOG CLIENT shared CONFIG {
        'protocol' = 'tcp', 'addr' = '127.0.0.1:5516', 'framing' = 'octet-counting'
      };
      CREATE SYSLOG LISTENER INGESTOR intake
        FROM shared MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SYSLOG COLLECTOR EMITTER publishing FROM events TO shared
        MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail
      """
      ALTER SYSLOG COLLECTOR EMITTER publishing SET CLIENT listening;
      """
    Then the last command error contains
      """
      SYSLOG COLLECTOR emitter 'publishing' in domain '{{domain}}' cannot use client 'listening': client config key 'max_message_size' is read only by ingestors
      """
    When these Plexus commands fail
      """
      ALTER SYSLOG LISTENER INGESTOR intake SET FROM collecting MODE NO ACK ON QUIESCE SUSPEND;
      """
    Then the last command error contains
      """
      SYSLOG LISTENER ingestor 'intake' in domain '{{domain}}' cannot use client 'collecting': client config key 'timeout_ms' is read only by emitters
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER SYSLOG COLLECTOR EMITTER publishing SET CLIENT collecting;
      ALTER SYSLOG LISTENER INGESTOR intake SET FROM listening MODE NO ACK ON QUIESCE SUSPEND;
      SHOW CREATE INGESTOR intake;
      """
    Then the last command output contains
      """
      FROM listening MODE NO ACK ON QUIESCE SUSPEND
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE EMITTER publishing;
      """
    Then the last command output contains
      """
      TO collecting
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Each side of a Syslog TLS client names its certificate authority with its own key
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has TLS resource directory "syslog_tls_dir" for hosts "127.0.0.1"
    When these Plexus commands are executed
      """
      CREATE RESOURCE syslog_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE syslog_tls VERSION "{{syslog_tls_dir}}";
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA event (host STRING, message STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (host string, message string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SYSLOG CLIENT trusting MOUNT syslog_tls VERSION 1 CONFIG {
        'protocol' = 'tls', 'addr' = '127.0.0.1:6514',
        'tls_cert_file' = '{{ syslog_tls }}/tls.crt',
        'tls_key_file' = '{{ syslog_tls }}/tls.key',
        'tls_ca_file' = '{{ syslog_tls }}/ca.crt'
      };
      CREATE SYSLOG CLIENT verifying MOUNT syslog_tls VERSION 1 CONFIG {
        'protocol' = 'tls', 'addr' = '127.0.0.1:6515',
        'tls_cert_file' = '{{ syslog_tls }}/tls.crt',
        'tls_key_file' = '{{ syslog_tls }}/tls.key',
        'tls_client_ca_file' = '{{ syslog_tls }}/ca.crt'
      };
      """
    When these Plexus commands fail
      """
      CREATE SYSLOG LISTENER INGESTOR refused
        FROM trusting MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      client config key 'tls_ca_file' is read only by emitters
      """
    When these Plexus commands fail
      """
      CREATE SYSLOG COLLECTOR EMITTER refused FROM events TO verifying
        MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      client config key 'tls_client_ca_file' is read only by ingestors
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A Syslog listener refuses a client it cannot bind or serve while a collector accepts it
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (host STRING, message STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (host string, message string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_main CONFIG { <config> };
      """
    When these Plexus commands fail
      """
      CREATE SYSLOG LISTENER INGESTOR refused
        FROM syslog_main MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      SYSLOG LISTENER ingestor 'refused' in domain '{{domain}}' cannot use client 'syslog_main': <expected>
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SYSLOG COLLECTOR EMITTER accepted FROM events TO syslog_main
        MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER accepted;
      """
    Then the last command output contains
      """
      TO syslog_main
      """

    Examples:
      | cluster_size | config                                                                       | expected                                                               |
      | 1            | 'protocol' = 'udp', 'addr' = 'syslog.example.test:5514'                      | invalid client config key 'addr': expected literal IP address and port |
      | 3            | 'protocol' = 'udp', 'addr' = 'syslog.example.test:5514'                      | invalid client config key 'addr': expected literal IP address and port |
      | 1            | 'protocol' = 'tcp', 'addr' = 'localhost:5514'                                | invalid client config key 'addr': expected literal IP address and port |
      | 3            | 'protocol' = 'tcp', 'addr' = 'localhost:5514'                                | invalid client config key 'addr': expected literal IP address and port |
      | 1            | 'protocol' = 'tcp', 'addr' = '127.0.0.1:5514', 'connect_timeout_ms' = '1000' | client config key 'connect_timeout_ms' requires 'protocol' to be tls   |
      | 3            | 'protocol' = 'tcp', 'addr' = '127.0.0.1:5514', 'connect_timeout_ms' = '1000' | client config key 'connect_timeout_ms' requires 'protocol' to be tls   |
      | 1            | 'protocol' = 'udp', 'addr' = '127.0.0.1:5514', 'connect_timeout_ms' = '1000' | client config key 'connect_timeout_ms' requires 'protocol' to be tls   |
      | 3            | 'protocol' = 'udp', 'addr' = '127.0.0.1:5514', 'connect_timeout_ms' = '1000' | client config key 'connect_timeout_ms' requires 'protocol' to be tls   |
      | 1            | 'protocol' = 'tls', 'addr' = '127.0.0.1:6514'                                | missing required client config key 'tls_cert_file'                     |
      | 3            | 'protocol' = 'tls', 'addr' = '127.0.0.1:6514'                                | missing required client config key 'tls_cert_file'                     |

  Scenario Outline: A Syslog client no side can read is refused at CREATE CLIENT
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail
      """
      CREATE SYSLOG CLIENT refused CONFIG { <config> };
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | config                                                                             | expected                                                             |
      | 1            | 'protocol' = 'udp', 'addr' = '127.0.0.1:5514', 'framing' = 'octet-counting'        | client config key 'framing' requires 'protocol' to be tcp or tls     |
      | 3            | 'protocol' = 'udp', 'addr' = '127.0.0.1:5514', 'framing' = 'octet-counting'        | client config key 'framing' requires 'protocol' to be tcp or tls     |
      | 1            | 'protocol' = 'udp', 'addr' = '127.0.0.1:5514', 'idle_timeout_ms' = '1000'          | client config key 'idle_timeout_ms' requires 'protocol' to be tcp or tls |
      | 3            | 'protocol' = 'udp', 'addr' = '127.0.0.1:5514', 'idle_timeout_ms' = '1000'          | client config key 'idle_timeout_ms' requires 'protocol' to be tcp or tls |
      | 1            | 'protocol' = 'tcp', 'addr' = '127.0.0.1:5514', 'tls_ca_file' = '/tmp/ca.crt'       | client config key 'tls_ca_file' requires 'protocol' to be tls        |
      | 3            | 'protocol' = 'tcp', 'addr' = '127.0.0.1:5514', 'tls_ca_file' = '/tmp/ca.crt'       | client config key 'tls_ca_file' requires 'protocol' to be tls        |
      | 1            | 'protocol' = 'udp', 'addr' = '127.0.0.1:5514', 'tls_client_ca_file' = '/tmp/ca.crt' | client config key 'tls_client_ca_file' requires 'protocol' to be tls |
      | 3            | 'protocol' = 'udp', 'addr' = '127.0.0.1:5514', 'tls_client_ca_file' = '/tmp/ca.crt' | client config key 'tls_client_ca_file' requires 'protocol' to be tls |
      | 1            | 'protocol' = 'tls', 'addr' = '127.0.0.1:6514', 'framing' = 'non-transparent'       | invalid client config key 'framing': expected octet-counting over tls |
      | 3            | 'protocol' = 'tls', 'addr' = '127.0.0.1:6514', 'framing' = 'non-transparent'       | invalid client config key 'framing': expected octet-counting over tls |
      | 1            | 'protocol' = 'tcp', 'addr' = 'missing-port'                                        | invalid client config key 'addr': expected host:port                 |
      | 3            | 'protocol' = 'tcp', 'addr' = 'missing-port'                                        | invalid client config key 'addr': expected host:port                 |
      | 1            | 'protocol' = 'tcp', 'addr' = 'syslog.example.test:5514/path'                       | invalid client config key 'addr': expected host:port                 |
      | 3            | 'protocol' = 'tcp', 'addr' = 'syslog.example.test:5514/path'                       | invalid client config key 'addr': expected host:port                 |

  Scenario Outline: Syslog declarations render the listener and the collector they declare
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (host STRING, message STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (host string, message string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_main CONFIG { 'protocol' = 'udp', 'addr' = '127.0.0.1:5514' };
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SYSLOG LISTENER INGESTOR intake
        FROM syslog_main MODE NO ACK ON QUIESCE <quiesce> DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      SHOW CREATE INGESTOR intake;
      """
    Then the last command output contains
      """
      FROM syslog_main MODE NO ACK ON QUIESCE <quiesce>
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SYSLOG COLLECTOR EMITTER publishing FROM events
        TO syslog_main MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING event_codec INHERIT ALL
        <batch>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER publishing;
      """
    Then the last command output contains
      """
      MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
      """
    And the last command output contains
      """
      <shown>
      """

    Examples:
      | cluster_size | quiesce                                      | batch                                | shown                                |
      | 1            | SUSPEND                                      |                                      | ENCODE USING event_codec             |
      | 3            | SUSPEND                                      |                                      | ENCODE USING event_codec             |
      | 1            | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | BATCH MAX MESSAGES 10 MAX SIZE 64KiB |
      | 3            | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | BATCH MAX MESSAGES 10 MAX SIZE 64KiB | BATCH MAX MESSAGES 10 MAX SIZE 64KiB |
      | 1            | DROP                                         |                                      | ENCODE USING event_codec             |
      | 3            | DROP                                         |                                      | ENCODE USING event_codec             |

  Scenario Outline: Syslog listeners refuse delivery, instancing and quiesce they do not declare
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (host STRING, message STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (host string, message string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_main CONFIG { 'protocol' = 'udp', 'addr' = '127.0.0.1:5514' };
      """
    When these Plexus commands fail
      """
      CREATE SYSLOG LISTENER INGESTOR refused
        FROM syslog_main <options>MODE <mode> ON QUIESCE <quiesce> DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | options     | mode                                                          | quiesce               | expected                       |
      | 1            |             | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | SUSPEND               | expected NO ACK, found ACK     |
      | 3            |             | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | SUSPEND               | expected NO ACK, found ACK     |
      | 1            | INSTANCES 2 | NO ACK                                                        | SUSPEND               | expected MODE, found INSTANCES |
      | 3            | INSTANCES 2 | NO ACK                                                        | SUSPEND               | expected MODE, found INSTANCES |
      | 1            |             | NO ACK                                                        | REJECT RETRY AFTER 5s | found REJECT                   |
      | 3            |             | NO ACK                                                        | REJECT RETRY AFTER 5s | found REJECT                   |

  Scenario Outline: Syslog collectors refuse publishing modes they do not declare
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (host STRING, message STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (host string, message string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_main CONFIG { 'protocol' = 'udp', 'addr' = '127.0.0.1:5514' };
      """
    When these Plexus commands fail
      """
      CREATE SYSLOG COLLECTOR EMITTER refused FROM events TO syslog_main
        MODE <mode> ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      found <found>
      """

    Examples:
      | cluster_size | mode                                   | found  |
      | 1            | ACK RETRY POLICY BACKOFF 1s MAX 30s    | ACK    |
      | 3            | ACK RETRY POLICY BACKOFF 1s MAX 30s    | ACK    |
      | 1            | SINGLE RETRY POLICY BACKOFF 1s MAX 30s | SINGLE |
      | 3            | SINGLE RETRY POLICY BACKOFF 1s MAX 30s | SINGLE |

  Scenario Outline: Syslog entities refuse transport headers on both sides
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (host STRING, message STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (host string, message string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE SYSLOG CLIENT syslog_main CONFIG { 'protocol' = 'udp', 'addr' = '127.0.0.1:5514' };
      """
    When these Plexus commands fail
      """
      CREATE SYSLOG LISTENER INGESTOR reading
        FROM syslog_main MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL SET host = coalesce(read_header('host'), 'absent')
        UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      read_header
      """
    When these Plexus commands fail
      """
      CREATE SYSLOG COLLECTOR EMITTER writing FROM events TO syslog_main
        MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING event_codec INHERIT ALL
        INVOKE write_header('host', input.host)
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      write_header
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A Syslog UDP collector sends a payload at the declared datagram limit and refuses a larger one
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA syslog_event (
        facility U8, severity U8, hostname STRING OPTIONAL, message STRING
      );
      CREATE SCHEMA outcome ( size I64 OPTIONAL, error_message STRING OPTIONAL );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA syslog_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY events SCHEMA syslog_event UNBRANCHED;
      CREATE RELAY outcomes SCHEMA outcome UNBRANCHED;
      CREATE VHOST edge syslog-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL SET message = concat(repeat('x', 65483), input.message)
          UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SYSLOG CLIENT syslog_listener CONFIG {
        'protocol' = 'udp', 'addr' = '{{syslog_ingest_addr}}'
      };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING syslog_codec
        TO outcomes SET size = octet_length(input.message) UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SYSLOG CLIENT syslog_main CONFIG {
        'protocol' = 'udp', 'addr' = '{{syslog_ingest_addr}}'
      };
      CREATE SYSLOG COLLECTOR EMITTER limited_syslog FROM events
        TO syslog_main MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING syslog_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO outcomes
          SET error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION outcomes_subscription TO outcomes;
      START;
      """
    # The encoded header `<34>1 - app-01 - - - - ` takes 23 bytes, so a 65,484-byte message fills
    # the 65,507 bytes one datagram carries and a 65,485-byte message needs one byte more.
    And http payload is posted to host "syslog-{{test_id}}.example.com" path "/events"
      """
      [{"facility":4,"severity":2,"hostname":"app-01","message":"y"},{"facility":4,"severity":2,"hostname":"app-01","message":"yz"}]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "size":65484
      encoded Syslog UDP payload is 65508 bytes | maximum is 65507
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A non-transparent Syslog TCP collector refuses a payload whose LF would end its frame
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA syslog_event (
        facility U8, severity U8, hostname STRING OPTIONAL, message STRING
      );
      CREATE SCHEMA outcome ( message STRING OPTIONAL, error_message STRING OPTIONAL );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA syslog_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY events SCHEMA syslog_event UNBRANCHED;
      CREATE RELAY outcomes SCHEMA outcome UNBRANCHED;
      CREATE VHOST edge syslog-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SYSLOG CLIENT syslog_listener CONFIG {
        'protocol' = 'tcp', 'addr' = '{{syslog_ingest_addr}}'
      };
      CREATE SYSLOG LISTENER INGESTOR syslog_intake
        FROM syslog_listener MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING syslog_codec
        TO outcomes SET message = input.message UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SYSLOG CLIENT syslog_main CONFIG {
        'protocol' = 'tcp', 'addr' = '{{syslog_ingest_addr}}', 'framing' = 'non-transparent'
      };
      CREATE SYSLOG COLLECTOR EMITTER framed_syslog FROM events
        TO syslog_main MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING syslog_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO outcomes
          SET error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION outcomes_subscription TO outcomes;
      START;
      """
    And http payload is posted to host "syslog-{{test_id}}.example.com" path "/events"
      """
      [{"facility":4,"severity":2,"hostname":"app-01","message":"single line"},{"facility":4,"severity":2,"hostname":"app-01","message":"first line\nsecond line"}]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "message":"single line"
      contains LF | non-transparent framing
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
