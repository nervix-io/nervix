Feature: Syslog listener transport metadata
  A listener projects the transport of each received frame and the subject of its verified
  TLS client certificate. A connection without a verified client identity has no TLS peer.
  The route renders a null TLS subject as "unverified" for the JSON observation.

  Scenario Outline: A Syslog UDP frame carries its transport and has no TLS peer
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA syslog_payload (message STRING);
      CREATE SCHEMA received_syslog (message STRING, peer_addr STRING OPTIONAL, transport STRING OPTIONAL, tls_peer STRING);
      CREATE WIRE JSON SCHEMA syslog_payload_wire MODE STRICT (message string);
      CREATE CODEC payload_json FROM WIRE JSON SCHEMA syslog_payload_wire TO SCHEMA syslog_payload;
      CREATE RELAY events SCHEMA received_syslog UNBRANCHED;
      CREATE SYSLOG CLIENT metadata_listener CONFIG {'protocol' = 'udp', 'addr' = '{{syslog_ingest_addr}}'};
      CREATE SYSLOG LISTENER INGESTOR intake
        FROM metadata_listener MODE NO ACK ON QUIESCE SUSPEND DECODE USING payload_json
        TO events INHERIT ALL
          SET peer_addr = metadata.peer_addr,
              transport = metadata.transport,
              tls_peer = coalesce(metadata.tls_peer, 'unverified')
          WHERE metadata.transport = 'udp'
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observed TO events;
      START;
      """
    Then within "10s" DESCRIBE INGESTOR "intake" on the leader node contains
      """
      status: running
      """
    When Syslog UDP message is published to "{{syslog_ingest_addr}}"
      """
      {"message":"datagram"}
      """
    Then the relay subscription receives a payload
      """
      "message":"datagram"
      """
    And the last relay subscription payload contains
      """
      "peer_addr":"127.0.0.1:
      "transport":"udp"
      "tls_peer":"unverified"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: Syslog TCP frames carry their transport and have no TLS peer
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA syslog_payload (message STRING);
      CREATE SCHEMA received_syslog (message STRING, peer_addr STRING OPTIONAL, transport STRING OPTIONAL, tls_peer STRING);
      CREATE WIRE JSON SCHEMA syslog_payload_wire MODE STRICT (message string);
      CREATE CODEC payload_json FROM WIRE JSON SCHEMA syslog_payload_wire TO SCHEMA syslog_payload;
      CREATE RELAY events SCHEMA received_syslog UNBRANCHED;
      CREATE SYSLOG CLIENT metadata_listener CONFIG {'protocol' = 'tcp', 'addr' = '{{syslog_ingest_addr}}'};
      CREATE SYSLOG LISTENER INGESTOR intake
        FROM metadata_listener MODE NO ACK ON QUIESCE SUSPEND DECODE USING payload_json
        TO events INHERIT ALL
          SET peer_addr = metadata.peer_addr,
              transport = metadata.transport,
              tls_peer = coalesce(metadata.tls_peer, 'unverified')
          WHERE metadata.transport = 'tcp'
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observed TO events;
      START;
      """
    Then within "10s" DESCRIBE INGESTOR "intake" on the leader node contains
      """
      status: running
      """
    When Syslog TCP messages are published with mixed framing to "{{syslog_ingest_addr}}"
      """
      {"message":"octet counted"}
      {"message":"line framed"}
      """
    Then the relay subscription receives a payload
      """
      "message":"octet counted"
      """
    And the last relay subscription payload contains
      """
      "peer_addr":"127.0.0.1:
      "transport":"tcp"
      "tls_peer":"unverified"
      """
    Then the relay subscription receives a payload
      """
      "message":"line framed"
      """
    And the last relay subscription payload contains
      """
      "transport":"tcp"
      "tls_peer":"unverified"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @syslog_metadata @deloxide_stress
  Scenario Outline: A Syslog TLS frame exposes a peer only when the client certificate is verified
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
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
      CREATE SCHEMA syslog_payload (message STRING);
      CREATE SCHEMA received_syslog (message STRING, peer_addr STRING OPTIONAL, transport STRING OPTIONAL, tls_peer STRING);
      CREATE WIRE JSON SCHEMA syslog_payload_wire MODE STRICT (message string);
      CREATE CODEC payload_json FROM WIRE JSON SCHEMA syslog_payload_wire TO SCHEMA syslog_payload;
      CREATE RELAY events SCHEMA received_syslog UNBRANCHED;
      CREATE SYSLOG CLIENT metadata_listener MOUNT syslog_tls VERSION 1 CONFIG {
        'protocol' = 'tls',
        'addr' = '{{syslog_ingest_addr}}',
        'tls_cert_file' = '{{ syslog_tls }}/tls.crt',
        'tls_key_file' = '{{ syslog_tls }}/tls.key'<client_authority>
      };
      CREATE SYSLOG LISTENER INGESTOR intake
        FROM metadata_listener MODE NO ACK ON QUIESCE SUSPEND DECODE USING payload_json
        TO events INHERIT ALL
          SET peer_addr = metadata.peer_addr,
              transport = metadata.transport,
              tls_peer = coalesce(metadata.tls_peer, 'unverified')
          WHERE metadata.transport = 'tls'
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observed TO events;
      START;
      """
    Then within "10s" DESCRIBE INGESTOR "intake" on the leader node contains
      """
      status: running
      """
    When Syslog TLS message is published to "{{syslog_ingest_addr}}" using identity and CA from resource directory "syslog_tls_dir"
      """
      {"message":"secure frame"}
      """
    Then the relay subscription receives a payload
      """
      "message":"secure frame"
      """
    And the last relay subscription payload contains
      """
      "peer_addr":"127.0.0.1:
      "transport":"tls"
      "tls_peer":<peer>
      """

    Examples:
      | cluster_size | replica_count | client_authority                            | peer           |
      | 1            | 0             | , 'tls_ca_file' = '{{ syslog_tls }}/ca.crt' | "CN=127.0.0.1" |
      | 3            | 1             | , 'tls_ca_file' = '{{ syslog_tls }}/ca.crt' | "CN=127.0.0.1" |
      | 1            | 0             |                                             | "unverified"   |
      | 3            | 1             |                                             | "unverified"   |
