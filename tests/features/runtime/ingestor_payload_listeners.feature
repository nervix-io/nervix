Feature: Ingest listener peer IPs
  A listener exposes the IP of its actual IPv6 peer without the connection's port.

  Scenario Outline: An HTTP endpoint records the IPv6 peer IP
    Given HTTP intake listens on IPv6 loopback
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (seq I64);
      CREATE SCHEMA measured (seq I64, remote_ip STRING OPTIONAL);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA measured UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-ipv6.example.com;
      CREATE HTTP ENDPOINT intake ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR measured_in
        FROM intake MODE NO ACK ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        FILTER WHERE payload.bytes > (0 AS U64)
        TO events INHERIT ALL SET remote_ip = metadata.remote_ip
          WHERE metadata.remote_ip = '::1'
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      START;
      """
    When http payload is posted to node "<intake_node>" with host "http-{{test_id}}-ipv6.example.com" path "/events"
      """
      {"seq":1}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "seq":1 | "remote_ip":"::1"
      """

    Examples:
      | cluster_size | intake_node |
      | 1            | node-1      |
      | 3            | node-3      |

  Scenario Outline: A WebSocket endpoint records the IPv6 peer IP
    Given HTTP intake listens on IPv6 loopback
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (seq I64);
      CREATE SCHEMA measured (seq I64, remote_ip STRING OPTIONAL);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA measured UNBRANCHED;
      CREATE VHOST edge ws-{{test_id}}-ipv6.example.com;
      CREATE WEBSOCKETS ENDPOINT intake ON edge PATH '/events';
      CREATE WEBSOCKETS ENDPOINT INGESTOR measured_in
        FROM intake MODE NO ACK ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        FILTER WHERE payload.bytes > (0 AS U64)
        TO events INHERIT ALL SET remote_ip = metadata.remote_ip
          WHERE metadata.remote_ip = '::1'
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      START;
      """
    When websocket message is published to node "<intake_node>" host "ws-{{test_id}}-ipv6.example.com" path "/events"
      """
      {"seq":1}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "seq":1 | "remote_ip":"::1"
      """

    Examples:
      | cluster_size | intake_node |
      | 1            | node-1      |
      | 3            | node-3      |

  Scenario Outline: A Syslog listener records the IPv6 peer IP
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" uses IPv6 loopback for Syslog intake
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (seq I64);
      CREATE SCHEMA measured (seq I64, remote_ip STRING OPTIONAL);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA measured UNBRANCHED;
      CREATE SYSLOG CLIENT listener CONFIG {'protocol' = 'udp', 'addr' = '{{syslog_ingest_addr}}'};
      CREATE SYSLOG LISTENER INGESTOR measured_in
        FROM listener MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        FILTER WHERE payload.bytes > (0 AS U64)
        TO events INHERIT ALL SET remote_ip = metadata.remote_ip
          WHERE metadata.remote_ip = '::1'
          UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observations TO events;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "measured_in" on the leader node contains
      """
      status: running
      """
    When an IPv6 Syslog UDP message is published to node "node-1"
      """
      {"seq":1}
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "seq":1 | "remote_ip":"::1"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
