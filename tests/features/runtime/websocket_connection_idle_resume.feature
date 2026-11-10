Feature: Outbound WebSocket activity after a delayed payload handoff
  Scenario Outline: A responsive WebSocket connection survives a delayed payload handoff with <idle_setting> idle bound on <cluster_size> nodes
    Given a responsive WebSocket handoff peer with <idle_setting> idle bound
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( user_id I64 );
      CREATE JAQ JSON CODEC event_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.';
      CREATE RELAY received SCHEMA event UNBRANCHED;
      CREATE WEBSOCKETS CLIENT socket CONFIG {
        'endpoint' = '{{websocket_peer}}', 'ping_interval_ms' = '1000' {{websocket_idle_config}}
      };
      CREATE WEBSOCKETS CONNECTION INGESTOR socket_events
        FROM socket MODE NO ACK ON QUIESCE DROP DECODE USING event_codec
        TO received INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION received_subscription TO received;
      START;
      """
    When the WebSocket payload handoff is held beyond its three-second idle bound
    Then within "60s" the relay subscription receives payloads in order
      """
      "user_id":42
      "user_id":43
      """

    Examples:
      | cluster_size | idle_setting |
      | 1            | derived      |
      | 3            | derived      |
      | 1            | configured   |
      | 3            | configured   |
