@syslog_encoding
Feature: Syslog encoding preservation
  Scenario Outline: A syslog encoder preserves exact headers and its BOM-free message
    Given Kafka is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "syslog_exact_{{test_id}}" exists with 1 partitions
    And Kafka topic "syslog_exact_{{test_id}}" is observed
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA syslog_event (
        facility U8,
        severity U8,
        hostname STRING OPTIONAL,
        app_name STRING OPTIONAL,
        proc_id STRING OPTIONAL,
        msg_id STRING OPTIONAL,
        structured_data STRING OPTIONAL,
        message STRING
      );
      CREATE CODEC ingest_codec FROM JSON TO SCHEMA syslog_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
      CREATE RELAY syslog_events SCHEMA syslog_event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT events_endpoint ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR http_events
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ingest_codec
        TO syslog_events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC EMITTER syslog_frames FROM syslog_events
        TO kafka_main TOPIC 'syslog_exact_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING syslog_codec
        INHERIT ALL
        <batch_clause>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/events"
      """
      [{"facility":16,"severity":6,"hostname":"edge-1","app_name":"orders","proc_id":"worker-a","msg_id":"event-a","structured_data":"[z@32473 second=\"a\\]b\" first=\"quote\\\"slash\\\\\"][a@32473 p=\"\u00e9\"]","message":"\ufeffhello \"\u4e16\u754c\"\nend"}]
      """
    Then within "30s" the observed broker receives exactly these encoded payloads
      """
      <payload>
      """

    Examples:
      | cluster_size | batch_clause                        | payload                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
      | 1            |                                     | hex:3c3133343e31202d20656467652d31206f726465727320776f726b65722d61206576656e742d61205b7a403332343733207365636f6e643d22615c5d62222066697273743d2271756f74655c22736c6173685c5c225d5b6140333234373320703d22c3a9225d2068656c6c6f2022e4b896e7958c220a656e64                                                                                                                                                                                                                                                   |
      | 1            | BATCH MAX MESSAGES 10 MAX SIZE 4KiB | hex:3c3133343e31202d20656467652d31206f726465727320776f726b65722d61206576656e742d61205b7a403332343733207365636f6e643d22615c5d62222066697273743d2271756f74655c22736c6173685c5c225d5b6140333234373320703d22c3a9225d205b223c3133343e31202d20656467652d31206f726465727320776f726b65722d61206576656e742d61205b7a403332343733207365636f6e643d5c22615c5c5d625c222066697273743d5c2271756f74655c5c5c22736c6173685c5c5c5c5c225d5b6140333234373320703d5c22c3a95c225d2068656c6c6f205c22e4b896e7958c5c225c6e656e64225d |
      | 3            |                                     | hex:3c3133343e31202d20656467652d31206f726465727320776f726b65722d61206576656e742d61205b7a403332343733207365636f6e643d22615c5d62222066697273743d2271756f74655c22736c6173685c5c225d5b6140333234373320703d22c3a9225d2068656c6c6f2022e4b896e7958c220a656e64                                                                                                                                                                                                                                                   |
      | 3            | BATCH MAX MESSAGES 10 MAX SIZE 4KiB | hex:3c3133343e31202d20656467652d31206f726465727320776f726b65722d61206576656e742d61205b7a403332343733207365636f6e643d22615c5d62222066697273743d2271756f74655c22736c6173685c5c225d5b6140333234373320703d22c3a9225d205b223c3133343e31202d20656467652d31206f726465727320776f726b65722d61206576656e742d61205b7a403332343733207365636f6e643d5c22615c5c5d625c222066697273743d5c2271756f74655c5c5c22736c6173685c5c5c5c5c225d5b6140333234373320703d5c22c3a95c225d2068656c6c6f205c22e4b896e7958c5c225c6e656e64225d |
