@runtime_installation_retry @deloxide_stress
Feature: Committed runtime installation recovers without another configuration change
  This single-node full-cluster restart isolates installation retries from peer-driven updates.
  The only node must install its unchanged persisted revision and retain its relay ownership.

  Scenario: A restarted node retries refused installations of its committed runtime
    Given the production sticky scheduler is configured
    And Syslog UDP emission endpoint "{{syslog_emit_addr}}" is observed
    And a 1 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA retry_event (
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
      CREATE CODEC retry_codec FROM SYSLOG TO SCHEMA retry_event;
      CREATE RELAY retry_events SCHEMA retry_event UNBRANCHED;
      CREATE SYSLOG CLIENT retry_listener
        CONFIG {
          'protocol' = 'udp',
          'addr' = '{{syslog_ingest_addr}}'
        };
      CREATE SYSLOG CLIENT retry_forwarder
        CONFIG {
          'protocol' = 'udp',
          'addr' = '{{syslog_emit_addr}}'
        };
      CREATE SYSLOG LISTENER INGESTOR retry_intake
        FROM retry_listener MODE NO ACK
        ON QUIESCE SUSPEND
        DECODE USING retry_codec
        TO retry_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SYSLOG COLLECTOR EMITTER retry_forward
        FROM retry_events
        TO retry_forwarder
          MODE NO ACK RETRY POLICY BACKOFF 50ms MAX 1s
          ENCODE USING retry_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "relay" "retry_events" is saved as placeholder "retry_owner"
    And node "node-1" eventually forwards Syslog UDP message "before-runtime-refusal" at "{{syslog_ingest_addr}}" to the observed endpoint
    When node "node-1" is stopped
    Given the next 8 runtime installations on node "node-1" are refused
    When node "node-1" is started without waiting for it to catch up
    Then node "node-1" eventually forwards Syslog UDP message "after-runtime-refusal" at "{{syslog_ingest_addr}}" to the observed endpoint
    And all runtime installation refusals on node "node-1" were delivered
    And within "30s" node "node-1" eventually reports scheduled "relay" "retry_events" owner equals placeholder "retry_owner"
