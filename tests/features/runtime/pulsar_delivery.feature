Feature: Pulsar receipt bounds and finishing accepted records

  @pulsar_delivery_finish @emitter_finish_at_stop @deloxide_stress
  Scenario Outline: STOP waits for the records a NO ACK Pulsar emitter's producer holds
    Given Pulsar is running
    And a stallable Pulsar endpoint is configured
    And drain timeout is configured as "60s"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar topic "delivery_out_{{test_id}}" is observed
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA delivery_event ( sequence I64 );
      CREATE WIRE JSON SCHEMA delivery_wire MODE STRICT ( sequence integer );
      CREATE CODEC delivery_codec FROM WIRE JSON SCHEMA delivery_wire TO SCHEMA delivery_event;
      CREATE RELAY delivery_events SCHEMA delivery_event UNBRANCHED;
      CREATE VHOST edge pulsar-{{test_id}}.example.com;
      CREATE ENDPOINT delivery_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR delivery_source
        FROM delivery_ingress MODE ACK MAX 100 ACK TIMEOUT 60s
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING delivery_codec
        TIMESTAMP NOW
        TO delivery_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE PULSAR CLIENT pulsar_sink CONFIG {
        'addr' = '{{pulsar_stallable_addr}}'
      };
      CREATE PULSAR TOPIC EMITTER delivery_output FROM delivery_events
        TO pulsar_sink TOPIC 'delivery_out_{{test_id}}'
          MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING delivery_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    When the Pulsar endpoint pauses after its producer is ready within "30s"
    And <message_count> sequenced HTTP payloads are posted to the Pulsar emitter
    Then within "30s" DESCRIBE EMITTER "delivery_output" on the leader node contains
      """
      messages_total sent relay=delivery_events
      """
    And the last command output owner is saved as placeholder "delivery_owner"
    And within "30s" DESCRIBE EMITTER "delivery_output" on the leader node contains
      """
      messages_total sent relay=delivery_events physical_node={{delivery_owner}} total=<message_count>
      """
    When STOP runs while the stallable endpoint "pulsar" resumes after "2s"
    Then STOP completed after the stallable endpoint resumed
    And within "30s" the observed broker receives exactly <message_count> sequenced Pulsar payloads

    Examples:
      | cluster_size | message_count |
      | 1            | 1             |
      | 3            | 1             |
      | 1            | 100           |
      | 3            | 100           |

  @pulsar_long_ack_timeout
  Scenario Outline: Pulsar ACK TIMEOUT above thirty seconds survives a delayed broker receipt
    Given Pulsar is running
    And a stallable Pulsar endpoint is configured
    And drain timeout is configured as "60s"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Pulsar topic "delivery_out_{{test_id}}" is observed
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA delivery_event ( sequence I64 );
      CREATE WIRE JSON SCHEMA delivery_wire MODE STRICT ( sequence integer );
      CREATE CODEC delivery_codec FROM WIRE JSON SCHEMA delivery_wire TO SCHEMA delivery_event;
      CREATE RELAY delivery_events SCHEMA delivery_event UNBRANCHED;
      CREATE VHOST edge pulsar-{{test_id}}.example.com;
      CREATE ENDPOINT delivery_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR delivery_source
        FROM delivery_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING delivery_codec
        TIMESTAMP NOW
        TO delivery_events
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE PULSAR CLIENT pulsar_sink CONFIG {
        'addr' = '{{pulsar_stallable_addr}}'
      };
      CREATE PULSAR TOPIC EMITTER delivery_output FROM delivery_events
        TO pulsar_sink TOPIC 'delivery_out_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 45s RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING delivery_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    When the Pulsar endpoint pauses after its producer is ready within "30s"
    And this HTTP payload is posted while the Pulsar endpoint resumes after "35s"
      """
      {"sequence":1}
      """
    Then the active session observes no server error containing "delivery_output" within "1s"
    And within "30s" the observed broker receives exactly these payloads
      """
      {"sequence":1}
      """
    And within "30s" DESCRIBE EMITTER "delivery_output" on the leader node contains
      """
      messages_total sent relay=delivery_events
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

