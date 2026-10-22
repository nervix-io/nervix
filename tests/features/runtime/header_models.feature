Feature: Header models
  Every entity whose messages carry transport headers declares a header model for each side: what
  a repeated name keeps, the order names travel in, and how names compare. DESCRIBE shows each
  side's model. Where a sink keeps one value per header name, an emitter that writes one literal
  name twice is refused when its statement is validated, because the later write would replace the
  earlier one on every record. No broker is involved: nothing here starts the domain.

  @header_models
  Scenario Outline: A repeated literal header name is refused where the sink keeps one value per name
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA event (event_id STRING, tenant STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (event_id string, tenant string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE CLIENT pulsar_main TYPE PULSAR CONFIG { 'addr' = 'pulsar://127.0.0.1:6650' };
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = 'http://127.0.0.1:19080', 'timeout_ms' = 5000
      };
      """
    When these Plexus statements fail with "emitter 'repeated' in domain '{{domain}}' writes header 'route' at INVOKE #1 and again at #3, but PULSAR TOPIC emitters keep one value per header name, so the later write would replace the earlier one on every record"
      """
      CREATE EMITTER repeated FROM outgoing
        TO PULSAR pulsar_main TOPIC events_out
          MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING event_codec
        INHERIT ALL
        INVOKE write_header('route', 'a'),
               write_header('tenant', input.tenant),
               write_header('route', 'b')
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements fail with "writes header 'x-route' at INVOKE #1 and again at #2, but HTTP REQUEST emitters keep one value per header name"
      """
      CREATE EMITTER repeated FROM outgoing
        TO HTTP api METHOD 'POST' PATH '/events'
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          WITHOUT BODY
        INVOKE write_header('X-Route', 'a'),
               write_header('x-route', 'b')
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE EMITTER repeated FROM outgoing
        TO KAFKA kafka_main TOPIC events_out
          MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING event_codec
        INHERIT ALL
        INVOKE write_header('route', 'a'),
               write_header('route', 'b')
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      DESCRIBE EMITTER repeated;
      """
    Then the last command output contains
      """
      headers: ordered_repeats, exact names
      """
    When these Plexus statements fail with "writes header 'route' at INVOKE #1 and again at #2, but PULSAR TOPIC emitters keep one value per header name"
      """
      ALTER EMITTER repeated
        SET TO PULSAR pulsar_main TOPIC events_out MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s;
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE EMITTER computed FROM outgoing
        TO PULSAR pulsar_main TOPIC events_out
          MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING event_codec
        INHERIT ALL
        INVOKE write_header(input.tenant, 'a'),
               write_header(input.tenant, 'b')
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      DESCRIBE EMITTER computed;
      """
    Then the last command output contains
      """
      headers: last_wins, exact names
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @header_models
  Scenario Outline: DESCRIBE shows the header model each side of an entity declares
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA event (event_id STRING, tenant STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (event_id string, tenant string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE VHOST edge header-models-{{test_id}}.example.com;
      CREATE ENDPOINT events_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR events_source
        FROM events_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO outgoing
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = 'http://127.0.0.1:19080', 'timeout_ms' = 5000
      };
      CREATE EMITTER requests FROM outgoing
        TO HTTP api METHOD 'POST' PATH '/events'
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          WITHOUT BODY
        INVOKE write_header('X-Tenant', input.tenant)
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT pushes TYPE ZEROMQ CONFIG {
        'addr' = 'tcp://127.0.0.1:63001', 'bind' = 'false'
      };
      CREATE EMITTER broadcasts FROM outgoing
        TO ZEROMQ pushes
          MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING event_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      DESCRIBE INGESTOR events_source;
      """
    Then the last command output contains
      """
      headers: repeats_by_name, lowercase names
      """
    When these Plexus statements are executed on the leader node
      """
      DESCRIBE EMITTER requests;
      """
    Then the last command output contains
      """
      headers: sorted_by_name, lowercase names
      """
    When these Plexus statements are executed on the leader node
      """
      DESCRIBE EMITTER broadcasts;
      """
    Then the last command output contains
      """
      headers: none
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
