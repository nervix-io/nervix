Feature: Connector entities share their declared presentation

  @connector_presentation
  Scenario Outline: DESCRIBE reports the effective declared options of a stopped ingestor
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA event (seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE PROMETHEUS CLIENT reader CONFIG { 'addr' = 'http://127.0.0.1:9090', 'timeout_ms' = '1000' };
      CREATE PROMETHEUS QUERY INGESTOR incoming FROM reader QUERY 'up' <body_limit> EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      DESCRIBE INGESTOR incoming;
      """
    Then the last command output contains
      """
      from: PROMETHEUS QUERY reader
      options: QUERY 'up' MAX BODY SIZE <effective_limit> EVERY 1s
      mode: NO ACK
      quiesce: SUSPEND
      """

    Examples:
      | cluster_size | body_limit         | effective_limit |
      | 1            |                    | 16MiB           |
      | 3            |                    | 16MiB           |
      | 1            | MAX BODY SIZE 2MiB | 2MiB            |
      | 3            | MAX BODY SIZE 2MiB | 2MiB            |

  @connector_presentation
  Scenario Outline: DESCRIBE reports an emitter's entity and mode in the shared order
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA event (seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE KAFKA TOPIC EMITTER outgoing FROM events TO broker TOPIC 'Orders'
        MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      DESCRIBE EMITTER outgoing;
      """
    Then the last command output contains
      """
      to: KAFKA TOPIC broker TOPIC 'Orders'
      options: none
      mode: NO ACK RETRY POLICY BACKOFF 100ms MAX 1s
      quiesce: none
      envelope: none
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @connector_presentation_listing
  Scenario Outline: SHOW EMITTERS names each configured connector and entity
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA event (seq I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (seq integer);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT broker CONFIG { 'bootstrap.servers' = '127.0.0.1:9092' };
      CREATE KAFKA TOPIC EMITTER outgoing FROM events TO broker TOPIC 'Orders'
        MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec
        INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW EMITTERS;
      """
    Then the last command output contains
      """
      outgoing to=KAFKA TOPIC
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @connector_presentation_listing
  Scenario Outline: SHOW EMITTERS reports an empty selected domain
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      SHOW EMITTERS;
      """
    Then the last command output contains
      """
      no emitters
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
