Feature: Endpoint kinds select their Plexus heads and bodies

  Scenario Outline: Endpoint inspection preserves the kinded head
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE VHOST edge kinded-{{test_id}}.example.com;
      CREATE <kind> ENDPOINT intake ON edge PATH '/events';
      """
    When these Plexus commands are executed
      """
      SHOW CREATE ENDPOINT intake;
      """
    Then the last command output contains
      """
      CREATE <kind> ENDPOINT intake
      """
    When these Plexus commands are executed
      """
      DESCRIBE ENDPOINT intake;
      """
    Then the last command output contains
      """
      kind: <kind> ENDPOINT
      """

    Examples:
      | cluster_size | kind       |
      | 1            | HTTP       |
      | 3            | HTTP       |
      | 1            | WEBSOCKETS |
      | 3            | WEBSOCKETS |

  Scenario Outline: An HTTP endpoint body has no signaling protocol clause
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE VHOST edge kinded-{{test_id}}.example.com;
      """
    When these Plexus statements fail
      """
      CREATE HTTP ENDPOINT intake ON edge PATH '/events' WITH SIGNALING PROTOCOL handshake;
      """
    Then the last command error contains
      """
      parse error
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An ingestor reference must have its declared endpoint kind
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value I64);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value integer);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge kinded-{{test_id}}.example.com;
      CREATE <endpoint_kind> ENDPOINT intake ON edge PATH '/events';
      """
    When these Plexus statements fail
      """
      CREATE <ingestor_kind> ENDPOINT INGESTOR rejected
        FROM intake MODE NO ACK ON QUIESCE REJECT RETRY AFTER 1s
        DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      requires a <ingestor_kind> ENDPOINT
      """
    And the last command error contains
      """
      endpoint 'intake' is <endpoint_kind> ENDPOINT
      """

    Examples:
      | cluster_size | endpoint_kind | ingestor_kind |
      | 1            | HTTP          | WEBSOCKETS    |
      | 3            | HTTP          | WEBSOCKETS    |
      | 1            | WEBSOCKETS    | HTTP          |
      | 3            | WEBSOCKETS    | HTTP          |
