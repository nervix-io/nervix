Feature: MQTT refusals identify the token and its legal session or QoS

  Scenario Outline: MQTT ingestor QoS refusals explain the supplied value on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "MQTT ingestor QOS must be 0, 1 or 2, found <qos>"
      """
      CREATE MQTT TOPIC INGESTOR incoming FROM broker TOPIC 'events'
        SESSION PERSISTENT QOS <qos> MODE NO ACK ON QUIESCE DROP
        DECODE USING event_codec TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes | qos |
      | 1     | 3   |
      | 3     | 3   |
      | 1     | 10  |
      | 3     | 10  |

  Scenario Outline: MQTT emitter QoS refusals explain the supplied value on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "MQTT emitter QOS must be 0, 1, or 2, found <qos>"
      """
      CREATE MQTT TOPIC EMITTER outgoing FROM events TO broker TOPIC 'events'
        MODE QOS <qos> RETRY POLICY BACKOFF 10ms MAX 1s ENCODE USING event_codec
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes | qos |
      | 1     | 3   |
      | 3     | 3   |

  Scenario Outline: MQTT SUSPEND refusals explain the required session on <nodes> nodes
    Given a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements fail with "MQTT ON QUIESCE SUSPEND requires SESSION PERSISTENT and QOS 1 or 2"
      """
      CREATE MQTT TOPIC INGESTOR incoming FROM broker TOPIC 'events'
        SESSION <session> QOS <qos> MODE NO ACK ON QUIESCE SUSPEND
        DECODE USING event_codec TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | nodes | session    | qos |
      | 1     | CLEAN      | 0   |
      | 3     | CLEAN      | 0   |
      | 1     | CLEAN      | 1   |
      | 3     | CLEAN      | 1   |
      | 1     | CLEAN      | 2   |
      | 3     | CLEAN      | 2   |
      | 1     | PERSISTENT | 0   |
      | 3     | PERSISTENT | 0   |
