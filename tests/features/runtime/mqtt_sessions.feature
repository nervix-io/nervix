Feature: MQTT sessions belong to their declared ingestor
  @mqtt_session_ownership
  Scenario Outline: MQTT ingestors with the same name in different domains own different broker sessions
    Given MQTT is running
    And a <cluster_size> node nervix cluster is started
    And the active domain is saved as placeholder "first_domain"
    And an MQTT session ingestor named "events" runs with 1 instances
    And the active domain is "second_{{test_id}}"
    And an MQTT session ingestor named "events" runs with 1 instances
    Then MQTT ingestors in domains "{{first_domain}}" and "second_{{test_id}}" both deliver their input
    And the MQTT broker retains session "{{first_domain}}~events"
    And the MQTT broker retains session "second_{{test_id}}~events"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @mqtt_session_ownership
  Scenario Outline: MQTT instances receive distinct default broker sessions
    Given MQTT is running
    And a <cluster_size> node nervix cluster is started
    And an MQTT session ingestor named "events" runs with 2 instances
    Then the MQTT broker retains session "{{domain}}~events~0"
    And the MQTT broker retains session "{{domain}}~events~1"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @mqtt_session_ownership
  @mqtt_source_retirement @deloxide_stress
  Scenario Outline: Dropping an MQTT ingestor releases its persistent broker session
    Given MQTT is running
    And a <cluster_size> node nervix cluster is started
    And an MQTT session ingestor named "events" runs with client id "owned_{{test_id}}"
    Then the MQTT broker retains session "owned_{{test_id}}"
    When these Plexus statements are executed
      """
      <before_drop>
      """
    Then the MQTT broker retains session "owned_{{test_id}}"
    When these Plexus statements are executed
      """
      DROP INGESTOR events;
      """
    Then the MQTT broker has released session "owned_{{test_id}}"

    Examples:
      | cluster_size | before_drop      |
      | 1            | DESCRIBE DOMAIN; |
      | 3            | DESCRIBE DOMAIN; |
      | 1            | STOP;            |
      | 3            | STOP;            |

  @mqtt_session_ownership
  @mqtt_source_retirement @deloxide_stress
  Scenario Outline: An altered MQTT topic replaces the persistent session subscription
    Given MQTT is running
    And a <cluster_size> node nervix cluster is started
    And an MQTT session ingestor named "events" runs with client id "altered_{{test_id}}"
    When these Plexus statements are executed
      """
      ALTER MQTT TOPIC INGESTOR events
        SET FROM broker TOPIC 'replacement_{{test_id}}'
        SESSION PERSISTENT QOS 1 MODE NO ACK ON QUIESCE SUSPEND;
      CREATE SUBSCRIPTION session_output TO output;
      """
    Then within "30s" DESCRIBE INGESTOR "events" on the leader node contains
      """
      ready: true
      """
    When MQTT QoS 1 message is published to topic "replacement_{{test_id}}"
      """
      {"user_id":51}
      """
    Then the relay subscription receives a payload
      """
      "user_id":51
      """
    When MQTT QoS 1 message is published to topic "session_{{test_id}}"
      """
      {"user_id":52}
      """
    And MQTT QoS 1 message is published to topic "replacement_{{test_id}}"
      """
      {"user_id":53}
      """
    Then the relay subscription receives a payload
      """
      "user_id":53
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @mqtt_session_ownership
  @mqtt_source_retirement @deloxide_stress
  Scenario Outline: An MQTT input moves away from an unavailable endpoint without retaining its topic
    Given MQTT is running
    And a stallable MQTT endpoint is configured
    And a <cluster_size> node nervix cluster is started
    And an MQTT session ingestor named "events" runs with client id "moving_{{test_id}}"
    When these Plexus statements are executed
      """
      CREATE CLIENT proxied TYPE MQTT CONFIG { 'addr' = '{{mqtt_stallable_addr}}', 'client_id' = 'moving_{{test_id}}' };
      ALTER MQTT TOPIC INGESTOR events
        SET FROM proxied TOPIC 'session_{{test_id}}'
        SESSION PERSISTENT QOS 1 MODE NO ACK ON QUIESCE SUSPEND;
      CREATE SUBSCRIPTION session_output TO output;
      """
    And MQTT QoS 1 message is published to topic "session_{{test_id}}"
      """
      {"user_id":61}
      """
    Then the relay subscription receives a payload
      """
      "user_id":61
      """
    When the stallable endpoint "mqtt" is paused
    And these Plexus statements are executed
      """
      ALTER MQTT TOPIC INGESTOR events
        SET FROM broker TOPIC 'moved_{{test_id}}'
        SESSION PERSISTENT QOS 1 MODE NO ACK ON QUIESCE SUSPEND;
      """
    And MQTT QoS 1 message is published to topic "moved_{{test_id}}"
      """
      {"user_id":62}
      """
    Then the relay subscription receives a payload
      """
      "user_id":62
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
