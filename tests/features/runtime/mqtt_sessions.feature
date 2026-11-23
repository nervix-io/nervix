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
      CREATE MQTT CLIENT proxied CONFIG { 'addr' = '{{mqtt_stallable_addr}}', 'client_id' = 'moving_{{test_id}}' };
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

  @mqtt_session_ownership
  @mqtt_source_retirement
  Scenario Outline: An altered MQTT topic whose shared session cannot be cleaned yet is retired by the replacement before it subscribes
    Given MQTT is running
    And a stallable MQTT endpoint is configured
    And a <cluster_size> node nervix cluster is started
    And an MQTT session ingestor named "events" runs with client id "unreached_{{test_id}}"
    When these Plexus statements are executed
      """
      CREATE MQTT CLIENT proxied CONFIG { 'addr' = '{{mqtt_stallable_addr}}', 'client_id' = 'unreached_{{test_id}}' };
      ALTER MQTT TOPIC INGESTOR events
        SET FROM proxied TOPIC 'session_{{test_id}}'
        SESSION PERSISTENT QOS 1 MODE NO ACK ON QUIESCE SUSPEND;
      CREATE SUBSCRIPTION session_output TO output;
      """
    And MQTT QoS 1 message is published to topic "session_{{test_id}}"
      """
      {"user_id":91}
      """
    Then the relay subscription receives a payload
      """
      "user_id":91
      """
    # The endpoint stops answering, so the alteration cannot clean the broker session the
    # replacement shares with the preceding input. The alteration still applies: the replacement
    # retires that session itself before it first subscribes, once the endpoint answers again, so
    # it never reads what the broker queued for the preceding topic.
    When the stallable endpoint "mqtt" is paused
    And these Plexus statements are executed
      """
      ALTER MQTT TOPIC INGESTOR events
        SET FROM proxied TOPIC 'replacement_{{test_id}}'
        SESSION PERSISTENT QOS 1 MODE NO ACK ON QUIESCE SUSPEND;
      """
    And MQTT QoS 2 message is published directly to broker topic "session_{{test_id}}"
      """
      {"user_id":92}
      """
    And the stallable endpoint "mqtt" resets its connections and each one ends
    And the stallable endpoint "mqtt" is resumed
    Then within "60s" DESCRIBE INGESTOR "events" on the leader node contains
      """
      ready: true
      """
    And the relay subscription does not receive a payload within "1s"
    When MQTT QoS 1 message is published to topic "replacement_{{test_id}}"
      """
      {"user_id":93}
      """
    Then the relay subscription receives a payload
      """
      "user_id":93
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @mqtt_session_ownership
  @mqtt_source_retirement
  Scenario Outline: A stopped and started domain still retires the MQTT session its altered topic owed before the replacement subscribes
    Given MQTT is running
    And a stallable MQTT endpoint is configured
    And the production sticky scheduler is configured
    And a <cluster_size> node nervix cluster is started
    And an MQTT session ingestor named "events" runs with client id "stopped_{{test_id}}"
    When these Plexus statements are executed
      """
      CREATE MQTT CLIENT proxied CONFIG { 'addr' = '{{mqtt_stallable_addr}}', 'client_id' = 'stopped_{{test_id}}' };
      ALTER MQTT TOPIC INGESTOR events
        SET FROM proxied TOPIC 'session_{{test_id}}'
        SESSION PERSISTENT QOS 1 MODE NO ACK ON QUIESCE SUSPEND;
      CREATE SUBSCRIPTION session_output TO output;
      """
    And MQTT QoS 1 message is published to topic "session_{{test_id}}"
      """
      {"user_id":101}
      """
    Then the relay subscription receives a payload
      """
      "user_id":101
      """
    # The endpoint stays paused from the alteration until the domain runs again, so nothing can
    # retire the broker session the replacement shares with the preceding topic in between. The
    # retirement the replacement owes outlives the stop, and the restarted replacement retires the
    # session before it subscribes, so it never reads what the broker queued for the preceding
    # topic.
    When the stallable endpoint "mqtt" is paused
    And these Plexus statements are executed
      """
      ALTER MQTT TOPIC INGESTOR events
        SET FROM proxied TOPIC 'replacement_{{test_id}}'
        SESSION PERSISTENT QOS 1 MODE NO ACK ON QUIESCE SUSPEND;
      """
    And MQTT QoS 2 message is published directly to broker topic "session_{{test_id}}"
      """
      {"user_id":102}
      """
    And these Plexus statements are executed
      """
      STOP;
      """
    And these Plexus statements are executed
      """
      START;
      """
    And the stallable endpoint "mqtt" resets its connections and each one ends
    And the stallable endpoint "mqtt" is resumed
    Then within "60s" DESCRIBE INGESTOR "events" on the leader node contains
      """
      ready: true
      """
    And the relay subscription does not receive a payload within "1s"
    When MQTT QoS 1 message is published to topic "replacement_{{test_id}}"
      """
      {"user_id":103}
      """
    Then the relay subscription receives a payload
      """
      "user_id":103
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @mqtt_session_ownership
  @mqtt_source_retirement
  Scenario: A relocated MQTT input retires the session its altered topic still owes before it runs on another node
    Given MQTT is running
    And a stallable MQTT endpoint is configured
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And an MQTT session ingestor named "events" runs with client id "relocated_{{test_id}}"
    When these Plexus statements are executed
      """
      CREATE MQTT CLIENT proxied CONFIG { 'addr' = '{{mqtt_stallable_addr}}', 'client_id' = 'relocated_{{test_id}}' };
      ALTER MQTT TOPIC INGESTOR events
        SET FROM proxied TOPIC 'session_{{test_id}}'
        SESSION PERSISTENT QOS 1 MODE NO ACK ON QUIESCE SUSPEND;
      CREATE SUBSCRIPTION session_output TO output;
      """
    And MQTT QoS 1 message is published to topic "session_{{test_id}}"
      """
      {"user_id":111}
      """
    Then the relay subscription receives a payload
      """
      "user_id":111
      """
    # While the endpoint is paused the alteration applies, but nothing can retire the broker session
    # the replacement shares with the preceding topic, so the node running the replacement still
    # owes that retirement when the ingestor is relocated. The node it moves to cannot know what the
    # session holds, so the relocation retires the session before it lets the instance go.
    When the stallable endpoint "mqtt" is paused
    And these Plexus statements are executed
      """
      ALTER MQTT TOPIC INGESTOR events
        SET FROM proxied TOPIC 'replacement_{{test_id}}'
        SESSION PERSISTENT QOS 1 MODE NO ACK ON QUIESCE SUSPEND;
      """
    And MQTT QoS 2 message is published directly to broker topic "session_{{test_id}}"
      """
      {"user_id":112}
      """
    And these Plexus commands are executed on the leader node
      """
      SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "ingestor" "events" is saved as placeholder "ingestor_owner"
    And a node other than placeholder "ingestor_owner" is saved as placeholder "destination"
    Given the entity swap in domain "{{domain}}" on node "{{ingestor_owner}}" pauses while replacing ingestor "events"
    When these Plexus statements begin executing in the background
      """
      RELOCATE INGESTOR events ONTO NODE {{destination}} IGNORE PREFERENCES;
      """
    Then the entity swap in domain "{{domain}}" on node "{{ingestor_owner}}" reaches its replacement pause
    # The relocation stopped the replacement, so only the relocation can retire the session now.
    When the stallable endpoint "mqtt" resets its connections and each one ends
    And the stallable endpoint "mqtt" is resumed
    And the entity swap in domain "{{domain}}" on node "{{ingestor_owner}}" is released from its replacement pause
    Then within "60s" the background Plexus execution reaches a successful outcome
    And within "30s" node "{{ingestor_owner}}" eventually reports scheduled "ingestor" "events" owner equals placeholder "destination"
    And within "60s" DESCRIBE INGESTOR "events" on the leader node contains
      """
      ready: true
      """
    And the relay subscription does not receive a payload within "1s"
    When MQTT QoS 1 message is published to topic "replacement_{{test_id}}"
      """
      {"user_id":113}
      """
    Then the relay subscription receives a payload
      """
      "user_id":113
      """
    And the relay subscription does not receive a payload within "1s"
