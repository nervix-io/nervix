Feature: MQTT ingestion
  Scenario Outline: MQTT ingestor delivers JSON payloads to a subscribed relay
    Given MQTT is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}?keep_alive=30',
          'client_id' = 'nervix-cucumber-ingestor-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_main TOPIC 'notifications_{{test_id}}' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "10s" DESCRIBE INGESTOR "mqtt_notifications" on the leader node contains
      """
      status: running
      """
    When MQTT message is published to topic "notifications_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the relay subscription receives a payload
      """
      "user_id":42
      """
    And the last relay subscription payload contains key fragment '{"user_id":42}'

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: MQTT ingestor reports transient source failures and recovers
    Given MQTT is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When ingestor "mqtt_notifications" enters fault mode
    And these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}?keep_alive=30',
          'client_id' = 'nervix-cucumber-ingestor-reconnect-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_main TOPIC 'notifications_reconnect_{{test_id}}' MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "5s" DESCRIBE INGESTOR "mqtt_notifications" on the leader node contains
      """
      transient error: ingestor fault injector failed source
      reconnect backoff: 250ms
      """
    When ingestor "mqtt_notifications" leaves fault mode
    And MQTT message is published to topic "notifications_reconnect_{{test_id}}"
      """
      {"user_id":43}
      """
    Then the relay subscription receives a payload
      """
      "user_id":43
      """
    And within "10s" DESCRIBE INGESTOR "mqtt_notifications" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: MQTT NO ACK at QoS 1 delivers QoS 1 payloads
    Given MQTT is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-ingestor-noack-parallel-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_main TOPIC 'notifications_noack_parallel_{{test_id}}' QOS 1 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "10s" DESCRIBE INGESTOR "mqtt_notifications" on the leader node contains
      """
      status: running
      """
    When MQTT QoS 1 message is published to topic "notifications_noack_parallel_{{test_id}}"
      """
      {"user_id":44}
      """
    Then the relay subscription receives a payload
      """
      "user_id":44
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: MQTT instances report fixed client id conflicts
    Given MQTT is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-ingestor-fixed-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_main TOPIC 'notifications_client_conflict_{{test_id}}' INSTANCES 2 SESSION PERSISTENT QOS 1 MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 100ms ACK TIMEOUT 2s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    Then within "10s" DESCRIBE INGESTOR "mqtt_notifications" on the leader node contains
      """
      transient error: MQTT client_id 'nervix-cucumber-ingestor-fixed-{{test_id}}' is shared by 2 instances; use {{instance}} in client_id for multi-instance MQTT ingestors
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: MQTT instances use the non-duplicating subscription model
    Given MQTT is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-ingestor-template-{{test_id}}-{{instance}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_main TOPIC 'notifications_client_template_{{test_id}}' INSTANCES 2 SESSION PERSISTENT QOS 1 MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 100ms ACK TIMEOUT 2s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "10s" DESCRIBE INGESTOR "mqtt_notifications" on the leader node contains
      """
      status: running
      """
    When 4 JSON messages with user id 47 are rapidly published to "MQTT_QOS1" input "notifications_client_template_{{test_id}}"
    Then within "10s" the relay subscription receives payloads
      """
      "user_id":47
      "user_id":47
      "user_id":47
      "user_id":47
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: MQTT start failures do not leave a silently stopped ingestor
    Given MQTT is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "invalid ack timeout 'oops'"
      """
      CREATE SCHEMA notification (
        user_id I64
      );

      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications SCHEMA notification UNBRANCHED;

      CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}'
        };

      CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_main TOPIC 'notifications_start_failure_{{test_id}}' INSTANCES 1 SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT oops RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      START;
      """
    Then the last command error contains
      """
      invalid ack timeout 'oops'
      """
    When these Plexus commands are executed
      """
      DESCRIBE INGESTOR mqtt_notifications;
      """
    Then the last command output contains
      """
      status: stopped
      """
    And the last command output contains
      """
      transient error: failed to initialize ingestor 'mqtt_notifications'
      """
    And the last command output contains
      """
      invalid ack timeout 'oops'
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario Outline: MQTT ACK SEQUENTIAL retries while an attached emitter is faulted
    Given MQTT is running
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MQTT topic "notifications_ack_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_ingress
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-ingestor-ack-seq-in-{{test_id}}'
        };
        CREATE CLIENT mqtt_out
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-ingestor-ack-seq-out-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'notifications_ack_seq_{{test_id}}' SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 500ms RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE EMITTER mqtt_forward FROM notifications TO MQTT mqtt_out TOPIC notifications_ack_out_{{test_id}} MODE QOS 0 RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec
        INHERIT ALL
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "10s" DESCRIBE INGESTOR "mqtt_notifications" on the leader node contains
      """
      status: running
      """
    When emitter "mqtt_forward" enters fault mode
    And MQTT QoS 1 message is published to topic "notifications_ack_seq_{{test_id}}"
      """
      {"user_id":45}
      """
    Then the relay subscription receives a payload
      """
      "user_id":45
      """
    And within "2s" the relay subscription receives payloads
      """
      "user_id":45
      """
    When emitter "mqtt_forward" leaves fault mode
    Then the observed broker receives a payload
      """
      {"user_id":45}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  Scenario: MQTT ACK PARALLEL instances continue after a node is drained
    Given MQTT is running
    And runtime replication is configured with replica count 1 and snapshot interval "100ms"
    And the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-ingestor-ack-parallel-{{test_id}}-{{instance}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_main TOPIC 'notifications_ack_parallel_{{test_id}}' INSTANCES 2 SESSION PERSISTENT QOS 1 MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 100ms ACK TIMEOUT 2s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "10s" DESCRIBE INGESTOR "mqtt_notifications" on the leader node contains
      """
      ready: true
      """
    When these Plexus commands are executed
      """
        DRAIN NODE node-1;
        SHOW CLUSTER STATUS;
      """
    Then the last cluster status owner for scheduled "ingestor" "mqtt_notifications" is saved as placeholder "mqtt_owner"
    And within "10s" DESCRIBE INGESTOR "mqtt_notifications" on the leader node contains
      """
      ready: true
      """
    When 4 JSON messages with user id 46 are rapidly published to "MQTT_QOS1" input "notifications_ack_parallel_{{test_id}}"
    Then within "10s" the relay subscription receives payloads
      """
      "user_id":46
      "user_id":46
      "user_id":46
      "user_id":46
      """
    And the relay subscription does not receive a payload within "1s"

  @mqtt_quiesce_buffer_acknowledgement
  Scenario Outline: MQTT publishes a buffering quiesce retains are acknowledged and replayed on release
    Given MQTT is running
    And entity gate deadline is configured as "30s"
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the entity gate for domain "{{domain}}" pauses after engagement
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE CODEC notification_codec_v2
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-ingestor-buffer-<mode_tag>-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_main TOPIC 'notifications_buffer_<mode_tag>_{{test_id}}' <mode>
        ON QUIESCE BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "10s" DESCRIBE INGESTOR "mqtt_notifications" on the leader node contains
      """
      ready: true
      """
    When MQTT QoS 1 message is published to topic "notifications_buffer_<mode_tag>_{{test_id}}"
      """
      {"user_id":51}
      """
    Then the relay subscription receives a payload
      """
      "user_id":51
      """
    When these Plexus commands begin executing in the background
      """
      ALTER MQTT TOPIC INGESTOR mqtt_notifications SET DECODE USING notification_codec_v2;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    And within "5s" DESCRIBE INGESTOR "mqtt_notifications" on the leader node contains
      """
      quiesce state: entity hold
      """
    When MQTT QoS 1 message is published to topic "notifications_buffer_<mode_tag>_{{test_id}}"
      """
      {"user_id":52}
      """
    And MQTT QoS 1 message is published to topic "notifications_buffer_<mode_tag>_{{test_id}}"
      """
      {"user_id":53}
      """
    Then within "5s" DESCRIBE INGESTOR "mqtt_notifications" on the leader node contains
      """
      nervix_ingestor_quiesce_buffered_records: 2
      """
    When the entity gate pause for domain "{{domain}}" is released
    Then the background Plexus execution succeeds
    And within "10s" the relay subscription receives payloads
      """
      "user_id":52
      "user_id":53
      """
    And the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count | mode_tag | mode                                                                                             |
      | 1            | 0             | noack    | MODE NO ACK                                                                                      |
      | 3            | 1             | noack    | MODE NO ACK                                                                                      |
      | 1            | 0             | ack      | SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms |
      | 3            | 1             | ack      | SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms |

  @mqtt_parallel_gather_at_quiesce @deloxide_stress
  Scenario Outline: MQTT ACK PARALLEL publishes a quiesce change meets half gathered reach the relay without waiting for the batch timeout
    Given MQTT is running
    And entity gate deadline is configured as "30s"
    And runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the entity gate for domain "{{domain}}" pauses after engagement
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE CODEC notification_codec_v2
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-ingestor-gather-{{test_id}}'
        };
      CREATE MQTT TOPIC INGESTOR mqtt_gather
        FROM mqtt_main TOPIC 'gather_{{test_id}}' SESSION PERSISTENT QOS 1 MODE ACK PARALLEL MAX 8 BATCH TIMEOUT 10m ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms
        ON QUIESCE BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then within "10s" DESCRIBE INGESTOR "mqtt_gather" on the leader node contains
      """
      ready: true
      """
    # Each batch holds up to eight publishes and waits up to ten minutes for the rest, so every read
    # below is still gathering when the hold engages or ends.
    When MQTT QoS 1 message is published to topic "gather_{{test_id}}"
      """
      {"user_id":1}
      """
    And MQTT QoS 1 message is published to topic "gather_{{test_id}}"
      """
      {"user_id":2}
      """
    And these Plexus commands begin executing in the background
      """
      ALTER MQTT TOPIC INGESTOR mqtt_gather SET DECODE USING notification_codec_v2;
      """
    Then the entity gate pause for domain "{{domain}}" is reached
    And within "5s" DESCRIBE INGESTOR "mqtt_gather" on the leader node contains
      """
      quiesce state: entity hold
      """
    When MQTT QoS 1 message is published to topic "gather_{{test_id}}"
      """
      {"user_id":3}
      """
    And MQTT QoS 1 message is published to topic "gather_{{test_id}}"
      """
      {"user_id":4}
      """
    And the entity gate pause for domain "{{domain}}" is released
    # The alteration restarts the source under its hold. MQTT confirms an acknowledgement once its
    # client queued it, so the broker may deliver a publish the stopping source took in again to its
    # replacement: delivery is at least once, and every publish arrives.
    Then within "30s" the relay subscription receives payloads
      """
      "user_id":1
      "user_id":2
      "user_id":3
      "user_id":4
      """
    And the background Plexus execution succeeds

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 1             |

  @decode_rejection
  Scenario Outline: MQTT NO ACK drops a payload its codec refuses with its record and delivers the next one
    Given MQTT is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT mqtt_main
        TYPE MQTT
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-ingestor-poison-{{test_id}}'
        };
      CREATE MQTT TOPIC INGESTOR mqtt_poison
        FROM mqtt_main TOPIC 'poison_{{test_id}}' QOS 1 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then within "10s" DESCRIBE INGESTOR "mqtt_poison" on the leader node contains
      """
      ready: true
      """
    When these MQTT messages are rapidly published to topic "poison_{{test_id}}"
      """
      {"user_id":"one"}
      {"user_id":2}
      """
    Then within "30s" the relay subscription receives a payload
      """
      "user_id":2
      """
    And within "30s" the active session observes a server error containing
      """
      ingestor 'mqtt_poison' general error in domain '{{domain}}': decode rejection
      """
    And the last server error contains
      """
      codec 'notification_codec' refused the payload: codec 'notification_codec' failed to parse field 'user_id'
      """
    And within "30s" DESCRIBE INGESTOR "mqtt_poison" on the leader node contains
      """
      nervix_ingestor_decode_rejections_total: 1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
