Feature: MongoDB emission
  @domain_execution_time
  Scenario Outline: MongoDB emitter inserts mapped documents from a relay
    Given MQTT is running
    And MongoDB is running
    Given runtime replication is configured with replica count <replicas> and snapshot interval "100ms"
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 100ms;
      """
    And MongoDB collection "notifications_mongodb_out_{{test_id}}" exists
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE MQTT CLIENT mqtt_ingress
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-mongodb-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'mongodb_notifications_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TIMESTAMP NOW
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE MONGODB CLIENT mongodb_client
        POOL SIZE MIN 2 MAX 8
        CONFIG {
          'addr' = '{{mongodb_addr}}',
          'database' = 'nervix'
        };
        CREATE MONGODB COLLECTION EMITTER to_mongodb FROM notifications TO mongodb_client COLLECTION 'notifications_mongodb_out_{{test_id}}' MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "mongodb_user_id" = input.user_id, "mongodb_small" = 42 AS I32, "mongodb_date" = '2000-01-01T00:00:00Z' AS DATETIME, "mongodb_now" = NOW() AS STRING, "mongodb_action" = CASE WHEN NOW() < ('2001-01-01T00:00:00Z' AS DATETIME) THEN LOWER(input.action) ELSE 'physical-time' END } BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START AT '2000-01-01T00:00:00Z' TIME RATE 1.0;
      """
    And emitter "to_mongodb" enters stall mode
    Then within "10s" repeatedly publishing MQTT message to topic "mongodb_notifications_in_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42,"action":"OPEN"}
      """
    And within "5s" DESCRIBE EMITTER "to_mongodb" on the leader node contains
      """
      transient error: fault injector stalled emitter publish
      """
    And the last command output contains
      """
      reconnect backoff:
      """
    And emitter "to_mongodb" leaves fault mode
    And the MongoDB collection eventually contains a document
      """
      {"mongodb_user_id":42,"mongodb_action":"open"}
      """
    And the MongoDB collection eventually matches this BSON filter
      """
      {"mongodb_user_id":{"$type":"long"},"mongodb_small":{"$type":"int"},"mongodb_date":{"$type":"date"}}
      """

    Examples:
      | nodes | replicas |
      | 1     | 0        |
      | 3     | 0        |
      | 3     | 1        |

  Scenario Outline: MongoDB emitter handles insert conflicts with <conflict_action>
    Given MQTT is running
    And MongoDB is running
    Given runtime replication is configured with replica count <replicas> and snapshot interval "100ms"
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MongoDB collection "notifications_mongodb_conflict_{{test_id}}" with unique user id exists
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
        CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
      );
        CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
        CREATE IF NOT EXISTS SCHEMA user_id_branch ( user_id I64 );
        CREATE IF NOT EXISTS BRANCH by_mqtt_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_mqtt_notifications;
        CREATE MQTT CLIENT mqtt_ingress
        CONFIG {
          'addr' = '{{mqtt_addr}}',
          'client_id' = 'nervix-cucumber-mongodb-conflict-{{test_id}}'
        };
        CREATE MQTT TOPIC INGESTOR mqtt_notifications
        FROM mqtt_ingress TOPIC 'mongodb_conflict_notifications_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
        ON QUIESCE DROP DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_mqtt_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE MONGODB CLIENT mongodb_client
        POOL SIZE MIN 2 MAX 8
        CONFIG {
          'addr' = '{{mongodb_addr}}',
          'database' = 'nervix'
        };
        CREATE MONGODB COLLECTION EMITTER to_mongodb FROM notifications TO mongodb_client COLLECTION 'notifications_mongodb_conflict_{{test_id}}' MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "mongodb_user_id" = input.user_id, "mongodb_now" = NOW() AS STRING, "mongodb_action" = LOWER(input.action) } ON CONFLICT ("mongodb_user_id") <conflict_action>
        BATCH MAX MESSAGES 2 MAX SIZE 1MiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION notifications_subscription TO notifications;
        START;
      """
    Then within "10s" repeatedly publishing MQTT message to topic "mongodb_conflict_notifications_in_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42,"action":"OPEN"}
      """
    And within "10s" repeatedly publishing MQTT message to topic "mongodb_conflict_notifications_in_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42,"action":"CLOSE"}
      """
    And the MongoDB collection eventually contains a document
      """
      {"mongodb_user_id":42,"mongodb_action":"<expected_action>"}
      """

    Examples:
      | nodes | replicas | conflict_action | expected_action |
      | 1     | 0        | DO UPDATE       | close           |
      | 3     | 0        | DO UPDATE       | close           |
      | 3     | 1        | DO UPDATE       | close           |
      | 1     | 0        | DO NOTHING      | open            |
      | 3     | 0        | DO NOTHING      | open            |
      | 3     | 1        | DO NOTHING      | open            |

  @database_emitter_modes @poison_isolation
  Scenario Outline: MongoDB poison isolation delivers healthy documents and routes only the rejected record
    Given MQTT is running
    And MongoDB is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MongoDB collection "poison_mongodb_{{test_id}}" rejecting poison actions exists
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
      CREATE SCHEMA emitter_error (
        error_code STRING,
        source_user_id I64,
        source_action STRING
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
      );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE RELAY emitter_errors SCHEMA emitter_error UNBRANCHED;
      CREATE MQTT CLIENT mqtt_ingress
      CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-mongodb-poison-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_notifications
      FROM mqtt_ingress TOPIC 'mongodb_poison_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE MONGODB CLIENT mongodb_client
      POOL SIZE MIN 2 MAX 8
      CONFIG {
        'addr' = '{{mongodb_addr}}',
        'database' = 'nervix'
      };
      CREATE MONGODB COLLECTION EMITTER to_mongodb
      FROM notifications
      TO mongodb_client COLLECTION 'poison_mongodb_{{test_id}}'
      MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s VALUES {
        "mongodb_user_id" = input.user_id,
        "mongodb_now" = NOW() AS STRING,
        "mongodb_action" = LOWER(input.action)
      } BATCH MAX MESSAGES 10 MAX SIZE 1MiB
      FLUSH EACH 2s MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR SEND TO emitter_errors
      SET error_code = error.code,
          source_user_id = input.user_id,
          source_action = input.action
      ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION emitter_errors_subscription TO emitter_errors;
      START;
      """
    When these MQTT messages are rapidly published to topic "mongodb_poison_in_{{test_id}}"
      """
      {"user_id":1,"action":"HEALTHY_A"}
      {"user_id":2,"action":"POISON"}
      {"user_id":3,"action":"HEALTHY_B"}
      """
    Then within "10s" the relay subscription receives a payload
      """
      "source_action":"POISON","source_user_id":2
      """
    And the relay subscription does not receive a payload within "1s"
    And the MongoDB collection eventually contains a document
      """
      {"mongodb_user_id":1,"mongodb_action":"healthy_a"}
      """
    And the MongoDB collection eventually contains a document
      """
      {"mongodb_user_id":3,"mongodb_action":"healthy_b"}
      """
    And the MongoDB collection eventually contains exactly 2 documents

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @database_emitter_modes @max_batch @mongodb_max_batch
  Scenario Outline: MongoDB BATCH MAX MESSAGES splits one oversized flush into multiple inserts
    Given MQTT is running
    And MongoDB is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MongoDB collection "batch_mongodb_{{test_id}}" recording insert command sizes exists
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MQTT CLIENT mqtt_ingress CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-mongodb-batch-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_notifications
      FROM mqtt_ingress TOPIC 'mongodb_batch_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE MONGODB CLIENT mongodb_client POOL SIZE MIN 2 MAX 8 CONFIG {
        'addr' = '{{mongodb_addr}}',
        'database' = 'nervix'
      };
      CREATE MONGODB COLLECTION EMITTER to_mongodb
      FROM notifications
      TO mongodb_client COLLECTION 'batch_mongodb_{{test_id}}'
      MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s VALUES { "mongodb_user_id" = input.user_id } BATCH MAX MESSAGES 2 MAX SIZE 1MiB
      FLUSH EACH 2s MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    When 5 JSON messages with user id 42 are rapidly published to "MQTT" input "mongodb_batch_in_{{test_id}}"
    Then the MongoDB collection eventually contains 5 documents across at least 3 inserts of at most 2 documents

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @database_emitter_modes @poison_isolation
  Scenario Outline: MongoDB rejects an unsigned value with no BSON integer instead of writing null
    Given MQTT is running
    And MongoDB is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MongoDB collection "range_mongodb_{{test_id}}" exists
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id U64 OPTIONAL,
        action STRING
      );
      CREATE SCHEMA emitter_error (
        error_code STRING,
        error_operation STRING,
        error_message STRING,
        affected_fields <affected_fields_type>,
        source_action STRING
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer OPTIONAL,
        action string
      );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE RELAY emitter_errors SCHEMA emitter_error UNBRANCHED;
      CREATE MQTT CLIENT mqtt_ingress
      CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-mongodb-range-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_notifications
      FROM mqtt_ingress TOPIC 'mongodb_range_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE MONGODB CLIENT mongodb_client
      POOL SIZE MIN 2 MAX 8
      CONFIG {
        'addr' = '{{mongodb_addr}}',
        'database' = 'nervix'
      };
      CREATE MONGODB COLLECTION EMITTER to_mongodb
      FROM notifications
      TO mongodb_client COLLECTION 'range_mongodb_{{test_id}}'
      MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s VALUES {
        "mongodb_user_id" = input.user_id,
        "mongodb_action" = LOWER(input.action)
      }
      <conflict_clause> BATCH MAX MESSAGES 10 MAX SIZE 1MiB
      FLUSH EACH 2s MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR SEND TO emitter_errors
      SET error_code = error.code,
          error_operation = error.operation,
          error_message = error.message,
          affected_fields = error.fields,
          source_action = input.action
      ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION emitter_errors_subscription TO emitter_errors;
      START;
      """
    When these MQTT messages are rapidly published to topic "mongodb_range_in_{{test_id}}"
      """
      {"user_id":42,"action":"REPRESENTABLE"}
      {"user_id":18446744073709551615,"action":"OUT_OF_RANGE"}
      {"user_id":null,"action":"GENUINE_NULL"}
      """
    Then within "10s" the relay subscription receives a payload
      """
      "source_action":"OUT_OF_RANGE"
      """
    And the last relay subscription payload contains
      """
      "affected_fields":["mongodb.mongodb_user_id"],"error_code":"validation"
      """
    And the last relay subscription payload contains
      """
      "error_message":"MongoDB VALUES field 'mongodb_user_id' holds an unsigned integer above the BSON signed 64-bit range","error_operation":"values"
      """
    And the last relay subscription payload does not contain "18446744073709551615"
    And the relay subscription does not receive a payload within "1s"
    And the MongoDB collection eventually contains a document
      """
      {"mongodb_user_id":42,"mongodb_action":"representable"}
      """
    And the MongoDB collection eventually contains a document
      """
      {"mongodb_user_id":null,"mongodb_action":"genuine_null"}
      """
    And the MongoDB collection eventually contains exactly 2 documents

    Examples:
      | cluster_size | conflict_clause                           | affected_fields_type |
      | 1            |                                           | VEC<STRING>          |
      | 3            |                                           | VEC<STRING>          |
      | 1            | ON CONFLICT ("mongodb_user_id") DO UPDATE | VEC<STRING>          |
      | 3            | ON CONFLICT ("mongodb_user_id") DO UPDATE | VEC<STRING>          |

  @database_emitter_modes
  @mongodb_write_concern
  Scenario Outline: A MongoDB write concern the replica set cannot satisfy keeps the documents the server wrote and retries only the rest
    Given MQTT is running
    And MongoDB replica set is running
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <nodes> node nervix cluster is started
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MongoDB replica set collection "write_concern_mongodb_{{test_id}}" <collection>
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
      );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MQTT CLIENT mqtt_ingress
      CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-cucumber-mongodb-write-concern-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_notifications
      FROM mqtt_ingress TOPIC 'mongodb_write_concern_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE MONGODB CLIENT mongodb_client
      POOL SIZE MIN 2 MAX 8
      CONFIG {
        'addr' = '{{mongodb_replica_set_addr}}&w=2',
        'database' = 'nervix'
      };
      CREATE MONGODB COLLECTION EMITTER to_mongodb
      FROM notifications
      TO mongodb_client COLLECTION 'write_concern_mongodb_{{test_id}}'
      MODE ACK RETRY POLICY BACKOFF 1s MAX 2s
      VALUES {
        "mongodb_user_id" = input.user_id,
        "mongodb_action" = LOWER(input.action)
      }
      <conflict_clause>
      BATCH MAX MESSAGES 2 MAX SIZE 1MiB
      FLUSH EACH 2s MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then within "20s" DESCRIBE EMITTER "to_mongodb" on the leader node contains
      """
      status: OK
      """
    And the last command output owner is saved as placeholder "write_concern_owner"
    When client "write_concern_observer" connects to node "{{write_concern_owner}}"
    When these MQTT messages are rapidly published to topic "mongodb_write_concern_in_{{test_id}}"
      """
      {"user_id":1,"action":"FIRST"}
      {"user_id":2,"action":"SECOND"}
      {"user_id":3,"action":"THIRD"}
      {"user_id":4,"action":"FOURTH"}
      {"user_id":5,"action":"FIFTH"}
      {"user_id":6,"action":"SIXTH"}
      """
    Then the MongoDB collection eventually holds exactly these documents
      """
      {"mongodb_user_id":1,"mongodb_action":"first"}
      {"mongodb_user_id":2,"mongodb_action":"second"}
      {"mongodb_user_id":3,"mongodb_action":"third"}
      {"mongodb_user_id":4,"mongodb_action":"fourth"}
      {"mongodb_user_id":5,"mongodb_action":"fifth"}
      {"mongodb_user_id":6,"mongodb_action":"sixth"}
      """
    When physical time passes for "3s"
    Then within "20s" DESCRIBE EMITTER "to_mongodb" on the leader node contains
      """
      status: OK
      """
    And within "20s" client "write_concern_observer" observes a server error containing
      """
      MongoDB <write_operation> write concern failed with code 100 (UnsatisfiableWriteConcern)
      """

    Examples:
      | nodes | collection                 | conflict_clause                           | write_operation |
      | 1     | exists                     |                                           | insert_many     |
      | 3     | exists                     |                                           | insert_many     |
      | 1     | with unique user id exists | ON CONFLICT ("mongodb_user_id") DO UPDATE | bulk_write      |
      | 3     | with unique user id exists | ON CONFLICT ("mongodb_user_id") DO UPDATE | bulk_write      |
