Feature: MQTT client credentials, identity, keep-alive and admitted publish size

  An MQTT client declares the username and password its connections authenticate with, the identity
  template each connection renders, the keep-alive it asks its broker for, and the largest publish
  its sources admit. The test broker has one listener that authenticates every client beside the
  listeners that admit clients without credentials, and it routes publishes across all of them.

  Scenario Outline: MQTT sources and emitters authenticate with their client's username and password
    Given MQTT is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MQTT topic "auth_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MQTT CLIENT mqtt_in CONFIG {
        'addr' = '{{mqtt_auth_addr}}',
        'client_id' = 'nervix-auth-in-{{test_id}}',
        'username' = '{{mqtt_auth_username}}',
        'password' = '{{mqtt_auth_password}}'
      };
      CREATE MQTT CLIENT mqtt_out CONFIG {
        'addr' = '{{mqtt_auth_addr}}',
        'client_id' = 'nervix-auth-out-{{test_id}}',
        'username' = '{{mqtt_auth_username}}',
        'password' = '{{mqtt_auth_password}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_in
      FROM mqtt_in TOPIC 'auth_in_{{test_id}}' SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
      ON QUIESCE SUSPEND DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE ATTACHED MQTT TOPIC EMITTER mqtt_out FROM notifications
      TO mqtt_out TOPIC 'auth_out_{{test_id}}' MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 200ms MAX 1s
      ENCODE USING notification_codec
      INHERIT ALL
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "mqtt_in" on the leader node contains
      """
      ready: true
      """
    And within "30s" the MQTT broker reports client "nervix-auth-in-{{test_id}}" connected as user "{{mqtt_auth_username}}"
    And within "30s" the MQTT broker reports client "nervix-auth-out-{{test_id}}" connected as user "{{mqtt_auth_username}}"
    When MQTT QoS 1 message is published to topic "auth_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @mqtt_client_credentials_refused @deloxide_stress
  Scenario Outline: The broker refuses an MQTT client without credentials, and its source and emitter report the refusal
    Given MQTT is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MQTT CLIENT anonymous CONFIG {
        'addr' = '{{mqtt_auth_addr}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_in
      FROM anonymous TOPIC 'anonymous_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE ATTACHED MQTT TOPIC EMITTER mqtt_out FROM notifications
      TO anonymous TOPIC 'anonymous_out_{{test_id}}' MODE QOS 0 RETRY POLICY BACKOFF 200ms MAX 1s
      ENCODE USING notification_codec
      INHERIT ALL
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "mqtt_in" on the leader node contains
      """
      Connection refused, return code: `BadUserNamePassword`
      """
    And within "30s" DESCRIBE EMITTER "mqtt_out" on the leader node contains
      """
      Connection refused, return code: `BadUserNamePassword`
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An MQTT source admits a publish above 10 KiB once its client declares a larger max_packet_size
    Given MQTT is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA padded (padding STRING);
      CREATE WIRE JSON SCHEMA padded_wire MODE STRICT (padding string);
      CREATE CODEC padded_codec FROM WIRE JSON SCHEMA padded_wire TO SCHEMA padded;
      CREATE RELAY padded_records SCHEMA padded UNBRANCHED;
      CREATE MQTT CLIENT mqtt_main CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-packet-size-{{test_id}}',
        'max_packet_size' = '32768'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_padded
      FROM mqtt_main TOPIC 'padded_{{test_id}}' SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 100ms MAX 200ms
      ON QUIESCE DROP DECODE USING padded_codec
      TO padded_records
      INHERIT ALL
      UNBRANCHED
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION padded_subscription TO padded_records;
      START;
      """
    Then within "10s" DESCRIBE INGESTOR "mqtt_padded" on the leader node contains
      """
      ready: true
      """
    When 1 MQTT QoS 1 messages of 20480 bytes are published to topic "padded_{{test_id}}"
    Then within "30s" the relay subscription receives a payload
      """
      "padding":"xxxxxxxxxxxxxxxx
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An MQTT client's identity template gives each source instance and the emitter the identity it renders
    Given MQTT is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And MQTT topic "identity_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MQTT CLIENT mqtt_in CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-identity-in-{{test_id}}-{{ instance }}'
      };
      CREATE MQTT CLIENT mqtt_out CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-identity-out-{{test_id}}-{{instance}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_in
      FROM mqtt_in TOPIC 'identity_in_{{test_id}}' INSTANCES 2 SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE ATTACHED MQTT TOPIC EMITTER mqtt_out FROM notifications
      TO mqtt_out TOPIC 'identity_out_{{test_id}}' MODE QOS 1 ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 200ms MAX 1s
      ENCODE USING notification_codec
      INHERIT ALL
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" the MQTT broker reports client "nervix-identity-in-{{test_id}}-0" connected
    And within "30s" the MQTT broker reports client "nervix-identity-in-{{test_id}}-1" connected
    And within "30s" the MQTT broker reports client "nervix-identity-out-{{test_id}}-0" connected
    When MQTT QoS 1 message is published to topic "identity_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An MQTT client without a client_id connects every source instance and the emitter under its domain-qualified default identity
    Given MQTT is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MQTT CLIENT mqtt_main CONFIG {
        'addr' = '{{mqtt_addr}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_in
      FROM mqtt_main TOPIC 'default_identity_in_{{test_id}}' INSTANCES 2 SESSION CLEAN QOS 0 MODE NO ACK
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE ATTACHED MQTT TOPIC EMITTER mqtt_out FROM notifications
      TO mqtt_main TOPIC 'default_identity_out_{{test_id}}' MODE QOS 0 RETRY POLICY BACKOFF 200ms MAX 1s
      ENCODE USING notification_codec
      INHERIT ALL
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" the MQTT broker reports client "{{domain}}~mqtt_in~0" connected
    And within "30s" the MQTT broker reports client "{{domain}}~mqtt_in~1" connected
    And within "30s" the MQTT broker reports client "{{domain}}~mqtt_out" connected

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: MQTT sources and emitters ask their broker for the keep-alive their client declares
    Given MQTT is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE MQTT CLIENT mqtt_in CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-keep-alive-in-{{test_id}}',
        'keep_alive_ms' = '7000'
      };
      CREATE MQTT CLIENT mqtt_out CONFIG {
        'addr' = '{{mqtt_addr}}',
        'client_id' = 'nervix-keep-alive-out-{{test_id}}'
      };
      CREATE MQTT TOPIC INGESTOR mqtt_in
      FROM mqtt_in TOPIC 'keep_alive_in_{{test_id}}' SESSION CLEAN QOS 0 MODE NO ACK
      ON QUIESCE DROP DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE ATTACHED MQTT TOPIC EMITTER mqtt_out FROM notifications
      TO mqtt_out TOPIC 'keep_alive_out_{{test_id}}' MODE QOS 0 RETRY POLICY BACKOFF 200ms MAX 1s
      ENCODE USING notification_codec
      INHERIT ALL
      FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" the MQTT broker reports client "nervix-keep-alive-in-{{test_id}}" connected with a keep-alive of 7 seconds
    And within "30s" the MQTT broker reports client "nervix-keep-alive-out-{{test_id}}" connected with a keep-alive of 60 seconds

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
