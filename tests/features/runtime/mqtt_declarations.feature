Feature: MQTT TOPIC declarations
  The MQTT topic contract keeps topic filters and publication topics exactly as written in both
  directions, with wildcards only where a subscription reads and the shared subscription left to
  Nervix. Its session, quality-of-service, delivery, quiesce and client rules apply at CREATE
  without connecting to a broker.

  Scenario Outline: MQTT TOPIC declarations preserve topic literals in both directions
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE MQTT CLIENT mqtt_main CONFIG { 'addr' = 'mqtt://127.0.0.1:1' };
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE MQTT TOPIC INGESTOR intake
        FROM mqtt_main TOPIC '<filter>' INSTANCES 2 <subscription>
        ON QUIESCE <quiesce> DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      SHOW CREATE INGESTOR intake;
      """
    Then the last command output contains
      """
      TOPIC '<filter>' INSTANCES 2 <subscription> ON QUIESCE <quiesce>
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE MQTT TOPIC EMITTER publishing FROM events
        TO mqtt_main TOPIC '<topic>'
        MODE <publishing>
        ENCODE USING event_codec INHERIT ALL
        <batch>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER publishing;
      """
    Then the last command output contains
      """
      TOPIC '<topic>'
      """
    And the last command output contains
      """
      MODE <publishing>
      """

    Examples:
      | cluster_size | filter           | subscription                                                                                                           | quiesce                                      | topic                | publishing                                                                 | batch                               |
      | 1            | Devices/+/Orders | MODE NO ACK                                                                                                            | DROP                                         | Devices/EU/Orders    | QOS 0 RETRY POLICY BACKOFF 250ms MAX 30s                                   |                                     |
      | 3            | Devices/+/Orders | MODE NO ACK                                                                                                            | DROP                                         | Devices/EU/Orders    | QOS 0 RETRY POLICY BACKOFF 250ms MAX 30s                                   |                                     |
      | 1            | telemetry/#      | SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s                         | SUSPEND                                      | telemetry/Événements | QOS 1 ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s | BATCH MAX MESSAGES 8 MAX SIZE 64KiB |
      | 3            | telemetry/#      | SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s                         | SUSPEND                                      | telemetry/Événements | QOS 1 ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s | BATCH MAX MESSAGES 8 MAX SIZE 64KiB |
      | 1            | /                | SESSION PERSISTENT QOS 1 MODE ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | sensors/Raw Topic    | QOS 2 ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s     |                                     |
      | 3            | /                | SESSION PERSISTENT QOS 1 MODE ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | sensors/Raw Topic    | QOS 2 ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s     |                                     |

  Scenario Outline: MQTT TOPIC ingestors refuse acknowledgement and suspension outside a persistent QoS 1 session
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE MQTT CLIENT mqtt_main CONFIG { 'addr' = 'mqtt://127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE MQTT TOPIC INGESTOR refused
        FROM mqtt_main TOPIC 'devices/+' <subscription>
        ON QUIESCE <quiesce> DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | subscription                                                                                                  | quiesce | expected                                  |
      | 1            | SESSION CLEAN QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s                        | DROP    | MQTT ACK modes require SESSION PERSISTENT |
      | 3            | SESSION CLEAN QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s                        | DROP    | MQTT ACK modes require SESSION PERSISTENT |
      | 1            | SESSION PERSISTENT MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | DROP    | MQTT ACK modes require QOS 1              |
      | 3            | SESSION PERSISTENT MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | DROP    | MQTT ACK modes require QOS 1              |
      | 1            | SESSION PERSISTENT QOS 2 MODE NO ACK                                                                          | DROP    | expected mqtt_qos                         |
      | 3            | SESSION PERSISTENT QOS 2 MODE NO ACK                                                                          | DROP    | expected mqtt_qos                         |
      | 1            | MODE NO ACK                                                                                                   | SUSPEND | BUFFER \| DROP                            |
      | 3            | MODE NO ACK                                                                                                   | SUSPEND | BUFFER \| DROP                            |
      | 1            | SESSION PERSISTENT QOS 0 MODE NO ACK                                                                          | SUSPEND | BUFFER \| DROP                            |
      | 3            | SESSION PERSISTENT QOS 0 MODE NO ACK                                                                          | SUSPEND | BUFFER \| DROP                            |

  Scenario Outline: MQTT TOPIC ingestors refuse a topic filter the filter rule excludes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE MQTT CLIENT mqtt_main CONFIG { 'addr' = 'mqtt://127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE MQTT TOPIC INGESTOR refused
        FROM mqtt_main TOPIC '<filter>' MODE NO ACK
        ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      invalid mqtt_topic_filter '<filter>': <reason>
      """

    Examples:
      | cluster_size | filter                 | reason                                                              |
      | 1            | devices/se+nsor        | holds the wildcard '+' inside a level instead of as the whole level |
      | 3            | devices/se+nsor        | holds the wildcard '+' inside a level instead of as the whole level |
      | 1            | devices/#/orders       | holds the wildcard '#' before its last level                        |
      | 3            | devices/#/orders       | holds the wildcard '#' before its last level                        |
      | 1            | $share/readers/devices | begins with the reserved level '$share'                             |
      | 3            | $share/readers/devices | begins with the reserved level '$share'                             |
      | 1            | $share                 | begins with the reserved level '$share'                             |
      | 3            | $share                 | begins with the reserved level '$share'                             |

  Scenario Outline: MQTT TOPIC emitters refuse a publication topic that holds a subscription wildcard
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE MQTT CLIENT mqtt_main CONFIG { 'addr' = 'mqtt://127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE MQTT TOPIC EMITTER refused FROM events
        TO mqtt_main TOPIC '<topic>'
        MODE QOS 0 RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      invalid mqtt_topic '<topic>': <reason>
      """

    Examples:
      | cluster_size | topic            | reason                           |
      | 1            | devices/+/orders | holds '+', which it may not hold |
      | 3            | devices/+/orders | holds '+', which it may not hold |
      | 1            | devices/#        | holds '#', which it may not hold |
      | 3            | devices/#        | holds '#', which it may not hold |

  Scenario Outline: MQTT TOPIC emitters acknowledge exactly the QoS levels a broker answers
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE MQTT CLIENT mqtt_main CONFIG { 'addr' = 'mqtt://127.0.0.1:1' };
      """
    When these Plexus commands fail
      """
      CREATE MQTT TOPIC EMITTER refused FROM events TO mqtt_main TOPIC 'devices/orders'
        MODE <publishing> ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | publishing                                                          | expected                         |
      | 1            | QOS 3 RETRY POLICY BACKOFF 1s MAX 30s                               | expected mqtt_qos                |
      | 3            | QOS 3 RETRY POLICY BACKOFF 1s MAX 30s                               | expected mqtt_qos                |
      | 1            | QOS 0 ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | expected RETRY POLICY, found ACK |
      | 3            | QOS 0 ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | expected RETRY POLICY, found ACK |
      | 1            | QOS 1 RETRY POLICY BACKOFF 1s MAX 30s                               | expected ACK, found RETRY        |
      | 3            | QOS 1 RETRY POLICY BACKOFF 1s MAX 30s                               | expected ACK, found RETRY        |

  Scenario Outline: MQTT client addresses name a broker by its scheme, host and port
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE MQTT CLIENT checked CONFIG {
        'addr' = '<addr>',
        'client_id' = 'Nervix-Reader'
      };
      SHOW CREATE CLIENT checked;
      """
    Then the last command output contains
      """
      'addr' = '<addr>'
      """
    And the last command output contains
      """
      'client_id' = 'Nervix-Reader'
      """

    Examples:
      | cluster_size | addr                           |
      | 1            | mqtt://127.0.0.1:1883          |
      | 3            | mqtt://127.0.0.1:1883          |
      | 1            | mqtt://broker.example.com:1883 |
      | 3            | mqtt://broker.example.com:1883 |
      | 1            | mqtt://[::1]:1883              |
      | 3            | mqtt://[::1]:1883              |
      | 1            | mqtt://127.0.0.1:1883/         |
      | 3            | mqtt://127.0.0.1:1883/         |

  Scenario Outline: MQTT client declarations refuse configuration no connection honors at CREATE
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail
      """
      CREATE MQTT CLIENT refused_client CONFIG {
        <config>
      };
      """
    Then the last command error contains
      """
      <error>
      """

    Examples:
      | cluster_size | config                                                                                             | error                                                                        |
      | 1            | 'client_id' = 'nervix'                                                                             | missing required client config key 'addr'                                    |
      | 3            | 'client_id' = 'nervix'                                                                             | missing required client config key 'addr'                                    |
      | 1            | 'addr' = 'mqtt://127.0.0.1:1883', 'adrr' = 'mqtt://127.0.0.1:1883'                                 | unknown client config key 'adrr'                                             |
      | 3            | 'addr' = 'mqtt://127.0.0.1:1883', 'adrr' = 'mqtt://127.0.0.1:1883'                                 | unknown client config key 'adrr'                                             |
      | 1            | 'addr' = 'tcp://127.0.0.1:1883'                                                                    | invalid client config key 'addr'                                             |
      | 3            | 'addr' = 'tcp://127.0.0.1:1883'                                                                    | invalid client config key 'addr'                                             |
      | 1            | 'addr' = 'mqtt://127.0.0.1'                                                                        | invalid client config key 'addr': expected URL naming only a host and a port |
      | 3            | 'addr' = 'mqtt://127.0.0.1'                                                                        | invalid client config key 'addr': expected URL naming only a host and a port |
      | 1            | 'addr' = 'mqtt://127.0.0.1:0'                                                                      | invalid client config key 'addr': expected URL naming only a host and a port |
      | 3            | 'addr' = 'mqtt://127.0.0.1:0'                                                                      | invalid client config key 'addr': expected URL naming only a host and a port |
      | 1            | 'addr' = 'mqtt://127.0.0.1:1883?keep_alive=30'                                                     | invalid client config key 'addr': expected URL naming only a host and a port |
      | 3            | 'addr' = 'mqtt://127.0.0.1:1883?keep_alive=30'                                                     | invalid client config key 'addr': expected URL naming only a host and a port |
      | 1            | 'addr' = 'mqtt://reader:secret@127.0.0.1:1883'                                                     | invalid client config key 'addr': expected URL naming only a host and a port |
      | 3            | 'addr' = 'mqtt://reader:secret@127.0.0.1:1883'                                                     | invalid client config key 'addr': expected URL naming only a host and a port |
      | 1            | 'addr' = 'mqtt://127.0.0.1:1883/devices'                                                           | invalid client config key 'addr': expected URL naming only a host and a port |
      | 3            | 'addr' = 'mqtt://127.0.0.1:1883/devices'                                                           | invalid client config key 'addr': expected URL naming only a host and a port |
      | 1            | 'addr' = 'mqtt://127.0.0.1:1883#devices'                                                           | invalid client config key 'addr': expected URL naming only a host and a port |
      | 3            | 'addr' = 'mqtt://127.0.0.1:1883#devices'                                                           | invalid client config key 'addr': expected URL naming only a host and a port |
      | 1            | 'addr' = 'mqtts://127.0.0.1:8883'                                                                  | client config key 'addr' requires 'tls_ca_file'                              |
      | 3            | 'addr' = 'mqtts://127.0.0.1:8883'                                                                  | client config key 'addr' requires 'tls_ca_file'                              |
      | 1            | 'addr' = 'mqtt://127.0.0.1:1883', 'tls_ca_file' = 'authority.pem'                                  | client config key 'tls_ca_file' requires 'addr' to use mqtts                 |
      | 3            | 'addr' = 'mqtt://127.0.0.1:1883', 'tls_ca_file' = 'authority.pem'                                  | client config key 'tls_ca_file' requires 'addr' to use mqtts                 |
      | 1            | 'addr' = 'mqtt://127.0.0.1:1883', 'tls_cert_file' = 'client.pem', 'tls_key_file' = 'client.key'    | client config key 'tls_cert_file' requires 'addr' to use mqtts               |
      | 3            | 'addr' = 'mqtt://127.0.0.1:1883', 'tls_cert_file' = 'client.pem', 'tls_key_file' = 'client.key'    | client config key 'tls_cert_file' requires 'addr' to use mqtts               |
      | 1            | 'addr' = 'mqtts://127.0.0.1:8883', 'tls_ca_file' = 'authority.pem', 'tls_cert_file' = 'client.pem' | client config key 'tls_cert_file' requires 'tls_key_file'                    |
      | 3            | 'addr' = 'mqtts://127.0.0.1:8883', 'tls_ca_file' = 'authority.pem', 'tls_cert_file' = 'client.pem' | client config key 'tls_cert_file' requires 'tls_key_file'                    |
