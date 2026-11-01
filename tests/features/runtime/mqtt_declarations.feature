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
        TO mqtt_main TOPIC '<topic>' <qos>
        MODE <publishing>
        ENCODE USING event_codec INHERIT ALL
        <batch>
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER publishing;
      """
    Then the last command output contains
      """
      TOPIC '<topic>' <qos>
      """
    And the last command output contains
      """
      MODE <publishing>
      """

    Examples:
      | cluster_size | filter           | subscription                                                                                                           | quiesce                                      | topic                | qos   | publishing                                                           | batch                               |
      | 1            | Devices/+/Orders | SESSION CLEAN QOS 0 MODE NO ACK                                                                                        | DROP                                         | Devices/EU/Orders    | QOS 0 | NO ACK RETRY POLICY BACKOFF 250ms MAX 30s                            |                                     |
      | 3            | Devices/+/Orders | SESSION CLEAN QOS 0 MODE NO ACK                                                                                        | DROP                                         | Devices/EU/Orders    | QOS 0 | NO ACK RETRY POLICY BACKOFF 250ms MAX 30s                            |                                     |
      | 1            | Devices/+/Alerts | SESSION CLEAN QOS 2 MODE NO ACK                                                                                        | DROP                                         | Devices/EU/Alerts    | QOS 0 | NO ACK RETRY POLICY BACKOFF 250ms MAX 30s                            |                                     |
      | 3            | Devices/+/Alerts | SESSION CLEAN QOS 2 MODE NO ACK                                                                                        | DROP                                         | Devices/EU/Alerts    | QOS 0 | NO ACK RETRY POLICY BACKOFF 250ms MAX 30s                            |                                     |
      | 1            | telemetry/#      | SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s                         | SUSPEND                                      | telemetry/Événements | QOS 1 | ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s | BATCH MAX MESSAGES 8 MAX SIZE 64KiB |
      | 3            | telemetry/#      | SESSION PERSISTENT QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s                         | SUSPEND                                      | telemetry/Événements | QOS 1 | ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s | BATCH MAX MESSAGES 8 MAX SIZE 64KiB |
      | 1            | /                | SESSION PERSISTENT QOS 2 MODE ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | sensors/Raw Topic    | QOS 2 | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s     |                                     |
      | 3            | /                | SESSION PERSISTENT QOS 2 MODE ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s | BUFFER MAX SIZE 1MiB ON OVERFLOW DROP OLDEST | sensors/Raw Topic    | QOS 2 | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s     |                                     |
      | 1            | sensors/#        | SESSION PERSISTENT QOS 2 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s                         | SUSPEND                                      | sensors/Raw Topic    | QOS 2 | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s     |                                     |
      | 3            | sensors/#        | SESSION PERSISTENT QOS 2 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s                         | SUSPEND                                      | sensors/Raw Topic    | QOS 2 | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 250ms MAX 30s     |                                     |

  Scenario Outline: MQTT TOPIC ingestors refuse acknowledgement and suspension outside a persistent session at QoS 1 or 2
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
      | cluster_size | subscription                                                                                                        | quiesce | expected                                                                                                                                   |
      | 1            | SESSION CLEAN QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s                              | DROP    | MQTT TOPIC ingestors name MODE ACK SEQUENTIAL only with SESSION PERSISTENT and QOS 1 or 2; the statement writes SESSION CLEAN and QOS 1    |
      | 3            | SESSION CLEAN QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s                              | DROP    | MQTT TOPIC ingestors name MODE ACK SEQUENTIAL only with SESSION PERSISTENT and QOS 1 or 2; the statement writes SESSION CLEAN and QOS 1    |
      | 1            | SESSION CLEAN QOS 2 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s                              | DROP    | MQTT TOPIC ingestors name MODE ACK SEQUENTIAL only with SESSION PERSISTENT and QOS 1 or 2; the statement writes SESSION CLEAN and QOS 2    |
      | 3            | SESSION CLEAN QOS 2 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s                              | DROP    | MQTT TOPIC ingestors name MODE ACK SEQUENTIAL only with SESSION PERSISTENT and QOS 1 or 2; the statement writes SESSION CLEAN and QOS 2    |
      | 1            | SESSION PERSISTENT QOS 0 MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | DROP    | MQTT TOPIC ingestors name MODE ACK PARALLEL only with SESSION PERSISTENT and QOS 1 or 2; the statement writes SESSION PERSISTENT and QOS 0 |
      | 3            | SESSION PERSISTENT QOS 0 MODE ACK PARALLEL MAX 2 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | DROP    | MQTT TOPIC ingestors name MODE ACK PARALLEL only with SESSION PERSISTENT and QOS 1 or 2; the statement writes SESSION PERSISTENT and QOS 0 |
      | 1            | SESSION PERSISTENT QOS 3 MODE NO ACK                                                                                | DROP    | MQTT QOS must be 0, 1 or 2, found 3                                                                                               |
      | 3            | SESSION PERSISTENT QOS 3 MODE NO ACK                                                                                | DROP    | MQTT QOS must be 0, 1 or 2, found 3                                                                                               |
      | 1            | SESSION CLEAN QOS 2 MODE NO ACK                                                                                     | SUSPEND | MQTT ON QUIESCE SUSPEND requires SESSION PERSISTENT and QOS 1 or 2                                                                         |
      | 3            | SESSION CLEAN QOS 2 MODE NO ACK                                                                                     | SUSPEND | MQTT ON QUIESCE SUSPEND requires SESSION PERSISTENT and QOS 1 or 2                                                                         |
      | 1            | SESSION PERSISTENT QOS 0 MODE NO ACK                                                                                | SUSPEND | MQTT ON QUIESCE SUSPEND requires SESSION PERSISTENT and QOS 1 or 2                                                                         |
      | 3            | SESSION PERSISTENT QOS 0 MODE NO ACK                                                                                | SUSPEND | MQTT ON QUIESCE SUSPEND requires SESSION PERSISTENT and QOS 1 or 2                                                                         |

  Scenario Outline: MQTT TOPIC ingestors refuse a statement that leaves out its session or quality of service
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
        ON QUIESCE DROP DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | subscription                         | expected                 |
      | 1            | MODE NO ACK                          | SESSION, found MODE      |
      | 3            | MODE NO ACK                          | SESSION, found MODE      |
      | 1            | QOS 0 MODE NO ACK                    | SESSION, found QOS       |
      | 3            | QOS 0 MODE NO ACK                    | SESSION, found QOS       |
      | 1            | SESSION CLEAN MODE NO ACK            | expected QOS, found MODE |
      | 3            | SESSION CLEAN MODE NO ACK            | expected QOS, found MODE |
      | 1            | QOS 1 SESSION PERSISTENT MODE NO ACK | SESSION, found QOS       |
      | 3            | QOS 1 SESSION PERSISTENT MODE NO ACK | SESSION, found QOS       |

  Scenario Outline: An MQTT TOPIC ingestor's SET FROM keeps the persistent session at QoS 1 or 2 its acknowledged mode requires
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE MQTT CLIENT mqtt_main CONFIG { 'addr' = 'mqtt://127.0.0.1:1' };
      CREATE MQTT TOPIC INGESTOR acknowledged
        FROM mqtt_main TOPIC 'devices/+' SESSION PERSISTENT QOS 2
        MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail
      """
      ALTER MQTT TOPIC INGESTOR acknowledged SET FROM mqtt_main TOPIC 'devices/+'
        SESSION CLEAN QOS 2 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s
        ON QUIESCE DROP;
      """
    Then the last command error contains
      """
      MQTT TOPIC ingestors name MODE ACK SEQUENTIAL only with SESSION PERSISTENT and QOS 1 or 2; the statement writes SESSION CLEAN and QOS 2
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER MQTT TOPIC INGESTOR acknowledged SET FROM mqtt_main TOPIC 'devices/+'
        SESSION PERSISTENT QOS 2
        MODE ACK PARALLEL MAX 4 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s
        ON QUIESCE SUSPEND;
      SHOW CREATE INGESTOR acknowledged;
      """
    Then the last command output contains
      """
      TOPIC 'devices/+' SESSION PERSISTENT QOS 2 MODE ACK PARALLEL MAX 4 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ON QUIESCE SUSPEND
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

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
        FROM mqtt_main TOPIC '<filter>' SESSION CLEAN QOS 0 MODE NO ACK
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
        QOS 0 MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
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

  Scenario Outline: MQTT TOPIC emitters pair each quality of service with the mode it can complete in
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
        <publishing> ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      <expected>
      """

    Examples:
      | cluster_size | publishing                                                                   | expected                                                                           |
      | 1            | QOS 3 MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s                            | MQTT QOS must be 0, 1 or 2, found 3                                                |
      | 3            | QOS 3 MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s                            | MQTT QOS must be 0, 1 or 2, found 3                                                |
      | 1            | QOS 0 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s     | MQTT TOPIC emitters name MODE ACK only with QOS 1 or 2; the statement writes QOS 0 |
      | 3            | QOS 0 MODE ACK PARALLEL MAX 2 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s | MQTT TOPIC emitters name MODE ACK only with QOS 1 or 2; the statement writes QOS 0 |
      | 1            | QOS 1 MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s                            | MQTT TOPIC emitters name MODE NO ACK only with QOS 0; the statement writes QOS 1   |
      | 3            | QOS 1 MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s                            | MQTT TOPIC emitters name MODE NO ACK only with QOS 0; the statement writes QOS 1   |
      | 1            | QOS 2 MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s                            | MQTT TOPIC emitters name MODE NO ACK only with QOS 0; the statement writes QOS 2   |
      | 3            | QOS 2 MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s                            | MQTT TOPIC emitters name MODE NO ACK only with QOS 0; the statement writes QOS 2   |
      | 1            | MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s                                  | expected QOS, found MODE                                                           |
      | 3            | MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s                                  | expected QOS, found MODE                                                           |

  Scenario Outline: An MQTT TOPIC emitter's ALTER keeps its mode beside the quality of service it pairs with
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (value STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (value string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE MQTT CLIENT mqtt_main CONFIG { 'addr' = 'mqtt://127.0.0.1:1' };
      CREATE MQTT TOPIC EMITTER publishing FROM events TO mqtt_main TOPIC 'devices/orders'
        QOS 1 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s
        ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail
      """
      ALTER MQTT TOPIC EMITTER publishing SET MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s;
      """
    Then the last command error contains
      """
      MQTT TOPIC emitters name MODE NO ACK only with QOS 0; the statement writes QOS 1
      """
    When these Plexus commands fail
      """
      ALTER MQTT TOPIC EMITTER publishing SET TO mqtt_main TOPIC 'devices/orders'
        QOS 0 MODE ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s
        ENCODE USING event_codec;
      """
    Then the last command error contains
      """
      MQTT TOPIC emitters name MODE ACK only with QOS 1 or 2; the statement writes QOS 0
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER MQTT TOPIC EMITTER publishing SET MODE ACK PARALLEL MAX 4 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s;
      SHOW CREATE EMITTER publishing;
      """
    Then the last command output contains
      """
      TOPIC 'devices/orders' QOS 1
      """
    And the last command output contains
      """
      MODE ACK PARALLEL MAX 4 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER MQTT TOPIC EMITTER publishing SET TO mqtt_main TOPIC 'devices/orders'
        QOS 0 MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s
        ENCODE USING event_codec;
      SHOW CREATE EMITTER publishing;
      """
    Then the last command output contains
      """
      TOPIC 'devices/orders' QOS 0
      """
    And the last command output contains
      """
      MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s
      """
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE EMITTER publishing;
      """
    Then the last command output contains
      """
      options: QOS 0
      mode: NO ACK RETRY POLICY BACKOFF 1s MAX 30s
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

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
