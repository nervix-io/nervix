Feature: Runtime persistence
  Scenario Outline: Persisted rules are reapplied after a full cluster restart
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
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
        CREATE IF NOT EXISTS BRANCH by_http_notifications SCHEMA user_id_branch TTL 5m;
        CREATE RELAY notifications SCHEMA notification BRANCHED BY by_http_notifications;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;
        CREATE INGESTOR http_notifications
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        BRANCHED BY by_http_notifications
        SET user_id = message.user_id
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        START;
      """
    When the cluster is restarted
    And these NSPL commands are executed on the leader node
      """
      SHOW CREATE INGESTOR http_notifications;
      """
    Then the last command output contains
      """
      CREATE INGESTOR http_notifications
      """
    Then within "5s" node "node-1" eventually reports describe relay as "not exists"
      """
      DESCRIBE RELAY notifications WHERE (user_id = 901);
      """
    And node "node-1" eventually accepts http traffic for host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":901}
      """
    When http payload is posted to node "node-1" with host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":901}
      """
    Then within "30s" node "node-1" eventually reports describe relay as "exists"
      """
      DESCRIBE RELAY notifications WHERE (user_id = 901);
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Persisted ingestors and reingestors run their planned routes after a full cluster restart
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA reading ( tenant STRING, sensor STRING, value I64 );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT ( tenant string, sensor string, value integer );
      CREATE CODEC reading_codec FROM WIRE JSON SCHEMA reading_wire TO SCHEMA reading;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS BRANCH by_restart_tenant SCHEMA tenant_branch TTL 5m;
      CREATE IF NOT EXISTS SCHEMA sensor_branch ( sensor STRING );
      CREATE IF NOT EXISTS BRANCH by_restart_sensor SCHEMA sensor_branch TTL 5m;
      CREATE RELAY readings SCHEMA reading BRANCHED BY by_restart_tenant;
      CREATE RELAY sensor_readings SCHEMA reading BRANCHED BY by_restart_sensor;
      CREATE VHOST edge http-{{test_id}}-restart-readings.example.com;
      CREATE ENDPOINT reading_ingress ON edge PATH '/readings' TYPE HTTP;
      CREATE INGESTOR reading_source
        FROM ENDPOINT reading_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_codec
        TO readings
        INHERIT ALL
        BRANCHED BY by_restart_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE REINGESTOR sensor_partition
        FROM readings
        FILTER WHERE input.value > 10
        TO sensor_readings
        INHERIT ALL
        BRANCHED BY by_restart_sensor
        SET sensor = message.sensor
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      START;
      """
    When the cluster is restarted
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION sensor_readings_subscription TO sensor_readings;
      """
    Then within "10s" repeatedly posting http payload to host "http-{{test_id}}-restart-readings.example.com" path "/readings" yields a relay subscription payload
      """
      {"tenant":"acme","sensor":"warmup","value":11}
      """
    When http payload is posted to host "http-{{test_id}}-restart-readings.example.com" path "/readings"
      """
      {"tenant":"acme","sensor":"dropped","value":1}
      """
    Then the relay subscription does not receive a payload containing fragments within "3s"
      """
      "sensor":"dropped"
      """
    When http payload is posted to host "http-{{test_id}}-restart-readings.example.com" path "/readings"
      """
      {"tenant":"beta","sensor":"kept","value":20}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"sensor":"kept"} | "tenant":"beta" | "value":20
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Persisted models reload from the on-disk tables a node flushed them into
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA reading ( tenant STRING, sensor STRING, value I64 );
      CREATE WIRE JSON SCHEMA reading_wire MODE STRICT ( tenant string, sensor string, value integer );
      CREATE CODEC reading_codec FROM WIRE JSON SCHEMA reading_wire TO SCHEMA reading;
      CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
      CREATE IF NOT EXISTS BRANCH by_flushed_tenant SCHEMA tenant_branch TTL 5m;
      CREATE IF NOT EXISTS SCHEMA sensor_branch ( sensor STRING );
      CREATE IF NOT EXISTS BRANCH by_flushed_sensor SCHEMA sensor_branch TTL 5m;
      CREATE RELAY readings SCHEMA reading BRANCHED BY by_flushed_tenant;
      CREATE RELAY sensor_readings SCHEMA reading BRANCHED BY by_flushed_sensor;
      CREATE VHOST edge http-{{test_id}}-flushed-readings.example.com;
      CREATE ENDPOINT reading_ingress ON edge PATH '/readings' TYPE HTTP;
      CREATE INGESTOR reading_source
        FROM ENDPOINT reading_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_codec
        TO readings
        INHERIT ALL
        BRANCHED BY by_flushed_tenant
        SET tenant = message.tenant
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE REINGESTOR sensor_partition
        FROM readings
        FILTER WHERE input.value > 10
        TO sensor_readings
        INHERIT ALL
        BRANCHED BY by_flushed_sensor
        SET sensor = message.sensor
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG;
      START;
      """
    When the cluster is restarted from state its nodes moved into on-disk tables
    Then node "node-1" eventually observes a stable leader
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE REINGESTOR sensor_partition;
      """
    Then the last command output contains
      """
      CREATE ATTACHED REINGESTOR sensor_partition
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION sensor_readings_subscription TO sensor_readings;
      """
    Then within "10s" repeatedly posting http payload to host "http-{{test_id}}-flushed-readings.example.com" path "/readings" yields a relay subscription payload
      """
      {"tenant":"acme","sensor":"warmup","value":11}
      """
    When http payload is posted to host "http-{{test_id}}-flushed-readings.example.com" path "/readings"
      """
      {"tenant":"beta","sensor":"kept","value":20}
      """
    Then within "10s" the relay subscription receives payloads containing all fragments
      """
      key={"sensor":"kept"} | "tenant":"beta" | "value":20
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
