Feature: Session subscription delivery options
  Scenario Outline: Dropping sampled session subscriptions suppress matching records
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA telemetry (
        device STRING,
        active BOOL,
        reading I64
      );
        CREATE STRICT WIRE JSON SCHEMA telemetry_wire (
        device string,
        active boolean,
        reading integer
      );
        CREATE CODEC telemetry_codec
        FROM WIRE JSON SCHEMA telemetry_wire
        TO SCHEMA telemetry;
        CREATE IF NOT EXISTS SCHEMA device_branch ( device STRING );
        CREATE IF NOT EXISTS BRANCH by_telemetry_http SCHEMA device_branch TTL 5m;
        CREATE RELAY telemetry SCHEMA telemetry BRANCHED BY by_telemetry_http;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT telemetry_endpoint
        ON edge
        PATH '/telemetry'
        TYPE HTTP;
        CREATE INGESTOR telemetry_http
        TO telemetry FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING telemetry_codec
        BRANCHED BY by_telemetry_http VALUES { device = telemetry.device }

        TIMESTAMP NOW
        FROM ENDPOINT telemetry_endpoint MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION telemetry_subscription TO telemetry DROPPING BATCH SAMPLE RATE 0.0 WHERE telemetry.active;
        START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/telemetry"
      """
      {"device":"edge-1","active":true,"reading":42}
      """
    Then the relay subscription does not receive a payload within "1s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Blocking sampled session subscriptions deliver records after filtering
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA telemetry (
        device STRING,
        active BOOL,
        reading I64
      );
        CREATE STRICT WIRE JSON SCHEMA telemetry_wire (
        device string,
        active boolean,
        reading integer
      );
        CREATE CODEC telemetry_codec
        FROM WIRE JSON SCHEMA telemetry_wire
        TO SCHEMA telemetry;
        CREATE IF NOT EXISTS SCHEMA device_branch ( device STRING );
        CREATE IF NOT EXISTS BRANCH by_telemetry_http SCHEMA device_branch TTL 5m;
        CREATE RELAY telemetry SCHEMA telemetry BRANCHED BY by_telemetry_http;
        CREATE VHOST edge http-{{test_id}}.example.com;
        CREATE ENDPOINT telemetry_endpoint
        ON edge
        PATH '/telemetry'
        TYPE HTTP;
        CREATE INGESTOR telemetry_http
        TO telemetry FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING telemetry_codec
        BRANCHED BY by_telemetry_http VALUES { device = telemetry.device }

        TIMESTAMP NOW
        FROM ENDPOINT telemetry_endpoint MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION telemetry_subscription TO telemetry BLOCKING BATCH SAMPLE RATE 1.0 WHERE telemetry.active;
        START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/telemetry"
      """
      {"device":"edge-1","active":false,"reading":7}
      """
    Then the relay subscription does not receive a payload within "500ms"
    When http payload is posted to host "http-{{test_id}}.example.com" path "/telemetry"
      """
      {"device":"edge-1","active":true,"reading":42}
      """
    Then within "5s" the relay subscription receives a payload
      """
      {"active":true,"device":"edge-1","reading":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
