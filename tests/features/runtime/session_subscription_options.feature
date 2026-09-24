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
        CREATE WIRE JSON SCHEMA telemetry_wire MODE STRICT (
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
        FROM ENDPOINT telemetry_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING telemetry_codec
        TIMESTAMP NOW
        TO telemetry
        INHERIT ALL
        BRANCHED BY by_telemetry_http
        SET device = message.device
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION telemetry_subscription TO telemetry DROPPING BATCH SAMPLE RATE 0.0 WHERE active;
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

  Scenario Outline: Rows a subscription filter cannot evaluate are skipped and reported
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA ratio (
        device STRING,
        reading I64,
        divisor I64
      );
      CREATE WIRE JSON SCHEMA ratio_wire MODE STRICT (
        device string,
        reading integer,
        divisor integer
      );
      CREATE CODEC ratio_codec
        FROM WIRE JSON SCHEMA ratio_wire
        TO SCHEMA ratio;
      CREATE RELAY ratios SCHEMA ratio UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT ratio_endpoint
        ON edge
        PATH '/ratios'
        TYPE HTTP;
      CREATE INGESTOR ratio_http
        FROM ENDPOINT ratio_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING ratio_codec
        TIMESTAMP NOW
        TO ratios
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION ratio_subscription TO ratios WHERE reading / divisor > 0;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/ratios"
      """
      {"device":"edge-1","reading":42,"divisor":0}
      """
    Then within "30s" the active session observes a server error containing
      """
      session subscription predicate failed
      """
    And the last server error contains
      """
      division by zero
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/ratios"
      """
      {"device":"edge-2","reading":42,"divisor":2}
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"device":"edge-2","divisor":2,"reading":42}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

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
        CREATE WIRE JSON SCHEMA telemetry_wire MODE STRICT (
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
        FROM ENDPOINT telemetry_endpoint MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING telemetry_codec
        TIMESTAMP NOW
        TO telemetry
        INHERIT ALL
        BRANCHED BY by_telemetry_http
        SET device = message.device
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
        CREATE SUBSCRIPTION telemetry_subscription TO telemetry BLOCKING BATCH SAMPLE RATE 1.0 WHERE active;
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
