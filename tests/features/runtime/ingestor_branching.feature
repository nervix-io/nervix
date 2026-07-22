Feature: Ingestor branching
  Scenario Outline: UNBRANCHED ingestors round-trip without synthetic branch schema
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE STRICT WIRE JSON SCHEMA notification_wire ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT http_notifications_endpoint ON edge PATH '/ingest' TYPE HTTP;
      CREATE INGESTOR http_notifications FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL DECODE USING notification_codec TO notifications INHERIT ALL UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE INGESTOR http_notifications;
      """
    Then the last command output contains
      """
      CREATE INGESTOR http_notifications FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL DECODE USING notification_codec TO notifications INHERIT ALL UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And the last command output does not contain
      """
      BY
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Message is reserved and cannot be used as a relay name
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands fail with "expected relay_name"
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE RELAY message SCHEMA notification UNBRANCHED;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
