Feature: Ingestor branching
  Scenario Outline: UNBRANCHED ingestors round-trip without synthetic branch schema
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT http_notifications_endpoint ON edge PATH '/ingest' TYPE HTTP;
      CREATE INGESTOR http_notifications FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec TO notifications INHERIT ALL UNBRANCHED FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE INGESTOR http_notifications;
      """
    Then the last command output contains
      """
      CREATE INGESTOR http_notifications
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON QUIESCE BUFFER MAX SIZE 1MiB
        DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
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

  Scenario Outline: A branch key that fails for one row sends only that row to its route's error relay
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA reading (
        id STRING,
        amount I64,
        divisor I64
      );
      CREATE SCHEMA branch_failure (
        failed_id STRING,
        error_code STRING,
        operation STRING,
        error_message STRING,
        captured_amount I64 OPTIONAL
      );
      CREATE CODEC reading_batch_codec
        FROM JSON
        TO SCHEMA reading
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA bucket_branch (bucket I64);
      CREATE BRANCH by_bucket SCHEMA bucket_branch TTL 5m;
      CREATE RELAY readings SCHEMA reading BRANCHED BY by_bucket;
      CREATE RELAY branch_failures SCHEMA branch_failure UNBRANCHED;
      CREATE VHOST edge ingestor-branch-error-{{test_id}}.example.com;
      CREATE ENDPOINT reading_ingress ON edge PATH '/readings' TYPE HTTP;
      CREATE INGESTOR reading_source
        FROM ENDPOINT reading_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING reading_batch_codec
        TO readings
          INHERIT ALL
          BRANCHED BY by_bucket SET bucket = message.amount / message.divisor
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR SEND TO branch_failures
          SET failed_id = input.id,
              error_code = error.code,
              operation = error.operation,
              error_message = error.message,
              captured_amount = partial_output.amount
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION readings_subscription TO readings;
      CREATE SUBSCRIPTION branch_failures_subscription TO branch_failures;
      START;
      """
    # One request is one decoded batch, so the branch program evaluates its four rows together. A
    # zero divisor fails only the branch key of its own row, which takes the route's error policy
    # with the output it was given, while the other rows are published into their own branches.
    And http payload is posted to node "node-1" with host "ingestor-branch-error-{{test_id}}.example.com" path "/readings"
      """
      [{"id":"kept-1","amount":10,"divisor":5},{"id":"failed-2","amount":4,"divisor":0},{"id":"kept-3","amount":9,"divisor":3},{"id":"failed-4","amount":1,"divisor":0}]
      """
    Then within "30s" the relay subscription receives exactly one payload for each fragment set
      """
      "id":"kept-1" | "amount":10
      "id":"kept-3" | "amount":9
      "failed_id":"failed-2" | "error_code":"evaluation" | "operation":"set" | "error_message":"branch SET failed with division_by_zero: integer division by zero at | "captured_amount":4
      "failed_id":"failed-4" | "error_code":"evaluation" | "operation":"set" | "error_message":"branch SET failed with division_by_zero: integer division by zero at | "captured_amount":1
      """
    And the relay subscription does not receive a payload within "3s"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
