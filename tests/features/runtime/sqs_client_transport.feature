Feature: SQS client transport
  An SQS client's connections follow its declared keys alone. Each request is made once and bounded
  by the client's `timeout_ms`, so a service that does not answer is the ingestor's transient status
  and is retried on its `RETRY POLICY`, never inside the AWS SDK. No connection takes a proxy from
  the node's environment.

  Scenario Outline: An SQS QUEUE ingestor retries an unanswered request on its RETRY POLICY, not inside its client
    Given a <cluster_size> node nervix cluster is started
    And a silent endpoint "sqs_peer" is listening
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
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = 'http://{{sqs_peer}}',
        'region' = 'us-east-1',
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret',
        'timeout_ms' = 1000
      };
      CREATE SQS QUEUE INGESTOR sqs_in
        FROM sqs_main QUEUE 'unanswered_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 4s MAX 4s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then the silent endpoint "sqs_peer" eventually accepts a connection
    And within "20s" DESCRIBE INGESTOR "sqs_in" on the leader node contains
      """
      the SQS queue lookup did not finish within timeout_ms 1s
      """
    And the silent endpoint "sqs_peer" eventually accepts 2 connections
    And the stallable endpoint "sqs_peer" accepted connection 2 at least "4s" after connection 1

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: SQS clients ignore the proxy environment of their node
    Given SQS is running
    And a silent endpoint "proxy" is listening
    And a <cluster_size> node nervix-server process cluster is started with its proxy environment naming the endpoint "proxy"
    And the active domain is "{{domain}}"
    And SQS queue "proxied_in_{{test_id}}" exists
    And SQS queue "proxied_out_{{test_id}}" is observed
    And the server process cluster is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE SQS CLIENT sqs_main CONFIG {
        'endpoint' = '{{sqs_endpoint}}',
        'region' = 'us-east-1',
        'access_key_id' = 'scenario-key',
        'secret_access_key' = 'scenario-secret',
        'timeout_ms' = 5000
      };
      CREATE SQS QUEUE INGESTOR sqs_in
        FROM sqs_main QUEUE 'proxied_in_{{test_id}}'
        MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SQS QUEUE EMITTER sqs_out FROM notifications
        TO sqs_main QUEUE 'proxied_out_{{test_id}}' SEND SINGLE MODE ACK RETRY POLICY BACKOFF 200ms MAX 1s
        ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    When SQS message is published to queue "proxied_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """
    And the stallable endpoint "proxy" accepts no connection beyond 0 within "1s"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
