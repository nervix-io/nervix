Feature: Generator node
  Scenario Outline: Generators cannot cross named branch boundaries
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands fail with "generator 'synth_notifications' branch name 'output_branch' does not match relay 'notifications' branch name 'input_branch'"
      """
      CREATE SCHEMA notification (
        tenant STRING,
        amount I64
      );

      CREATE SCHEMA tenant_branch (
        tenant STRING
      );

      CREATE BRANCH input_branch
        SCHEMA tenant_branch
        TTL 5m;

      CREATE BRANCH output_branch
        SCHEMA tenant_branch
        TTL 5m;

      CREATE RELAY notifications
        SCHEMA notification
        BRANCHED BY input_branch
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;

      CREATE RELAY generated_notifications
        SCHEMA notification
        BRANCHED BY output_branch;

      CREATE GENERATOR synth_notifications
        TO generated_notifications
        BRANCHED BY output_branch
        EACH 100ms
        FLUSH IMMEDIATE
        SET generated_notifications.tenant = notifications.tenant,
            generated_notifications.amount = notifications.amount
        ON MESSAGE ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Generator materialized state and output remain isolated per branch
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        tenant STRING,
        amount I64
      );

      CREATE SCHEMA generated_notification (
        tenant STRING,
        total I64
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        tenant string,
        amount integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE SCHEMA tenant_branch_schema (
        tenant STRING
      );

      CREATE BRANCH tenant_branch
        SCHEMA tenant_branch_schema
        TTL 5m;

      CREATE RELAY left_notifications
        SCHEMA notification
        BRANCHED BY tenant_branch
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;

      CREATE RELAY right_notifications
        SCHEMA notification
        BRANCHED BY tenant_branch
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;

      CREATE RELAY generated_notifications
        SCHEMA generated_notification
        BRANCHED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT left_endpoint
        ON edge
        PATH '/left'
        TYPE HTTP;

      CREATE ENDPOINT right_endpoint
        ON edge
        PATH '/right'
        TYPE HTTP;

      CREATE INGESTOR left_ingestor
        TO left_notifications FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY tenant_branch VALUES { tenant = left_notifications.tenant }

        FROM ENDPOINT left_endpoint MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;

      CREATE INGESTOR right_ingestor
        TO right_notifications FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        BRANCHED BY tenant_branch VALUES { tenant = right_notifications.tenant }

        FROM ENDPOINT right_endpoint MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;

      CREATE GENERATOR synth_notifications
        TO generated_notifications
        BRANCHED BY tenant_branch
        EACH 100ms
        FLUSH IMMEDIATE
        SET generated_notifications.tenant = left_notifications.tenant,
            generated_notifications.total = left_notifications.amount + right_notifications.amount
        ON MESSAGE ERROR LOG;

      CREATE SUBSCRIPTION generated_notifications_subscription TO generated_notifications;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/left"
      """
      {"tenant":"acme","amount":10}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/left"
      """
      {"tenant":"beta","amount":20}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/right"
      """
      {"tenant":"beta","amount":2}
      """
    And http payload is posted to host "http-{{test_id}}.example.com" path "/right"
      """
      {"tenant":"acme","amount":1}
      """
    Then within "5s" the relay subscription receives payloads
      """
      key={"tenant":"acme"} payload={"tenant":"acme","total":11}
      key={"tenant":"beta"} payload={"tenant":"beta","total":22}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Generator emits records from materialized relay state
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        amount I64
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        amount integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications
        SCHEMA notification UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;

      CREATE RELAY generated_notifications
        SCHEMA notification UNBRANCHED;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;

      CREATE INGESTOR http_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        UNBRANCHED

        TIMESTAMP NOW
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;

      CREATE GENERATOR synth_notifications
        TO generated_notifications
        UNBRANCHED
        EACH 100ms
        FLUSH IMMEDIATE
        SET generated_notifications.user_id = notifications.user_id,
            generated_notifications.amount = notifications.amount ON MESSAGE ERROR LOG;

      CREATE SUBSCRIPTION generated_notifications_subscription TO generated_notifications;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42,"amount":7}
      """
    Then the relay subscription receives a payload
      """
      {"amount":7,"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Generator flushes buffered records on flush cadence
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        amount I64
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        amount integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications
        SCHEMA notification UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;

      CREATE RELAY generated_notifications
        SCHEMA notification UNBRANCHED;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;

      CREATE INGESTOR http_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        UNBRANCHED

        TIMESTAMP NOW
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;

      CREATE GENERATOR synth_notifications
        TO generated_notifications
        UNBRANCHED
        EACH 100ms
        FLUSH EACH 1s MAX BATCH SIZE 1MiB
        SET generated_notifications.user_id = notifications.user_id,
            generated_notifications.amount = notifications.amount ON MESSAGE ERROR LOG;

      CREATE SUBSCRIPTION generated_notifications_subscription TO generated_notifications;
      START;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42,"amount":7}
      """
    Then the relay subscription does not receive a payload within "500ms"
    And within "3s" the relay subscription receives a payload
      """
      {"amount":7,"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
  Scenario Outline: Paced generators follow domain logical time
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 1s;
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        amount I64
      );

      CREATE STRICT WIRE JSON SCHEMA notification_wire (
        user_id integer,
        amount integer
      );

      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;

      CREATE RELAY notifications
        SCHEMA notification UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;

      CREATE RELAY generated_notifications
        SCHEMA notification UNBRANCHED;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT http_notifications_endpoint
        ON edge
        PATH '/ingest'
        TYPE HTTP;

      CREATE INGESTOR http_notifications
        TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG
        DECODE USING notification_codec
        UNBRANCHED

        TIMESTAMP NOW
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;

      CREATE GENERATOR synth_notifications
        TO generated_notifications
        UNBRANCHED
        EACH 2s
        FLUSH IMMEDIATE
        SET generated_notifications.user_id = notifications.user_id,
            generated_notifications.amount = notifications.amount ON MESSAGE ERROR LOG;

      CREATE SUBSCRIPTION generated_notifications_subscription TO generated_notifications;
      START AT NOW TIME RATE 20.0;
      """
    When http payload is posted to host "http-{{test_id}}.example.com" path "/ingest"
      """
      {"user_id":42,"amount":7}
      """
    Then within "500ms" the relay subscription receives a payload
      """
      {"amount":7,"user_id":42}
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
