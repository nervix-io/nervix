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
        USING MATERIALIZED STATE notifications
        EACH 100ms
        BRANCHED BY output_branch
        TO generated_notifications
          SET tenant = relay_state.notifications.tenant,
              amount = relay_state.notifications.amount
          FLUSH IMMEDIATE
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

      CREATE RELAY generated_notifications
        SCHEMA generated_notification
        BRANCHED BY tenant_branch;

      CREATE VHOST edge http-{{test_id}}.example.com;

      CREATE ENDPOINT left_endpoint
        ON edge
        PATH '/left'
        TYPE HTTP;

      CREATE INGESTOR left_ingestor
        FROM ENDPOINT left_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TIMESTAMP NOW
        TO left_notifications
          INHERIT ALL
          BRANCHED BY tenant_branch
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE GENERATOR synth_notifications
        USING MATERIALIZED STATE left_notifications
        EACH 100ms
        BRANCHED BY tenant_branch
        TO generated_notifications
          SET tenant = relay_state.left_notifications.tenant,
              total = relay_state.left_notifications.amount
          FLUSH IMMEDIATE
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
    Then within "5s" the relay subscription receives payloads
      """
      key={"tenant":"acme"} payload={"tenant":"acme","total":10}
      key={"tenant":"beta"} payload={"tenant":"beta","total":20}
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
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TIMESTAMP NOW
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE GENERATOR synth_notifications
        USING MATERIALIZED STATE notifications
        EACH 100ms
        UNBRANCHED
        TO generated_notifications
          SET user_id = relay_state.notifications.user_id,
              amount = relay_state.notifications.amount
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;

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
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TIMESTAMP NOW
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE GENERATOR synth_notifications
        USING MATERIALIZED STATE notifications
        EACH 100ms
        UNBRANCHED
        TO generated_notifications
          SET user_id = relay_state.notifications.user_id,
              amount = relay_state.notifications.amount
          FLUSH EACH 1s MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG;

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
        FROM ENDPOINT http_notifications_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING notification_codec
        TIMESTAMP NOW
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH EACH 100ms MAX BATCH SIZE 1MiB
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;

      CREATE GENERATOR synth_notifications
        USING MATERIALIZED STATE notifications
        EACH 2s
        UNBRANCHED
        TO generated_notifications
          SET user_id = relay_state.notifications.user_id,
              amount = relay_state.notifications.amount
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;

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

  Scenario Outline: Generator routes share one immutable state snapshot per tick
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA generator_source_value (
        id STRING,
        value I64
      );
      CREATE SCHEMA generated_value (
        id STRING,
        route STRING,
        value I64,
        tick DATETIME
      );
      CREATE STRICT WIRE JSON SCHEMA generator_source_wire (
        id string,
        value integer
      );
      CREATE CODEC generator_source_codec
        FROM WIRE JSON SCHEMA generator_source_wire
        TO SCHEMA generator_source_value;
      CREATE RELAY generator_source_values
        SCHEMA generator_source_value
        UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY generated_values SCHEMA generated_value UNBRANCHED;
      CREATE VHOST edge generator-snapshot-{{test_id}}.example.com;
      CREATE ENDPOINT generator_source_endpoint ON edge PATH '/values' TYPE HTTP;
      CREATE INGESTOR generator_source
        FROM ENDPOINT generator_source_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING generator_source_codec
        TIMESTAMP NOW
        TO generator_source_values
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE GENERATOR project_generator_state
        USING MATERIALIZED STATE generator_source_values
        EACH 100ms
        UNBRANCHED
        TO generated_values
          SET id = relay_state.generator_source_values.id,
              route = 'original',
              value = relay_state.generator_source_values.value,
              tick = now()
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        TO generated_values
          SET id = relay_state.generator_source_values.id,
              route = 'doubled',
              value = relay_state.generator_source_values.value * 2,
              tick = now()
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION generated_values_subscription TO generated_values;
      START;
      """
    When http payload is posted to node "node-1" with host "generator-snapshot-{{test_id}}.example.com" path "/values"
      """
      {"id":"source-1","value":7}
      """
    Then within "5s" generated routes "original" value 7 and "doubled" value 14 share field "tick"

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |

  Scenario Outline: Generator message errors expose state and partial output to standard SET functions
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA generator_error_source (
        id STRING,
        divisor I64
      );
      CREATE SCHEMA generator_error_output (
        id STRING,
        result I64
      );
      CREATE SCHEMA generator_error_record (
        id STRING,
        code STRING,
        attempted_id STRING OPTIONAL
      );
      CREATE STRICT WIRE JSON SCHEMA generator_error_wire (
        id string,
        divisor integer
      );
      CREATE CODEC generator_error_codec
        FROM WIRE JSON SCHEMA generator_error_wire
        TO SCHEMA generator_error_source;
      CREATE RELAY generator_error_sources
        SCHEMA generator_error_source
        UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY generator_error_outputs SCHEMA generator_error_output UNBRANCHED;
      CREATE RELAY generator_error_records SCHEMA generator_error_record UNBRANCHED;
      CREATE VHOST edge generator-errors-{{test_id}}.example.com;
      CREATE ENDPOINT generator_error_endpoint ON edge PATH '/values' TYPE HTTP;
      CREATE INGESTOR generator_error_ingestor
        FROM ENDPOINT generator_error_endpoint MODE NO_ACK SEQUENTIAL
        DECODE USING generator_error_codec
        TIMESTAMP NOW
        TO generator_error_sources
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE GENERATOR failing_generator
        USING MATERIALIZED STATE generator_error_sources
        EACH 100ms
        UNBRANCHED
        TO generator_error_outputs
          SET id = relay_state.generator_error_sources.id,
              result = 10 / relay_state.generator_error_sources.divisor
          FLUSH IMMEDIATE
          ON MESSAGE ERROR SEND TO generator_error_records
          SET id = relay_state.generator_error_sources.id,
              code = upper(error.code),
              attempted_id = partial_output.id;
      CREATE SUBSCRIPTION generator_error_subscription TO generator_error_records;
      START;
      """
    When http payload is posted to node "node-1" with host "generator-errors-{{test_id}}.example.com" path "/values"
      """
      {"id":"generator-division-by-zero","divisor":0}
      """
    Then within "5s" the relay subscription receives a payload
      """
      "attempted_id":"generator-division-by-zero","code":"EVALUATION","id":"generator-division-by-zero"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
