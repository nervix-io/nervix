Feature: Web console visual emitter creation

  @visual_emitter_http
  Scenario Outline: A visually created HTTP emitter sends its selected body and computed header
    Given HTTP receiver "visual_api" is running
    And HTTP receiver "visual_api" answers unscripted requests with "respond 204"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_event (message STRING);
      CREATE WIRE JSON SCHEMA visual_wire MODE STRICT (message string);
      CREATE WIRE JSON CODEC visual_codec FROM WIRE SCHEMA visual_wire TO SCHEMA visual_event;
      CREATE RELAY visual_input SCHEMA visual_event UNBRANCHED;
      CREATE VHOST visual_host visual-emitter-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT visual_endpoint ON visual_host PATH '/input';
      CREATE HTTP ENDPOINT INGESTOR visual_ingest FROM visual_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_codec
        TO visual_input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT visual_api CONFIG {
        'endpoint' = '{{http_receiver.visual_api}}', 'timeout_ms' = 5000
      };
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='emitter']" is clicked
    And selector ".create-name" is filled with "visual_http_output"
    And selector ".create-emitter-entity [data-entity='http-request']" is clicked
    And selector ".create-emitter-reference .create-choice-search" is filled with "visual_api"
    Then selector ".create-emitter-reference [data-value='visual_api']" exists
    When selector ".create-emitter-reference [data-value='visual_api']" is clicked
    And selector ".create-emitter-input .create-choice-search" is filled with "visual_input"
    Then selector ".create-emitter-input [data-value='visual_input']" exists
    When selector ".create-emitter-input [data-value='visual_input']" is clicked
    And selector ".create-emitter-method" is filled with "'POST'"
    And selector ".create-emitter-path" is filled with "'/visual'"
    And selector ".create-emitter-publishing [data-mode='ACK']" is clicked
    And selector ".create-emitter-retry-backoff" is filled with "100ms"
    And selector ".create-emitter-retry-max" is filled with "1s"
    And selector ".create-emitter-body [data-body='codec']" is clicked
    And selector ".create-emitter-codec .create-choice-search" is filled with "visual_codec"
    Then selector ".create-emitter-codec [data-value='visual_codec']" exists
    When selector ".create-emitter-codec [data-value='visual_codec']" is clicked
    And selector ".create-emitter-body [data-body='<body>']" is clicked
    And selector ".create-emitter-inherit [data-inherit='<inherit>']" is clicked
    And selector ".create-emitter-add-invocation" is clicked
    And selector ".create-emitter-invocation-function" is filled with "write_header"
    And selector ".create-emitter-add-argument" is clicked
    And selector ".create-emitter-invocation-argument" is filled with "'X-Visual'"
    And selector ".create-emitter-add-argument" is clicked
    And selector ".create-emitter-invocation-arguments [data-argument='1'] textarea" is filled with "input.message"
    And selector ".create-emitter-flush [data-flush='immediate']" is clicked
    And selector ".create-emitter-message-error [data-error='log']" is clicked
    And selector ".create-emitter-general-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE HTTP REQUEST EMITTER visual_http_output"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus commands are executed on the leader node
      """
      START;
      """
    And http payload is posted to host "visual-emitter-{{test_id}}.example.com" path "/input"
      """
      {"message":"created visually"}
      """
    Then HTTP receiver "visual_api" eventually receives at least 1 requests
    And HTTP receiver "visual_api" request 1 is
      """
      POST /visual
      X-Visual: created visually

      <payload>
      """

    Examples:
      | cluster_size | body         | inherit | payload                        |
      | 1            | without-body | none    |                                |
      | 3            | without-body | none    |                                |
      | 1            | codec        | all     | {"message":"created visually"} |
      | 3            | codec        | all     | {"message":"created visually"} |

  @visual_emitter_incomplete
  Scenario Outline: An incomplete emitter remains editable with a precise validation error
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='emitter']" is clicked
    And selector ".create-name" is filled with "visual_incomplete_emitter"
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "emitter entity type"
    And selector ".create-name" has value "visual_incomplete_emitter"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_emitter_kafka
  Scenario Outline: A visually created confirmed Kafka emitter publishes from inputs with different branches
    Given Kafka is running
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "visual_output_{{test_id}}" exists with 1 partitions
    And Kafka topic "visual_output_{{test_id}}" is observed
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_event (user_id I64, action STRING);
      CREATE WIRE JSON SCHEMA visual_wire MODE STRICT (user_id integer, action string);
      CREATE WIRE JSON CODEC visual_codec FROM WIRE SCHEMA visual_wire TO SCHEMA visual_event;
      CREATE RELAY visual_input SCHEMA visual_event UNBRANCHED;
      CREATE VHOST visual_host visual-emitter-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT visual_endpoint ON visual_host PATH '/input';
      CREATE HTTP ENDPOINT INGESTOR visual_ingest FROM visual_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_codec
        TO visual_input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE KAFKA CLIENT visual_client CONFIG {'bootstrap.servers' = '{{kafka_addr}}'};
      CREATE SCHEMA visual_key (user_id I64);
      CREATE BRANCH visual_branch SCHEMA visual_key TTL 5m;
      CREATE RELAY visual_branched SCHEMA visual_event BRANCHED BY visual_branch;
      CREATE HTTP ENDPOINT visual_other_endpoint ON visual_host PATH '/other';
      CREATE HTTP ENDPOINT INGESTOR visual_other_ingest FROM visual_other_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_codec
        TO visual_branched INHERIT ALL BRANCHED BY visual_branch SET user_id = message.user_id
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='emitter']" is clicked
    And selector ".create-name" is filled with "visual_output"
    And selector ".create-emitter-entity [data-entity='kafka-topic']" is clicked
    And selector ".create-emitter-reference .create-choice-search" is filled with "visual_client"
    Then selector ".create-emitter-reference [data-value='visual_client']" exists
    When selector ".create-emitter-reference [data-value='visual_client']" is clicked
    And selector ".create-emitter-input .create-choice-search" is filled with "visual_input"
    Then selector ".create-emitter-input [data-value='visual_input']" exists
    When selector ".create-emitter-input [data-value='visual_input']" is clicked
    And selector ".create-emitter-locator" is filled with "visual_output_{{test_id}}"
    And selector ".create-emitter-publishing [data-mode='ACK']" is clicked
    And selector ".create-emitter-window [data-window='parallel']" is clicked
    And selector ".create-emitter-ack-max" is filled with "0"
    And selector ".create-emitter-ack-timeout" is filled with "5s"
    And selector ".create-emitter-add-input" is clicked
    And selector ".create-emitter-input .create-choice-search" is filled with "visual_branched"
    Then selector ".create-emitter-input [data-value='visual_branched']" exists
    When selector ".create-emitter-input [data-value='visual_branched']" is clicked
    And selector ".create-emitter-codec .create-choice-search" is filled with "visual_codec"
    Then selector ".create-emitter-codec [data-value='visual_codec']" exists
    When selector ".create-emitter-codec [data-value='visual_codec']" is clicked
    And selector ".create-emitter-inherit [data-inherit='all']" is clicked
    And selector ".create-emitter-retry-backoff" is filled with "100ms"
    And selector ".create-emitter-retry-max" is filled with "1s"
    And selector ".create-emitter-flush [data-flush='immediate']" is clicked
    And selector ".create-emitter-message-error [data-error='log']" is clicked
    And selector ".create-emitter-general-error [data-error='log']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-validation" contains "ACK maximum"
    When selector ".create-emitter-ack-max" is filled with "2"
    And selector ".create-emitter-collect" is clicked
    And selector ".create-emitter-collect-for" is filled with "100ms"
    And selector ".create-emitter-collect-size" is filled with "0B"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Failed"
    And selector ".create-error" contains "must be greater than zero"
    When selector ".create-emitter-collect-size" is filled with "1MiB"
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus commands are executed on the leader node
      """
      START;
      """
    And http payload is posted to host "visual-emitter-{{test_id}}.example.com" path "/input"
      """
      {"user_id":42,"action":"visual"}
      """
    And http payload is posted to host "visual-emitter-{{test_id}}.example.com" path "/other"
      """
      {"user_id":43,"action":"branched"}
      """
    Then within "30s" the observed broker receives exactly these payloads
      """
      {"user_id":42,"action":"visual"}
      {"user_id":43,"action":"branched"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_emitter_postgres
  Scenario Outline: A visually created Postgres emitter inserts ordered values into a provisioned table
    Given Postgres is running
    And Postgres table "VisualOutput_{{test_id}}" exists in schema "Sales_{{test_id}}"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_event (user_id I64, action STRING);
      CREATE WIRE JSON SCHEMA visual_wire MODE STRICT (user_id integer, action string);
      CREATE WIRE JSON CODEC visual_codec FROM WIRE SCHEMA visual_wire TO SCHEMA visual_event;
      CREATE RELAY visual_input SCHEMA visual_event UNBRANCHED;
      CREATE VHOST visual_host visual-emitter-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT visual_endpoint ON visual_host PATH '/input';
      CREATE HTTP ENDPOINT INGESTOR visual_ingest FROM visual_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_codec
        TO visual_input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE POSTGRES CLIENT visual_client POOL SIZE MIN 1 MAX 2 CONFIG {'addr' = '{{postgres_addr}}'};
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='emitter']" is clicked
    And selector ".create-name" is filled with "visual_output"
    And selector ".create-emitter-entity [data-entity='postgres-table']" is clicked
    And selector ".create-emitter-reference .create-choice-search" is filled with "visual_client"
    Then selector ".create-emitter-reference [data-value='visual_client']" exists
    When selector ".create-emitter-reference [data-value='visual_client']" is clicked
    And selector ".create-emitter-input .create-choice-search" is filled with "visual_input"
    Then selector ".create-emitter-input [data-value='visual_input']" exists
    When selector ".create-emitter-input [data-value='visual_input']" is clicked
    And selector ".create-emitter-locator" is filled with "Sales_{{test_id}}.VisualOutput_{{test_id}}"
    And selector ".create-emitter-publishing [data-mode='ACK']" is clicked
    And selector ".create-emitter-values .create-emitter-add-value" is clicked
    And selector ".create-emitter-values [data-row='0'] .create-emitter-value-name" is filled with "postgres_user_id"
    And selector ".create-emitter-values [data-row='0'] .create-emitter-value-expression" is filled with "input.user_id"
    And selector ".create-emitter-values .create-emitter-add-value" is clicked
    And selector ".create-emitter-values [data-row='1'] .create-emitter-value-name" is filled with "postgres_action"
    And selector ".create-emitter-values [data-row='1'] .create-emitter-value-expression" is filled with "input.action"
    And selector ".create-emitter-batch-messages" is filled with "2"
    And selector ".create-emitter-batch-size" is filled with "1MiB"
    And selector ".create-emitter-retry-backoff" is filled with "100ms"
    And selector ".create-emitter-retry-max" is filled with "1s"
    And selector ".create-emitter-flush [data-flush='immediate']" is clicked
    And selector ".create-emitter-message-error [data-error='log']" is clicked
    And selector ".create-emitter-general-error [data-error='log']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus commands are executed on the leader node
      """
      START;
      """
    And http payload is posted to host "visual-emitter-{{test_id}}.example.com" path "/input"
      """
      {"user_id":42,"action":"visual"}
      """
    Then the Postgres table eventually contains a row
      """
      {"postgres_user_id":42,"postgres_action":"visual"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_emitter_iceberg
  Scenario Outline: A visually created Iceberg emitter commits values to a provisioned table
    Given Iceberg dependencies are running
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And Iceberg table "visual_output_{{test_id}}" exists at "s3://nervix-iceberg/tables/visual_output_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_event (user_id I64, action STRING);
      CREATE WIRE JSON SCHEMA visual_wire MODE STRICT (user_id integer, action string);
      CREATE WIRE JSON CODEC visual_codec FROM WIRE SCHEMA visual_wire TO SCHEMA visual_event;
      CREATE RELAY visual_input SCHEMA visual_event UNBRANCHED;
      CREATE VHOST visual_host visual-emitter-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT visual_endpoint ON visual_host PATH '/input';
      CREATE HTTP ENDPOINT INGESTOR visual_ingest FROM visual_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_codec
        TO visual_input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE S3 CLIENT visual_client CONFIG {
        'endpoint' = '{{rustfs_addr}}', 'region' = 'us-east-1',
        'access_key_id' = 'rustfsadmin', 'secret_access_key' = 'rustfsadmin', 'path_style_access' = true
      };
      CREATE ICEBERG_REST CLIENT visual_catalog CONFIG {'uri' = '{{iceberg_rest_addr}}', 'warehouse' = 's3://nervix-iceberg/warehouse'};
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='emitter']" is clicked
    And selector ".create-name" is filled with "visual_output"
    And selector ".create-emitter-entity [data-entity='iceberg-table']" is clicked
    And selector ".create-emitter-reference .create-choice-search" is filled with "visual_client"
    Then selector ".create-emitter-reference [data-value='visual_client']" exists
    When selector ".create-emitter-reference [data-value='visual_client']" is clicked
    And selector ".create-emitter-input .create-choice-search" is filled with "visual_input"
    Then selector ".create-emitter-input [data-value='visual_input']" exists
    When selector ".create-emitter-input [data-value='visual_input']" is clicked
    And selector ".create-emitter-locator" is filled with "visual_output_{{test_id}}"
    And selector ".create-emitter-namespace" is filled with "{{domain}}"
    And selector ".create-emitter-location" is filled with "s3://nervix-iceberg/tables/visual_output_{{test_id}}"
    And selector ".create-emitter-catalog .create-choice-search" is filled with "visual_catalog"
    Then selector ".create-emitter-catalog [data-value='visual_catalog']" exists
    When selector ".create-emitter-catalog [data-value='visual_catalog']" is clicked
    And selector ".create-emitter-commit-each" is filled with "100ms"
    And selector ".create-emitter-commit-size" is filled with "1MiB"
    And selector ".create-emitter-publishing [data-mode='ACK']" is clicked
    And selector ".create-emitter-values .create-emitter-add-value" is clicked
    And selector ".create-emitter-values [data-row='0'] .create-emitter-value-name" is filled with "user_id"
    And selector ".create-emitter-values [data-row='0'] .create-emitter-value-expression" is filled with "input.user_id"
    And selector ".create-emitter-values .create-emitter-add-value" is clicked
    And selector ".create-emitter-values [data-row='1'] .create-emitter-value-name" is filled with "action"
    And selector ".create-emitter-values [data-row='1'] .create-emitter-value-expression" is filled with "input.action"
    And selector ".create-emitter-retry-backoff" is filled with "100ms"
    And selector ".create-emitter-retry-max" is filled with "1s"
    And selector ".create-emitter-flush [data-flush='immediate']" is clicked
    And selector ".create-emitter-message-error [data-error='log']" is clicked
    And selector ".create-emitter-general-error [data-error='log']" is clicked
    And selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus commands are executed on the leader node
      """
      START;
      """
    And http payload is posted to host "visual-emitter-{{test_id}}.example.com" path "/input"
      """
      {"user_id":42,"action":"visual"}
      """
    Then the Iceberg table "visual_output_{{test_id}}" eventually contains a row
      """
      {"user_id":42,"action":"visual"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
  @visual_emitter_sensitivity
  Scenario Outline: A visual HTTP emitter requires explicit leakage of a sensitive inherited field
    Given HTTP receiver "visual_api" is running
    And HTTP receiver "visual_api" answers unscripted requests with "respond 204"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_event (message STRING);
      CREATE SCHEMA visual_secret (message STRING SENSITIVE);
      CREATE WIRE JSON SCHEMA visual_wire MODE STRICT (message string);
      CREATE WIRE JSON CODEC visual_codec FROM WIRE SCHEMA visual_wire TO SCHEMA visual_event;
      CREATE WIRE JSON CODEC visual_input_codec FROM WIRE SCHEMA visual_wire TO SCHEMA visual_secret;
      CREATE RELAY visual_input SCHEMA visual_secret UNBRANCHED;
      CREATE VHOST visual_host visual-emitter-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT visual_endpoint ON visual_host PATH '/input';
      CREATE HTTP ENDPOINT INGESTOR visual_ingest FROM visual_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_input_codec
        TO visual_input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT visual_api CONFIG {
        'endpoint' = '{{http_receiver.visual_api}}', 'timeout_ms' = 5000
      };
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='emitter']" is clicked
    And selector ".create-name" is filled with "visual_http_output"
    And selector ".create-emitter-entity [data-entity='http-request']" is clicked
    And selector ".create-emitter-reference .create-choice-search" is filled with "visual_api"
    Then selector ".create-emitter-reference [data-value='visual_api']" exists
    When selector ".create-emitter-reference [data-value='visual_api']" is clicked
    And selector ".create-emitter-input .create-choice-search" is filled with "visual_input"
    Then selector ".create-emitter-input [data-value='visual_input']" exists
    When selector ".create-emitter-input [data-value='visual_input']" is clicked
    And selector ".create-emitter-method" is filled with "'POST'"
    And selector ".create-emitter-path" is filled with "'/visual'"
    And selector ".create-emitter-publishing [data-mode='ACK']" is clicked
    And selector ".create-emitter-retry-backoff" is filled with "100ms"
    And selector ".create-emitter-retry-max" is filled with "1s"
    And selector ".create-emitter-body [data-body='codec']" is clicked
    And selector ".create-emitter-codec .create-choice-search" is filled with "visual_codec"
    Then selector ".create-emitter-codec [data-value='visual_codec']" exists
    When selector ".create-emitter-codec [data-value='visual_codec']" is clicked
    And selector ".create-emitter-inherit [data-inherit='fields']" is clicked
    And selector ".create-emitter-inherit-field .create-choice-search" is filled with "message"
    Then selector ".create-emitter-inherit-field [data-value='message']" contains "SENSITIVE"
    When selector ".create-emitter-inherit-field [data-value='message']" is clicked
    And selector ".create-emitter-flush [data-flush='immediate']" is clicked
    And selector ".create-emitter-message-error [data-error='log']" is clicked
    And selector ".create-emitter-general-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE HTTP REQUEST EMITTER visual_http_output"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Failed"
    When selector ".create-emitter-inherited-field .create-emitter-leak" is clicked
    Then selector ".create-preview" contains "LEAK SENSITIVE"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus commands are executed on the leader node
      """
      START;
      """
    And http payload is posted to host "visual-emitter-{{test_id}}.example.com" path "/input"
      """
      {"message":"created visually"}
      """
    Then HTTP receiver "visual_api" eventually receives at least 1 requests
    And HTTP receiver "visual_api" request 1 is
      """
      POST /visual

      <payload>
      """

    Examples:
      | cluster_size | payload                        |
      | 1            | {"message":"created visually"} |
      | 3            | {"message":"created visually"} |

  @visual_emitter_route_error
  Scenario Outline: A visual HTTP emitter sends a refused record through its selected error relay
    Given HTTP receiver "visual_api" is running
    And HTTP receiver "visual_api" answers unscripted requests with "respond 204"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_event (message STRING);
      CREATE WIRE JSON SCHEMA visual_wire MODE STRICT (message string);
      CREATE WIRE JSON CODEC visual_codec FROM WIRE SCHEMA visual_wire TO SCHEMA visual_event;
      CREATE RELAY visual_input SCHEMA visual_event UNBRANCHED;
      CREATE SCHEMA visual_error (reason STRING);
      CREATE RELAY visual_errors SCHEMA visual_error UNBRANCHED;
      CREATE VHOST visual_host visual-emitter-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT visual_endpoint ON visual_host PATH '/input';
      CREATE HTTP ENDPOINT INGESTOR visual_ingest FROM visual_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_codec
        TO visual_input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT visual_api CONFIG {
        'endpoint' = '{{http_receiver.visual_api}}', 'timeout_ms' = 5000
      };
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='emitter']" is clicked
    And selector ".create-name" is filled with "visual_http_output"
    And selector ".create-emitter-entity [data-entity='http-request']" is clicked
    And selector ".create-emitter-reference .create-choice-search" is filled with "visual_api"
    Then selector ".create-emitter-reference [data-value='visual_api']" exists
    When selector ".create-emitter-reference [data-value='visual_api']" is clicked
    And selector ".create-emitter-input .create-choice-search" is filled with "visual_input"
    Then selector ".create-emitter-input [data-value='visual_input']" exists
    When selector ".create-emitter-input [data-value='visual_input']" is clicked
    And selector ".create-emitter-method" is filled with "input.message"
    And selector ".create-emitter-path" is filled with "'/visual'"
    And selector ".create-emitter-publishing [data-mode='ACK']" is clicked
    And selector ".create-emitter-retry-backoff" is filled with "100ms"
    And selector ".create-emitter-retry-max" is filled with "1s"
    And selector ".create-emitter-body [data-body='without-body']" is clicked
    And selector ".create-emitter-inherit [data-inherit='none']" is clicked
    And selector ".create-emitter-flush [data-flush='immediate']" is clicked
    And selector ".create-emitter-message-error [data-error='send-to']" is clicked
    And selector ".create-emitter-error-relay .create-choice-search" is filled with "visual_errors"
    Then selector ".create-emitter-error-relay [data-value='visual_errors']" exists
    When selector ".create-emitter-error-relay [data-value='visual_errors']" is clicked
    And selector ".create-emitter-error-field .create-choice-search" is filled with "reason"
    Then selector ".create-emitter-error-field [data-value='reason']" exists
    When selector ".create-emitter-error-field [data-value='reason']" is clicked
    And selector ".create-emitter-assignment-expression" is filled with "error.message"
    And selector ".create-emitter-general-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE HTTP REQUEST EMITTER visual_http_output"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus commands are executed on the leader node
      """
      CREATE SUBSCRIPTION visual_rejections TO visual_errors;
      START;
      """
    And http payload is posted to host "visual-emitter-{{test_id}}.example.com" path "/input"
      """
      {"message":"BAD METHOD"}
      """
    Then within "5s" the relay subscription receives payloads containing all fragments
      """
      "reason":
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @visual_emitter_transaction
  Scenario: A visual emitter uses a client staged earlier in its transaction
    Given HTTP receiver "visual_api" is running
    And HTTP receiver "visual_api" answers unscripted requests with "respond 204"
    And a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_event (message STRING);
      CREATE WIRE JSON SCHEMA visual_wire MODE STRICT (message string);
      CREATE WIRE JSON CODEC visual_codec FROM WIRE SCHEMA visual_wire TO SCHEMA visual_event;
      CREATE RELAY visual_input SCHEMA visual_event UNBRANCHED;
      CREATE VHOST visual_host visual-emitter-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT visual_endpoint ON visual_host PATH '/input';
      CREATE HTTP ENDPOINT INGESTOR visual_ingest FROM visual_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_codec
        TO visual_input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT visual_api CONFIG {
        'endpoint' = '{{http_receiver.visual_api}}', 'timeout_ms' = 5000
      };
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".prompt-row input" is filled with "BEGIN;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".prompt-row" contains "{{domain}} tx"
    When selector ".prompt-row input" is filled with "CREATE HTTP CLIENT staged_api CONFIG {'endpoint' = '{{http_receiver.visual_api}}', 'timeout_ms' = 5000};"
    And selector ".prompt-row input" is pressed with "Enter"
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='emitter']" is clicked
    And selector ".create-name" is filled with "visual_http_output"
    And selector ".create-emitter-entity [data-entity='http-request']" is clicked
    And selector ".create-emitter-reference .create-choice-search" is filled with "staged_api"
    Then selector ".create-emitter-reference [data-value='staged_api']" exists
    When selector ".create-emitter-reference [data-value='staged_api']" is clicked
    And selector ".create-emitter-input .create-choice-search" is filled with "visual_input"
    Then selector ".create-emitter-input [data-value='visual_input']" exists
    When selector ".create-emitter-input [data-value='visual_input']" is clicked
    And selector ".create-emitter-method" is filled with "'POST'"
    And selector ".create-emitter-path" is filled with "'/visual'"
    And selector ".create-emitter-publishing [data-mode='ACK']" is clicked
    And selector ".create-emitter-retry-backoff" is filled with "100ms"
    And selector ".create-emitter-retry-max" is filled with "1s"
    And selector ".create-emitter-body [data-body='without-body']" is clicked
    And selector ".create-emitter-inherit [data-inherit='none']" is clicked
    And selector ".create-emitter-flush [data-flush='immediate']" is clicked
    And selector ".create-emitter-message-error [data-error='log']" is clicked
    And selector ".create-emitter-general-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE HTTP REQUEST EMITTER visual_http_output"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Queued in transaction"
    When selector ".create-close" is clicked
    And selector ".transaction-indicator" is clicked
    Then selector ".transaction-inspector .inspector-summary" contains "2 accepted"
    And selector ".transaction-inspector .inspector-summary" contains "COMPLETE"
    When selector ".transaction-inspector button[aria-label='Close transaction inspector']" is clicked
    And selector ".prompt-row input" is filled with "COMMIT;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".transaction-indicator" eventually disappears
    When selector ".prompt-row input" is filled with "SHOW CREATE EMITTER visual_http_output;"
    And selector ".prompt-row input" is pressed with "Enter"
    Then selector ".terminal" contains "TO staged_api"

  @visual_emitter_reconnect
  Scenario: An emitter draft survives a leader reconnection and publishes after creation
    Given HTTP receiver "visual_api" is running
    And HTTP receiver "visual_api" answers unscripted requests with "respond 204"
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_event (message STRING);
      CREATE WIRE JSON SCHEMA visual_wire MODE STRICT (message string);
      CREATE WIRE JSON CODEC visual_codec FROM WIRE SCHEMA visual_wire TO SCHEMA visual_event;
      CREATE RELAY visual_input SCHEMA visual_event UNBRANCHED;
      CREATE VHOST visual_host visual-emitter-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT visual_endpoint ON visual_host PATH '/input';
      CREATE HTTP ENDPOINT INGESTOR visual_ingest FROM visual_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_codec
        TO visual_input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT visual_api CONFIG {
        'endpoint' = '{{http_receiver.visual_api}}', 'timeout_ms' = 5000
      };
      """
    Then the current leader node is saved as placeholder "old_leader"
    And a node other than placeholder "old_leader" is saved as placeholder "new_leader"
    When leadership is transferred from node "{{new_leader}}" to node "{{old_leader}}"
    And the web console is opened on node "{{old_leader}}"
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='emitter']" is clicked
    And selector ".create-name" is filled with "visual_http_output"
    And selector ".create-emitter-entity [data-entity='http-request']" is clicked
    And selector ".create-emitter-reference .create-choice-search" is filled with "visual_api"
    Then selector ".create-emitter-reference [data-value='visual_api']" exists
    When selector ".create-emitter-reference [data-value='visual_api']" is clicked
    And selector ".create-emitter-input .create-choice-search" is filled with "visual_input"
    Then selector ".create-emitter-input [data-value='visual_input']" exists
    When selector ".create-emitter-input [data-value='visual_input']" is clicked
    And selector ".create-emitter-method" is filled with "'POST'"
    And selector ".create-emitter-path" is filled with "'/visual'"
    And selector ".create-emitter-publishing [data-mode='ACK']" is clicked
    And selector ".create-emitter-retry-backoff" is filled with "100ms"
    And selector ".create-emitter-retry-max" is filled with "1s"
    And selector ".create-emitter-body [data-body='codec']" is clicked
    And selector ".create-emitter-codec .create-choice-search" is filled with "visual_codec"
    Then selector ".create-emitter-codec [data-value='visual_codec']" exists
    When selector ".create-emitter-codec [data-value='visual_codec']" is clicked
    And selector ".create-emitter-body [data-body='codec']" is clicked
    And selector ".create-emitter-inherit [data-inherit='all']" is clicked
    And selector ".create-emitter-add-invocation" is clicked
    And selector ".create-emitter-invocation-function" is filled with "write_header"
    And selector ".create-emitter-add-argument" is clicked
    And selector ".create-emitter-invocation-argument" is filled with "'X-Visual'"
    And selector ".create-emitter-add-argument" is clicked
    And selector ".create-emitter-invocation-arguments [data-argument='1'] textarea" is filled with "input.message"
    And selector ".create-emitter-flush [data-flush='immediate']" is clicked
    And selector ".create-emitter-message-error [data-error='log']" is clicked
    And selector ".create-emitter-general-error [data-error='log']" is clicked
    Then selector ".create-preview" contains "CREATE HTTP REQUEST EMITTER visual_http_output"
    When leadership is transferred from node "{{old_leader}}" to node "{{new_leader}}"
    Then node "{{old_leader}}" eventually reports leader "{{new_leader}}"
    And selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".create-preview" contains "TO visual_api"
    When selector ".create-submit" is clicked
    Then selector ".create-status" contains "Completed"
    When selector ".create-close" is clicked
    And these Plexus commands are executed on the leader node
      """
      START;
      """
    And http payload is posted to host "visual-emitter-{{test_id}}.example.com" path "/input"
      """
      {"message":"created visually"}
      """
    Then HTTP receiver "visual_api" eventually receives at least 1 requests
    And HTTP receiver "visual_api" request 1 is
      """
      POST /visual
      X-Visual: created visually

      {"message":"created visually"}
      """

  @visual_emitter_scope
  Scenario: Moving a retained emitter draft to another domain invalidates its references
    Given HTTP receiver "visual_api" is running
    And HTTP receiver "visual_api" answers unscripted requests with "respond 204"
    And a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}}_other;
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA visual_event (message STRING);
      CREATE WIRE JSON SCHEMA visual_wire MODE STRICT (message string);
      CREATE WIRE JSON CODEC visual_codec FROM WIRE SCHEMA visual_wire TO SCHEMA visual_event;
      CREATE RELAY visual_input SCHEMA visual_event UNBRANCHED;
      CREATE VHOST visual_host visual-emitter-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT visual_endpoint ON visual_host PATH '/input';
      CREATE HTTP ENDPOINT INGESTOR visual_ingest FROM visual_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING visual_codec
        TO visual_input INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT visual_api CONFIG {
        'endpoint' = '{{http_receiver.visual_api}}', 'timeout_ms' = 5000
      };
      """
    And the web console is opened on the leader node
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='emitter']" is clicked
    And selector ".create-name" is filled with "visual_http_output"
    And selector ".create-emitter-entity [data-entity='http-request']" is clicked
    And selector ".create-emitter-reference .create-choice-search" is filled with "visual_api"
    Then selector ".create-emitter-reference [data-value='visual_api']" exists
    When selector ".create-emitter-reference [data-value='visual_api']" is clicked
    And selector ".create-emitter-input .create-choice-search" is filled with "visual_input"
    Then selector ".create-emitter-input [data-value='visual_input']" exists
    When selector ".create-emitter-input [data-value='visual_input']" is clicked
    And selector ".create-emitter-method" is filled with "'POST'"
    And selector ".create-emitter-path" is filled with "'/visual'"
    And selector ".create-emitter-publishing [data-mode='ACK']" is clicked
    And selector ".create-emitter-retry-backoff" is filled with "100ms"
    And selector ".create-emitter-retry-max" is filled with "1s"
    And selector ".create-emitter-body [data-body='without-body']" is clicked
    And selector ".create-emitter-inherit [data-inherit='none']" is clicked
    And selector ".create-emitter-flush [data-flush='immediate']" is clicked
    And selector ".create-emitter-message-error [data-error='log']" is clicked
    And selector ".create-emitter-general-error [data-error='log']" is clicked
    When selector ".create-close" is clicked
    And selector ".domain-select" is clicked
    And selector ".domain-menu [data-domain='{{domain}}_other']" is clicked
    And selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='emitter']" is clicked
    Then selector ".create-scope" contains "{{domain}}"
    When selector ".create-scope-change" is clicked
    Then selector ".create-scope" contains "{{domain}}_other"
    And selector ".create-reference-invalid" exists
    When selector ".create-submit" is clicked
    Then selector ".create-validation" contains "emitter reference"
