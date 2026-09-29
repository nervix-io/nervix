Feature: HTTP emitter inspection, errors and metrics
  DESCRIBE EMITTER reports why an HTTP request is still pending for as long as it stays pending,
  and message errors, runtime events and emitter status name the operation and the status of a
  failure without an evaluated target, a header value or a body. The sent counters count each
  delivered record once, however many attempts it took, and never a rejected one; payload bytes
  count the codec body's record and never the request's method, target or headers.

  @http_emitter_inspection
  Scenario Outline: HTTP emitters with and without a body report a pending failure safely and count each delivered record once
    Given HTTP receiver "encoded_api" is running
    And HTTP receiver "encoded_api" answers with
      """
      respond 404
      respond 503
      hold response until released
      """
    And HTTP receiver "encoded_api" answers unscripted requests with "respond 204"
    And HTTP receiver "empty_api" is running
    And HTTP receiver "empty_api" answers with
      """
      respond 404
      respond 503
      hold response until released
      """
    And HTTP receiver "empty_api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING, token STRING SENSITIVE, payload STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA event_body (event_id STRING, payload STRING);
      CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (event_id string, payload string);
      CREATE CODEC event_body_codec FROM WIRE JSON SCHEMA event_body_wire TO SCHEMA event_body;
      CREATE SCHEMA rejected_request (
        emitter STRING,
        event_id STRING,
        error_code STRING,
        operation STRING,
        operation_index U32 OPTIONAL,
        error_message STRING
      );
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY rejected_requests SCHEMA rejected_request UNBRANCHED;
      CREATE VHOST edge http-emitter-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR outgoing_source
        FROM ENDPOINT outgoing_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT encoded_client TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.encoded_api}}', 'timeout_ms' = 60000
      };
      CREATE CLIENT empty_client TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.empty_api}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER encoded FROM outgoing
        TO HTTP encoded_client
          METHOD 'POST'
          PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms
          ENCODE USING event_body_codec
        INHERIT event_id, payload
        INVOKE write_header('Authorization', leak_sensitive(input.token)),
               write_header('X-Padding', repeat('x', 200))
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_requests
          SET emitter = 'encoded',
              event_id = input.event_id,
              error_code = error.code,
              operation = error.operation,
              operation_index = error.operation_index,
              error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE EMITTER empty FROM outgoing
        TO HTTP empty_client
          METHOD 'POST'
          PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms
          WITHOUT BODY
        INVOKE write_header('Authorization', leak_sensitive(input.token)),
               write_header('X-Padding', repeat('x', 200))
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_requests
          SET emitter = 'empty',
              event_id = input.event_id,
              error_code = error.code,
              operation = error.operation,
              operation_index = error.operation_index,
              error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_requests_subscription TO rejected_requests;
      START;
      """
    # The leading space makes record evt-0's token an invalid header value, so both emitters reject
    # it before sending anything. Record evt-1 is refused with 404, and record evt-2 answers 503
    # before its retry is held until the scenario releases it.
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"evt-0","token":" s3cr3t-token-0","payload":"zero"},{"event_id":"evt-1","token":"s3cr3t-token-1","payload":"one"},{"event_id":"evt-2","token":"s3cr3t-token-2","payload":"two"}]
      """
    # Each error message is matched through its closing quote, so it holds nothing else.
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "emitter":"encoded" | "event_id":"evt-0" | "error_code":"validation" | "operation":"invoke" | "operation_index":0 | "error_message":"emitter 'encoded' cannot publish its HTTP request: header value is invalid"
      "emitter":"empty" | "event_id":"evt-0" | "error_code":"validation" | "operation":"invoke" | "operation_index":0 | "error_message":"emitter 'empty' cannot publish its HTTP request: header value is invalid"
      "emitter":"encoded" | "event_id":"evt-1" | "error_code":"external" | "operation":"publish" | "error_message":"HTTP endpoint answered with status 404"
      "emitter":"empty" | "event_id":"evt-1" | "error_code":"external" | "operation":"publish" | "error_message":"HTTP endpoint answered with status 404"
      """
    And HTTP receiver "encoded_api" eventually receives at least 3 requests
    And HTTP receiver "empty_api" eventually receives at least 3 requests
    And HTTP receiver "encoded_api" request 2 is
      """
      POST /events/evt-2
      Authorization: s3cr3t-token-2

      {"event_id":"evt-2","payload":"two"}
      """
    And HTTP receiver "encoded_api" request 3 repeats request 2
    And HTTP receiver "empty_api" request 2 is
      """
      POST /events/evt-2
      Authorization: s3cr3t-token-2
      """
    And HTTP receiver "empty_api" request 3 repeats request 2
    # The retried request is held unresolved, so the failure that made it pending stays reported,
    # naming its status and neither the explicitly leaked token nor the evaluated target.
    And within "30s" DESCRIBE EMITTER "encoded" on the leader node contains
      """
      transient error: HTTP endpoint answered with retryable status 503
      reconnect backoff: 100ms
      """
    And the last command output owner is saved as placeholder "encoded_owner"
    And the last command output does not contain
      """
      s3cr3t
      """
    And the last command output does not contain
      """
      evt-2
      """
    And within "30s" DESCRIBE EMITTER "empty" on the leader node contains
      """
      transient error: HTTP endpoint answered with retryable status 503
      reconnect backoff: 100ms
      """
    And the last command output owner is saved as placeholder "empty_owner"
    And the last command output does not contain
      """
      s3cr3t
      """
    And the last command output does not contain
      """
      evt-2
      """
    And within "30s" the active session observes a server error containing
      """
      HTTP endpoint answered with retryable status 503
      """
    And the last server error does not contain
      """
      s3cr3t
      """
    And the last server error does not contain
      """
      evt-2
      """
    When HTTP receiver "encoded_api" releases its held responses with "respond 204"
    And HTTP receiver "empty_api" releases its held responses with "respond 204"
    # Record evt-2 took two attempts and counts once; the rejected record evt-1 is not sent. The
    # codec body's record is well under the 200-byte header the request also carried, and a
    # request without a body carries no payload bytes.
    Then within "30s" DESCRIBE EMITTER "encoded" on the leader node contains
      """
      messages_total sent relay=outgoing physical_node={{encoded_owner}} total=1
      """
    And the last command output contains
      """
      transient error: -
      """
    And the last command output metric "messages_total" "sent" relay "outgoing" physical node "{{encoded_owner}}" has values
      """
      total=1
      """
    And the last command output metric "batches_total" "sent" relay "outgoing" physical node "{{encoded_owner}}" has values
      """
      total=1
      """
    And the last command output metric "bytes_total" "sent" relay "outgoing" physical node "{{encoded_owner}}" has numeric values
      """
      total<=64
      """
    And within "30s" DESCRIBE EMITTER "empty" on the leader node contains
      """
      messages_total sent relay=outgoing physical_node={{empty_owner}} total=1
      """
    And the last command output contains
      """
      transient error: -
      """
    And the last command output metric "messages_total" "sent" relay "outgoing" physical node "{{empty_owner}}" has values
      """
      total=1
      """
    And the last command output metric "bytes_total" "sent" relay "outgoing" physical node "{{empty_owner}}" has values
      """
      total=0
      """
    And node "{{encoded_owner}}" observability metric "nervix_bytes_total" with labels eventually reaches at least 1
      """
      target_kind="EMITTER"
      target="encoded"
      direction="sent"
      relay="outgoing"
      """
    And node "{{empty_owner}}" observability metric "nervix_messages_total" with labels eventually equals 1
      """
      target_kind="EMITTER"
      target="empty"
      direction="sent"
      relay="outgoing"
      """
    And node "{{empty_owner}}" observability metric "nervix_bytes_total" with labels eventually equals 0
      """
      target_kind="EMITTER"
      target="empty"
      direction="sent"
      relay="outgoing"
      """
    And HTTP receiver "encoded_api" has captured exactly 3 requests
    And HTTP receiver "empty_api" has captured exactly 3 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_inspection
  Scenario Outline: ON GENERAL ERROR IGNORE never delivers an HTTP request whose endpoint has not accepted it
    Given Kafka is running
    And HTTP receiver "api" is running
    And HTTP receiver "api" answers with
      """
      respond 503
      hold response until released
      """
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "http_ignore_in_{{test_id}}" exists with 1 partitions
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}',
        'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR kafka_events
        FROM KAFKA kafka_main TOPIC http_ignore_in_{{test_id}}
          OFFSET BY CONSUMER GROUP http_ignore_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 1s RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING outbound_events_codec
        TO outgoing
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 60000
      };
      CREATE ATTACHED EMITTER published FROM outgoing
        TO HTTP api METHOD 'POST' PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR IGNORE;
      START;
      """
    Then Kafka consumer group "http_ignore_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "http_ignore_in_{{test_id}}"
      """
      [{"event_id":"1"}]
      """
    Then HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 2 repeats request 1
    # The retried request is held until the scenario releases it, so it is pending for as long as
    # these steps observe it: the emitter reports why, and the general error policy acknowledges
    # nothing, so the attached source's offset stays uncommitted past its one-second ACK TIMEOUT.
    And within "30s" DESCRIBE EMITTER "published" on the leader node contains
      """
      transient error: HTTP endpoint answered with retryable status 503
      """
    And within "3s" Kafka consumer group "http_ignore_group_{{test_id}}" next offset for topic "http_ignore_in_{{test_id}}" partition 0 is "below 1"
    When HTTP receiver "api" releases its held responses with "respond 204"
    Then within "30s" Kafka consumer group "http_ignore_group_{{test_id}}" next offset for topic "http_ignore_in_{{test_id}}" partition 0 is "at least 1"
    And within "30s" DESCRIBE EMITTER "published" on the leader node contains
      """
      transient error: -
      """
    And HTTP receiver "api" has captured exactly 2 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
