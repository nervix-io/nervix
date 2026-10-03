Feature: HTTP emitter response classification
  One HTTP request is sent for each eligible relay record. The emitter answers a definitive
  refusal through the message error route and continues with later records.

  @http_emitter_responses
  Scenario Outline: HTTP terminal response <status> rejects only its record
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers with
      """
      respond <status>; header Location: /not-followed
      respond 204
      """
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA rejected_request (event_id STRING, code STRING, operation STRING, message STRING);
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY rejected_requests SCHEMA rejected_request UNBRANCHED;
      CREATE VHOST edge http-emitter-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR outgoing_source
        FROM ENDPOINT outgoing_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE EMITTER published FROM outgoing
        TO HTTP api
          METHOD 'POST'
          PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_requests
          SET event_id = input.event_id,
              code = error.code,
              operation = error.operation,
              message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_requests_subscription TO rejected_requests;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"},{"event_id":"2"}]
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      "event_id":"1" | "code":"external" | "operation":"publish" | "message":"HTTP endpoint answered with status <status>"
      """
    And HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 1 is
      """
      POST /events/1
      """
    And HTTP receiver "api" request 2 is
      """
      POST /events/2
      """
    And HTTP receiver "api" has captured exactly 2 requests

    Examples:
      | cluster_size | status |
      | 1            | 101    |
      | 3            | 101    |
      | 1            | 301    |
      | 3            | 301    |
      | 1            | 304    |
      | 3            | 304    |
      | 1            | 400    |
      | 3            | 400    |
      | 1            | 404    |
      | 3            | 404    |
      | 1            | 409    |
      | 3            | 409    |
      | 1            | 413    |
      | 3            | 413    |

  @http_emitter_responses
  Scenario Outline: HTTP retryable response <status> retains the current request and reports it while it is pending
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers with
      """
      respond <status>; header WWW-Authenticate: Basic realm=guarded
      hold response until released
      """
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE VHOST edge http-emitter-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR outgoing_source
        FROM ENDPOINT outgoing_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER published FROM outgoing
        TO HTTP api
          METHOD 'POST'
          PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          WITHOUT BODY
        INVOKE write_header('X-Event', input.event_id)
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"},{"event_id":"2"}]
      """
    Then HTTP receiver "api" eventually receives at least 2 requests
    # The retry is held unresolved, so the answer that left record 1 pending stays reported.
    And within "30s" DESCRIBE EMITTER "published" on the leader node contains
      """
      transient error: HTTP endpoint answered with <failure> <status>
      """
    When HTTP receiver "api" releases its held responses with "respond 204"
    Then HTTP receiver "api" eventually receives at least 3 requests
    And HTTP receiver "api" request 1 is
      """
      POST /events/1
      X-Event: 1
      Accept: */*
      """
    And HTTP receiver "api" request 2 repeats request 1
    And HTTP receiver "api" request 2 arrived at least "200ms" after request 1
    And HTTP receiver "api" request 3 is
      """
      POST /events/2
      X-Event: 2
      """
    And HTTP receiver "api" request 1 has no header "Accept-Encoding"
    And HTTP receiver "api" request 1 has no header "Content-Type"
    And HTTP receiver "api" has captured exactly 3 requests

    Examples:
      | cluster_size | status | failure                                |
      | 1            | 401    | authentication or authorization status |
      | 3            | 401    | authentication or authorization status |
      | 1            | 403    | authentication or authorization status |
      | 3            | 403    | authentication or authorization status |
      | 1            | 407    | authentication or authorization status |
      | 3            | 407    | authentication or authorization status |
      | 1            | 408    | retryable status                       |
      | 3            | 408    | retryable status                       |
      | 1            | 425    | retryable status                       |
      | 3            | 425    | retryable status                       |
      | 1            | 429    | retryable status                       |
      | 3            | 429    | retryable status                       |
      | 1            | 500    | retryable status                       |
      | 3            | 500    | retryable status                       |
      | 1            | 503    | retryable status                       |
      | 3            | 503    | retryable status                       |
      | 1            | 599    | retryable status                       |
      | 3            | 599    | retryable status                       |

  @http_emitter_responses
  Scenario Outline: Valid HTTP response headers <case> deliver without reading a body
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers with
      """
      <response>
      """
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE VHOST edge http-emitter-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR outgoing_source
        FROM ENDPOINT outgoing_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE EMITTER published FROM outgoing
        TO HTTP api
          METHOD 'POST'
          PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"},{"event_id":"2"}]
      """
    Then HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 1 is
      """
      POST /events/1
      """
    And HTTP receiver "api" request 2 is
      """
      POST /events/2
      """
    And HTTP receiver "api" has captured exactly 2 requests

    Examples:
      | cluster_size | case                   | response                                            |
      | 1            | 200                    | respond 200                                         |
      | 3            | 200                    | respond 200                                         |
      | 1            | 202                    | respond 202                                         |
      | 3            | 202                    | respond 202                                         |
      | 1            | 204                    | respond 204                                         |
      | 3            | 204                    | respond 204                                         |
      | 1            | 128 fields             | respond 204; extra headers 128                      |
      | 3            | 128 fields             | respond 204; extra headers 128                      |
      | 1            | 64 KiB fields          | respond 204; header value bytes 65522               |
      | 3            | 64 KiB fields          | respond 204; header value bytes 65522               |
      | 1            | bounded interim fields | respond 204; interim 103; interim extra headers 128 |
      | 3            | bounded interim fields | respond 204; interim 103; interim extra headers 128 |

  @http_emitter_responses
  Scenario Outline: A successful response whose body <case> is neither awaited nor read to its end
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers with
      """
      <response>
      """
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE VHOST edge http-emitter-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR outgoing_source
        FROM ENDPOINT outgoing_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE EMITTER published FROM outgoing
        TO HTTP api
          METHOD 'POST'
          PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"},{"event_id":"2"}]
      """
    # Complete successful headers deliver record 1, so record 2 follows at once and record 1 is
    # never sent again. The receiver can finish record 1's body only if its client reads it, and it
    # is the client that closes the connection instead.
    Then HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 1 is
      """
      POST /events/1
      """
    And HTTP receiver "api" request 2 is
      """
      POST /events/2
      """
    And HTTP receiver "api" eventually sees the client abandon at least 1 unfinished response
    And HTTP receiver "api" has captured exactly 2 requests

    Examples:
      | cluster_size | case         | response                              |
      | 1            | stalls       | respond 200; body ignored; stall body |
      | 3            | stalls       | respond 200; body ignored; stall body |
      | 1            | holds 32 MiB | respond 200; body bytes 33554432      |
      | 3            | holds 32 MiB | respond 200; body bytes 33554432      |

  @http_emitter_responses
  Scenario Outline: Invalid HTTP response headers <case> fail the attempt before delivery and are reported while the request is pending
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers with
      """
      <response>
      hold response until released
      """
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE VHOST edge http-emitter-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR outgoing_source
        FROM ENDPOINT outgoing_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 60000
      };
      CREATE EMITTER published FROM outgoing
        TO HTTP api
          METHOD 'POST'
          PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"},{"event_id":"2"}]
      """
    Then HTTP receiver "api" eventually receives at least 2 requests
    # The retry is held unresolved, so the response that failed record 1's attempt stays reported.
    And within "30s" DESCRIBE EMITTER "published" on the leader node contains
      """
      transient error: HTTP response header exchange failed: HTTP response headers <failure>
      """
    When HTTP receiver "api" releases its held responses with "respond 204"
    Then HTTP receiver "api" eventually receives at least 3 requests
    And HTTP receiver "api" request 1 is
      """
      POST /events/1
      """
    And HTTP receiver "api" request 2 repeats request 1
    And HTTP receiver "api" request 3 is
      """
      POST /events/2
      """
    And HTTP receiver "api" has captured exactly 3 requests

    Examples:
      | cluster_size | case                       | response                                                   | failure                     |
      | 1            | 129 fields                 | respond 204; extra headers 129                             | exceed 128 fields or 64 KiB |
      | 3            | 129 fields                 | respond 204; extra headers 129                             | exceed 128 fields or 64 KiB |
      | 1            | 129 fields with status 200 | respond 200; extra headers 129                             | exceed 128 fields or 64 KiB |
      | 3            | 129 fields with status 200 | respond 200; extra headers 129                             | exceed 128 fields or 64 KiB |
      | 1            | over 64 KiB fields         | respond 204; header value bytes 65523                      | exceed 128 fields or 64 KiB |
      | 3            | over 64 KiB fields         | respond 204; header value bytes 65523                      | exceed 128 fields or 64 KiB |
      | 1            | 129 interim fields         | respond 204; interim 103; interim extra headers 129        | exceed 128 fields or 64 KiB |
      | 3            | 129 interim fields         | respond 204; interim 103; interim extra headers 129        | exceed 128 fields or 64 KiB |
      | 1            | over 64 KiB interim        | respond 204; interim 103; interim header value bytes 65523 | exceed 128 fields or 64 KiB |
      | 3            | over 64 KiB interim        | respond 204; interim 103; interim header value bytes 65523 | exceed 128 fields or 64 KiB |
      | 1            | malformed final framing    | respond 200; header Content-Length: nonsense               | are malformed               |
      | 3            | malformed final framing    | respond 200; header Content-Length: nonsense               | are malformed               |
      | 1            | malformed final name       | respond 200; header Bad Name: value                        | are malformed               |
      | 3            | malformed final name       | respond 200; header Bad Name: value                        | are malformed               |
      | 1            | outside the status range   | respond 600                                                | are malformed               |
      | 3            | outside the status range   | respond 600                                                | are malformed               |
