Feature: HTTP emitter
  An HTTP emitter sends one request for each eligible relay record to an operator-provisioned
  endpoint, with an independently configured method, path, headers, and body. See
  docs/specifications/http-emitter.md and tests/http-emitter-acceptance-ledger.md.

  @http_emitter_configuration
  Scenario Outline: HTTP emitter body selections round-trip through public configuration
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event (event_id STRING, request_method STRING, request_path STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (
        event_id string, request_method string, request_path string
      );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = 'http://127.0.0.1:19080',
        'timeout_ms' = 5000
      };
      CREATE EMITTER encoded FROM outgoing TO HTTP api
        METHOD input.request_method PATH input.request_path
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING event_codec INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER empty FROM outgoing TO HTTP api
        METHOD 'DELETE' PATH input.request_path
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        WITHOUT BODY INVOKE write_header('X-Event', input.event_id)
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then SHOW CREATE EMITTER on the leader node renders these clauses
      | emitter | clause                      |
      | encoded | METHOD input.request_method |
      | encoded | PATH input.request_path     |
      | encoded | ENCODE USING event_codec    |
      | empty   | WITHOUT BODY                |
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE EMITTER encoded;
      """
    Then the last command output contains
      """
      codec: event_codec
      body: codec
      sink: HTTP client=api method=input.request_method path=input.request_path
      batch: none
      flush: FLUSH IMMEDIATE
      publishing mode: ACK RETRY POLICY BACKOFF 250ms MAX 30s
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE EMITTER empty;
      """
    Then the last command output contains
      """
      codec: none
      body: without body
      sink: HTTP client=api method='DELETE' path=input.request_path
      batch: none
      flush: FLUSH IMMEDIATE
      publishing mode: ACK RETRY POLICY BACKOFF 250ms MAX 30s
      """
    When these NSPL commands fail with "HTTP METHOD and PATH require exact non-sensitive STRING values"
      """
      CREATE EMITTER invalid_method FROM outgoing TO HTTP api
        METHOD 42 PATH '/events'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER EMITTER encoded SET CLIENT api,
        SET MODE ACK RETRY POLICY BACKOFF 500ms MAX 30s,
        SET ENCODE USING event_codec;
      """
    Then SHOW CREATE EMITTER on the leader node renders these clauses
      | emitter | clause                             |
      | encoded | RETRY POLICY BACKOFF 500ms MAX 30s |
      | encoded | ENCODE USING event_codec           |
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE EMITTER encoded;
      """
    Then the last command output contains
      """
      codec: event_codec
      body: codec
      sink: HTTP client=api method=input.request_method path=input.request_path
      batch: none
      flush: FLUSH IMMEDIATE
      publishing mode: ACK RETRY POLICY BACKOFF 500ms MAX 30s
      """
    When these NSPL commands fail with "emitter body selection does not support the retained construction"
      """
      ALTER EMITTER encoded SET TO HTTP api
        METHOD 'DELETE' PATH '/events'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s WITHOUT BODY;
      """
    Then SHOW CREATE EMITTER on the leader node renders these clauses
      | emitter | clause                   |
      | encoded | ENCODE USING event_codec |
    When these NSPL commands fail with "HTTP emitters select an absent body"
      """
      ALTER EMITTER empty DROP ENCODE;
      """
    When these NSPL commands fail with "HTTP emitters send one request per record"
      """
      ALTER EMITTER empty SET BATCH MAX MESSAGES 2 MAX SIZE 1MiB;
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER EMITTER empty SET TO HTTP api
        METHOD 'HEAD' PATH '/health'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s WITHOUT BODY;
      """
    Then SHOW CREATE EMITTER on the leader node renders these clauses
      | emitter | clause         |
      | empty   | METHOD 'HEAD'  |
      | empty   | PATH '/health' |
      | empty   | WITHOUT BODY   |
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE EMITTER empty;
      """
    Then the last command output contains
      """
      codec: none
      body: without body
      sink: HTTP client=api method='HEAD' path='/health'
      batch: none
      flush: FLUSH IMMEDIATE
      publishing mode: ACK RETRY POLICY BACKOFF 250ms MAX 30s
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_configuration @http_emitter_timeout
  Scenario Outline: HTTP emitter requires an explicit usable client timeout before activation
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (id STRING);
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE CLIENT api TYPE HTTP CONFIG {'endpoint' = 'https://api.example.com'};
      """
    When these NSPL commands fail with "timeout_ms"
      """
      CREATE EMITTER invalid FROM outgoing TO HTTP api
        METHOD 'POST' PATH '/events'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_configuration @http_emitter_validation
  Scenario Outline: HTTP emitter rejects unsafe literal request fields and implicit header leakage
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (
        id STRING, secret STRING SENSITIVE, optional_path STRING OPTIONAL, attempts I64
      );
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE SCHEMA body (id STRING);
      CREATE WIRE JSON SCHEMA body_wire MODE STRICT (id string);
      CREATE CODEC body_codec FROM WIRE JSON SCHEMA body_wire TO SCHEMA body;
      CREATE SCHEMA sensitive_body (id STRING, secret STRING SENSITIVE);
      CREATE WIRE JSON SCHEMA sensitive_body_wire MODE STRICT (id string, secret string);
      CREATE CODEC sensitive_body_codec
        FROM WIRE JSON SCHEMA sensitive_body_wire TO SCHEMA sensitive_body;
      CREATE SCHEMA state_snapshot (path STRING);
      CREATE RELAY latest_path SCHEMA state_snapshot UNBRANCHED
        WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = 'https://api.example.com', 'timeout_ms' = 5000
      };
      CREATE CLIENT other TYPE SENTRY CONFIG {
        'dsn' = 'http://public@127.0.0.1:8000/1'
      };
      """
    When these NSPL commands fail with "requires a HTTP client"
      """
      CREATE EMITTER invalid FROM outgoing TO HTTP other
        METHOD 'POST' PATH '/events'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "no matching USING MATERIALIZED STATE declaration"
      """
      CREATE EMITTER invalid FROM outgoing TO HTTP api
        METHOD 'POST' PATH relay_state.latest_path.path
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "header"
      """
      CREATE EMITTER invalid FROM outgoing TO HTTP api
        METHOD 'POST' PATH '/events'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s WITHOUT BODY
        INVOKE write_header('Authorization', input.secret)
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "sensitive"
      """
      CREATE EMITTER invalid FROM outgoing TO HTTP api
        METHOD 'POST' PATH '/events'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING sensitive_body_codec
        INHERIT id, secret
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "publish method"
      """
      CREATE EMITTER invalid FROM outgoing TO HTTP api
        METHOD 'TRACE' PATH '/events'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "publish path"
      """
      CREATE EMITTER invalid FROM outgoing TO HTTP api
        METHOD 'POST' PATH '//other.example/events'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "header name"
      """
      CREATE EMITTER invalid FROM outgoing TO HTTP api
        METHOD 'POST' PATH '/events'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s WITHOUT BODY
        INVOKE write_header('Host', 'other.example')
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "header value"
      """
      CREATE EMITTER invalid FROM outgoing TO HTTP api
        METHOD 'POST' PATH '/events'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s WITHOUT BODY
        INVOKE write_header('X-Reason', ' bad')
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "STRING"
      """
      CREATE EMITTER invalid FROM outgoing TO HTTP api
        METHOD input.attempts PATH '/events'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "null"
      """
      CREATE EMITTER invalid FROM outgoing TO HTTP api
        METHOD 'POST' PATH input.optional_path
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "output"
      """
      CREATE EMITTER invalid FROM outgoing TO HTTP api
        METHOD 'POST' PATH output.id
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "WITHOUT BODY"
      """
      CREATE EMITTER invalid FROM outgoing TO HTTP api
        METHOD 'GET' PATH '/events'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING body_codec
        INHERIT id
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE EMITTER allowed FROM outgoing TO HTTP api
        METHOD 'POST' PATH '/events'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s WITHOUT BODY
        INVOKE write_header('Authorization', leak_sensitive(input.secret))
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT unsafe_endpoint TYPE HTTP CONFIG {
        'endpoint' = 'https://key@api.example.com', 'timeout_ms' = 5000
      };
      CREATE CLIENT zero_timeout TYPE HTTP CONFIG {
        'endpoint' = 'https://api.example.com', 'timeout_ms' = 0
      };
      CREATE CLIENT text_timeout TYPE HTTP CONFIG {
        'endpoint' = 'https://api.example.com', 'timeout_ms' = 'invalid'
      };
      CREATE CLIENT overflow_timeout TYPE HTTP CONFIG {
        'endpoint' = 'https://api.example.com',
        'timeout_ms' = '18446744073709551616'
      };
      CREATE CLIENT incomplete_tls TYPE HTTP CONFIG {
        'endpoint' = 'https://api.example.com', 'timeout_ms' = 5000,
        'tls_cert_file' = '/tmp/client.pem'
      };
      CREATE EMITTER encoded FROM outgoing TO HTTP api
        METHOD 'POST' PATH output.id
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING body_codec
        INHERIT id
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER leaked_body FROM outgoing TO HTTP api
        METHOD 'POST' PATH '/events'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING sensitive_body_codec
        INHERIT id SET secret = leak_sensitive(input.secret)
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER with_state FROM outgoing
        USING MATERIALIZED STATE latest_path REQUIRED SKIP
        TO HTTP api METHOD 'POST' PATH relay_state.latest_path.path
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these NSPL commands fail with "origin"
      """
      ALTER EMITTER allowed SET CLIENT unsafe_endpoint;
      """
    When these NSPL commands fail with "timeout_ms"
      """
      ALTER EMITTER allowed SET CLIENT zero_timeout;
      """
    When these NSPL commands fail with "timeout_ms"
      """
      ALTER EMITTER allowed SET CLIENT text_timeout;
      """
    When these NSPL commands fail with "timeout_ms"
      """
      ALTER EMITTER allowed SET CLIENT overflow_timeout;
      """
    When these NSPL commands fail with "tls_key_file"
      """
      ALTER EMITTER allowed SET CLIENT incomplete_tls;
      """
    Then SHOW CREATE EMITTER on the leader node renders these clauses
      | emitter | clause      |
      | allowed | TO HTTP api |

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_requests
  Scenario Outline: An HTTP emitter sends each record with its own method, path, headers, and codec body
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (
        event_id STRING,
        request_method STRING,
        request_path STRING,
        tenant STRING,
        payload STRING
      );
      CREATE WIRE JSON SCHEMA outbound_event_wire MODE STRICT (
        event_id string,
        request_method string,
        request_path string,
        tenant string,
        payload string
      );
      CREATE CODEC outbound_event_codec
        FROM WIRE JSON SCHEMA outbound_event_wire
        TO SCHEMA outbound_event;
      CREATE SCHEMA event_body (event_id STRING, payload STRING);
      CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (
        event_id string,
        payload string
      );
      CREATE CODEC event_body_codec
        FROM WIRE JSON SCHEMA event_body_wire
        TO SCHEMA event_body;
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE VHOST edge http-emitter-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR outgoing_source
        FROM ENDPOINT outgoing_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_event_codec
        TO outgoing
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT api
        TYPE HTTP
        CONFIG {
          'endpoint' = '{{http_receiver.api}}',
          'timeout_ms' = 5000
        };
      CREATE ATTACHED EMITTER deliver_event
        FROM outgoing
        TO HTTP api
          METHOD input.request_method
          PATH input.request_path
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING event_body_codec
        INHERIT event_id, payload
        INVOKE write_header('Content-Type', 'application/json'),
               write_header('X-Tenant', input.tenant),
               write_header('Idempotency-Key', input.event_id)
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      {"event_id":"42","request_method":"PATCH","request_path":"/v1/events/42?notify=true","tenant":"north","payload":"first"}
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      {"event_id":"43","request_method":"POST","request_path":"/v2/tenants/south/events","tenant":"south","payload":"second"}
      """
    Then HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 1 is
      """
      PATCH /v1/events/42?notify=true
      Content-Type: application/json
      X-Tenant: north
      Idempotency-Key: 42

      {"event_id":"42","payload":"first"}
      """
    And HTTP receiver "api" request 2 is
      """
      POST /v2/tenants/south/events
      Content-Type: application/json
      X-Tenant: south
      Idempotency-Key: 43

      {"event_id":"43","payload":"second"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_requests
  Scenario Outline: An HTTP emitter declared without a body sends zero content bytes
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (
        event_id STRING,
        request_method STRING,
        request_path STRING,
        tenant STRING,
        payload STRING
      );
      CREATE WIRE JSON SCHEMA outbound_event_wire MODE STRICT (
        event_id string,
        request_method string,
        request_path string,
        tenant string,
        payload string
      );
      CREATE CODEC outbound_event_codec
        FROM WIRE JSON SCHEMA outbound_event_wire
        TO SCHEMA outbound_event;
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE VHOST edge http-emitter-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR outgoing_source
        FROM ENDPOINT outgoing_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_event_codec
        TO outgoing
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT api
        TYPE HTTP
        CONFIG {
          'endpoint' = '{{http_receiver.api}}',
          'timeout_ms' = 5000
        };
      CREATE EMITTER delete_event
        FROM outgoing WHERE input.request_method = 'DELETE'
        TO HTTP api
          METHOD 'DELETE'
          PATH input.request_path
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          WITHOUT BODY
        INVOKE write_header('Idempotency-Key', input.event_id)
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      {"event_id":"7","request_method":"DELETE","request_path":"/v1/events/7","tenant":"north","payload":"ignored"}
      """
    Then HTTP receiver "api" eventually receives at least 1 request
    And HTTP receiver "api" request 1 is
      """
      DELETE /v1/events/7
      Idempotency-Key: 7
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_requests
  Scenario Outline: HTTP emitters send constant requests, and GET and HEAD only without a body
    Given HTTP receiver "posted" is running
    And HTTP receiver "posted" answers unscripted requests with "respond 202"
    And HTTP receiver "probed" is running
    And HTTP receiver "computed" is running
    And HTTP receiver "computed" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING, request_method STRING, payload STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA event_body (event_id STRING, payload STRING);
      CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (event_id string, payload string);
      CREATE CODEC event_body_codec FROM WIRE JSON SCHEMA event_body_wire TO SCHEMA event_body;
      CREATE SCHEMA rejected_request (
        event_id STRING, error_code STRING, operation STRING, affected_fields <fields_type>
      );
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY rejected_requests SCHEMA rejected_request UNBRANCHED;
      CREATE VHOST edge http-emitter-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR outgoing_source
        FROM ENDPOINT outgoing_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT posted_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.posted}}', 'timeout_ms' = 5000
      };
      CREATE CLIENT probed_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.probed}}', 'timeout_ms' = 5000
      };
      CREATE CLIENT computed_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.computed}}', 'timeout_ms' = 5000
      };
      CREATE EMITTER post_event FROM outgoing
        TO HTTP posted_api
          METHOD 'POST'
          PATH '/v1/events'
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING event_body_codec
        INHERIT event_id, payload
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER probe_event FROM outgoing
        TO HTTP probed_api
          METHOD input.request_method
          PATH concat('/v1/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE EMITTER put_event FROM outgoing
        TO HTTP computed_api
          METHOD input.request_method
          PATH concat('/v1/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING event_body_codec
        INHERIT event_id, payload
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_requests
          SET event_id = input.event_id,
              error_code = error.code,
              operation = error.operation,
              affected_fields = error.fields
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_requests_subscription TO rejected_requests;
      START;
      """
    # GET and HEAD are sent only by the emitter without a body. The emitter with a codec body
    # rejects both records before publication and sends only the PUT.
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1","request_method":"GET","payload":"first"},{"event_id":"2","request_method":"HEAD","payload":"second"},{"event_id":"3","request_method":"PUT","payload":"third"}]
      """
    Then HTTP receiver "posted" eventually receives at least 3 requests
    And HTTP receiver "posted" request 1 is
      """
      POST /v1/events

      {"event_id":"1","payload":"first"}
      """
    And HTTP receiver "posted" request 2 is
      """
      POST /v1/events

      {"event_id":"2","payload":"second"}
      """
    And HTTP receiver "posted" request 3 is
      """
      POST /v1/events

      {"event_id":"3","payload":"third"}
      """
    And HTTP receiver "probed" eventually receives at least 3 requests
    And HTTP receiver "probed" request 1 is
      """
      GET /v1/events/1
      """
    And HTTP receiver "probed" request 2 is
      """
      HEAD /v1/events/2
      """
    And HTTP receiver "probed" request 3 is
      """
      PUT /v1/events/3
      """
    And within "30s" the relay subscription receives payloads containing all fragments
      """
      "event_id":"1" | "error_code":"validation" | "operation":"publish" | "affected_fields":["input.request_method","method"]
      "event_id":"2" | "error_code":"validation" | "operation":"publish" | "affected_fields":["input.request_method","method"]
      """
    And HTTP receiver "computed" eventually receives at least 1 request
    And HTTP receiver "computed" request 1 is
      """
      PUT /v1/events/3

      {"event_id":"3","payload":"third"}
      """
    And HTTP receiver "computed" has captured exactly 1 request

    Examples:
      | cluster_size | fields_type |
      | 1            | VEC<STRING> |
      | 3            | VEC<STRING> |

  @http_emitter_requests
  Scenario Outline: HTTP request fields read the original input and the finalized output, and a filtered record evaluates none of them
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING, tenant STRING, payload STRING, divisor I64);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA event_body (event_id STRING, payload STRING);
      CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (event_id string, payload string);
      CREATE CODEC event_body_codec FROM WIRE JSON SCHEMA event_body_wire TO SCHEMA event_body;
      CREATE SCHEMA rejected_request (
        event_id STRING, error_code STRING, operation STRING, operation_index U32 OPTIONAL
      );
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY rejected_requests SCHEMA rejected_request UNBRANCHED;
      CREATE VHOST edge http-emitter-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR outgoing_source
        FROM ENDPOINT outgoing_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE EMITTER shaped FROM outgoing
        TO HTTP api
          METHOD 'PUT'
          PATH concat('/v1/', output.payload, '/', event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING event_body_codec
        INHERIT event_id
        SET payload = upper(input.payload)
        WHERE input.tenant != 'skipped'
        INVOKE write_header('X-Source-Payload', input.payload),
               write_header('X-Message-Payload', message.payload),
               write_header('X-Ratio', coalesce(TRY_CAST(100 / input.divisor AS STRING), 'none'))
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_requests
          SET event_id = input.event_id,
              error_code = error.code,
              operation = error.operation,
              operation_index = error.operation_index
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_requests_subscription TO rejected_requests;
      START;
      """
    # Record 2 is filtered by the route: its path would contain a fragment and its ratio would
    # divide by zero, but no request field is evaluated for it. Record 3 passes the route, and its
    # ratio fails.
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1","tenant":"north","payload":"first","divisor":4},{"event_id":"2","tenant":"skipped","payload":"a#b","divisor":0},{"event_id":"3","tenant":"south","payload":"third","divisor":0},{"event_id":"4","tenant":"west","payload":"fourth","divisor":5}]
      """
    Then HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 1 is
      """
      PUT /v1/FIRST/1
      X-Source-Payload: first
      X-Message-Payload: FIRST
      X-Ratio: 25

      {"event_id":"1","payload":"FIRST"}
      """
    And HTTP receiver "api" request 2 is
      """
      PUT /v1/FOURTH/4
      X-Source-Payload: fourth
      X-Message-Payload: FOURTH
      X-Ratio: 20

      {"event_id":"4","payload":"FOURTH"}
      """
    And within "30s" the relay subscription receives payloads containing all fragments
      """
      "event_id":"3" | "error_code":"evaluation" | "operation":"invoke" | "operation_index":2
      """
    And the relay subscription does not receive a payload containing fragments within "3s"
      """
      "event_id":"2"
      """
    And HTTP receiver "api" has captured exactly 2 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_requests
  Scenario Outline: A record whose HTTP body cannot be encoded sends no part of its request, and its error reads the original input, the captured state and the attempted body
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING, tenant STRING, payload STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA route (route_key STRING, prefix STRING);
      CREATE CODEC routes_codec FROM JSON TO SCHEMA route
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA event_body (event_id STRING, payload STRING);
      CREATE CODEC guarded_body_codec FROM JSON TO SCHEMA event_body
        WITH JAQ TRANSFORMATIONS
          ON EMITTING 'if .event_id == "2" then error("unencodable") else {event_id: .event_id, payload: .payload} end';
      CREATE SCHEMA rejected_request (
        event_id STRING,
        tenant STRING,
        error_code STRING,
        error_message STRING,
        operation STRING,
        state_prefix STRING,
        attempted_payload STRING OPTIONAL
      );
      CREATE RELAY routes SCHEMA route UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY rejected_requests SCHEMA rejected_request UNBRANCHED;
      CREATE VHOST edge http-emitter-{{test_id}}.example.com;
      CREATE ENDPOINT routes_ingress ON edge PATH '/routes' TYPE HTTP;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR routes_source
        FROM ENDPOINT routes_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING routes_codec
        TO routes
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR outgoing_source
        FROM ENDPOINT outgoing_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE EMITTER guarded FROM outgoing
        USING MATERIALIZED STATE routes REQUIRED WAIT
        TO HTTP api
          METHOD 'POST'
          PATH concat(relay_state.routes.prefix, '/events/', event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING guarded_body_codec
        INHERIT event_id
        SET payload = upper(input.payload)
        INVOKE write_header('X-Tenant', input.tenant)
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_requests
          SET event_id = input.event_id,
              tenant = input.tenant,
              error_code = error.code,
              error_message = error.message,
              operation = error.operation,
              state_prefix = relay_state.routes.prefix,
              attempted_payload = partial_output.payload
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_requests_subscription TO rejected_requests;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/routes"
      """
      [{"route_key":"main","prefix":"/tenants/north"}]
      """
    # Every request field of record 2 is valid when it is admitted. Its codec fails when the flush
    # encodes its body, so neither its method, its path, its header nor any body is sent. Its error
    # reads the tenant, which only the original input holds.
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1","tenant":"north","payload":"first"},{"event_id":"2","tenant":"south","payload":"second"},{"event_id":"3","tenant":"west","payload":"third"}]
      """
    # The JAQ transformation writes its JSON with a space after each separator, and each body is
    # exactly those bytes.
    Then HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 1 is
      """
      POST /tenants/north/events/1
      X-Tenant: north

      {"event_id": "1", "payload": "FIRST"}
      """
    And HTTP receiver "api" request 2 is
      """
      POST /tenants/north/events/3
      X-Tenant: west

      {"event_id": "3", "payload": "THIRD"}
      """
    And within "30s" the relay subscription receives payloads containing all fragments
      """
      "event_id":"2" | "tenant":"south" | "error_code":"external" | "operation":"encode" | "state_prefix":"/tenants/north" | "attempted_payload":"SECOND" | emitter 'guarded' failed to encode record
      """
    And HTTP receiver "api" has captured exactly 2 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_requests
  Scenario Outline: HTTP header writes replace without case and every invalid write rejects its record without a request
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And HTTP receiver "crowded" is running
    And HTTP receiver "crowded" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (
        event_id STRING,
        replacement STRING,
        header_name STRING,
        header_value STRING,
        first_bytes I64,
        second_bytes I64,
        crowd BOOL
      );
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA rejected_request (
        event_id STRING,
        error_code STRING,
        operation STRING,
        operation_index U32 OPTIONAL,
        affected_fields <fields_type>
      );
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY rejected_requests SCHEMA rejected_request UNBRANCHED;
      CREATE VHOST edge http-emitter-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR outgoing_source
        FROM ENDPOINT outgoing_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE CLIENT crowded_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.crowded}}', 'timeout_ms' = 5000
      };
      CREATE EMITTER headed FROM outgoing WHERE NOT input.crowd
        TO HTTP api
          METHOD 'POST'
          PATH concat('/headers/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          WITHOUT BODY
        INVOKE write_header('X-Replaced', input.replacement),
               write_header('x-replaced', 'final'),
               write_header('X-Empty', ''),
               write_header('X-Spaced', 'a  b'),
               write_header(input.header_name, input.header_value),
               write_header('X-First', repeat('a', input.first_bytes)),
               write_header('X-Second', repeat('b', input.second_bytes))
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_requests
          SET event_id = input.event_id,
              error_code = error.code,
              operation = error.operation,
              operation_index = error.operation_index,
              affected_fields = error.fields
        ON GENERAL ERROR LOG;
      CREATE EMITTER crowded FROM outgoing
        TO HTTP crowded_api
          METHOD 'POST'
          PATH concat('/crowded/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          WITHOUT BODY
        INVOKE write_header(CASE WHEN input.crowd THEN 'X-1' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-2' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-3' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-4' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-5' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-6' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-7' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-8' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-9' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-10' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-11' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-12' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-13' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-14' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-15' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-16' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-17' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-18' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-19' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-20' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-21' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-22' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-23' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-24' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-25' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-26' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-27' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-28' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-29' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-30' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-31' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-32' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-33' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-34' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-35' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-36' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-37' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-38' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-39' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-40' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-41' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-42' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-43' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-44' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-45' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-46' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-47' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-48' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-49' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-50' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-51' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-52' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-53' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-54' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-55' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-56' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-57' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-58' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-59' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-60' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-61' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-62' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-63' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-64' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-65' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-66' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-67' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-68' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-69' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-70' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-71' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-72' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-73' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-74' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-75' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-76' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-77' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-78' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-79' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-80' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-81' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-82' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-83' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-84' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-85' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-86' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-87' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-88' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-89' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-90' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-91' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-92' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-93' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-94' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-95' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-96' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-97' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-98' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-99' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-100' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-101' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-102' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-103' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-104' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-105' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-106' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-107' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-108' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-109' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-110' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-111' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-112' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-113' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-114' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-115' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-116' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-117' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-118' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-119' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-120' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-121' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-122' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-123' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-124' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-125' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-126' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-127' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-128' ELSE 'X-Same' END, ''),
               write_header(CASE WHEN input.crowd THEN 'X-129' ELSE 'X-Same' END, '')
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_requests
          SET event_id = input.event_id,
              error_code = error.code,
              operation = error.operation,
              operation_index = error.operation_index,
              affected_fields = error.fields
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_requests_subscription TO rejected_requests;
      START;
      """
    # Every rejected record precedes the records each emitter sends, so a rejected record that
    # was sent would be captured first.
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"c1","replacement":"first","header_name":"X-Custom","header_value":"v","first_bytes":0,"second_bytes":0,"crowd":true},{"event_id":"h1","replacement":" bad","header_name":"X-Custom","header_value":"v","first_bytes":0,"second_bytes":0,"crowd":false},{"event_id":"h2","replacement":"first","header_name":"Host","header_value":"other.example","first_bytes":0,"second_bytes":0,"crowd":false},{"event_id":"h3","replacement":"first","header_name":"Bad Name","header_value":"v","first_bytes":0,"second_bytes":0,"crowd":false},{"event_id":"h4","replacement":"first","header_name":"X-Injected","header_value":"a\r\nInjected: yes","first_bytes":0,"second_bytes":0,"crowd":false},{"event_id":"h5","replacement":"first","header_name":"X-Edge","header_value":"v ","first_bytes":0,"second_bytes":0,"crowd":false},{"event_id":"h6","replacement":"first","header_name":"X-Custom","header_value":"v","first_bytes":40000,"second_bytes":0,"crowd":false},{"event_id":"h7","replacement":"first","header_name":"X-Custom","header_value":"v","first_bytes":20000,"second_bytes":20000,"crowd":false},{"event_id":"h8","replacement":"first","header_name":"X-Custom","header_value":"custom value","first_bytes":1,"second_bytes":0,"crowd":false},{"event_id":"h9","replacement":"first","header_name":"x-EMPTY","header_value":"now set","first_bytes":0,"second_bytes":0,"crowd":false}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "event_id":"c1" | "error_code":"validation" | "operation":"invoke" | "operation_index":128 | "affected_fields":["input.crowd"]
      "event_id":"h1" | "error_code":"validation" | "operation":"invoke" | "operation_index":0 | "affected_fields":["input.replacement"]
      "event_id":"h2" | "error_code":"validation" | "operation":"invoke" | "operation_index":4 | "affected_fields":["input.header_name","input.header_value"]
      "event_id":"h3" | "error_code":"validation" | "operation":"invoke" | "operation_index":4 | "affected_fields":["input.header_name","input.header_value"]
      "event_id":"h4" | "error_code":"validation" | "operation":"invoke" | "operation_index":4 | "affected_fields":["input.header_name","input.header_value"]
      "event_id":"h5" | "error_code":"validation" | "operation":"invoke" | "operation_index":4 | "affected_fields":["input.header_name","input.header_value"]
      "event_id":"h6" | "error_code":"validation" | "operation":"invoke" | "operation_index":5 | "affected_fields":["input.first_bytes"]
      "event_id":"h7" | "error_code":"validation" | "operation":"invoke" | "operation_index":6 | "affected_fields":["input.second_bytes"]
      """
    And HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 1 is
      """
      POST /headers/h8
      X-Replaced: final
      X-Empty:
      X-Spaced: a  b
      X-Custom: custom value
      X-First: a
      X-Second:
      """
    And HTTP receiver "api" request 2 is
      """
      POST /headers/h9
      X-Replaced: final
      X-Empty: now set
      """
    And HTTP receiver "api" has captured exactly 2 requests
    And HTTP receiver "crowded" eventually receives at least 9 requests
    And HTTP receiver "crowded" request 1 is
      """
      POST /crowded/h1
      X-Same:
      """
    And HTTP receiver "crowded" has captured exactly 9 requests

    Examples:
      | cluster_size | fields_type |
      | 1            | VEC<STRING> |
      | 3            | VEC<STRING> |

  @http_emitter_requests
  Scenario Outline: HTTP request targets are normalized on the client origin, and invalid targets are rejected before publication
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING, request_path STRING, padding I64);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA rejected_request (
        event_id STRING, error_code STRING, operation STRING, affected_fields <fields_type>
      );
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY rejected_requests SCHEMA rejected_request UNBRANCHED;
      CREATE VHOST edge http-emitter-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR outgoing_source
        FROM ENDPOINT outgoing_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE EMITTER targeted FROM outgoing
        TO HTTP api
          METHOD 'POST'
          PATH concat(input.request_path, repeat('a', input.padding))
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          WITHOUT BODY
        INVOKE write_header('X-Event', input.event_id)
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_requests
          SET event_id = input.event_id,
              error_code = error.code,
              operation = error.operation,
              affected_fields = error.fields
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_requests_subscription TO rejected_requests;
      START;
      """
    # The rejected targets precede the accepted ones, so a rejected target that was sent would be
    # captured first. Target r6 is one byte longer than the 8 KiB limit.
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"r1","request_path":"//other.example/events","padding":0},{"event_id":"r2","request_path":"/a/..//events","padding":0},{"event_id":"r3","request_path":"/events?value=%ZZ","padding":0},{"event_id":"r4","request_path":"/events#fragment","padding":0},{"event_id":"r5","request_path":"/objects\\a","padding":0},{"event_id":"r6","request_path":"/","padding":8192},{"event_id":"r7","request_path":"https://other.example/events","padding":0},{"event_id":"a1","request_path":"/v1/a/../events?tag=a&tag=b&q=a+b","padding":0},{"event_id":"a2","request_path":"/objects/a%2Fb","padding":0},{"event_id":"a3","request_path":"/café","padding":0},{"event_id":"a4","request_path":"/events?","padding":0}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "event_id":"r1" | "error_code":"validation" | "operation":"publish" | "affected_fields":["input.padding","input.request_path","path"]
      "event_id":"r2" | "error_code":"validation" | "operation":"publish" | "affected_fields":["input.padding","input.request_path","path"]
      "event_id":"r3" | "error_code":"validation" | "operation":"publish" | "affected_fields":["input.padding","input.request_path","path"]
      "event_id":"r4" | "error_code":"validation" | "operation":"publish" | "affected_fields":["input.padding","input.request_path","path"]
      "event_id":"r5" | "error_code":"validation" | "operation":"publish" | "affected_fields":["input.padding","input.request_path","path"]
      "event_id":"r6" | "error_code":"validation" | "operation":"publish" | "affected_fields":["input.padding","input.request_path","path"]
      "event_id":"r7" | "error_code":"validation" | "operation":"publish" | "affected_fields":["input.padding","input.request_path","path"]
      """
    And HTTP receiver "api" eventually receives at least 4 requests
    And HTTP receiver "api" request 1 is
      """
      POST /v1/events?tag=a&tag=b&q=a+b
      X-Event: a1
      """
    And HTTP receiver "api" request 2 is
      """
      POST /objects/a%2Fb
      X-Event: a2
      """
    And HTTP receiver "api" request 3 is
      """
      POST /caf%C3%A9
      X-Event: a3
      """
    And HTTP receiver "api" request 4 is
      """
      POST /events?
      X-Event: a4
      """
    And HTTP receiver "api" has captured exactly 4 requests

    Examples:
      | cluster_size | fields_type |
      | 1            | VEC<STRING> |
      | 3            | VEC<STRING> |

  @http_emitter_requests
  Scenario Outline: HTTP requests of interleaved branches keep their own values, and their errors stay in their branch
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (
        event_id STRING, tenant STRING, request_path STRING, payload STRING
      );
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA event_body (event_id STRING, payload STRING);
      CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (event_id string, payload string);
      CREATE CODEC event_body_codec FROM WIRE JSON SCHEMA event_body_wire TO SCHEMA event_body;
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE SCHEMA rejected_request (event_id STRING, operation STRING);
      CREATE RELAY outgoing SCHEMA outbound_event BRANCHED BY by_tenant;
      CREATE RELAY rejected_requests SCHEMA rejected_request BRANCHED BY by_tenant;
      CREATE VHOST edge http-emitter-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR outgoing_source
        FROM ENDPOINT outgoing_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing
          INHERIT ALL
          BRANCHED BY by_tenant
          SET tenant = message.tenant
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE EMITTER tenant_events FROM outgoing
        TO HTTP api
          METHOD 'POST'
          PATH concat('/tenants/', input.tenant, input.request_path)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING event_body_codec
        INHERIT event_id, payload
        INVOKE write_header('X-Tenant', input.tenant),
               write_header('X-Event', event_id)
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR SEND TO rejected_requests
          SET event_id = input.event_id,
              operation = error.operation
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_requests_subscription TO rejected_requests;
      START;
      """
    # The first record of each branch is rejected before any record of that branch is sent.
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"n1","tenant":"north","request_path":"/events#n1","payload":"north-1"},{"event_id":"s1","tenant":"south","request_path":"/events#s1","payload":"south-1"},{"event_id":"n2","tenant":"north","request_path":"/events/n2","payload":"north-2"},{"event_id":"s2","tenant":"south","request_path":"/events/s2","payload":"south-2"},{"event_id":"n3","tenant":"north","request_path":"/events/n3","payload":"north-3"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "event_id":"n1" | "operation":"publish" | key={"tenant":"north"}
      "event_id":"s1" | "operation":"publish" | key={"tenant":"south"}
      """
    And HTTP receiver "api" eventually receives at least 3 requests
    And HTTP receiver "api" captured one request that is
      """
      POST /tenants/north/events/n2
      X-Tenant: north
      X-Event: n2

      {"event_id":"n2","payload":"north-2"}
      """
    And HTTP receiver "api" captured one request that is
      """
      POST /tenants/north/events/n3
      X-Tenant: north
      X-Event: n3

      {"event_id":"n3","payload":"north-3"}
      """
    And HTTP receiver "api" captured one request that is
      """
      POST /tenants/south/events/s2
      X-Tenant: south
      X-Event: s2

      {"event_id":"s2","payload":"south-2"}
      """
    And HTTP receiver "api" has captured exactly 3 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_requests
  Scenario Outline: A retried HTTP request repeats its generated header and its encoded body exactly
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers with
      """
      lose response
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
      CREATE SCHEMA outbound_event (event_id STRING, payload STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA stamped_body (event_id STRING, payload STRING, nonce STRING);
      CREATE CODEC stamped_body_codec FROM JSON TO SCHEMA stamped_body
        WITH JAQ TRANSFORMATIONS ON EMITTING '. + {encoded_at: now}';
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE VHOST edge http-emitter-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR outgoing_source
        FROM ENDPOINT outgoing_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE EMITTER stamped FROM outgoing
        TO HTTP api
          METHOD 'POST'
          PATH concat('/v1/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          ENCODE USING stamped_body_codec
        INHERIT event_id, payload
        SET nonce = uuid_v4()
        INVOKE write_header('X-Attempt-Key', uuid_v4())
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    # The receiver reads the first request in full and closes the connection without answering,
    # so the emitter retries a request whose response it lost. The header value, the body nonce
    # and the codec's encoding time are all generated once, when the request is prepared.
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1","payload":"first"}]
      """
    Then HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 1 carries header "X-Attempt-Key" and a body containing "encoded_at"
    And HTTP receiver "api" request 2 repeats request 1

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_requests
  Scenario Outline: HTTP request field failures keep their typed error, the captured materialized state and the attempted body
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING, request_path STRING, payload STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA route (route_key STRING, method STRING, prefix STRING, token STRING SENSITIVE);
      CREATE CODEC routes_codec FROM JSON TO SCHEMA route
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA event_body (event_id STRING, payload STRING);
      CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (event_id string, payload string);
      CREATE CODEC event_body_codec FROM WIRE JSON SCHEMA event_body_wire TO SCHEMA event_body;
      CREATE SCHEMA rejected_request (
        event_id STRING,
        error_code STRING,
        error_message STRING,
        operation STRING,
        operation_index U32 OPTIONAL,
        affected_fields <fields_type>,
        state_method STRING,
        attempted_payload STRING OPTIONAL
      );
      CREATE RELAY routes SCHEMA route UNBRANCHED WITH MATERIALIZED STATE LAST BY TIMESTAMP;
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY rejected_requests SCHEMA rejected_request UNBRANCHED;
      CREATE VHOST edge http-emitter-{{test_id}}.example.com;
      CREATE ENDPOINT routes_ingress ON edge PATH '/routes' TYPE HTTP;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR routes_source
        FROM ENDPOINT routes_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING routes_codec
        TO routes
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE INGESTOR outgoing_source
        FROM ENDPOINT outgoing_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE EMITTER routed FROM outgoing
        USING MATERIALIZED STATE routes REQUIRED WAIT
        TO HTTP api
          METHOD relay_state.routes.method
          PATH concat(relay_state.routes.prefix, input.request_path)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
          ENCODE USING event_body_codec
        INHERIT event_id
        SET payload = upper(input.payload)
        INVOKE write_header(
          'Authorization', concat('Bearer ', leak_sensitive(relay_state.routes.token))
        )
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_requests
          SET event_id = input.event_id,
              error_code = error.code,
              error_message = error.message,
              operation = error.operation,
              operation_index = error.operation_index,
              affected_fields = error.fields,
              state_method = relay_state.routes.method,
              attempted_payload = partial_output.payload
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_requests_subscription TO rejected_requests;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/routes"
      """
      [{"route_key":"main","method":"PATCH","prefix":"/tenants/north","token":"secret-route-token"}]
      """
    # Record 2's target normalizes to a path that begins with //, which cannot leave the origin.
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1","request_path":"/events/1","payload":"first"},{"event_id":"2","request_path":"/../..//evil","payload":"second"},{"event_id":"3","request_path":"/events/3","payload":"third"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "event_id":"2" | "error_code":"validation" | "operation":"publish" | "affected_fields":["input.request_path","path","relay_state.routes.prefix"] | "state_method":"PATCH" | "attempted_payload":"SECOND"
      """
    And the last relay subscription payload does not contain "operation_index"
    And the last relay subscription payload does not contain "evil"
    And the last relay subscription payload does not contain "secret-route-token"
    And HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 1 is
      """
      PATCH /tenants/north/events/1
      Authorization: Bearer secret-route-token

      {"event_id":"1","payload":"FIRST"}
      """
    And HTTP receiver "api" request 2 is
      """
      PATCH /tenants/north/events/3
      Authorization: Bearer secret-route-token

      {"event_id":"3","payload":"THIRD"}
      """
    And HTTP receiver "api" has captured exactly 2 requests

    Examples:
      | cluster_size | fields_type |
      | 1            | VEC<STRING> |
      | 3            | VEC<STRING> |
