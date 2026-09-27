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
      body: without body
      sink: HTTP client=api method='HEAD' path='/health'
      batch: none
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

  # The scenarios tagged @http_emitter_expected_failure are the initial public cases of the HTTP
  # emitter epic. They fail until the delivery task the ledger names lands the capability, are
  # excluded from the ordinary suite, and run only by explicit tag selection.

  @http_emitter_expected_failure
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

  @http_emitter_expected_failure
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
