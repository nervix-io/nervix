Feature: HTTP emitter
  An HTTP emitter sends one request for each eligible relay record to an operator-provisioned
  endpoint, with an independently configured method, path, headers, and body. See
  docs/specifications/http-emitter.md and tests/http-emitter-acceptance-ledger.md.

  The scenarios tagged @http_emitter_expected_failure are the initial public cases of the HTTP
  emitter epic. They fail until the delivery task the ledger names lands the capability, are
  excluded from the ordinary suite, and run only by explicit tag selection.

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
