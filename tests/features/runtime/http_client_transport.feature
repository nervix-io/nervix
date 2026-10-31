Feature: HTTP client transport contract
  An HTTP client means one thing whichever statement uses it. HTTP REQUEST polling and the HTTP
  REQUEST emitter follow a redirect only when their client declares follow_redirects, and then
  only to the client's own origin, repeating the same request there; they send their requests
  through the proxy the client declares and never through one the environment names; they speak
  HTTP/2 to an endpoint whose TLS offers it, directly or inside the declared proxy's tunnel; and a
  failed poll's report names no URL.

  @http_client_transport
  Scenario Outline: Without follow_redirects a <status> redirect is the answer on both sides on <cluster_size> nodes
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers requests for "/" with "respond <status>; header Location: /moved/poll"
    And HTTP receiver "api" answers requests for "/events/1" with "respond <status>; header Location: /moved/events/1"
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING, payload STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (event_id string, payload string);
      CREATE CODEC event_body_codec FROM WIRE JSON SCHEMA event_body_wire TO SCHEMA outbound_event;
      CREATE SCHEMA polled_event (user_id I64);
      CREATE WIRE JSON SCHEMA polled_wire MODE STRICT (user_id integer);
      CREATE CODEC polled_codec FROM WIRE JSON SCHEMA polled_wire TO SCHEMA polled_event;
      CREATE SCHEMA rejected_request (event_id STRING, message STRING);
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY polled SCHEMA polled_event UNBRANCHED;
      CREATE RELAY rejected_requests SCHEMA rejected_request UNBRANCHED;
      CREATE VHOST edge http-transport-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR outgoing_source
        FROM outgoing_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST INGESTOR poller FROM api METHOD 'GET' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING polled_codec
        TO polled INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        TO api
          METHOD 'POST'
          PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          ENCODE USING event_body_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_requests
          SET event_id = input.event_id, message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_requests_subscription TO rejected_requests;
      START;
      """
    And http payload is posted to host "http-transport-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1","payload":"one"}]
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      "event_id":"1" | "message":"HTTP endpoint answered with status <status>"
      """
    And within "20s" DESCRIBE INGESTOR "poller" on the leader node contains
      """
      HTTP ingestor returned status <status>
      """
    And HTTP receiver "api" captured no request with request line "POST /moved/events/1"
    And HTTP receiver "api" captured no request with request line "GET /moved/poll"

    Examples:
      | cluster_size | status |
      | 1            | 302    |
      | 3            | 302    |
      | 1            | 308    |
      | 3            | 308    |

  @http_client_transport
  Scenario Outline: With follow_redirects both sides repeat their request at a <status> redirect on the client's origin on <cluster_size> nodes
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers requests for "/" with "respond <status>; header Location: /moved/poll"
    And HTTP receiver "api" answers requests for "/moved/poll" with 'respond 200; header Content-Type: application/json; body {"user_id":42}'
    And HTTP receiver "api" answers requests for "/events/1" with "respond <status>; header Location: /moved/events/1?attempt=redirected"
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING, payload STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (event_id string, payload string);
      CREATE CODEC event_body_codec FROM WIRE JSON SCHEMA event_body_wire TO SCHEMA outbound_event;
      CREATE SCHEMA polled_event (user_id I64);
      CREATE WIRE JSON SCHEMA polled_wire MODE STRICT (user_id integer);
      CREATE CODEC polled_codec FROM WIRE JSON SCHEMA polled_wire TO SCHEMA polled_event;
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY polled SCHEMA polled_event UNBRANCHED;
      CREATE VHOST edge http-transport-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR outgoing_source
        FROM outgoing_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000, 'follow_redirects' = true
      };
      CREATE HTTP REQUEST INGESTOR poller FROM api METHOD 'GET' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING polled_codec
        TO polled INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        TO api
          METHOD 'POST'
          PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          ENCODE USING event_body_codec
        INHERIT ALL
        INVOKE write_header('X-Tenant', 'north')
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION polled_subscription TO polled;
      START;
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"user_id":42}
      """
    When http payload is posted to host "http-transport-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1","payload":"one"}]
      """
    Then HTTP receiver "api" eventually captures one request that is
      """
      POST /moved/events/1?attempt=redirected
      X-Tenant: north

      {"event_id":"1","payload":"one"}
      """
    And HTTP receiver "api" captured one request that is
      """
      POST /events/1
      X-Tenant: north

      {"event_id":"1","payload":"one"}
      """

    Examples:
      | cluster_size | status |
      | 1            | 301    |
      | 3            | 301    |
      | 1            | 302    |
      | 3            | 302    |
      | 1            | 307    |
      | 3            | 307    |
      | 1            | 308    |
      | 3            | 308    |

  @http_client_transport
  Scenario Outline: With follow_redirects a redirect off the client's origin is still the answer on both sides on <cluster_size> nodes
    Given HTTP receiver "api" is running
    And HTTP receiver "elsewhere" is running
    And HTTP receiver "api" answers requests for "/" with "respond 302; header Location: {{http_receiver.elsewhere}}/moved/poll"
    And HTTP receiver "api" answers requests for "/events/1" with "respond 307; header Location: {{http_receiver.elsewhere}}/moved/events/1"
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And HTTP receiver "elsewhere" answers unscripted requests with 'respond 200; header Content-Type: application/json; body {"user_id":42}'
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING, payload STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (event_id string, payload string);
      CREATE CODEC event_body_codec FROM WIRE JSON SCHEMA event_body_wire TO SCHEMA outbound_event;
      CREATE SCHEMA polled_event (user_id I64);
      CREATE WIRE JSON SCHEMA polled_wire MODE STRICT (user_id integer);
      CREATE CODEC polled_codec FROM WIRE JSON SCHEMA polled_wire TO SCHEMA polled_event;
      CREATE SCHEMA rejected_request (event_id STRING, message STRING);
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY polled SCHEMA polled_event UNBRANCHED;
      CREATE RELAY rejected_requests SCHEMA rejected_request UNBRANCHED;
      CREATE VHOST edge http-transport-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR outgoing_source
        FROM outgoing_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000, 'follow_redirects' = true
      };
      CREATE HTTP REQUEST INGESTOR poller FROM api METHOD 'GET' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING polled_codec
        TO polled INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        TO api
          METHOD 'POST'
          PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          ENCODE USING event_body_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_requests
          SET event_id = input.event_id, message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_requests_subscription TO rejected_requests;
      START;
      """
    And http payload is posted to host "http-transport-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1","payload":"one"}]
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      "event_id":"1" | "message":"HTTP endpoint answered with status 307"
      """
    And within "20s" DESCRIBE INGESTOR "poller" on the leader node contains
      """
      HTTP ingestor returned status 302
      """
    And HTTP receiver "elsewhere" has captured exactly 0 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_client_transport
  Scenario Outline: With follow_redirects a 303 redirect repeats a GET but is the answer to a POST on <cluster_size> nodes
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers requests for "/" with "respond 303; header Location: /moved/poll"
    And HTTP receiver "api" answers requests for "/moved/poll" with 'respond 200; header Content-Type: application/json; body {"user_id":42}'
    And HTTP receiver "api" answers requests for "/events/1" with "respond 303; header Location: /moved/events/1"
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING, payload STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (event_id string, payload string);
      CREATE CODEC event_body_codec FROM WIRE JSON SCHEMA event_body_wire TO SCHEMA outbound_event;
      CREATE SCHEMA polled_event (user_id I64);
      CREATE WIRE JSON SCHEMA polled_wire MODE STRICT (user_id integer);
      CREATE CODEC polled_codec FROM WIRE JSON SCHEMA polled_wire TO SCHEMA polled_event;
      CREATE SCHEMA routed_event (user_id I64, event_id STRING OPTIONAL, message STRING OPTIONAL);
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY observed SCHEMA routed_event UNBRANCHED;
      CREATE VHOST edge http-transport-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR outgoing_source
        FROM outgoing_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000, 'follow_redirects' = true
      };
      CREATE HTTP REQUEST INGESTOR poller FROM api METHOD 'GET' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING polled_codec
        TO observed INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        TO api
          METHOD 'POST'
          PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          ENCODE USING event_body_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO observed
          SET user_id = 0, event_id = input.event_id, message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION observed_subscription TO observed;
      START;
      """
    Then within "30s" the relay subscription receives a payload
      """
      "user_id":42
      """
    When http payload is posted to host "http-transport-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1","payload":"one"}]
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      "event_id":"1" | "message":"HTTP endpoint answered with status 303"
      """
    And HTTP receiver "api" captured no request with request line "POST /moved/events/1"
    And HTTP receiver "api" captured no request with request line "GET /moved/events/1"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_client_transport
  Scenario Outline: With follow_redirects one request follows at most 10 redirects on <cluster_size> nodes
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers requests for "/events/1" with "respond 307; header Location: /events/1"
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING, payload STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (event_id string, payload string);
      CREATE CODEC event_body_codec FROM WIRE JSON SCHEMA event_body_wire TO SCHEMA outbound_event;
      CREATE SCHEMA rejected_request (event_id STRING, message STRING);
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY rejected_requests SCHEMA rejected_request UNBRANCHED;
      CREATE VHOST edge http-transport-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR outgoing_source
        FROM outgoing_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000, 'follow_redirects' = true
      };
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        TO api
          METHOD 'POST'
          PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          ENCODE USING event_body_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_requests
          SET event_id = input.event_id, message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_requests_subscription TO rejected_requests;
      START;
      """
    And http payload is posted to host "http-transport-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1","payload":"one"}]
      """
    Then within "20s" the relay subscription receives payloads containing all fragments
      """
      "event_id":"1" | "message":"HTTP endpoint answered with status 307"
      """
    And HTTP receiver "api" has captured exactly 11 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_client_transport
  Scenario Outline: A declared proxy carries both sides' plain requests to a host only it resolves on <cluster_size> nodes
    Given HTTP receiver "api" is running
    And HTTP proxy "edge" is running
    And HTTP receiver "api" answers requests for "/" with 'respond 200; header Content-Type: application/json; body {"user_id":42}'
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING, payload STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (event_id string, payload string);
      CREATE CODEC event_body_codec FROM WIRE JSON SCHEMA event_body_wire TO SCHEMA outbound_event;
      CREATE SCHEMA polled_event (user_id I64);
      CREATE WIRE JSON SCHEMA polled_wire MODE STRICT (user_id integer);
      CREATE CODEC polled_codec FROM WIRE JSON SCHEMA polled_wire TO SCHEMA polled_event;
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY polled SCHEMA polled_event UNBRANCHED;
      CREATE VHOST edge http-transport-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR outgoing_source
        FROM outgoing_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = 'http://receiver-{{test_id}}.invalid:{{http_receiver_port.api}}',
        'timeout_ms' = 5000,
        'proxy' = '{{http_proxy.edge}}'
      };
      CREATE HTTP REQUEST INGESTOR poller FROM api METHOD 'GET' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING polled_codec
        TO polled INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        TO api
          METHOD 'POST'
          PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          ENCODE USING event_body_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION polled_subscription TO polled;
      START;
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"user_id":42}
      """
    And HTTP proxy "edge" eventually forwards "GET http://receiver-{{test_id}}.invalid:{{http_receiver_port.api}}/ HTTP/1.1"
    When http payload is posted to host "http-transport-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1","payload":"one"}]
      """
    Then HTTP proxy "edge" eventually forwards "POST http://receiver-{{test_id}}.invalid:{{http_receiver_port.api}}/events/1 HTTP/1.1"
    And HTTP receiver "api" eventually captures one request that is
      """
      POST /events/1
      Host: receiver-{{test_id}}.invalid:{{http_receiver_port.api}}

      {"event_id":"1","payload":"one"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_client_transport
  Scenario Outline: A declared proxy with credentials tunnels both sides' HTTPS requests on <cluster_size> nodes
    Given HTTP proxy "edge" is running and requires user "scenario" with password "proxy-secret"
    And HTTPS receiver "secure" is running with a certificate for "receiver-{{test_id}}.invalid"
    And HTTP receiver "secure" answers requests for "/" with 'respond 200; header Content-Type: application/json; body {"user_id":42}'
    And HTTP receiver "secure" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has the TLS files of HTTP receiver "secure" in resource directory "receiver_tls"
    When these Plexus commands are executed
      """
      CREATE RESOURCE receiver_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE receiver_tls VERSION "{{receiver_tls}}";
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA outbound_event (event_id STRING, payload STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (event_id string, payload string);
      CREATE CODEC event_body_codec FROM WIRE JSON SCHEMA event_body_wire TO SCHEMA outbound_event;
      CREATE SCHEMA polled_event (user_id I64);
      CREATE WIRE JSON SCHEMA polled_wire MODE STRICT (user_id integer);
      CREATE CODEC polled_codec FROM WIRE JSON SCHEMA polled_wire TO SCHEMA polled_event;
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY polled SCHEMA polled_event UNBRANCHED;
      CREATE VHOST edge http-transport-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR outgoing_source
        FROM outgoing_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT api MOUNT receiver_tls VERSION 1 CONFIG {
        'endpoint' = 'https://receiver-{{test_id}}.invalid:{{http_receiver_port.secure}}',
        'timeout_ms' = 5000,
        'proxy' = 'http://scenario:proxy-secret@127.0.0.1:{{http_proxy_port.edge}}',
        'tls_ca_file' = '{{receiver_tls}}/ca.pem'
      };
      CREATE HTTP REQUEST INGESTOR poller FROM api METHOD 'GET' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING polled_codec
        TO polled INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        TO api
          METHOD 'POST'
          PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          ENCODE USING event_body_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION polled_subscription TO polled;
      START;
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"user_id":42}
      """
    And HTTP proxy "edge" eventually tunnels to "receiver-{{test_id}}.invalid:{{http_receiver_port.secure}}"
    When http payload is posted to host "http-transport-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1","payload":"one"}]
      """
    Then HTTP receiver "secure" eventually captures one request that is
      """
      POST /events/1
      Host: receiver-{{test_id}}.invalid:{{http_receiver_port.secure}}

      {"event_id":"1","payload":"one"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_client_transport
  Scenario Outline: Both sides speak HTTP/2 to an endpoint whose TLS offers it on <cluster_size> nodes
    Given HTTPS receiver "secure" is running with a certificate for "127.0.0.1" and offers HTTP/2
    And HTTP receiver "secure" answers requests for "/" with 'respond 200; header Content-Type: application/json; body {"user_id":42}'
    And HTTP receiver "secure" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has the TLS files of HTTP receiver "secure" in resource directory "receiver_tls"
    When these Plexus commands are executed
      """
      CREATE RESOURCE receiver_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE receiver_tls VERSION "{{receiver_tls}}";
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA outbound_event (event_id STRING, payload STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (event_id string, payload string);
      CREATE CODEC event_body_codec FROM WIRE JSON SCHEMA event_body_wire TO SCHEMA outbound_event;
      CREATE SCHEMA polled_event (user_id I64);
      CREATE WIRE JSON SCHEMA polled_wire MODE STRICT (user_id integer);
      CREATE CODEC polled_codec FROM WIRE JSON SCHEMA polled_wire TO SCHEMA polled_event;
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY polled SCHEMA polled_event UNBRANCHED;
      CREATE VHOST edge http-transport-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR outgoing_source
        FROM outgoing_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT api MOUNT receiver_tls VERSION 1 CONFIG {
        'endpoint' = 'https://127.0.0.1:{{http_receiver_port.secure}}',
        'timeout_ms' = 5000,
        'tls_ca_file' = '{{receiver_tls}}/ca.pem'
      };
      CREATE HTTP REQUEST INGESTOR poller FROM api METHOD 'GET' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING polled_codec
        TO polled INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        TO api
          METHOD 'POST'
          PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          ENCODE USING event_body_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION polled_subscription TO polled;
      START;
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"user_id":42}
      """
    And HTTP receiver "secure" eventually captures a request with request line "GET /" over HTTP/2
    When http payload is posted to host "http-transport-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1","payload":"one"}]
      """
    Then HTTP receiver "secure" eventually captures a request with request line "POST /events/1" over HTTP/2

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_client_transport
  Scenario Outline: Both sides speak HTTP/2 inside a declared proxy's tunnel to an endpoint whose TLS offers it on <cluster_size> nodes
    Given HTTP proxy "edge" is running
    And HTTPS receiver "secure" is running with a certificate for "receiver-{{test_id}}.invalid" and offers HTTP/2
    And HTTP receiver "secure" answers requests for "/" with 'respond 200; header Content-Type: application/json; body {"user_id":42}'
    And HTTP receiver "secure" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has the TLS files of HTTP receiver "secure" in resource directory "receiver_tls"
    When these Plexus commands are executed
      """
      CREATE RESOURCE receiver_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE receiver_tls VERSION "{{receiver_tls}}";
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA outbound_event (event_id STRING, payload STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (event_id string, payload string);
      CREATE CODEC event_body_codec FROM WIRE JSON SCHEMA event_body_wire TO SCHEMA outbound_event;
      CREATE SCHEMA polled_event (user_id I64);
      CREATE WIRE JSON SCHEMA polled_wire MODE STRICT (user_id integer);
      CREATE CODEC polled_codec FROM WIRE JSON SCHEMA polled_wire TO SCHEMA polled_event;
      CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY polled SCHEMA polled_event UNBRANCHED;
      CREATE VHOST edge http-transport-{{test_id}}.example.com;
      CREATE ENDPOINT outgoing_ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR outgoing_source
        FROM outgoing_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT api MOUNT receiver_tls VERSION 1 CONFIG {
        'endpoint' = 'https://receiver-{{test_id}}.invalid:{{http_receiver_port.secure}}',
        'timeout_ms' = 5000,
        'proxy' = 'http://127.0.0.1:{{http_proxy_port.edge}}',
        'tls_ca_file' = '{{receiver_tls}}/ca.pem'
      };
      CREATE HTTP REQUEST INGESTOR poller FROM api METHOD 'GET' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING polled_codec
        TO polled INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        TO api
          METHOD 'POST'
          PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          ENCODE USING event_body_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION polled_subscription TO polled;
      START;
      """
    Then within "30s" the relay subscription receives a payload
      """
      {"user_id":42}
      """
    And HTTP receiver "secure" eventually captures a request with request line "GET /" over HTTP/2
    And HTTP proxy "edge" eventually tunnels to "receiver-{{test_id}}.invalid:{{http_receiver_port.secure}}"
    When http payload is posted to host "http-transport-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1","payload":"one"}]
      """
    Then HTTP receiver "secure" eventually captures a request with request line "POST /events/1" over HTTP/2

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_client_transport
  Scenario Outline: Proxy environment variables carry neither side's requests on <cluster_size> server processes
    Given HTTP receiver "api" is running
    And HTTP proxy "environment" is running
    And HTTP receiver "api" answers requests for "/" with 'respond 200; header Content-Type: application/json; body {"user_id":42}'
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And a <cluster_size> node nervix-server process cluster is started with proxy environment variables naming HTTP proxy "environment"
    And the server process cluster is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA polled_event (user_id I64);
      CREATE WIRE JSON SCHEMA polled_wire MODE STRICT (user_id integer);
      CREATE CODEC polled_codec FROM WIRE JSON SCHEMA polled_wire TO SCHEMA polled_event;
      CREATE RELAY polled SCHEMA polled_event UNBRANCHED;
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST INGESTOR poller FROM api METHOD 'GET' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING polled_codec
        TO polled INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP REQUEST EMITTER published FROM polled
        TO api
          METHOD 'POST'
          PATH '/events'
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s
          ENCODE USING polled_codec
        INHERIT ALL
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then HTTP receiver "api" eventually captures a request with request line "GET /"
    And HTTP receiver "api" eventually captures a request with request line "POST /events"
    And HTTP proxy "environment" has carried no request

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_client_transport
  Scenario Outline: A failed poll's report names its failure but not its URL when <case> on <cluster_size> nodes
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers requests for "/private/{{test_id}}?token=secret-{{test_id}}" with "<answer>"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA polled_event (user_id I64);
      CREATE WIRE JSON SCHEMA polled_wire MODE STRICT (user_id integer);
      CREATE CODEC polled_codec FROM WIRE JSON SCHEMA polled_wire TO SCHEMA polled_event;
      CREATE RELAY polled SCHEMA polled_event UNBRANCHED;
      CREATE HTTP CLIENT api CONFIG {
        'endpoint' = '{{http_receiver.api}}/private/{{test_id}}?token=secret-{{test_id}}',
        'timeout_ms' = 5000
      };
      CREATE HTTP REQUEST INGESTOR poller FROM api METHOD 'GET' EVERY 1s
        MODE NO ACK ON QUIESCE SUSPEND DECODE USING polled_codec
        TO polled INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "20s" DESCRIBE INGESTOR "poller" on the leader node contains
      """
      <reported>
      """
    And the last command output does not contain
      """
      secret-{{test_id}}
      """
    And the last command output does not contain
      """
      /private/
      """

    Examples:
      | cluster_size | case                                  | answer        | reported                             |
      | 1            | the endpoint answers 500              | respond 500   | HTTP ingestor returned status 500    |
      | 3            | the endpoint answers 500              | respond 500   | HTTP ingestor returned status 500    |
      | 1            | the connection ends without an answer | lose response | failed to send HTTP ingestor request |
      | 3            | the connection ends without an answer | lose response | failed to send HTTP ingestor request |
