Feature: HTTP emitter transport failures and TLS identity
  Outbound attempts use the configured physical timeout and TLS identity. A failed attempt
  retains its request for the emitter's retry policy.

  @http_emitter_transport
  Scenario Outline: An HTTP emitter presents its mounted client certificate
    Given HTTPS receiver "secure" is running with a certificate for "127.0.0.1" and requires a client certificate
    And HTTP receiver "secure" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has the TLS files of HTTP receiver "secure" in resource directory "receiver_tls"
    When these NSPL commands are executed
      """
      CREATE RESOURCE receiver_tls;
      """
    And these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE receiver_tls VERSION "{{receiver_tls}}";
      """
    And these NSPL commands are executed
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
      CREATE CLIENT api TYPE HTTP MOUNT receiver_tls VERSION 1 CONFIG {
        'endpoint' = '{{http_receiver.secure}}', 'timeout_ms' = 5000,
        'tls_ca_file' = '{{receiver_tls}}/ca.pem',
        'tls_cert_file' = '{{receiver_tls}}/client.pem',
        'tls_key_file' = '{{receiver_tls}}/client-key.pem'
      };
      CREATE EMITTER published FROM outgoing
        TO HTTP api METHOD 'POST' PATH '/events'
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s WITHOUT BODY
        INVOKE write_header('Accept', 'application/json')
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then HTTP receiver "secure" has captured exactly 0 requests
    When http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"}]
      """
    Then HTTP receiver "secure" eventually receives at least 1 request
    And HTTP receiver "secure" request 1 is
      """
      POST /events
      Accept: application/json
      """
    And HTTP receiver "secure" has captured exactly 1 request

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_transport
  Scenario Outline: An HTTP emitter retries after a failed TLS handshake for <case>
    Given HTTPS receiver "secure" is running with a certificate for "<certificate_host>"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has the TLS files of HTTP receiver "secure" in resource directory "receiver_tls"
    When these NSPL commands are executed
      """
      CREATE RESOURCE receiver_tls;
      """
    And these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE receiver_tls VERSION "{{receiver_tls}}";
      """
    And these NSPL commands are executed
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
        'endpoint' = '{{http_receiver.secure}}', 'timeout_ms' = 5000<ca_setting>
      };
      CREATE EMITTER published FROM outgoing
        TO HTTP api METHOD 'POST' PATH '/events'
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"}]
      """
    Then HTTP receiver "secure" eventually records a failed TLS handshake
    # The request stays pending on every retry, and the emitter reports the handshake's own cause.
    And within "30s" DESCRIBE EMITTER "published" on the leader node contains
      """
      transient error: HTTP TLS handshake failed: invalid peer certificate: <tls_failure>
      """
    And HTTP receiver "secure" has captured exactly 0 requests

    Examples:
      | cluster_size | case       | certificate_host | ca_setting                                  | tls_failure                    |
      | 1            | no trust   | 127.0.0.1        |                                             | UnknownIssuer                  |
      | 3            | no trust   | 127.0.0.1        |                                             | UnknownIssuer                  |
      | 1            | wrong host | localhost        | , 'tls_ca_file' = '{{receiver_tls}}/ca.pem' | certificate not valid for name |
      | 3            | wrong host | localhost        | , 'tls_ca_file' = '{{receiver_tls}}/ca.pem' | certificate not valid for name |

  @http_emitter_transport
  Scenario Outline: An HTTP emitter without the client certificate its receiver requires never delivers its request
    Given HTTPS receiver "secure" is running with a certificate for "127.0.0.1" and requires a client certificate
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has the TLS files of HTTP receiver "secure" in resource directory "receiver_tls"
    When these NSPL commands are executed
      """
      CREATE RESOURCE receiver_tls;
      """
    And these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE receiver_tls VERSION "{{receiver_tls}}";
      """
    And these NSPL commands are executed
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
      CREATE CLIENT api TYPE HTTP MOUNT receiver_tls VERSION 1 CONFIG {
        'endpoint' = '{{http_receiver.secure}}', 'timeout_ms' = 5000,
        'tls_ca_file' = '{{receiver_tls}}/ca.pem'
      };
      CREATE EMITTER published FROM outgoing
        TO HTTP api METHOD 'POST' PATH '/events'
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"}]
      """
    # The client trusts the receiver, which refuses a client that presents no certificate, so no
    # request is ever read and the emitter keeps reporting the refusal.
    Then HTTP receiver "secure" eventually records a failed TLS handshake
    And within "30s" DESCRIBE EMITTER "published" on the leader node contains
      """
      CertificateRequired
      """
    And HTTP receiver "secure" has captured exactly 0 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_transport
  Scenario Outline: An HTTP emitter retries after <failure> without passing the next record
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
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 300
      };
      CREATE EMITTER published FROM outgoing
        TO HTTP api METHOD 'POST' PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"},{"event_id":"2"}]
      """
    Then HTTP receiver "api" eventually receives at least 3 requests
    And HTTP receiver "api" request 1 is
      """
      POST /events/1
      """
    And HTTP receiver "api" request 2 repeats request 1
    And HTTP receiver "api" request 2 arrived at least "<minimum_gap>" after request 1
    And HTTP receiver "api" request 3 is
      """
      POST /events/2
      """
    And HTTP receiver "api" has captured exactly 3 requests

    Examples:
      | cluster_size | failure          | response      | minimum_gap |
      | 1            | physical timeout | hold response | 300ms       |
      | 3            | physical timeout | hold response | 300ms       |
      | 1            | lost response    | lose response | 200ms       |
      | 3            | lost response    | lose response | 200ms       |

  @http_emitter_transport
  Scenario Outline: One HTTP emitter waits across its source relays
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers with
      """
      respond 204; after 1s
      respond 204
      """
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
      CREATE RELAY source_a SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY source_b SCHEMA outbound_event UNBRANCHED;
      CREATE VHOST edge http-emitter-{{test_id}}.example.com;
      CREATE ENDPOINT ingress_a ON edge PATH '/a' TYPE HTTP;
      CREATE ENDPOINT ingress_b ON edge PATH '/b' TYPE HTTP;
      CREATE INGESTOR from_a FROM ENDPOINT ingress_a MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO source_a INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INGESTOR from_b FROM ENDPOINT ingress_b MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO source_b INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE EMITTER published FROM source_a, source_b
        TO HTTP api METHOD 'POST' PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/a"
      """
      [{"event_id":"a"}]
      """
    Then HTTP receiver "api" eventually receives at least 1 request
    When http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/b"
      """
      [{"event_id":"b"}]
      """
    Then HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 1 is
      """
      POST /events/a
      """
    And HTTP receiver "api" request 2 is
      """
      POST /events/b
      """
    And HTTP receiver "api" request 2 arrived at least "1s" after request 1
    And HTTP receiver "api" never had more than 1 request awaiting a response
    And HTTP receiver "api" has captured exactly 2 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_transport
  Scenario Outline: A request whose attempts time out stays pending and reported, releases each timed-out connection, and is delivered once the endpoint answers
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers requests for "/events/1" with "hold response"
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
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 300
      };
      CREATE EMITTER published FROM outgoing
        TO HTTP api METHOD 'POST' PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"},{"event_id":"2"}]
      """
    # The receiver holds every attempt at record 1 past the client timeout, so each attempt ends at
    # its physical timeout, closes its connection, and leaves the same request pending.
    Then HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 2 repeats request 1
    And HTTP receiver "api" request 2 arrived at least "300ms" after request 1
    And HTTP receiver "api" eventually sees the client abandon at least 1 unfinished response
    And within "30s" DESCRIBE EMITTER "published" on the leader node contains
      """
      transient error: HTTP request timed out before complete final response headers
      """
    And HTTP receiver "api" captured no request with request line "POST /events/2"
    When HTTP receiver "api" answers requests for "/events/1" with "respond 204"
    Then HTTP receiver "api" eventually captures one request that is
      """
      POST /events/2
      """
    And within "30s" DESCRIBE EMITTER "published" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_transport
  Scenario Outline: A request whose response is lost stays pending and reported until the endpoint answers it
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers requests for "/events/1" with "lose response"
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
        TO HTTP api METHOD 'POST' PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"},{"event_id":"2"}]
      """
    # The receiver reads every attempt at record 1 in full and closes the connection without an
    # answer, so the request stays pending on each retry.
    Then HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 2 repeats request 1
    And within "30s" DESCRIBE EMITTER "published" on the leader node contains
      """
      transient error: HTTP response header exchange failed: HTTP connection closed before complete final response headers
      """
    And HTTP receiver "api" captured no request with request line "POST /events/2"
    When HTTP receiver "api" answers requests for "/events/1" with "respond 204"
    Then HTTP receiver "api" eventually captures one request that is
      """
      POST /events/2
      """
    And within "30s" DESCRIBE EMITTER "published" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_transport @http_emitter_dns
  Scenario Outline: An HTTP emitter keeps its request pending while its endpoint name does not resolve or refuses connections
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And cluster peers are addressed by "DNS names"
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
        'endpoint' = 'http://api.nervix.test:{{http_receiver_port.api}}', 'timeout_ms' = 5000
      };
      CREATE EMITTER published FROM outgoing
        TO HTTP api METHOD 'POST' PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"},{"event_id":"2"}]
      """
    # The node resolver asks the DNS fixture, which does not know the endpoint's name yet.
    Then within "30s" DESCRIBE EMITTER "published" on the leader node contains
      """
      transient error: HTTP DNS resolution failed: resolving 'api.nervix.test' failed: the name does not exist
      """
    And the DNS fixture eventually receives a question for "api.nervix.test"
    # The name then resolves to an address nothing listens on, so every connection is refused.
    When the DNS fixture answers "api.nervix.test" with addresses "127.0.5.9"
    Then within "30s" DESCRIBE EMITTER "published" on the leader node contains
      """
      transient error: HTTP connection failed: Connection refused
      """
    And HTTP receiver "api" has captured exactly 0 requests
    When the DNS fixture answers "api.nervix.test" with addresses "127.0.0.1"
    Then HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 1 is
      """
      POST /events/1
      Host: api.nervix.test:{{http_receiver_port.api}}
      """
    And HTTP receiver "api" request 2 is
      """
      POST /events/2
      Host: api.nervix.test:{{http_receiver_port.api}}
      """
    And HTTP receiver "api" has captured exactly 2 requests
    And within "30s" DESCRIBE EMITTER "published" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
