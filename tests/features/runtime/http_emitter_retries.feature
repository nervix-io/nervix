Feature: HTTP emitter retries, acknowledgements and backpressure
  A request whose outcome is unresolved stays with the emitter host, which resends exactly that
  prepared request on the declared physical backoff, or later when a retryable response names a
  valid Retry-After delay. Delivered and definitively rejected requests leave every retry, later
  work waits behind the unresolved request, and attached upstream acknowledgements stay open until
  the request resolves. A definitive rejection routes its record's message error and later records
  continue.

  @http_emitter_retries
  Scenario Outline: A retry resends only the unresolved requests of a flush, ahead of later work
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers with
      """
      respond 200
      respond 503; header Retry-After: 3
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
      CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (event_id string, payload string);
      CREATE CODEC event_body_codec FROM WIRE JSON SCHEMA event_body_wire TO SCHEMA outbound_event;
      CREATE RELAY first_events SCHEMA outbound_event UNBRANCHED;
      CREATE RELAY later_events SCHEMA outbound_event UNBRANCHED;
      CREATE VHOST edge http-emitter-{{test_id}}.example.com;
      CREATE ENDPOINT first_ingress ON edge PATH '/first' TYPE HTTP;
      CREATE ENDPOINT later_ingress ON edge PATH '/later' TYPE HTTP;
      CREATE INGESTOR first_source
        FROM ENDPOINT first_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO first_events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INGESTOR later_source
        FROM ENDPOINT later_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO later_events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE EMITTER published FROM first_events, later_events
        TO HTTP api
          METHOD 'POST'
          PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 200ms
          ENCODE USING event_body_codec
        INHERIT ALL
        INVOKE write_header('X-Attempt-Key', uuid_v4())
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/first"
      """
      [{"event_id":"1","payload":"one"},{"event_id":"2","payload":"two"},{"event_id":"3","payload":"three"}]
      """
    Then HTTP receiver "api" eventually receives at least 2 requests
    # Record 4 arrives on another source relay while record 2's request waits out the three
    # seconds its 503 asked for, which is longer than the declared backoff maximum.
    When http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/later"
      """
      [{"event_id":"4","payload":"four"}]
      """
    Then HTTP receiver "api" eventually receives at least 5 requests
    And HTTP receiver "api" request 1 is
      """
      POST /events/1

      {"event_id":"1","payload":"one"}
      """
    And HTTP receiver "api" request 2 is
      """
      POST /events/2

      {"event_id":"2","payload":"two"}
      """
    And HTTP receiver "api" request 3 repeats request 2
    And HTTP receiver "api" request 3 arrived at least "3s" after request 2
    And HTTP receiver "api" request 4 is
      """
      POST /events/3

      {"event_id":"3","payload":"three"}
      """
    And HTTP receiver "api" request 5 is
      """
      POST /events/4

      {"event_id":"4","payload":"four"}
      """
    And HTTP receiver "api" has captured exactly 5 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_retries
  Scenario Outline: A retryable response's Retry-After <case> extends the wait beyond the declared backoff
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
        TO HTTP api METHOD 'POST' PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY
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
    And HTTP receiver "api" request 2 has no header "Idempotency-Key"
    And HTTP receiver "api" request 3 is
      """
      POST /events/2
      """
    And HTTP receiver "api" has captured exactly 3 requests

    Examples:
      | cluster_size | case                         | response                            | minimum_gap |
      | 1            | in seconds                   | respond 503; header Retry-After: 2  | 2s          |
      | 3            | in seconds                   | respond 503; header Retry-After: 2  | 2s          |
      | 1            | on an authentication failure | respond 401; header Retry-After: 2  | 2s          |
      | 3            | on an authentication failure | respond 401; header Retry-After: 2  | 2s          |
      | 1            | as an HTTP date              | respond 429; retry after date in 3s | 3s          |
      | 3            | as an HTTP date              | respond 429; retry after date in 3s | 3s          |

  @http_emitter_retries
  Scenario Outline: A Retry-After <case> adds no delay to the declared backoff
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
        TO HTTP api METHOD 'POST' PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"},{"event_id":"2"}]
      """
    # Each value would ask for an hour or more if it were honored, far beyond the receiver's wait.
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
      | cluster_size | case                             | response                                                        |
      | 1            | date in the past                 | respond 503; header Retry-After: Sun, 06 Nov 1994 08:49:37 GMT  |
      | 3            | date in the past                 | respond 503; header Retry-After: Sun, 06 Nov 1994 08:49:37 GMT  |
      | 1            | with fractional seconds          | respond 503; header Retry-After: 3600.5                         |
      | 3            | with fractional seconds          | respond 503; header Retry-After: 3600.5                         |
      | 1            | repeated in two fields           | respond 503; header Retry-After: 3600; header Retry-After: 3600 |
      | 3            | repeated in two fields           | respond 503; header Retry-After: 3600; header Retry-After: 3600 |
      | 1            | beyond every representable delay | respond 503; header Retry-After: 99999999999999999999999        |
      | 3            | beyond every representable delay | respond 503; header Retry-After: 99999999999999999999999        |

  @http_emitter_retries
  Scenario Outline: Retries double the declared backoff and continue until the request resolves
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers with
      """
      respond 503
      respond 503
      respond 503
      respond 503
      respond 503
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
        TO HTTP api METHOD 'POST' PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 300ms MAX 600ms WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"},{"event_id":"2"}]
      """
    Then HTTP receiver "api" eventually receives at least 7 requests
    And HTTP receiver "api" request 2 arrived at least "300ms" after request 1
    And HTTP receiver "api" request 3 arrived at least "600ms" after request 2
    And HTTP receiver "api" request 4 arrived at least "600ms" after request 3
    And HTTP receiver "api" request 6 repeats request 1
    And HTTP receiver "api" request 7 is
      """
      POST /events/2
      """
    And HTTP receiver "api" has captured exactly 7 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_retries
  Scenario Outline: An <attachment> HTTP emitter <acknowledgement> while its request is unresolved
    Given Kafka is running
    And HTTP receiver "api" is running
    And HTTP receiver "api" answers with
      """
      respond 503; header Retry-After: 2
      hold response until released
      """
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And Kafka topic "http_leases_in_{{test_id}}" exists with 1 partitions
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
        FROM KAFKA kafka_main TOPIC http_leases_in_{{test_id}}
          OFFSET BY CONSUMER GROUP http_leases_group_{{test_id}}
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
      CREATE <attachment> EMITTER published FROM outgoing
        TO HTTP api METHOD 'POST' PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY
        INVOKE write_header('X-Attempt-Key', uuid_v4())
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "http_leases_group_{{test_id}}" eventually has 1 consumers
    # One Kafka message carries both records. The source's one-second ACK TIMEOUT is shorter than
    # the two-second wait the 503 asks for and than the held retry, so an attached source keeps
    # the message only while the emitter keeps the records' acknowledgements alive.
    When Kafka message is published to topic "http_leases_in_{{test_id}}"
      """
      [{"event_id":"1"},{"event_id":"2"}]
      """
    Then HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 1 is
      """
      POST /events/1
      """
    And HTTP receiver "api" request 2 repeats request 1
    And HTTP receiver "api" request 2 arrived at least "2s" after request 1
    # The receiver holds the retried request until it is released, so record 1 is unresolved and
    # record 2 waits behind it for as long as this step observes the committed offset.
    And within "<held_window>" Kafka consumer group "http_leases_group_{{test_id}}" next offset for topic "http_leases_in_{{test_id}}" partition 0 is "<held_offset>"
    When HTTP receiver "api" releases its held responses with "respond 204"
    Then HTTP receiver "api" eventually receives at least 3 requests
    And HTTP receiver "api" request 3 is
      """
      POST /events/2
      """
    And within "30s" Kafka consumer group "http_leases_group_{{test_id}}" next offset for topic "http_leases_in_{{test_id}}" partition 0 is "at least 1"
    And HTTP receiver "api" has captured exactly 3 requests

    Examples:
      | cluster_size | attachment | acknowledgement                         | held_window | held_offset |
      | 1            | ATTACHED   | holds its upstream acknowledgement      | 3s          | below 1     |
      | 3            | ATTACHED   | holds its upstream acknowledgement      | 3s          | below 1     |
      | 1            | DETACHED   | acknowledges upstream but still retries | 30s         | at least 1  |
      | 3            | DETACHED   | acknowledges upstream but still retries | 30s         | at least 1  |

  @http_emitter_retries
  Scenario Outline: A terminal <status> routes its record's error with the original input, captured state and attempted body, and later records continue
    Given HTTP receiver "api" is running
    And HTTP receiver "elsewhere" is running
    And HTTP receiver "api" answers with
      """
      respond 204
      respond <status>; header Location: {{http_receiver.elsewhere}}/moved; header Retry-After: 3600
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
      CREATE SCHEMA outbound_event (event_id STRING, tenant STRING, payload STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA route (route_key STRING, prefix STRING);
      CREATE CODEC routes_codec FROM JSON TO SCHEMA route
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA event_body (event_id STRING, payload STRING);
      CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (event_id string, payload string);
      CREATE CODEC event_body_codec FROM WIRE JSON SCHEMA event_body_wire TO SCHEMA event_body;
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
        TO routes INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE INGESTOR outgoing_source
        FROM ENDPOINT outgoing_ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING outbound_events_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE EMITTER guarded FROM outgoing
        USING MATERIALIZED STATE routes REQUIRED WAIT
        TO HTTP api
          METHOD 'POST'
          PATH concat(relay_state.routes.prefix, '/events/', event_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms
          ENCODE USING event_body_codec
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
    # Record 2's refusal is final whatever its response asks for: it names no Location that is
    # followed and no Retry-After that is waited for, and the tenant its error reads is held only
    # by the original input.
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1","tenant":"north","payload":"first"},{"event_id":"2","tenant":"south","payload":"second"},{"event_id":"3","tenant":"west","payload":"third"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "event_id":"2" | "tenant":"south" | "error_code":"external" | "error_message":"HTTP endpoint answered with status <status>" | "operation":"publish" | "state_prefix":"/tenants/north" | "attempted_payload":"SECOND"
      """
    And HTTP receiver "api" eventually receives at least 3 requests
    And HTTP receiver "api" request 2 is
      """
      POST /tenants/north/events/2
      X-Tenant: south

      {"event_id":"2","payload":"SECOND"}
      """
    And HTTP receiver "api" request 3 is
      """
      POST /tenants/north/events/3
      X-Tenant: west

      {"event_id":"3","payload":"THIRD"}
      """
    And HTTP receiver "api" has captured exactly 3 requests
    And HTTP receiver "elsewhere" has captured exactly 0 requests

    Examples:
      | cluster_size | status |
      | 1            | 404    |
      | 3            | 404    |
      | 1            | 409    |
      | 3            | 409    |
      | 1            | 413    |
      | 3            | 413    |
      | 1            | 308    |
      | 3            | 308    |

  @http_emitter_retries
  Scenario Outline: Terminal responses in interleaved branches route each error in its own branch while the other records are delivered
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers requests for "/tenants/north/events/n1" with "respond 404"
    And HTTP receiver "api" answers requests for "/tenants/south/events/s2" with "respond 409"
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA outbound_event (event_id STRING, tenant STRING);
      CREATE CODEC outbound_events_codec FROM JSON TO SCHEMA outbound_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA tenant_branch (tenant STRING);
      CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m;
      CREATE SCHEMA rejected_request (event_id STRING, error_message STRING);
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
          PATH concat('/tenants/', input.tenant, '/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms
          WITHOUT BODY
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_requests
          SET event_id = input.event_id,
              error_message = error.message
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION rejected_requests_subscription TO rejected_requests;
      START;
      """
    # The branches have no order between them, so the receiver answers each request by its target.
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"n1","tenant":"north"},{"event_id":"s1","tenant":"south"},{"event_id":"n2","tenant":"north"},{"event_id":"s2","tenant":"south"},{"event_id":"n3","tenant":"north"}]
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "event_id":"n1" | "error_message":"HTTP endpoint answered with status 404" | key={"tenant":"north"}
      "event_id":"s2" | "error_message":"HTTP endpoint answered with status 409" | key={"tenant":"south"}
      """
    And HTTP receiver "api" eventually receives at least 5 requests
    And HTTP receiver "api" captured one request that is
      """
      POST /tenants/north/events/n1
      """
    And HTTP receiver "api" captured one request that is
      """
      POST /tenants/south/events/s1
      """
    And HTTP receiver "api" captured one request that is
      """
      POST /tenants/north/events/n2
      """
    And HTTP receiver "api" captured one request that is
      """
      POST /tenants/south/events/s2
      """
    And HTTP receiver "api" captured one request that is
      """
      POST /tenants/north/events/n3
      """
    And HTTP receiver "api" has captured exactly 5 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_retries
  Scenario Outline: A paced domain accelerates the flush cadence and expression time but not the physical <case>
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
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 1s;
      """
    When these NSPL commands are executed
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
        TIMESTAMP NOW
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = <timeout_ms>
      };
      CREATE EMITTER published FROM outgoing
        TO HTTP api METHOD 'POST' PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF <backoff> MAX <backoff> WITHOUT BODY
        INVOKE write_header('X-Domain-Year', format_datetime('%Y', now()))
        FLUSH EACH 300s MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START AT '2000-01-01T00:00:00Z' TIME RATE 100.0;
      """
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"}]
      """
    # At TIME RATE 100 the 300-second logical flush cadence releases the record after three
    # physical seconds, well inside the receiver's wait, while the retry waits stay physical.
    Then HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 1 is
      """
      POST /events/1
      X-Domain-Year: 2000
      """
    And HTTP receiver "api" request 2 repeats request 1
    And HTTP receiver "api" request 2 arrived at least "2s" after request 1
    And HTTP receiver "api" has captured exactly 2 requests

    Examples:
      | cluster_size | case              | response                           | timeout_ms | backoff |
      | 1            | retry backoff     | respond 503                        | 5000       | 2s      |
      | 3            | retry backoff     | respond 503                        | 5000       | 2s      |
      | 1            | Retry-After delay | respond 503; header Retry-After: 2 | 5000       | 100ms   |
      | 3            | Retry-After delay | respond 503; header Retry-After: 2 | 5000       | 100ms   |
      | 1            | request timeout   | hold response                      | 2000       | 100ms   |
      | 3            | request timeout   | hold response                      | 2000       | 100ms   |

  @http_emitter_retries
  Scenario Outline: An endpoint that applied a request whose response was lost receives the same request again
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
      CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (event_id string, payload string);
      CREATE CODEC event_body_codec FROM WIRE JSON SCHEMA event_body_wire TO SCHEMA outbound_event;
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
          METHOD 'PUT'
          PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms
          ENCODE USING event_body_codec
        INHERIT ALL
        INVOKE write_header('Idempotency-Key', input.event_id)
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    # The receiver reads the whole request, as an endpoint that applies it does, and closes the
    # connection before answering. Nervix cannot tell that from a request that never arrived, so it
    # sends the same request again; only the endpoint's own key can recognize the duplicate.
    And http payload is posted to host "http-emitter-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"evt-1","payload":"once"}]
      """
    Then HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 1 is
      """
      PUT /events/evt-1
      Idempotency-Key: evt-1

      {"event_id":"evt-1","payload":"once"}
      """
    And HTTP receiver "api" request 2 repeats request 1
    And HTTP receiver "api" has captured exactly 2 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
