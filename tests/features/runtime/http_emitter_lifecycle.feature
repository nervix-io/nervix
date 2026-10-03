Feature: HTTP emitter lifecycle
  A configuration change drains requests with the client and request fields that admitted them.
  A stopped emitter retains no prepared HTTP request; an attached source decides redelivery.

  @http_emitter_lifecycle
  Scenario Outline: ALTER drains an admitted HTTP request before changing its destination
    Given HTTP receiver "first" is running
    And HTTP receiver "first" answers with
      """
      hold response until released
      """
    And HTTP receiver "first" answers unscripted requests with "respond 204"
    And HTTP receiver "second" is running
    And HTTP receiver "second" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event (event_id STRING);
      CREATE CODEC event_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-lifecycle-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT first_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.first}}', 'timeout_ms' = 60000
      };
      CREATE CLIENT second_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.second}}', 'timeout_ms' = 5000
      };
      CREATE ATTACHED EMITTER published FROM outgoing
        TO HTTP first_api METHOD 'POST' PATH concat('/first/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-lifecycle-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"}]
      """
    Then HTTP receiver "first" eventually receives at least 1 requests
    When these NSPL commands begin executing in the background
      """
      ALTER EMITTER published SET TO HTTP second_api
        METHOD 'PUT' PATH concat('/second/', input.event_id)
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY;
      """
    And HTTP receiver "first" releases its held responses with "respond 204"
    Then the background NSPL execution succeeds
    And the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When http payload is posted to host "http-lifecycle-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"2"}]
      """
    Then HTTP receiver "second" eventually receives at least 1 requests
    And HTTP receiver "first" request 1 is
      """
      POST /first/1
      """
    And HTTP receiver "second" request 1 is
      """
      PUT /second/2
      """
    And HTTP receiver "first" has captured exactly 1 requests
    And HTTP receiver "second" has captured exactly 1 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_failed_drain
  Scenario Outline: A failed HTTP emitter drain leaves the admitted request and active destination intact
    Given HTTP receiver "first" is running
    And HTTP receiver "first" answers with
      """
      hold response until released
      """
    And HTTP receiver "first" answers unscripted requests with "respond 204"
    And HTTP receiver "second" is running
    And HTTP receiver "second" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event (event_id STRING);
      CREATE CODEC event_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-drain-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT first_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.first}}', 'timeout_ms' = 60000
      };
      CREATE CLIENT second_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.second}}', 'timeout_ms' = 5000
      };
      CREATE ATTACHED EMITTER published FROM outgoing
        TO HTTP first_api METHOD 'POST' PATH concat('/first/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-drain-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"}]
      """
    Then HTTP receiver "first" eventually receives at least 1 requests
    Given the next pending entity drain in domain "{{domain}}" is forced to time out
    When these NSPL commands fail with "timed out draining domain"
      """
      ALTER EMITTER published SET TO HTTP second_api
        METHOD 'PUT' PATH concat('/second/', input.event_id)
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY;
      """
    And these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER published;
      """
    Then the last command output contains
      """
      TO HTTP first_api METHOD 'POST' PATH concat('/first/', input.event_id)
      """
    When HTTP receiver "first" releases its held responses with "respond 204"
    And http payload is posted to host "http-drain-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"2"}]
      """
    Then HTTP receiver "first" eventually receives at least 2 requests
    And HTTP receiver "first" request 2 is
      """
      POST /first/2
      """
    And HTTP receiver "second" has captured exactly 0 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_body_replacement
  Scenario Outline: A body-mode change validates retained clauses and a transaction replaces complete HTTP construction
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
      CREATE SCHEMA event (id STRING);
      CREATE CODEC event_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE SCHEMA required_body (id STRING);
      CREATE WIRE JSON SCHEMA required_wire MODE STRICT (id string);
      CREATE CODEC required_codec FROM WIRE JSON SCHEMA required_wire TO SCHEMA required_body;
      CREATE SCHEMA optional_body (id STRING OPTIONAL);
      CREATE WIRE JSON SCHEMA optional_wire MODE STRICT (id string OPTIONAL);
      CREATE CODEC optional_codec FROM WIRE JSON SCHEMA optional_wire TO SCHEMA optional_body;
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE RELAY spare SCHEMA event UNBRANCHED;
      CREATE SCHEMA rejected (attempted_id STRING OPTIONAL);
      CREATE RELAY rejected_requests SCHEMA rejected UNBRANCHED;
      CREATE VHOST edge http-body-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT api_client TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE ATTACHED EMITTER published FROM outgoing
        TO HTTP api_client METHOD 'POST' PATH '/coded'
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms ENCODE USING required_codec
        INHERIT id INVOKE write_header('X-Mode', 'coded')
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER set_body FROM spare
        TO HTTP api_client METHOD 'POST' PATH '/set'
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms ENCODE USING required_codec
        SET id = input.id
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE EMITTER scoped FROM spare
        TO HTTP api_client METHOD 'POST' PATH '/scoped'
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms ENCODE USING optional_codec
        FLUSH IMMEDIATE
        ON MESSAGE ERROR SEND TO rejected_requests
          SET attempted_id = partial_output.id
        ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-body-{{test_id}}.example.com" path "/events"
      """
      [{"id":"1"}]
      """
    Then HTTP receiver "api" eventually receives at least 1 requests
    And HTTP receiver "api" request 1 is
      """
      POST /coded
      X-Mode: coded

      {"id":"1"}
      """
    When these NSPL commands fail with "emitter body selection does not support the retained construction"
      """
      ALTER EMITTER published SET TO HTTP api_client
        METHOD 'DELETE' PATH '/empty'
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY;
      """
    And these NSPL commands are executed on the leader node
      """
      SHOW CREATE EMITTER published;
      """
    Then the last command output contains
      """
      ENCODE USING required_codec
      """
    And the last command output contains
      """
      INHERIT id
      """
    When these NSPL commands fail with "emitter body selection does not support the retained construction"
      """
      ALTER EMITTER set_body SET TO HTTP api_client
        METHOD 'DELETE' PATH '/empty'
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY;
      """
    When these NSPL commands fail with "partial_output"
      """
      ALTER EMITTER scoped SET TO HTTP api_client
        METHOD 'DELETE' PATH '/empty'
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY;
      """
    Then SHOW CREATE EMITTER on the leader node renders these clauses
      | emitter  | clause                      |
      | set_body | ENCODE USING required_codec |
      | scoped   | ENCODE USING optional_codec |
    # A transactional replacement is validated whole before anything activates: a commit whose
    # replacement cannot support its request fields or its construction fails, and the active
    # emitter keeps delivering as it was.
    Given client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      DROP EMITTER published;
      CREATE ATTACHED EMITTER published FROM outgoing
        TO HTTP api_client METHOD 'GET' PATH '/refused'
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms ENCODE USING required_codec
        INHERIT id
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then client "owner" transaction id is saved as placeholder "refused_request_transaction"
    When client "owner" attempts to commit its transaction
    Then the last command error contains
      """
      GET and HEAD require WITHOUT BODY
      """
    And transaction "{{refused_request_transaction}}" eventually has state "FAILED"
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      DROP EMITTER published;
      CREATE ATTACHED EMITTER published FROM outgoing
        TO HTTP api_client METHOD 'DELETE' PATH '/refused'
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms ENCODE USING required_codec
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then client "owner" transaction id is saved as placeholder "refused_construction_transaction"
    When client "owner" attempts to commit its transaction
    Then the last command error contains
      """
      remains uninitialized
      """
    And transaction "{{refused_construction_transaction}}" eventually has state "FAILED"
    When http payload is posted to host "http-body-{{test_id}}.example.com" path "/events"
      """
      [{"id":"kept"}]
      """
    Then HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 2 is
      """
      POST /coded
      X-Mode: coded

      {"id":"kept"}
      """
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      DROP EMITTER published;
      CREATE ATTACHED EMITTER published FROM outgoing
        TO HTTP api_client METHOD 'DELETE' PATH '/empty'
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms ENCODE USING optional_codec
        INVOKE write_header('X-Mode', 'replacement')
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    When client "owner" executes these NSPL commands
      """
      COMMIT;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    And transaction "{{transaction_id}}" report step 1 eventually records planned quiesce "ENTITY_PAUSE", actual quiesce "ENTITY_PAUSE", execution "APPLIED", and outcomes "REQUESTED,CONFIRMED,RELEASED"
    When these NSPL commands are executed on the leader node
      """
      ALTER EMITTER published SET TO HTTP api_client
        METHOD 'DELETE' PATH '/empty'
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When http payload is posted to host "http-body-{{test_id}}.example.com" path "/events"
      """
      [{"id":"2"}]
      """
    Then HTTP receiver "api" eventually receives at least 3 requests
    And HTTP receiver "api" request 3 is
      """
      DELETE /empty
      X-Mode: replacement
      """
    And HTTP receiver "api" has captured exactly 3 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_impact
  Scenario Outline: HTTP emitter changes use their effective quiesce levels
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
      CREATE SCHEMA event (id STRING);
      CREATE CODEC event_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE RELAY backup SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-impact-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT first_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE CLIENT second_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE EMITTER published FROM outgoing
        TO HTTP first_api METHOD 'POST' PATH '/first'
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY
        FLUSH EACH 1h MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER EMITTER published SET MODE ACK RETRY POLICY BACKOFF 200ms MAX 200ms;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER EMITTER published SET CLIENT second_api;
      """
    Then the last command output contains
      """
      quiesce level: ENTITY_PAUSE
      """
    When http payload is posted to host "http-impact-{{test_id}}.example.com" path "/events"
      """
      [{"id":"1"}]
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER EMITTER published SET FLUSH IMMEDIATE;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    And HTTP receiver "api" eventually receives at least 1 requests
    And HTTP receiver "api" request 1 is
      """
      POST /first
      """
    When these NSPL commands are executed on the leader node
      """
      ALTER EMITTER published ADD FROM backup;
      """
    Then the last command output contains
      """
      quiesce level: DOMAIN_PAUSE
      """
    Given client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      DROP CLIENT second_api;
      CREATE CLIENT second_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 6000
      };
      """
    When client "owner" executes these NSPL commands
      """
      COMMIT;
      """
    Then the last command output contains
      """
      quiesce level: DOMAIN_PAUSE
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @http_emitter_shutdown_recovery
  Scenario Outline: A shutdown deadline leaves unresolved HTTP work for Kafka redelivery after restart
    Given Kafka is running
    And HTTP receiver "api" is running
    And HTTP receiver "api" answers with
      """
      lose response
      hold response until released
      """
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And graceful shutdown drain is enabled
    And drain timeout is configured as "3s"
    And the production sticky scheduler is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "http_recovery_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      <cordon>
      CREATE SCHEMA event (id STRING);
      CREATE CODEC event_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}', 'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR source
        FROM KAFKA kafka_main TOPIC http_recovery_{{test_id}}
          OFFSET BY CONSUMER GROUP http_recovery_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT api_client TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 60000
      };
      CREATE ATTACHED EMITTER published FROM outgoing
        TO HTTP api_client METHOD 'POST' PATH concat('/events/', input.id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "http_recovery_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "http_recovery_{{test_id}}"
      """
      [{"id":"1"}]
      """
    Then HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 2 repeats request 1
    When node "node-1" is gracefully stopped
    Then the last cluster operation completes within "20s"
    # The forced ending closes the retried request's connection rather than leaving it open.
    And HTTP receiver "api" eventually sees the client abandon at least 1 unfinished response
    And within "2s" Kafka consumer group "http_recovery_group_{{test_id}}" next offset for topic "http_recovery_{{test_id}}" partition 0 is "below 1"
    When node "node-1" is started
    Then HTTP receiver "api" eventually receives at least 3 requests
    And HTTP receiver "api" request 3 repeats request 1
    And within "30s" Kafka consumer group "http_recovery_group_{{test_id}}" next offset for topic "http_recovery_{{test_id}}" partition 0 is "at least 1"
    And HTTP receiver "api" has captured exactly 3 requests

    Examples:
      | cluster_size | cordon                                  |
      | 1            |                                         |
      | 3            | CORDON NODE node-2; CORDON NODE node-3; |

  @http_emitter_shutdown_flush
  Scenario Outline: Graceful shutdown force-flushes an HTTP emitter inside the drain deadline
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And graceful shutdown drain is enabled
    And drain timeout is configured as "20s"
    And the production sticky scheduler is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      <cordon>
      CREATE SCHEMA event (id STRING);
      CREATE CODEC event_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-shutdown-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT api_client TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 5000
      };
      CREATE ATTACHED EMITTER published FROM outgoing
        TO HTTP api_client METHOD 'POST' PATH concat('/events/', input.id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY
        FLUSH EACH 1h MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "http-shutdown-{{test_id}}.example.com" path "/events"
      """
      [{"id":"1"}]
      """
    When node "node-1" begins stopping
    Then HTTP receiver "api" eventually receives at least 1 requests
    And HTTP receiver "api" request 1 is
      """
      POST /events/1
      """
    When node "node-1" is stopped while timing shutdown
    Then the last cluster operation completes within "20s"
    And HTTP receiver "api" has captured exactly 1 requests

    Examples:
      | cluster_size | cordon                                  |
      | 1            |                                         |
      | 3            | CORDON NODE node-2; CORDON NODE node-3; |

  @http_emitter_stop_recovery
  Scenario Outline: Stopping a domain whose HTTP request cannot drain releases it for the source to redeliver to the reconfigured destination
    Given Kafka is running
    And HTTP receiver "first" is running
    And HTTP receiver "first" answers unscripted requests with "hold response until released"
    And HTTP receiver "second" is running
    And HTTP receiver "second" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "http_stop_{{test_id}}" exists with 1 partitions
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event (id STRING);
      CREATE CODEC event_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE CLIENT kafka_main TYPE KAFKA CONFIG {
        'bootstrap.servers' = '{{kafka_addr}}', 'auto.offset.reset' = 'earliest'
      };
      CREATE INGESTOR source
        FROM KAFKA kafka_main TOPIC http_stop_{{test_id}}
          OFFSET BY CONSUMER GROUP http_stop_group_{{test_id}}
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING event_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLIENT first_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.first}}', 'timeout_ms' = 60000
      };
      CREATE CLIENT second_api TYPE HTTP CONFIG {
        'endpoint' = '{{http_receiver.second}}', 'timeout_ms' = 5000
      };
      CREATE ATTACHED EMITTER published FROM outgoing
        TO HTTP first_api METHOD 'POST' PATH concat('/first/', input.id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then Kafka consumer group "http_stop_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "http_stop_{{test_id}}"
      """
      [{"id":"1"}]
      """
    Then HTTP receiver "first" eventually receives at least 1 request
    # The first destination never answers, so a live alteration cannot drain the admitted request.
    Given the next pending entity drain in domain "{{domain}}" is forced to time out
    When these NSPL commands fail with "timed out draining domain"
      """
      ALTER EMITTER published SET TO HTTP second_api
        METHOD 'PUT' PATH concat('/second/', input.id)
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY;
      """
    # Stopping the domain ends the request instead: its connection is released and its record stays
    # unacknowledged at the attached source.
    When these NSPL commands are executed on the leader node
      """
      STOP;
      """
    Then HTTP receiver "first" eventually sees the client abandon at least 1 unfinished response
    And within "2s" Kafka consumer group "http_stop_group_{{test_id}}" next offset for topic "http_stop_{{test_id}}" partition 0 is "below 1"
    # While the domain is stopped, the replacement needs no drain, and the restarted source
    # redelivers the record, which the replacement sends to the second destination.
    When these NSPL commands are executed on the leader node
      """
      ALTER EMITTER published SET TO HTTP second_api
        METHOD 'PUT' PATH concat('/second/', input.id)
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY;
      START;
      """
    Then HTTP receiver "second" eventually receives at least 1 request
    And HTTP receiver "second" request 1 is
      """
      PUT /second/1
      """
    And within "30s" Kafka consumer group "http_stop_group_{{test_id}}" next offset for topic "http_stop_{{test_id}}" partition 0 is "at least 1"
    And HTTP receiver "first" has captured exactly 1 request
    And HTTP receiver "second" has captured exactly 1 request

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
