Feature: HTTP receiver fixture
  The HTTP receiver stands in for an operator-provisioned endpoint. These scenarios qualify it
  against the HTTP client Nervix already has, the polling ingestor's, so the receiver's capture,
  scripted status sequences and TLS identity are proven before an HTTP emitter depends on them.

  Scenario Outline: The HTTP receiver captures a polling client's requests and answers from its script
    Given HTTP receiver "api" is running
    And HTTP receiver "api" answers with
      """
      respond 503; header Retry-After: 1
      respond 200; header Content-Type: application/json; body {"user_id":42}
      """
    And HTTP receiver "api" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT receiver_api
        TYPE HTTP
        CONFIG {
          'endpoint' = '{{http_receiver.api}}/poll/{{test_id}}?page=1',
          'method' = 'GET',
          'timeout_ms' = 5000
        };
      CREATE INGESTOR poll_receiver
        FROM HTTP receiver_api EVERY 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then the relay subscription receives a payload
      """
      {"user_id":42}
      """
    And HTTP receiver "api" eventually receives at least 2 requests
    And HTTP receiver "api" request 1 is
      """
      GET /poll/{{test_id}}?page=1
      """
    And HTTP receiver "api" request 2 is
      """
      GET /poll/{{test_id}}?page=1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An HTTPS receiver serves a client that presents the certificate it issued
    Given HTTPS receiver "secure" is running with a certificate for "127.0.0.1" and requires a client certificate
    And HTTP receiver "secure" answers with
      """
      respond 200; header Content-Type: application/json; body {"user_id":7}
      """
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
      CREATE SCHEMA notification (
        user_id I64
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT receiver_secure
        TYPE HTTP
        MOUNT receiver_tls VERSION 1
        CONFIG {
          'endpoint' = '{{http_receiver.secure}}/poll/{{test_id}}',
          'method' = 'GET',
          'timeout_ms' = 5000,
          'tls_ca_file' = '{{receiver_tls}}/ca.pem',
          'tls_cert_file' = '{{receiver_tls}}/client.pem',
          'tls_key_file' = '{{receiver_tls}}/client-key.pem'
        };
      CREATE INGESTOR poll_secure
        FROM HTTP receiver_secure EVERY 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    Then the relay subscription receives a payload
      """
      {"user_id":7}
      """
    And HTTP receiver "secure" eventually receives at least 1 request
    And HTTP receiver "secure" request 1 is
      """
      GET /poll/{{test_id}}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A client that does not trust the HTTPS receiver fails its TLS handshake
    Given HTTPS receiver "untrusted" is running with a certificate for "127.0.0.1"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer
      );
      CREATE CODEC notification_codec
        FROM WIRE JSON SCHEMA notification_wire
        TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE CLIENT receiver_untrusted
        TYPE HTTP
        CONFIG {
          'endpoint' = '{{http_receiver.untrusted}}/poll/{{test_id}}',
          'method' = 'GET',
          'timeout_ms' = 5000
        };
      CREATE INGESTOR poll_untrusted
        FROM HTTP receiver_untrusted EVERY 1s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
        INHERIT ALL
        UNBRANCHED
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then HTTP receiver "untrusted" eventually records a failed TLS handshake

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
