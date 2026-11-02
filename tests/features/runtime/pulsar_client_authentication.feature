Feature: Pulsar clients authenticate to their broker
  A Pulsar client presents a token, a client certificate or both, and both directions connect
  through the one client they share. These scenarios run against a broker that refuses every
  client without a credential, so a message that crosses it proves the credential reached it.

  Scenario Outline: A Pulsar client with <credential> reads and writes through a broker that requires it
    Given Pulsar with authentication is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has dev TLS resource directory "dev_tls"
    And authenticated Pulsar topic "authenticated_out_{{test_id}}" is observed
    When these Plexus commands are executed
      """
      CREATE RESOURCE dev_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE dev_tls VERSION "{{dev_tls}}";
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE PULSAR CLIENT authenticated
        MOUNT dev_tls VERSION 1
        CONFIG { <client_config> };
      CREATE PULSAR TOPIC INGESTOR authenticated_in
        FROM authenticated TOPIC 'authenticated_in_{{test_id}}'
          SUBSCRIPTION 'authenticated_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE PULSAR TOPIC EMITTER authenticated_out FROM notifications
        TO authenticated TOPIC 'authenticated_out_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 250ms MAX 5s
          ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    And authenticated Pulsar message is published to topic "authenticated_in_{{test_id}}"
      """
      {"user_id":42}
      """
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size | credential                       | client_config                                                                                                                                                                                            |
      | 1            | a token                          | 'addr' = '{{pulsar_auth_addr}}', 'auth_token' = '{{pulsar_auth_token}}'                                                                                                                                  |
      | 3            | a token                          | 'addr' = '{{pulsar_auth_addr}}', 'auth_token' = '{{pulsar_auth_token}}'                                                                                                                                  |
      | 1            | a client certificate             | 'addr' = '{{pulsar_auth_tls_addr}}', 'tls_ca_file' = '{{dev_tls}}/ca.pem', 'tls_cert_file' = '{{dev_tls}}/node.pem', 'tls_key_file' = '{{dev_tls}}/node-key.pem'                                         |
      | 3            | a client certificate             | 'addr' = '{{pulsar_auth_tls_addr}}', 'tls_ca_file' = '{{dev_tls}}/ca.pem', 'tls_cert_file' = '{{dev_tls}}/node.pem', 'tls_key_file' = '{{dev_tls}}/node-key.pem'                                         |
      | 1            | a client certificate and a token | 'addr' = '{{pulsar_auth_tls_addr}}', 'auth_token' = '{{pulsar_auth_token}}', 'tls_ca_file' = '{{dev_tls}}/ca.pem', 'tls_cert_file' = '{{dev_tls}}/node.pem', 'tls_key_file' = '{{dev_tls}}/node-key.pem' |
      | 3            | a client certificate and a token | 'addr' = '{{pulsar_auth_tls_addr}}', 'auth_token' = '{{pulsar_auth_token}}', 'tls_ca_file' = '{{dev_tls}}/ca.pem', 'tls_cert_file' = '{{dev_tls}}/node.pem', 'tls_key_file' = '{{dev_tls}}/node-key.pem' |

  # An emitter that cannot connect reports why as its transient status on whichever node runs it,
  # and opens again on its RETRY POLICY, so the refusal shows the same way on any topology.
  Scenario Outline: A broker's refusal of <credential> is a Pulsar emitter's transient status and says why
    Given Pulsar with authentication is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has dev TLS resource directory "dev_tls"
    When these Plexus commands are executed
      """
      CREATE RESOURCE dev_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE dev_tls VERSION "{{dev_tls}}";
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE PULSAR CLIENT refused
        MOUNT dev_tls VERSION 1
        CONFIG { <client_config> };
      CREATE PULSAR TOPIC EMITTER refused_out FROM notifications
        TO refused TOPIC 'refused_out_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 250ms MAX 5s
          ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then within "60s" DESCRIBE EMITTER "refused_out" on the leader node contains
      """
      transient error: <reported>
      """

    Examples:
      | cluster_size | credential            | client_config                                                                                                     | reported                                                                          |
      | 1            | a missing token       | 'addr' = '{{pulsar_auth_addr}}'                                                                                   | the PULSAR broker refused the connection with AuthenticationError                 |
      | 3            | a missing token       | 'addr' = '{{pulsar_auth_addr}}'                                                                                   | the PULSAR broker refused the connection with AuthenticationError                 |
      | 1            | a forged token        | 'addr' = '{{pulsar_auth_addr}}', 'auth_token' = '{{pulsar_auth_forged_token}}'                                    | the PULSAR broker refused the connection with AuthenticationError                 |
      | 3            | a forged token        | 'addr' = '{{pulsar_auth_addr}}', 'auth_token' = '{{pulsar_auth_forged_token}}'                                    | the PULSAR broker refused the connection with AuthenticationError                 |
      | 1            | a missing certificate | 'addr' = '{{pulsar_auth_tls_addr}}', 'tls_ca_file' = '{{dev_tls}}/ca.pem', 'auth_token' = '{{pulsar_auth_token}}' | no connection to the PULSAR broker opened: Connection error: received fatal alert |
      | 3            | a missing certificate | 'addr' = '{{pulsar_auth_tls_addr}}', 'tls_ca_file' = '{{dev_tls}}/ca.pem', 'auth_token' = '{{pulsar_auth_token}}' | no connection to the PULSAR broker opened: Connection error: received fatal alert |

  Scenario Outline: A Pulsar client refuses a broker certificate <reason>
    Given Pulsar is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has dev TLS resource directory "dev_tls"
    When these Plexus commands are executed
      """
      CREATE RESOURCE dev_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE dev_tls VERSION "{{dev_tls}}";
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE PULSAR CLIENT checked
        MOUNT dev_tls VERSION 1
        CONFIG { <client_config> };
      CREATE PULSAR TOPIC EMITTER checked_out FROM notifications
        TO checked TOPIC 'checked_{{test_id}}'
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 250ms MAX 5s
          ENCODE USING notification_codec
        INHERIT ALL
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then within "60s" DESCRIBE EMITTER "checked_out" on the leader node contains
      """
      transient error: no connection to the PULSAR broker opened: Connection error: invalid peer certificate: <reported>
      """

    Examples:
      | cluster_size | reason                              | client_config                                                                | reported                                   |
      | 1            | that names another address          | 'addr' = '{{pulsar_tls_unnamed_addr}}', 'tls_ca_file' = '{{dev_tls}}/ca.pem' | certificate not valid for name "127.0.0.2" |
      | 3            | that names another address          | 'addr' = '{{pulsar_tls_unnamed_addr}}', 'tls_ca_file' = '{{dev_tls}}/ca.pem' | certificate not valid for name "127.0.0.2" |
      | 1            | from an authority it does not trust | 'addr' = '{{pulsar_tls_addr}}'                                               | UnknownIssuer                              |
      | 3            | from an authority it does not trust | 'addr' = '{{pulsar_tls_addr}}'                                               | UnknownIssuer                              |

  Scenario Outline: A Pulsar client accepts a broker certificate its keys tell it not to confirm
    Given Pulsar is running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has dev TLS resource directory "dev_tls"
    When these Plexus commands are executed
      """
      CREATE RESOURCE dev_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE dev_tls VERSION "{{dev_tls}}";
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE PULSAR CLIENT relaxed
        MOUNT dev_tls VERSION 1
        CONFIG { <client_config> };
      CREATE PULSAR TOPIC INGESTOR relaxed_in
        FROM relaxed TOPIC 'relaxed_{{test_id}}'
          SUBSCRIPTION 'relaxed_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And Pulsar TLS message is published to topic "relaxed_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "60s" the relay subscription receives a payload
      """
      "user_id":42
      """

    Examples:
      | cluster_size | client_config                                                                                                               |
      | 1            | 'addr' = '{{pulsar_tls_unnamed_addr}}', 'tls_ca_file' = '{{dev_tls}}/ca.pem', 'tls_hostname_verification_enabled' = 'false' |
      | 3            | 'addr' = '{{pulsar_tls_unnamed_addr}}', 'tls_ca_file' = '{{dev_tls}}/ca.pem', 'tls_hostname_verification_enabled' = 'false' |
      | 1            | 'addr' = '{{pulsar_tls_addr}}', 'tls_allow_insecure_connection' = 'true'                                                    |
      | 3            | 'addr' = '{{pulsar_tls_addr}}', 'tls_allow_insecure_connection' = 'true'                                                    |

  # The ingestor reads through a scenario-owned stallable endpoint. Resetting its connections while
  # it is paused leaves every new connection unanswered, so each attempt to reconnect ends at the
  # driver's 10-second connection timeout. A driver that retried by itself would keep reconnecting
  # for minutes while the host waits and its backoff stays at its first step; with the host owning
  # every retry, each failed attempt reaches the host and its backoff grows on the declared policy.
  @pulsar_ingestor_reconnect @deloxide_stress
  Scenario Outline: A Pulsar ingestor leaves reconnecting to its declared RETRY POLICY
    Given Pulsar is running
    And a stallable Pulsar endpoint is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE PULSAR CLIENT pulsar_ingress CONFIG { 'addr' = '{{pulsar_stallable_addr}}' };
      CREATE PULSAR TOPIC INGESTOR reconnecting
        FROM pulsar_ingress TOPIC 'reconnecting_{{test_id}}'
          SUBSCRIPTION 'reconnecting_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION notifications_subscription TO notifications;
      START;
      """
    And Pulsar message is published to topic "reconnecting_{{test_id}}"
      """
      {"user_id":42}
      """
    Then within "60s" the relay subscription receives a payload
      """
      "user_id":42
      """
    When the stallable endpoint "pulsar" is paused
    And the stallable endpoint "pulsar" resets its connections
    Then within "90s" DESCRIBE INGESTOR "reconnecting" on the leader node reports a reconnect backoff of at least "1s"
    When the stallable endpoint "pulsar" is resumed
    And Pulsar message is published to topic "reconnecting_{{test_id}}"
      """
      {"user_id":43}
      """
    Then within "120s" the relay subscription receives a payload
      """
      "user_id":43
      """
    And within "60s" DESCRIBE INGESTOR "reconnecting" on the leader node contains
      """
      transient error: -
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # Nothing answers the connection while the endpoint is paused, so the domain's start fails on the
  # node that runs the ingestor. Which node that is decides what START itself reports, so the
  # scenario accepts its outcome and observes the subscription instead: what brings the ingestor up
  # is the node's own retry of the start, without a further statement.
  Scenario Outline: A Pulsar ingestor whose broker is away when its domain starts joins once the broker returns
    Given Pulsar is running
    And a stallable Pulsar endpoint is configured
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification ( user_id I64 );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT ( user_id integer );
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE PULSAR CLIENT pulsar_ingress CONFIG { 'addr' = '{{pulsar_stallable_addr}}' };
      CREATE PULSAR TOPIC INGESTOR returning
        FROM pulsar_ingress TOPIC 'returning_{{test_id}}'
          SUBSCRIPTION 'returning_{{test_id}}' OWNED BY INGESTOR START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 2s
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      """
    And the stallable endpoint "pulsar" is paused
    And these Plexus commands are attempted on node "node-1"
      """
      START;
      """
    And the stallable endpoint "pulsar" is resumed
    Then within "120s" Pulsar subscription "returning_{{test_id}}" of topic "returning_{{test_id}}" has 1 consumer

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
