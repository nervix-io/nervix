Feature: NATS client authentication
  Nervix uses each declared authentication method for both subscriptions and publications.
  The brokers enforce token, user nkey and account-signed credentials independently.

  @nats_authentication @deloxide_stress
  Scenario Outline: NATS ingestors and emitters authenticate with <method>
    Given NATS authentication servers are running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has NATS credentials in resource directory "nats_identity_path"
    When these Plexus commands are executed
      """
      CREATE RESOURCE nats_identity;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE nats_identity VERSION "{{nats_identity_path}}";
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE RELAY delivered SCHEMA notification UNBRANCHED;
      CREATE NATS CLIENT authenticated MOUNT nats_identity VERSION 1 CONFIG {
        'addr' = '{{nats_<method>_addr}}', '<key>' = '<value>'
      };
      CREATE NATS SUBJECT INGESTOR intake
        FROM authenticated SUBJECT 'auth_in_{{test_id}}' QUEUE GROUP 'input' INSTANCES 1
        MODE NO ACK ON QUIESCE DROP DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE NATS SUBJECT EMITTER forward FROM notifications
        TO authenticated SUBJECT 'auth_out_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING notification_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE NATS SUBJECT INGESTOR returning
        FROM authenticated SUBJECT 'auth_out_{{test_id}}' QUEUE GROUP 'output' INSTANCES 1
        MODE NO ACK ON QUIESCE DROP DECODE USING notification_codec
        TO delivered INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION received TO delivered;
      START;
      """
    Then within "30s" repeatedly publishing an authenticated NATS <method> message to subject "auth_in_{{test_id}}" yields a relay subscription payload
      """
      {"user_id":42}
      """
    And the last relay subscription payload contains '{"user_id":42}'
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE CLIENT authenticated;
      """
    Then the last command output contains
      """
      '<key>' = '********'
      """

    Examples:
      | cluster_size | method      | key              | value                        |
      | 1            | token       | token            | {{nats_token}}               |
      | 3            | token       | token            | {{nats_token}}               |
      | 1            | nkey        | nkey_seed        | {{nats_seed}}                |
      | 3            | nkey        | nkey_seed        | {{nats_seed}}                |
      | 1            | credentials | credentials_file | {{nats_identity}}/user.creds |
      | 3            | credentials | credentials_file | {{nats_identity}}/user.creds |

  @nats_authentication_refusal @deloxide_stress
  Scenario Outline: A NATS broker refuses a client without its required <method> identity
    Given NATS authentication servers are running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE NATS CLIENT unauthenticated CONFIG { 'addr' = '{{nats_<method>_addr}}' };
      CREATE NATS SUBJECT INGESTOR refused
        FROM unauthenticated SUBJECT 'auth_{{test_id}}' QUEUE GROUP 'refused' INSTANCES 1
        MODE NO ACK ON QUIESCE DROP DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE NATS SUBJECT EMITTER refused_out FROM notifications
        TO unauthenticated SUBJECT 'auth_out_{{test_id}}' MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s
        ENCODE USING notification_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands are executed
      """
      START;
      """
    Then within "30s" DESCRIBE INGESTOR "refused" on the leader node contains
      """
      authorization violation
      """
    And within "30s" DESCRIBE EMITTER "refused_out" on the leader node contains
      """
      authorization violation
      """

    Examples:
      | cluster_size | method      |
      | 1            | token       |
      | 3            | token       |
      | 1            | nkey        |
      | 3            | nkey        |
      | 1            | credentials |
      | 3            | credentials |

  Scenario Outline: NATS clients refuse simultaneous authentication methods at CREATE
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail
      """
      CREATE NATS CLIENT ambiguous CONFIG {
        'addr' = 'nats://127.0.0.1:1', '<first>' = 'first', '<second>' = 'second'
      };
      """
    Then the last command error contains
      """
      mutually exclusive
      """

    Examples:
      | cluster_size | first            | second    |
      | 1            | credentials_file | token     |
      | 3            | credentials_file | token     |
      | 1            | credentials_file | nkey_seed |
      | 3            | credentials_file | nkey_seed |
      | 1            | nkey_seed        | token     |
      | 3            | nkey_seed        | token     |
