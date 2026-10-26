Feature: ZeroMQ client declarations
  Nervix validates the client's socket settings before storing the configuration.

  Scenario Outline: ZeroMQ clients refuse invalid socket configuration at CREATE
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "invalid client config key '<key>'"
      """
      CREATE ZEROMQ CLIENT checked CONFIG {
        'addr' = '<address>',
        'bind' = '<bind>'
      };
      """

    Examples:
      | cluster_size | address               | bind  | key  |
      | 1            | tcp://127.0.0.1:5555  | yes   | bind |
      | 3            | tcp://127.0.0.1:5555  | yes   | bind |
      | 1            | 127.0.0.1:5555        | false | addr |
      | 3            | 127.0.0.1:5555        | false | addr |
      | 1            | http://127.0.0.1:5555 | false | addr |
      | 3            | http://127.0.0.1:5555 | false | addr |
      | 1            | tcp://127.0.0.1       | false | addr |
      | 3            | tcp://127.0.0.1       | false | addr |

  Scenario Outline: ZeroMQ clients preserve their socket settings through SHOW CREATE
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE ZEROMQ CLIENT checked CONFIG {
        'addr' = '<address>',
        'bind' = '<bind>'
      };
      SHOW CREATE CLIENT checked;
      """
    Then the last command output contains
      """
      'addr' = '<address>'
      """
    And the last command output contains
      """
      'bind' = '<bind>'
      """

    Examples:
      | cluster_size | address                 | bind  |
      | 1            | tcp://127.0.0.1:5555    | true  |
      | 3            | tcp://127.0.0.1:5555    | false |
      | 1            | ipc:///tmp/checked.sock | false |
      | 3            | ipc:///tmp/checked.sock | true  |

  Scenario Outline: Typed ZeroMQ bind and connect clients round-trip and deliver through PULL and PUSH
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ZeroMQ emission endpoint "{{zeromq_emit_addr}}" is observed
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE ZEROMQ CLIENT incoming CONFIG {
        'addr' = '{{zeromq_ingest_addr}}',
        'bind' = 'true'
      };
      CREATE ZEROMQ PULL INGESTOR receive FROM incoming MODE NO ACK
        ON QUIESCE SUSPEND DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE ZEROMQ CLIENT outgoing CONFIG {
        'addr' = '{{zeromq_emit_addr}}',
        'bind' = 'false'
      };
      CREATE ZEROMQ PUSH EMITTER send FROM notifications TO outgoing
        MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
        ENCODE USING notification_codec INHERIT ALL FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And ZeroMQ message is published
      """
      {"user_id":42}
      """
    Then the observed broker receives a payload
      """
      {"user_id":42}
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE CLIENT incoming;
      """
    Then the last command output contains
      """
      'bind' = 'true'
      """
    When these Plexus commands are executed on the leader node
      """
      SHOW CREATE CLIENT outgoing;
      """
    Then the last command output contains
      """
      'bind' = 'false'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
