Feature: CLI public session dispatch
  Scenario Outline: CLI applies semantic completion edits at the real cursor
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_event ( value I64, secret STRING );
      CREATE SCHEMA unrelated ( unrelated_field I64 );
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI suggests for "ALTER SCHEMA order_event DROP FIELD val|x" on node "{{leader}}"
    Then the CLI suggestion "value" applied to "ALTER SCHEMA order_event DROP FIELD val|x" yields "ALTER SCHEMA order_event DROP FIELD value"
    And the CLI output does not contain "unrelated_field"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: CLI completion uses UTF-8 byte edits after Unicode text
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_event ( value I64 );
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI suggests for "// 😊\nALTER SCHEMA order_event DROP FIELD val|x" on node "{{leader}}"
    Then the CLI suggestion "value" applied to "// 😊\nALTER SCHEMA order_event DROP FIELD val|x" yields "// 😊\nALTER SCHEMA order_event DROP FIELD value"

  Scenario: CLI completion searches local upload paths
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI suggests for "UPLOAD RESOURCE bundle VERSION 'Cargo.t|ml'" on node "{{leader}}"
    Then the CLI suggestion "Cargo.toml" applied to "UPLOAD RESOURCE bundle VERSION 'Cargo.t|ml'" yields "UPLOAD RESOURCE bundle VERSION 'Cargo.toml'"

  Scenario Outline: CLI dispatches a command through a public session
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI executes "LIST DOMAINS;" on node "{{leader}}"
    Then the CLI output contains "{{domain}}"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: CLI follows the leader from a follower session
    Given a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "follower"
    When the CLI executes "LIST DOMAINS;" on node "{{follower}}"
    Then the CLI output contains "{{domain}}"

  Scenario: CLI rejects incorrect credentials through a public session
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI executes "LIST DOMAINS;" on node "{{leader}}" with password "incorrect_password"
    Then the CLI fails with "Unauthenticated"

  Scenario Outline: CLI subscription keeps receiving after a high volume of rows
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA metric ( value I32 );
      CREATE WIRE JSON SCHEMA metric_wire MODE STRICT ( value integer );
      CREATE CODEC metric_codec FROM WIRE JSON SCHEMA metric_wire TO SCHEMA metric;
      CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}.example.com;
      CREATE ENDPOINT raw_metrics_endpoint ON edge PATH '/metrics' TYPE HTTP;
      CREATE INGESTOR raw_metrics_source FROM ENDPOINT raw_metrics_endpoint MODE NO_ACK SEQUENTIAL ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING metric_codec TO raw_metrics INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI subscribes to relay "raw_metrics" on node "{{leader}}"
    Then the CLI subscription output eventually contains "listening for events from relay 'raw_metrics'"
    When 270 sequential metric http payloads are posted to host "http-{{test_id}}.example.com" path "/metrics"
    Then the CLI subscription output eventually contains '"value":270'

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
