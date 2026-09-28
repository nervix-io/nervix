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

  Scenario Outline: CLI connects by hostname over <mode> through the configured DNS fixture
    Given client grpc transport is configured with mode "<mode>"
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI executes "LIST DOMAINS;" on node "{{leader}}" through fixture DNS
    Then the CLI output contains "{{domain}}"
    And the DNS fixture eventually receives a question for "native-session.nervix.test"

    Examples:
      | cluster_size | mode  |
      | 1            | http  |
      | 3            | http  |
      | 1            | https |
      | 3            | https |

  Scenario: CLI rejects a TLS certificate for a different DNS hostname
    Given client grpc transport is configured with mode "https"
    And cluster peers are addressed by "DNS names"
    And a 1 node nervix cluster is started
    When the CLI executes "LIST DOMAINS;" on node "node-1" through fixture DNS name "unlisted-session.test"
    Then the CLI fails with "certificate"
    And the DNS fixture eventually receives a question for "unlisted-session.test"

  Scenario: CLI connects by hostname and follows the leader from a follower session
    Given cluster peers are addressed by "DNS names"
    And a 3 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "follower"
    When the CLI executes "LIST DOMAINS;" on node "{{follower}}" through fixture DNS
    Then the CLI output contains "{{domain}}"
    And the DNS fixture eventually receives a question for "native-session.nervix.test"

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

  Scenario Outline: CLI attaches its session to the active domain's clock
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 1s SKEW 100ms;
      START AT '2030-01-01T00:00:00Z' TIME RATE 2.0;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI executes "ATTACH DOMAIN CLOCK;" on node "{{leader}}"
    Then the CLI output contains "attached to the clock of domain '{{domain}}': generation 1, paced"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: The CLI follows a domain clock through its generations
    Given a <cluster_size> node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 10ms;
      START AT '2030-01-01T00:00:00Z' TIME RATE 2.0;
      """
    When the CLI follows the clock of domain "{{domain}}" on node "node-1"
    Then within "10s" the CLI clock output contains "attached to the clock of domain '{{domain}}': generation 1, paced: period 100ms, skew 10ms, logical origin 2030-01-01T00:00:00Z, UTC anchor"
    And within "10s" the CLI clock output contains "time rate 2"
    And within "10s" the CLI clock output has 3 increasing ticks for generation 1 of domain "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      STOP;
      """
    Then within "10s" the CLI clock output contains "[events] domain clock [{{domain}}]: generation 1, stopped"
    When the domain clock is started at now with time rate "1.0" on the leader node
    Then within "10s" the CLI clock output contains "[events] domain clock [{{domain}}]: generation 2, paced: period 100ms, skew 10ms"
    And within "10s" the CLI clock output has a tick for generation 2 after its state of domain "{{domain}}"
    When the CLI clock process receives Ctrl-C
    Then the CLI clock process exits successfully

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: The CLI refuses to follow the clock of a missing domain
    Given a 1 node nervix cluster is started
    When the CLI attempts to follow the clock of missing domain "absent-clock" on node "node-1"
    Then the CLI fails with "domain 'absent-clock' does not exist"

  Scenario: The CLI restores a domain clock after transport loss
    Given a 1 node nervix cluster is started
    When these NSPL commands are executed on the leader node
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 100ms SKEW 10ms;
      START AT '2030-01-01T00:00:00Z' TIME RATE 2.0;
      """
    Given the CLI clock connection to node "node-1" is forwarded
    When the CLI follows the clock of domain "{{domain}}" through its TCP forwarder
    Then within "10s" the CLI clock output contains "attached to the clock of domain '{{domain}}': generation 1, paced"
    When the TCP forwarder at "127.0.0.1" stops
    Then within "20s" the CLI clock output contains "[events] domain clock [{{domain}}] notice: the session was interrupted"
    When the TCP forwarder at "127.0.0.1" restarts
    Then within "20s" the CLI clock output has a fresh state for generation 1 after interruption of domain "{{domain}}"
    And within "20s" the CLI clock output has a tick for generation 1 after its state of domain "{{domain}}"
    When the CLI clock process receives Ctrl-C
    Then the CLI clock process exits successfully
