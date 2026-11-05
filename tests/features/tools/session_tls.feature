Feature: Operator-provisioned session TLS

  Scenario Outline: Operator certificates serve CLI and Rust clients on <cluster_size> nodes
    Given client grpc transport is configured with mode "https"
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then every native listener presents the operator certificate "node-2"
    And the current leader node is saved as placeholder "leader"
    And the Rust client lists domain "{{domain}}" through native listener "{{leader}}"
    When the CLI executes "LIST DOMAINS;" on node "{{leader}}" through fixture DNS
    Then the CLI output contains "{{domain}}"
    And the DNS fixture eventually receives a question for "native-session.nervix.test"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Native TLS replacement takes effect after restart on <cluster_size> nodes
    Given client grpc transport is configured with mode "https"
    And cluster peers are addressed by "DNS names"
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the native listener on node "node-1" presents the operator certificate "node-2"
    When the native TLS files on node "node-1" are replaced with certificate "node-3"
    Then the native listener on node "node-1" presents the operator certificate "node-2"
    When node "node-1" is stopped
    And node "node-1" is started
    Then the native listener on node "node-1" presents the operator certificate "node-3"
    And the Rust client lists domain "{{domain}}" through native listener "node-1"
    When the CLI executes "LIST DOMAINS;" on node "node-1" through fixture DNS
    Then the CLI output contains "{{domain}}"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A node refuses session TLS files without HTTPS mode
    When a nervix-server process is started with command-line option "--grpc-tls-cert" set to "/operator/session.pem"
    Then the server process exits with status 1
    And the server process log contains "gRPC TLS certificate/key requires https mode"

  Scenario: A follower console redirects to the leader's explicitly advertised HTTPS endpoint
    Given the web console listener is configured for HTTPS
    And a 3 node nervix cluster is started
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "follower"
    When the HTTPS web console is opened on node "{{follower}}"
    Then selector ".topbar-status .pill.ok" contains "CONNECTED"
    And selector ".terminal" contains "connected to leader '{{leader}}'"
    And the console session reaches node "{{leader}}" over WSS
