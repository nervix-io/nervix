Feature: Vhost persistence
  Scenario Outline: Vhost definitions are persisted and rendered through SHOW CREATE
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed
      """
      CREATE VHOST edge api.example.com, ws.example.com;
      SHOW CREATE VHOST edge;
      """
    Then the last command output contains
      """
      CREATE VHOST edge api.example.com, ws.example.com;
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  @command_completion
  Scenario: A required TLS binding refresh failure fails its resource upload
    Given a 1 node nervix cluster is started
    And node "node-1" has TLS resource directory "valid_tls_bundle" for hosts "api.example.com"
    And node "node-1" has resource directory "invalid_tls_bundle" containing
      """
      {
        "tls.crt": "not a certificate",
        "tls.key": "not a private key"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE tls_bundle;
      UPLOAD RESOURCE tls_bundle VERSION '{{valid_tls_bundle}}';
      CREATE VHOST edge api.example.com WITH TLS tls_bundle;
      """
    When these NSPL commands fail with "for version 2 failed: failed to refresh HTTP TLS config"
      """
      UPLOAD RESOURCE tls_bundle VERSION '{{invalid_tls_bundle}}';
      """

  Scenario Outline: TLS vhost definitions preserve explicit resource version through SHOW CREATE
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has TLS resource directory "tls_bundle" for hosts "api.example.com, ws.example.com"
    When these NSPL commands are executed
      """
      CREATE RESOURCE tls_bundle;
      UPLOAD RESOURCE tls_bundle VERSION "{{tls_bundle}}";
      CREATE VHOST edge api.example.com, ws.example.com WITH TLS tls_bundle VERSION 1;
      SHOW CREATE VHOST edge;
      """
    Then the last command output contains
      """
      CREATE VHOST edge api.example.com, ws.example.com WITH TLS tls_bundle VERSION 1;
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
