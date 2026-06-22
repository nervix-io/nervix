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
