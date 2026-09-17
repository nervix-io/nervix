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
  Scenario Outline: Uploads are catalog-only and VHOST bindings keep their pinned certificate
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has resource directory "invalid_tls_bundle" containing
      """
      {
        "ca.crt": "not a certificate",
        "tls.crt": "not a certificate",
        "tls.key": "not a private key"
      }
      """
    And node "node-1" has TLS resource directory "tls_v1" for hosts "pinned-{{test_id}}.example.com"
    And node "node-1" has TLS resource directory "tls_v2" for hosts "pinned-{{test_id}}.example.com"
    When these NSPL commands are executed
      """
      CREATE RESOURCE invalid_tls;
      UPLOAD RESOURCE invalid_tls VERSION "{{invalid_tls_bundle}}";
      DESCRIBE RESOURCE invalid_tls;
      """
    Then the last command output contains
      """
      resource: invalid_tls
      latest: 1
      versions: 1
      """
    When these NSPL commands fail with "invalid TLS resource for VHOST 'edge': no certificates found"
      """
      CREATE VHOST edge pinned-{{test_id}}.example.com WITH TLS invalid_tls VERSION 1;
      """
    When these NSPL commands are executed
      """
      CREATE RESOURCE tls_bundle;
      UPLOAD RESOURCE tls_bundle VERSION "{{tls_v1}}";
      CREATE VHOST edge pinned-{{test_id}}.example.com WITH TLS tls_bundle VERSION 1;
      UPLOAD RESOURCE tls_bundle VERSION "{{tls_v2}}";
      DESCRIBE RESOURCE tls_bundle;
      """
    Then the last command output contains
      """
      resource: tls_bundle
      latest: 2
      versions: 1,2
      """
    When these NSPL commands are executed
      """
      SHOW CREATE VHOST edge;
      """
    Then the last command output contains
      """
      CREATE VHOST edge pinned-{{test_id}}.example.com WITH TLS tls_bundle VERSION 1;
      """
    And the leader HTTPS listener for host "pinned-{{test_id}}.example.com" presents the certificate from resource directory "tls_v1"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

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
