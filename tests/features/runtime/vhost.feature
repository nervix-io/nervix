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
    When these NSPL commands fail with "invalid TLS resource for VHOST 'edge' in domain '{{domain}}'"
      """
      CREATE VHOST edge pinned-{{test_id}}.example.com WITH TLS invalid_tls VERSION 1;
      """
    Then the last command error contains
      """
      from 'invalid_tls@1'
      """
    And the last command error contains
      """
      no certificates found in TLS CA certificate
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

  @command_completion
  Scenario Outline: Rebinding a VHOST to another version of its TLS bundle refreshes every HTTPS listener without pausing ingestion
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has TLS resource directory "tls_v1" for hosts "rotate-{{test_id}}.example.com"
    And node "node-1" has TLS resource directory "tls_v2" for hosts "rotate-{{test_id}}.example.com"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE tls_bundle;
      UPLOAD RESOURCE tls_bundle VERSION '{{tls_v1}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA event ( id I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( id integer );
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge rotate-{{test_id}}.example.com WITH TLS tls_bundle VERSION 1;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE INGESTOR event_source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
        TO events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      START;
      """
    Then the HTTPS listener of every node for host "rotate-{{test_id}}.example.com" presents the certificate from resource directory "tls_v1"
    When these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE tls_bundle VERSION '{{tls_v2}}';
      """
    Then the HTTPS listener of every node for host "rotate-{{test_id}}.example.com" presents the certificate from resource directory "tls_v1"
    When https payloads begin posting in the background to every node with host "rotate-{{test_id}}.example.com" path "/events" trusting resource directories "tls_v1" and "tls_v2"
      """
      {"id":1}
      """
    And these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE tls_bundle TO VERSION LATEST;
      """
    Then the last command output contains
      """
      rebound 1 of 1 usage(s) of resource 'tls_bundle' to version 2 (latest)
      quiesce level: DYNAMIC
      """
    And the last command output contains
      """
      - kind=vhost name=edge from=1 to=2
      """
    And the HTTPS listener of every node for host "rotate-{{test_id}}.example.com" presents the certificate from resource directory "tls_v2"
    And the background https publishing accepted every payload
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE VHOST edge;
      """
    Then the last command output contains
      """
      CREATE VHOST edge rotate-{{test_id}}.example.com WITH TLS tls_bundle VERSION 2;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @command_completion
  Scenario Outline: Rebinding a stopped domain's VHOST TLS refreshes every HTTPS listener
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has TLS resource directory "tls_v1" for hosts "stopped-{{test_id}}.example.com"
    And node "node-1" has TLS resource directory "tls_v2" for hosts "stopped-{{test_id}}.example.com"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE tls_bundle;
      UPLOAD RESOURCE tls_bundle VERSION '{{tls_v1}}';
      UPLOAD RESOURCE tls_bundle VERSION '{{tls_v2}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE VHOST edge stopped-{{test_id}}.example.com WITH TLS tls_bundle VERSION 1;
      """
    Then the HTTPS listener of every node for host "stopped-{{test_id}}.example.com" presents the certificate from resource directory "tls_v1"
    When these NSPL commands are executed on the leader node
      """
      REBIND RESOURCE tls_bundle TO VERSION 2 FOR VHOST edge;
      """
    Then the last command output contains
      """
      rebound 1 of 1 usage(s) of resource 'tls_bundle' to version 2
      quiesce level: DYNAMIC
      """
    And the HTTPS listener of every node for host "stopped-{{test_id}}.example.com" presents the certificate from resource directory "tls_v2"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @command_completion
  Scenario Outline: Rebinding a VHOST to an unusable TLS bundle is rejected and keeps its certificate
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has TLS resource directory "tls_v1" for hosts "unusable-{{test_id}}.example.com"
    And node "node-1" has resource directory "invalid_tls_bundle" containing
      """
      {
        "ca.crt": "not a certificate",
        "tls.crt": "not a certificate",
        "tls.key": "not a private key"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE tls_bundle;
      UPLOAD RESOURCE tls_bundle VERSION '{{tls_v1}}';
      UPLOAD RESOURCE tls_bundle VERSION '{{invalid_tls_bundle}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE VHOST edge unusable-{{test_id}}.example.com WITH TLS tls_bundle VERSION 1;
      START;
      """
    When these NSPL commands fail with "invalid TLS resource for VHOST 'edge' in domain '{{domain}}'"
      """
      REBIND RESOURCE tls_bundle TO VERSION 2;
      """
    Then the last command error contains
      """
      from 'tls_bundle@2'
      """
    And the last command error contains
      """
      no certificates found in TLS CA certificate
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE VHOST edge;
      """
    Then the last command output contains
      """
      CREATE VHOST edge unusable-{{test_id}}.example.com WITH TLS tls_bundle VERSION 1;
      """
    And the HTTPS listener of every node for host "unusable-{{test_id}}.example.com" presents the certificate from resource directory "tls_v1"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @command_completion
  Scenario Outline: An HTTPS listener installation failure on any node rolls the TLS rebinding back
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has TLS resource directory "tls_v1" for hosts "rollback-{{test_id}}.example.com"
    And node "node-1" has TLS resource directory "tls_v2" for hosts "rollback-{{test_id}}.example.com"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE tls_bundle;
      UPLOAD RESOURCE tls_bundle VERSION '{{tls_v1}}';
      UPLOAD RESOURCE tls_bundle VERSION '{{tls_v2}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE VHOST edge rollback-{{test_id}}.example.com WITH TLS tls_bundle VERSION 1;
      START;
      """
    Given the next HTTPS listener installation on node "<failing_node>" fails
    When these NSPL commands fail with "failed to install the HTTPS listener TLS configuration on node '<failing_node>'"
      """
      REBIND RESOURCE tls_bundle TO VERSION 2;
      """
    Then the last command error contains
      """
      the model batch was rolled back
      """
    When these NSPL commands are executed on the leader node
      """
      SHOW CREATE VHOST edge;
      """
    Then the last command output contains
      """
      CREATE VHOST edge rollback-{{test_id}}.example.com WITH TLS tls_bundle VERSION 1;
      """
    And the HTTPS listener of every node for host "rollback-{{test_id}}.example.com" presents the certificate from resource directory "tls_v1"

    Examples:
      | cluster_size | failing_node |
      | 1            | node-1       |
      | 3            | node-3       |

  Scenario Outline: Changing a TLS VHOST's hostnames still pauses the domain and refreshes every HTTPS listener
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has TLS resource directory "tls_bundle" for hosts "before-{{test_id}}.example.com, after-{{test_id}}.example.com"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE RESOURCE tls_bundle;
      UPLOAD RESOURCE tls_bundle VERSION '{{tls_bundle}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE VHOST edge before-{{test_id}}.example.com WITH TLS tls_bundle VERSION 1;
      START;
      """
    Given client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      DROP VHOST edge;
      CREATE VHOST edge after-{{test_id}}.example.com WITH TLS tls_bundle VERSION 1;
      """
    Then the last command output contains
      """
      quiesce level: DOMAIN_PAUSE
      """
    When client "owner" executes these NSPL commands
      """
      COMMIT;
      """
    Then the last command output contains
      """
      quiesce level: DOMAIN_PAUSE
      """
    And the HTTPS listener of every node for host "after-{{test_id}}.example.com" presents the certificate from resource directory "tls_bundle"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
