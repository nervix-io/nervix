@outward_tls_policy
Feature: Committed outward TLS algorithm policy

  Scenario Outline: Outward TLS policy resolves aliases and applies on every node without restart on <cluster_size> nodes
    Given client grpc transport is configured with mode "https"
    And the web console listener is configured for HTTPS
    And a <cluster_size> node nervix cluster is started
    Then every native and console listener refuses the ChaCha20 and X25519 peer
    When these Plexus statements are executed on the leader node
      """
      ALTER CLUSTER SET TLS_CIPHER_SUITES = ('fips', 'TLS13_CHACHA20_POLY1305_SHA256');
      """
    Then every native and console listener refuses the ChaCha20 and X25519 peer
    When these Plexus statements are executed on the leader node
      """
      ALTER CLUSTER SET TLS_CIPHER_SUITES = ('fips', 'TLS13_CHACHA20_POLY1305_SHA256'), TLS_KEY_EXCHANGE_GROUPS = ('fips', 'X25519');
      """
    Then node "node-1" eventually reports status containing "tls-cipher-suites committed="
    And node "node-<cluster_size>" eventually reports status containing "node=node-<cluster_size> tls-key-exchange-groups running="
    And every native and console listener accepts and retains the ChaCha20 and X25519 peer
    When these Plexus statements are executed on the leader node
      """
      ALTER CLUSTER SET TLS_CIPHER_SUITES = ('fips'), TLS_KEY_EXCHANGE_GROUPS = ('fips');
      """
    Then every native and console listener refuses the ChaCha20 and X25519 peer
    And the retained restricted TLS connections still carry application traffic

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A retained shared-provider HTTP connector adopts committed algorithms on <cluster_size> nodes
    Given HTTPS receiver "policy_peer" offers only ChaCha20 and X25519
    And HTTP receiver "policy_peer" answers unscripted requests with "respond 204"
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has the TLS files of HTTP receiver "policy_peer" in resource directory "policy_tls"
    When these Plexus commands are executed
      """
      CREATE RESOURCE policy_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE policy_tls VERSION "{{policy_tls}}";
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA policy_event (event_id STRING);
      CREATE JAQ JSON CODEC policy_codec TO SCHEMA policy_event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA policy_event UNBRANCHED;
      CREATE VHOST edge tls-policy-{{test_id}}.example.com;
      CREATE HTTP ENDPOINT policy_ingress ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR policy_ingestor
        FROM policy_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING policy_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT policy_client MOUNT policy_tls VERSION 1 CONFIG {
        'endpoint' = '{{http_receiver.policy_peer}}', 'timeout_ms' = 5000,
        'tls_ca_file' = '{{policy_tls}}/ca.pem'
      };
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        TO policy_client METHOD 'POST' PATH '/events'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 1s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Given constructions of emitter "published" are observed
    When these Plexus commands are executed
      """
      START;
      """
    And http payload is posted to host "tls-policy-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"policy-change"}]
      """
    Then HTTPS receiver "policy_peer" records a TLS algorithm refusal
    And HTTP receiver "policy_peer" has captured exactly 0 requests
    When these Plexus statements are executed on the leader node
      """
      ALTER CLUSTER SET TLS_CIPHER_SUITES = ('fips', 'TLS13_CHACHA20_POLY1305_SHA256'), TLS_KEY_EXCHANGE_GROUPS = ('fips', 'X25519');
      """
    Then HTTP receiver "policy_peer" eventually receives at least 1 request
    And HTTP receiver "policy_peer" has captured exactly 1 request
    And emitter "published" has been constructed exactly 1 time

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: HTTPS dynamic certificate publication keeps the live algorithm policy on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has TLS resource directory "tls_first" for hosts "policy-{{test_id}}.example.com"
    And node "node-1" has TLS resource directory "tls_next" for hosts "policy-{{test_id}}.example.com"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE RESOURCE policy_certificates;
      UPLOAD RESOURCE policy_certificates VERSION '{{tls_first}}';
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE VHOST edge policy-{{test_id}}.example.com WITH TLS policy_certificates VERSION 1;
      START;
      """
    Then every HTTPS listener for host "policy-{{test_id}}.example.com" refuses the restricted peer trusting resource directory "tls_first"
    When these Plexus statements are executed on the leader node
      """
      ALTER CLUSTER SET TLS_CIPHER_SUITES = ('fips', 'TLS13_CHACHA20_POLY1305_SHA256'), TLS_KEY_EXCHANGE_GROUPS = ('fips', 'X25519');
      """
    Then every HTTPS listener for host "policy-{{test_id}}.example.com" accepts the restricted peer trusting resource directory "tls_first"
    When these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE policy_certificates VERSION '{{tls_next}}';
      """
    And these Plexus commands are executed on the leader node
      """
      REBIND RESOURCE policy_certificates TO VERSION LATEST;
      """
    Then the last command output contains
      """
      quiesce level: DYNAMIC
      """
    And every HTTPS listener for host "policy-{{test_id}}.example.com" accepts the restricted peer trusting resource directory "tls_next"
    When these Plexus statements are executed on the leader node
      """
      ALTER CLUSTER SET TLS_CIPHER_SUITES = ('fips'), TLS_KEY_EXCHANGE_GROUPS = ('fips');
      """
    Then every HTTPS listener for host "policy-{{test_id}}.example.com" refuses the restricted peer trusting resource directory "tls_next"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A cluster forms with an outward-only ChaCha20 policy on <cluster_size> nodes
    Given node-1 starts with tls-cipher-suites = TLS13_CHACHA20_POLY1305_SHA256
    And node-1 starts with tls-key-exchange-groups = X25519
    And a <cluster_size> node nervix cluster is started
    Then node "node-1" eventually reports status containing "tls-cipher-suites committed=('TLS13_CHACHA20_POLY1305_SHA256')"
    And node "node-<cluster_size>" eventually reports status containing "node=node-<cluster_size> tls-key-exchange-groups running=('X25519')"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
