Feature: ClickHouse admission and case-preserving delivery
  CREATE and ALTER validate complete mapped rows before committing them, and valid clients write
  into the exact provisioned database and table over HTTP and trusted HTTPS.

  Scenario Outline: ClickHouse refuses invalid mapped rows at CREATE and ALTER on <cluster_size> nodes
    Given ClickHouse is running
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (id I64);
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE CLICKHOUSE CLIENT checked CONFIG {
        'addr' = '{{clickhouse_addr}}', 'user' = 'default', 'password' = 'nervix',
        'timeout_ms' = '5000'
      };
      CREATE CLICKHOUSE TABLE EMITTER accepted FROM events
        TO checked TABLE 'Accepted_{{test_id}}'
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        VALUES { 'clickhouse_user_id' = input.id }
        BATCH MAX MESSAGES 10 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    When these Plexus commands fail with "<diagnostic>"
      """
      <change> checked TABLE 'Refused_{{test_id}}'
        MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        VALUES { <values> } <finish>;
      """
    And these Plexus commands are executed
      """
      SHOW CREATE EMITTER accepted;
      """
    Then the last command output contains
      """
      TABLE 'Accepted_{{test_id}}'
      """
    And the last command output contains
      """
      'clickhouse_user_id' = input.id
      """

    Examples:
      | cluster_size | change                                                 | values                                                      | finish                                                                                        | diagnostic                                                      |
      | 1            | CREATE CLICKHOUSE TABLE EMITTER refused FROM events TO | 'id' = input.id, 'id' = input.id                            | BATCH MAX MESSAGES 10 MAX SIZE 1MiB FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG | duplicate VALUES key 'id'                                       |
      | 3            | CREATE CLICKHOUSE TABLE EMITTER refused FROM events TO | 'id' = input.id, 'id' = input.id                            | BATCH MAX MESSAGES 10 MAX SIZE 1MiB FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG | duplicate VALUES key 'id'                                       |
      | 1            | ALTER CLICKHOUSE TABLE EMITTER accepted SET TO         | 'id' = input.id, 'id' = input.id                            |                                                                                               | duplicate VALUES key 'id'                                       |
      | 3            | ALTER CLICKHOUSE TABLE EMITTER accepted SET TO         | 'id' = input.id, 'id' = input.id                            |                                                                                               | duplicate VALUES key 'id'                                       |
      | 1            | CREATE CLICKHOUSE TABLE EMITTER refused FROM events TO | 'clickhouse_user_id' = input.id, 'clickhouse_action' = NULL | BATCH MAX MESSAGES 10 MAX SIZE 1MiB FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG | VALUES key 'clickhouse_action' expression type inference failed |
      | 3            | CREATE CLICKHOUSE TABLE EMITTER refused FROM events TO | 'clickhouse_user_id' = input.id, 'clickhouse_action' = NULL | BATCH MAX MESSAGES 10 MAX SIZE 1MiB FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG | VALUES key 'clickhouse_action' expression type inference failed |
      | 1            | ALTER CLICKHOUSE TABLE EMITTER accepted SET TO         | 'clickhouse_user_id' = input.id, 'clickhouse_action' = NULL |                                                                                               | VALUES key 'clickhouse_action' expression type inference failed |
      | 3            | ALTER CLICKHOUSE TABLE EMITTER accepted SET TO         | 'clickhouse_user_id' = input.id, 'clickhouse_action' = NULL |                                                                                               | VALUES key 'clickhouse_action' expression type inference failed |

  Scenario Outline: ClickHouse clients refuse unknown keys and TLS identity on HTTP on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "<diagnostic>"
      """
      CREATE CLICKHOUSE CLIENT refused CONFIG {
        'addr' = 'http://127.0.0.1:8123', 'timeout_ms' = '5000', <entry>
      };
      """

    Examples:
      | cluster_size | entry                                                                                           | diagnostic                                                |
      | 1            | 'unknown_option' = 'value'                                                                      | unknown client config key 'unknown_option'                |
      | 3            | 'unknown_option' = 'value'                                                                      | unknown client config key 'unknown_option'                |
      | 1            | 'tls_cert_file' = '/tmp/client-certificate.pem'                                                 | client config key 'tls_cert_file' requires 'tls_key_file' |
      | 3            | 'tls_cert_file' = '/tmp/client-certificate.pem'                                                 | client config key 'tls_cert_file' requires 'tls_key_file' |
      | 1            | 'tls_key_file' = '/tmp/client-private-key.pem'                                                  | client config key 'tls_key_file' requires 'tls_cert_file' |
      | 3            | 'tls_key_file' = '/tmp/client-private-key.pem'                                                  | client config key 'tls_key_file' requires 'tls_cert_file' |
      | 1            | 'tls_cert_file' = '/tmp/client-certificate.pem', 'tls_key_file' = '/tmp/client-private-key.pem' | invalid client config key 'tls_cert_file'                 |
      | 3            | 'tls_cert_file' = '/tmp/client-certificate.pem', 'tls_key_file' = '/tmp/client-private-key.pem' | invalid client config key 'tls_cert_file'                 |

  Scenario Outline: ClickHouse writes to a mixed-case <locator> over <transport> on <cluster_size> nodes
    Given ClickHouse is running
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has dev TLS resource directory "dev_tls"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And ClickHouse database "<database>" exists
    And ClickHouse <table_kind> "<table>" exists
    When these Plexus commands are executed
      """
      CREATE RESOURCE dev_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE dev_tls VERSION "{{dev_tls}}";
      """
    And these Plexus commands are executed
      """
      CREATE SCHEMA event (user_id I64, action STRING);
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT (user_id integer, action string);
      CREATE CODEC event_codec FROM WIRE JSON SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge clickhouse-{{test_id}}.example.com;
      CREATE ENDPOINT intake ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR incoming
        FROM intake MODE NO ACK ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE CLICKHOUSE CLIENT checked MOUNT dev_tls VERSION 1 CONFIG {
        'addr' = '<address>', 'database' = 'default',
        'user' = 'default', 'password' = 'nervix', 'timeout_ms' = '5000' <trust>
      };
      CREATE CLICKHOUSE TABLE EMITTER outgoing FROM events
        TO checked TABLE '<table>' MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
        VALUES { 'clickhouse_user_id' = input.user_id,
          'clickhouse_action' = input.action, 'clickhouse_now' = 'qualified' }
        BATCH MAX MESSAGES 10 MAX SIZE 1MiB FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER outgoing;
      """
    Then the last command output contains
      """
      TABLE '<table>'
      """
    When these Plexus commands are executed
      """
      START;
      """
    And http payload is posted to node "node-1" with host "clickhouse-{{test_id}}.example.com" path "/events"
      """
      {"user_id":42,"action":"MixedCase"}
      """
    Then the ClickHouse table eventually contains a row
      """
      {"clickhouse_user_id":42,"clickhouse_action":"MixedCase"}
      """

    Examples:
      | cluster_size | locator        | transport | database              | table                                    | table_kind | address                 | trust                                  |
      | 1            | table          | HTTP      | default               | Events_{{test_id}}                       | table      | {{clickhouse_addr}}     |                                        |
      | 3            | table          | HTTP      | default               | Events_{{test_id}}                       | table      | {{clickhouse_addr}}     |                                        |
      | 1            | database.table | HTTP      | Analytics_{{test_id}} | Analytics_{{test_id}}.Events_{{test_id}} | table      | {{clickhouse_addr}}     |                                        |
      | 3            | database.table | HTTP      | Analytics_{{test_id}} | Analytics_{{test_id}}.Events_{{test_id}} | table      | {{clickhouse_addr}}     |                                        |
      | 1            | table          | HTTPS     | default               | Events_{{test_id}}                       | TLS table  | {{clickhouse_tls_addr}} | , 'tls_ca_file' = '{{dev_tls}}/ca.pem' |
      | 3            | table          | HTTPS     | default               | Events_{{test_id}}                       | TLS table  | {{clickhouse_tls_addr}} | , 'tls_ca_file' = '{{dev_tls}}/ca.pem' |
      | 1            | database.table | HTTPS     | Analytics_{{test_id}} | Analytics_{{test_id}}.Events_{{test_id}} | TLS table  | {{clickhouse_tls_addr}} | , 'tls_ca_file' = '{{dev_tls}}/ca.pem' |
      | 3            | database.table | HTTPS     | Analytics_{{test_id}} | Analytics_{{test_id}}.Events_{{test_id}} | TLS table  | {{clickhouse_tls_addr}} | , 'tls_ca_file' = '{{dev_tls}}/ca.pem' |
