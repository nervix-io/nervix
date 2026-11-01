Feature: Iceberg declarations
  Scenario Outline: An Iceberg emitter refuses a LOCATION belonging to another storage connector
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (id I64);
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE S3 CLIENT storage CONFIG {
        'endpoint' = 'http://127.0.0.1:9000',
        'region' = 'us-east-1',
        'access_key_id' = 'fixture',
        'secret_access_key' = 'fixture'
      };
      CREATE ICEBERG_REST CLIENT catalog CONFIG {
        'uri' = 'http://127.0.0.1:8181',
        'warehouse' = 's3://warehouse'
      };
      """
    When these Plexus commands fail with "LOCATION"
      """
      CREATE ICEBERG TABLE EMITTER output FROM events
      TO storage TABLE 'Events' NAMESPACE '{{domain}}' LOCATION '<location>' CATALOG catalog
      COMMIT EACH 1s MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
      VALUES { 'id' = input.id } FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size | location                 |
      | 1            | gs://warehouse/events    |
      | 3            | gs://warehouse/events    |
      | 1            | abfss://warehouse/events |
      | 3            | abfss://warehouse/events |

  Scenario Outline: An Iceberg emitter refuses a catalog client that sets a file I/O property its storage client sets
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (id I64);
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE <connector> CLIENT storage CONFIG { <storage> };
      CREATE ICEBERG_REST CLIENT catalog CONFIG {
        'uri' = 'http://127.0.0.1:8181',
        <catalog>
      };
      """
    When these Plexus commands fail with "Iceberg property '<property>' is set by the storage client as '<storage_key>' and by the catalog client"
      """
      CREATE ICEBERG TABLE EMITTER output FROM events
      TO storage TABLE 'Events' NAMESPACE '{{domain}}' LOCATION '<location>' CATALOG catalog
      COMMIT EACH 1s MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
      VALUES { 'id' = input.id } FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """
    Then the last command error contains
      """
      emitter 'output' in domain '{{domain}}' cannot pair storage client 'storage' with catalog client 'catalog'
      """

    Examples:
      | cluster_size | connector  | storage                                                      | catalog                                 | property            | storage_key         | location                                              |
      | 1            | S3         | 'region' = 'us-east-1', 'endpoint' = 'http://127.0.0.1:9000' | 's3.endpoint' = 'http://127.0.0.1:9001' | s3.endpoint         | endpoint            | s3://warehouse/events                                 |
      | 3            | S3         | 'region' = 'us-east-1', 'endpoint' = 'http://127.0.0.1:9000' | 's3.endpoint' = 'http://127.0.0.1:9001' | s3.endpoint         | endpoint            | s3://warehouse/events                                 |
      | 1            | S3         | 'region' = 'us-east-1', 's3.driver_extension' = 'storage'    | 's3.driver_extension' = 'catalog'       | s3.driver_extension | s3.driver_extension | s3://warehouse/events                                 |
      | 3            | S3         | 'region' = 'us-east-1', 's3.driver_extension' = 'storage'    | 's3.driver_extension' = 'catalog'       | s3.driver_extension | s3.driver_extension | s3://warehouse/events                                 |
      | 1            | GCS        | 'no_auth' = true                                             | 'gcs.no-auth' = 'false'                 | gcs.no-auth         | no_auth             | gs://warehouse/events                                 |
      | 3            | GCS        | 'no_auth' = true                                             | 'gcs.no-auth' = 'false'                 | gcs.no-auth         | no_auth             | gs://warehouse/events                                 |
      | 1            | AZURE_BLOB | 'account_name' = 'fixture', 'account_key' = 'ZmFrZQ=='       | 'adls.account-name' = 'other'           | adls.account-name   | account_name        | abfss://container@fixture.dfs.core.windows.net/events |
      | 3            | AZURE_BLOB | 'account_name' = 'fixture', 'account_key' = 'ZmFrZQ=='       | 'adls.account-name' = 'other'           | adls.account-name   | account_name        | abfss://container@fixture.dfs.core.windows.net/events |

  Scenario Outline: An Iceberg emitter preserves its explicit catalog namespace independently of the Nervix domain
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (id I64);
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE S3 CLIENT storage CONFIG {
        'endpoint' = 'http://127.0.0.1:9000',
        'region' = 'us-east-1',
        'access_key_id' = 'fixture',
        'secret_access_key' = 'fixture'
      };
      CREATE ICEBERG_REST CLIENT catalog CONFIG {
        'uri' = 'http://127.0.0.1:8181',
        'warehouse' = 's3://warehouse'
      };
      """
    When these Plexus commands are executed
      """
      CREATE ICEBERG TABLE EMITTER output FROM events
      TO storage TABLE 'Events' NAMESPACE 'Analytics.Events'
      LOCATION 's3://warehouse/events' CATALOG catalog
      COMMIT EACH 1s MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
      VALUES { 'id' = input.id } FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      SHOW CREATE EMITTER output;
      """
    Then the last command output contains
      """
      TABLE 'Events' NAMESPACE 'Analytics.Events'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An Iceberg emitter uses the positive whole-byte size contract for COMMIT MAX SIZE
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (id I64);
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE S3 CLIENT storage CONFIG {
        'endpoint' = 'http://127.0.0.1:9000',
        'region' = 'us-east-1',
        'access_key_id' = 'fixture',
        'secret_access_key' = 'fixture'
      };
      CREATE ICEBERG_REST CLIENT catalog CONFIG {
        'uri' = 'http://127.0.0.1:8181',
        'warehouse' = 's3://warehouse'
      };
      """
    When these Plexus commands fail with "COMMIT MAX SIZE"
      """
      CREATE ICEBERG TABLE EMITTER output FROM events
      TO storage TABLE 'Events' NAMESPACE '{{domain}}' LOCATION 's3://warehouse/events' CATALOG catalog
      COMMIT EACH 1s MAX SIZE <limit> MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s
      VALUES { 'id' = input.id } FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size | limit  |
      | 1            | 0B     |
      | 3            | 0B     |
      | 1            | 0.5MiB |
      | 3            | 0.5MiB |

  Scenario Outline: An Iceberg object-store client refuses a nonboolean no_auth value
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "no_auth"
      """
      CREATE GCS CLIENT storage CONFIG {
        'service_path' = 'http://127.0.0.1:4443',
        'no_auth' = 'yes'
      };
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An Iceberg emitter commits to a case-preserving multilevel catalog namespace
    Given Iceberg dependencies are running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "namespace_{{test_id}}" in namespace "Analytics_{{test_id}}.Events" exists at "s3://nervix-iceberg/tables/namespace_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
      );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge iceberg-namespace-{{test_id}}.example.com;
      CREATE ENDPOINT notification_ingress
      ON edge
      PATH '/notifications'
      TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR notification_source
      FROM notification_ingress MODE NO ACK
      ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE S3 CLIENT s3_main
      CONFIG {
        <storage_endpoint>'region' = 'us-east-1',
        'access_key_id' = 'rustfsadmin',
        'secret_access_key' = 'rustfsadmin',
        'path_style_access' = true
      };
      CREATE ICEBERG_REST CLIENT iceberg_catalog
      CONFIG {
        'uri' = '{{iceberg_rest_addr}}',
        'warehouse' = 's3://nervix-iceberg/warehouse'<catalog_properties>
      };
      CREATE ICEBERG TABLE EMITTER iceberg_notifications FROM notifications TO s3_main TABLE 'namespace_{{test_id}}' NAMESPACE 'Analytics_{{test_id}}.Events' LOCATION 's3://nervix-iceberg/tables/namespace_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'user_id' = input.user_id, 'action' = input.action } BATCH MAX MESSAGES 2 MAX SIZE 1MiB
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    When http payload is posted to host "iceberg-namespace-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":41,"action":"OPEN"}
      """
    And http payload is posted to host "iceberg-namespace-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":42,"action":"UPDATE"}
      """
    And http payload is posted to host "iceberg-namespace-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":43,"action":"CLOSE"}
      """
    Then the Iceberg table "namespace_{{test_id}}" in namespace "Analytics_{{test_id}}.Events" eventually contains a row
      """
      {"user_id":41,"action":"OPEN"}
      """
    And the Iceberg table "namespace_{{test_id}}" in namespace "Analytics_{{test_id}}.Events" eventually contains a row
      """
      {"user_id":42,"action":"UPDATE"}
      """
    And the Iceberg table "namespace_{{test_id}}" in namespace "Analytics_{{test_id}}.Events" eventually contains a row
      """
      {"user_id":43,"action":"CLOSE"}
      """

    Examples:
      | cluster_size | storage_endpoint                | catalog_properties                  |
      | 1            | 'endpoint' = '{{rustfs_addr}}', |                                     |
      | 3            | 'endpoint' = '{{rustfs_addr}}', |                                     |
      | 1            |                                 | , 's3.endpoint' = '{{rustfs_addr}}' |
      | 3            |                                 | , 's3.endpoint' = '{{rustfs_addr}}' |


  @iceberg_namespace_alteration
  Scenario Outline: ALTER ICEBERG TABLE EMITTER SET TO moves an emitter to a table in another catalog namespace
    Given Iceberg dependencies are running
    And runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "moved_{{test_id}}" exists at "s3://nervix-iceberg/tables/moved_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And Iceberg table "moved_{{test_id}}" in namespace "Archive_{{test_id}}.Events" exists at "s3://nervix-iceberg/tables/archive_moved_{{test_id}}" with columns
      """
      user_id I64
      action STRING
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (
        user_id I64,
        action STRING
      );
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
        user_id integer,
        action string
      );
      CREATE CODEC notification_codec
      FROM WIRE JSON SCHEMA notification_wire
      TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge iceberg-alteration-{{test_id}}.example.com;
      CREATE ENDPOINT notification_ingress
      ON edge
      PATH '/notifications'
      TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR notification_source
      FROM notification_ingress MODE NO ACK
      ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
      TO notifications
      INHERIT ALL
      UNBRANCHED
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      CREATE S3 CLIENT s3_main
      CONFIG {
        'endpoint' = '{{rustfs_addr}}',
        'region' = 'us-east-1',
        'access_key_id' = 'rustfsadmin',
        'secret_access_key' = 'rustfsadmin',
        'path_style_access' = true
      };
      CREATE ICEBERG_REST CLIENT iceberg_catalog
      CONFIG {
        'uri' = '{{iceberg_rest_addr}}',
        'warehouse' = 's3://nervix-iceberg/warehouse'
      };
      CREATE ICEBERG TABLE EMITTER iceberg_notifications FROM notifications TO s3_main TABLE 'moved_{{test_id}}' NAMESPACE '{{domain}}' LOCATION 's3://nervix-iceberg/tables/moved_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s VALUES { 'user_id' = input.user_id, 'action' = input.action }
      FLUSH EACH 100ms MAX BATCH SIZE 1MiB
      ON MESSAGE ERROR LOG
      ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "iceberg-alteration-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":61,"action":"FIRST"}
      """
    Then the Iceberg table "moved_{{test_id}}" eventually contains a row
      """
      {"user_id":61,"action":"FIRST"}
      """
    When these Plexus commands are executed on the leader node
      """
      ALTER ICEBERG TABLE EMITTER iceberg_notifications SET TO s3_main TABLE 'moved_{{test_id}}' NAMESPACE 'Archive_{{test_id}}.Events' LOCATION 's3://nervix-iceberg/tables/archive_moved_{{test_id}}' CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s VALUES { 'user_id' = input.user_id, 'action' = input.action };
      SHOW CREATE EMITTER iceberg_notifications;
      """
    Then the last command output contains
      """
      TABLE 'moved_{{test_id}}' NAMESPACE 'Archive_{{test_id}}.Events'
      """
    And within "10s" DESCRIBE EMITTER "iceberg_notifications" on the leader node contains
      """
      NAMESPACE 'Archive_{{test_id}}.Events'
      """
    When http payload is posted to host "iceberg-alteration-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":62,"action":"SECOND"}
      """
    Then the Iceberg table "moved_{{test_id}}" in namespace "Archive_{{test_id}}.Events" eventually contains a row
      """
      {"user_id":62,"action":"SECOND"}
      """
    And within "30s" the Iceberg table "moved_{{test_id}}" holds exactly these rows
      """
      {"user_id":61,"action":"FIRST"}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An Iceberg emitter refuses unsupported publishing and construction clauses
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (id I64);
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE S3 CLIENT storage CONFIG {
        'endpoint' = 'http://127.0.0.1:9000',
        'region' = 'us-east-1',
        'access_key_id' = 'fixture',
        'secret_access_key' = 'fixture'
      };
      CREATE ICEBERG_REST CLIENT catalog CONFIG {
        'uri' = 'http://127.0.0.1:8181',
        'warehouse' = 's3://warehouse'
      };
      """
    When these Plexus commands fail
      """
      CREATE ICEBERG TABLE EMITTER output FROM events
      TO storage TABLE 'Events' NAMESPACE 'Analytics.Events'
      LOCATION 's3://warehouse/events' CATALOG catalog
      COMMIT EACH 1s MAX SIZE 1MiB MODE <mode> RETRY POLICY BACKOFF 100ms MAX 1s
      VALUES { 'id' = input.id } <clause> FLUSH IMMEDIATE
      ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      """

    Examples:
      | cluster_size | mode   | clause                                 |
      | 1            | NO ACK |                                        |
      | 3            | NO ACK |                                        |
      | 1            | ACK    | INVOKE write_header('X-Test', 'value') |
      | 3            | ACK    | INVOKE write_header('X-Test', 'value') |
      | 1            | ACK    | ON CONFLICT ('id') DO NOTHING          |
      | 3            | ACK    | ON CONFLICT ('id') DO NOTHING          |

  Scenario Outline: An Iceberg Azure data lake location reaches the configured REST catalog
    Given HTTP receiver "azure_catalog" is running
    And HTTP receiver "azure_catalog" answers unscripted requests with "respond 503"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE AZURE_BLOB CLIENT azure_main CONFIG {
        'account_name' = 'fixture',
        'account_key' = 'ZmFrZS1hY2NvdW50LWtleQ=='
      };
      CREATE ICEBERG_REST CLIENT iceberg_catalog CONFIG {
        'uri' = '{{http_receiver.azure_catalog}}'
      };
      CREATE ICEBERG TABLE EMITTER iceberg_notifications
        FROM notifications
        TO azure_main TABLE 'events' NAMESPACE 'Analytics.Events'
        LOCATION '<scheme>://container@fixture.dfs.core.windows.net/events'
        CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
        VALUES { 'user_id' = input.user_id }
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "5s" DESCRIBE EMITTER "iceberg_notifications" on the leader node contains
      """
      failed to load ICEBERG table 'events' in namespace 'Analytics.Events'
      """
    And the last command output contains
      """
      503 Service Unavailable
      """
    And HTTP receiver "azure_catalog" eventually receives at least 1 request

    Examples:
      | cluster_size | scheme |
      | 1            | abfs   |
      | 3            | abfs   |
      | 1            | abfss  |
      | 3            | abfss  |

  Scenario Outline: An Iceberg client refuses malformed declared options and offline driver properties
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands fail with "<key>"
      """
      CREATE <connector> CLIENT client_fixture CONFIG { <config> };
      """

    Examples:
      | cluster_size | connector    | key                    | config                                                                         |
      | 1            | S3           | path_style_access      | 'path_style_access' = 'yes'                                                    |
      | 3            | S3           | path_style_access      | 'path_style_access' = 'yes'                                                    |
      | 1            | AZURE_BLOB   | account_nmae           | 'account_name' = 'fixture', 'account_key' = 'fixture', 'account_nmae' = 'typo' |
      | 3            | AZURE_BLOB   | account_nmae           | 'account_name' = 'fixture', 'account_key' = 'fixture', 'account_nmae' = 'typo' |
      | 1            | ICEBERG_REST | uri                    | 'uri' = 'file:///catalog'                                                      |
      | 3            | ICEBERG_REST | uri                    | 'uri' = 'file:///catalog'                                                      |
      | 1            | ICEBERG_REST | tls_key_file           | 'uri' = 'http://127.0.0.1:8181', 'tls_cert_file' = 'certificate.pem'           |
      | 3            | ICEBERG_REST | tls_key_file           | 'uri' = 'http://127.0.0.1:8181', 'tls_cert_file' = 'certificate.pem'           |
      | 1            | S3           | endpoint_typo          | 'region' = 'us-east-1', 'endpoint_typo' = 'http://127.0.0.1:9000'              |
      | 3            | S3           | endpoint_typo          | 'region' = 'us-east-1', 'endpoint_typo' = 'http://127.0.0.1:9000'              |
      | 1            | GCS          | no_auth_typo           | 'no_auth' = true, 'no_auth_typo' = true                                        |
      | 3            | GCS          | no_auth_typo           | 'no_auth' = true, 'no_auth_typo' = true                                        |
      | 1            | ICEBERG_REST | warehouse_typo         | 'uri' = 'http://127.0.0.1:8181', 'warehouse_typo' = 'fixture'                  |
      | 3            | ICEBERG_REST | warehouse_typo         | 'uri' = 'http://127.0.0.1:8181', 'warehouse_typo' = 'fixture'                  |
      | 1            | S3           | s3.sse.type            | 's3.sse.type' = 'not-an-encryption-kind'                                       |
      | 3            | S3           | s3.sse.type            | 's3.sse.type' = 'not-an-encryption-kind'                                       |
      | 1            | AZURE_BLOB   | adls.connection-string | 'adls.connection-string' = 'opaque'                                            |
      | 3            | AZURE_BLOB   | adls.connection-string | 'adls.connection-string' = 'opaque'                                            |

  Scenario Outline: An Iceberg client accepts its declared driver property namespace
    Given a <cluster_size> node nervix cluster is started
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus commands are executed
      """
      CREATE <connector> CLIENT client_fixture CONFIG { <config>, '<property>' = 'opaque' };
      SHOW CREATE CLIENT client_fixture;
      """
    Then the last command output contains
      """
      <property>
      """

    Examples:
      | cluster_size | connector    | property              | config                          |
      | 1            | S3           | s3.driver_extension   | 'region' = 'us-east-1'          |
      | 3            | S3           | s3.driver_extension   | 'region' = 'us-east-1'          |
      | 1            | GCS          | gcs.driver_extension  | 'no_auth' = true                |
      | 3            | GCS          | gcs.driver_extension  | 'no_auth' = true                |
      | 1            | AZURE_BLOB   | adls.driver_extension | 'account_name' = 'fixture'      |
      | 3            | AZURE_BLOB   | adls.driver_extension | 'account_name' = 'fixture'      |
      | 1            | ICEBERG_REST | header.x-fixture      | 'uri' = 'http://127.0.0.1:8181' |
      | 3            | ICEBERG_REST | header.x-fixture      | 'uri' = 'http://127.0.0.1:8181' |
