Feature: Iceberg TLS resource mounts
  Scenario Outline: An Iceberg REST catalog client trusts its mounted CA and presents its mounted identity
    Given Iceberg dependencies are running
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "tls_catalog_{{test_id}}" exists at "s3://nervix-iceberg/tables/tls_catalog_{{test_id}}" with columns
      """
      user_id I64
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has dev TLS resource directory "dev_tls"
    When these Plexus commands are executed
      """
      CREATE RESOURCE dev_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE dev_tls VERSION "{{dev_tls}}";
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge iceberg-tls-catalog-{{test_id}}.example.com;
      CREATE ENDPOINT notification_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR notification_input
        FROM notification_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE S3 CLIENT s3_main CONFIG {
        'endpoint' = '{{rustfs_addr}}',
        'region' = 'us-east-1',
        'access_key_id' = 'rustfsadmin',
        'secret_access_key' = 'rustfsadmin',
        'path_style_access' = true
      };
      CREATE ICEBERG_REST CLIENT catalog_tls
        MOUNT dev_tls VERSION 1
        CONFIG {
          'uri' = '{{iceberg_rest_tls_addr}}',
          'warehouse' = 's3://nervix-iceberg/warehouse',
          'tls_ca_file' = '{{dev_tls}}/ca.pem',
          'tls_cert_file' = '{{dev_tls}}/node.pem',
          'tls_key_file' = '{{dev_tls}}/node-key.pem'
        };
      CREATE ICEBERG TABLE EMITTER iceberg_output FROM notifications
        TO s3_main TABLE 'tls_catalog_{{test_id}}' NAMESPACE '{{domain}}'
        LOCATION 's3://nervix-iceberg/tables/tls_catalog_{{test_id}}'
        CATALOG catalog_tls COMMIT EACH 100ms MAX SIZE 1MiB
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 5s
        VALUES { 'user_id' = input.user_id }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE EMITTER "iceberg_output" on the leader node contains
      """
      status: OK
      """
    When http payload is posted to host "iceberg-tls-catalog-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":41}
      """
    Then the Iceberg table "tls_catalog_{{test_id}}" eventually contains a row
      """
      {"user_id":41}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An Iceberg emitter commits through a TLS catalog and a TLS object store with mounted trust and identity
    Given Iceberg dependencies are running
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "tls_objects_{{test_id}}" exists at "s3://nervix-iceberg/tables/tls_objects_{{test_id}}" with columns
      """
      user_id I64
      """
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And node "node-1" has dev TLS resource directory "dev_tls"
    When these Plexus commands are executed
      """
      CREATE RESOURCE dev_tls;
      """
    And these Plexus commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE dev_tls VERSION "{{dev_tls}}";
      """
    And these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge iceberg-tls-objects-{{test_id}}.example.com;
      CREATE ENDPOINT notification_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR notification_input
        FROM notification_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE S3 CLIENT s3_tls
        MOUNT dev_tls VERSION 1
        CONFIG {
          'endpoint' = '{{rustfs_tls_addr}}',
          'region' = 'us-east-1',
          'access_key_id' = 'rustfsadmin',
          'secret_access_key' = 'rustfsadmin',
          'path_style_access' = true,
          'tls_ca_file' = '{{dev_tls}}/ca.pem',
          'tls_cert_file' = '{{dev_tls}}/node.pem',
          'tls_key_file' = '{{dev_tls}}/node-key.pem'
        };
      CREATE ICEBERG_REST CLIENT catalog_tls
        MOUNT dev_tls VERSION 1
        CONFIG {
          'uri' = '{{iceberg_rest_tls_addr}}',
          'warehouse' = 's3://nervix-iceberg/warehouse',
          'tls_ca_file' = '{{dev_tls}}/ca.pem',
          'tls_cert_file' = '{{dev_tls}}/node.pem',
          'tls_key_file' = '{{dev_tls}}/node-key.pem'
        };
      CREATE ICEBERG TABLE EMITTER iceberg_output FROM notifications
        TO s3_tls TABLE 'tls_objects_{{test_id}}' NAMESPACE '{{domain}}'
        LOCATION 's3://nervix-iceberg/tables/tls_objects_{{test_id}}'
        CATALOG catalog_tls COMMIT EACH 100ms MAX SIZE 1MiB
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 5s
        VALUES { 'user_id' = input.user_id }
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE EMITTER "iceberg_output" on the leader node contains
      """
      status: OK
      """
    When http payload is posted to host "iceberg-tls-objects-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":42}
      """
    Then the Iceberg table "tls_objects_{{test_id}}" eventually contains a row
      """
      {"user_id":42}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
