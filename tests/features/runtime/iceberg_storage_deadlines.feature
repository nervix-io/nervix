Feature: Iceberg storage deadlines and retry ownership

  Scenario Outline: A paused Iceberg object store reports timeout_ms and recovers on the emitter retry policy
    Given Iceberg dependencies are running
    And a <cluster_size> node nervix cluster is started
    And Iceberg table "storage_timeout_{{test_id}}" exists at "s3://nervix-iceberg/tables/storage_timeout_{{test_id}}" with columns
      """
      user_id I64
      """
    And a stallable endpoint "iceberg_objects" stands in front of "rustfs_addr"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these Plexus statements are executed on the leader node
      """
      CREATE SCHEMA notification (user_id I64);
      CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (user_id integer);
      CREATE CODEC notification_codec FROM WIRE JSON SCHEMA notification_wire TO SCHEMA notification;
      CREATE RELAY notifications SCHEMA notification UNBRANCHED;
      CREATE VHOST edge iceberg-storage-{{test_id}}.example.com;
      CREATE ENDPOINT notification_endpoint ON edge PATH '/notifications' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR notification_input
        FROM notification_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
        TO notifications INHERIT ALL UNBRANCHED
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE S3 CLIENT s3_main CONFIG {
        'endpoint' = '{{iceberg_objects}}',
        'region' = 'us-east-1',
        'access_key_id' = 'rustfsadmin',
        'secret_access_key' = 'rustfsadmin',
        'path_style_access' = true,
        'timeout_ms' = 1000
      };
      CREATE ICEBERG_REST CLIENT iceberg_catalog CONFIG {
        'uri' = '{{iceberg_rest_addr}}',
        'warehouse' = 's3://nervix-iceberg/warehouse'
      };
      CREATE ICEBERG TABLE EMITTER iceberg_output FROM notifications
        TO s3_main TABLE 'storage_timeout_{{test_id}}'
        LOCATION 's3://nervix-iceberg/tables/storage_timeout_{{test_id}}'
        CATALOG iceberg_catalog COMMIT EACH 100ms MAX SIZE 1MiB MODE ACK RETRY POLICY BACKOFF 2s MAX 2s VALUES { 'user_id' = input.user_id } FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then within "30s" DESCRIBE EMITTER "iceberg_output" on the leader node contains
      """
      status: OK
      """
    When the stallable endpoint "iceberg_objects" is paused
    And http payload is posted to host "iceberg-storage-{{test_id}}.example.com" path "/notifications"
      """
      {"user_id":73}
      """
    Then the silent endpoint "iceberg_objects" eventually accepts a connection
    And within "20s" DESCRIBE EMITTER "iceberg_output" on the leader node contains
      """
      timeout_ms: 1000
      """
    And within "20s" DESCRIBE EMITTER "iceberg_output" on the leader node contains
      """
      reconnect backoff: 2s
      """
    When the stallable endpoint "iceberg_objects" is resumed
    Then within "60s" the Iceberg table "storage_timeout_{{test_id}}" contains a row no sooner than "3s" after it was published
      """
      {"user_id":73}
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
