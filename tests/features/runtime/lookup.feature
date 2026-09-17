Feature: Resource-backed lookups
  Scenario Outline: Hash map lookups load resource records and answer direct queries
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "zip_codes_dir" containing
      """
      {
        "lookup.jsonl": "{\"zip\":\"60601\",\"city\":\"Chicago\"}\n{\"zip\":\"10001\",\"city\":\"New York\"}\n"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE zip_codes;
      UPLOAD RESOURCE zip_codes VERSION '{{zip_codes_dir}}';
      """
    Then the last command output contains
      """
      uploaded resource version 1
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA zip_code_entry (
        zip STRING,
        city STRING
      );

      CREATE WIRE JSON SCHEMA zip_code_entry_wire MODE STRICT (
        zip string,
        city string
      );

      CREATE CODEC zip_code_entry_codec
        FROM WIRE JSON SCHEMA zip_code_entry_wire
        TO SCHEMA zip_code_entry;

      CREATE HASH MAP zip_codes_by_zip
        KEY zip
        FROM RESOURCE zip_codes VERSION 1
        PATH 'lookup.jsonl'
        DECODE USING zip_code_entry_codec;
      """
    When these NSPL commands are executed
      """
      DESCRIBE HASH MAP zip_codes_by_zip;
      """
    Then the last command output contains
      """
      hash map: zip_codes_by_zip
      kind: HASH MAP
      """
    And the last command output contains
      """
      key: zip
      resource: zip_codes@1
      path: lookup.jsonl
      codec: zip_code_entry_codec
      """
    And the last command output contains
      """
      owner: node-
      """
    And the last command output contains
      """
      replicas:
      """
    And the last command output contains
      """
      entries: 2
      """
    When these NSPL commands are executed
      """
      LOOKUP zip_codes_by_zip KEY '60601';
      """
    Then the last command output contains
      """
      "city":"Chicago"
      """
    And the last command output contains
      """
      "zip":"60601"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  @command_completion
  Scenario: Malformed lookup records fail hash map creation
    Given a 1 node nervix cluster is started
    And node "node-1" has resource directory "malformed_lookup_dir" containing
      """
      {
        "lookup.jsonl": "{not-json}\n"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE DOMAIN {{domain}};
      CREATE RESOURCE malformed_lookup;
      UPLOAD RESOURCE malformed_lookup VERSION '{{malformed_lookup_dir}}';
      CREATE SCHEMA malformed_entry ( key STRING, value STRING );
      CREATE WIRE JSON SCHEMA malformed_entry_wire MODE STRICT (
        key string,
        value string
      );
      CREATE CODEC malformed_entry_codec
        FROM WIRE JSON SCHEMA malformed_entry_wire
        TO SCHEMA malformed_entry;
      """
    When these NSPL commands fail with "failed to decode lookup 'malformed_by_key' line 1"
      """
      CREATE HASH MAP malformed_by_key
        KEY key
        FROM RESOURCE malformed_lookup VERSION 1
        PATH 'lookup.jsonl'
        DECODE USING malformed_entry_codec;
      """

  Scenario Outline: Hash map lookups report a missing key clearly
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "zip_codes_dir" containing
      """
      {
        "lookup.jsonl": "{\"zip\":\"60601\",\"city\":\"Chicago\"}\n"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE zip_codes;
      UPLOAD RESOURCE zip_codes VERSION '{{zip_codes_dir}}';
      """
    When these NSPL commands are executed on the leader node
      """

      CREATE SCHEMA zip_code_entry (
        zip STRING,
        city STRING
      );

      CREATE WIRE JSON SCHEMA zip_code_entry_wire MODE STRICT (
        zip string,
        city string
      );

      CREATE CODEC zip_code_entry_codec
        FROM WIRE JSON SCHEMA zip_code_entry_wire
        TO SCHEMA zip_code_entry;

      CREATE HASH MAP zip_codes_by_zip
        KEY zip
        FROM RESOURCE zip_codes VERSION 1
        PATH 'lookup.jsonl'
        DECODE USING zip_code_entry_codec;
      """
    When these NSPL commands fail with "hash map 'zip_codes_by_zip' has no entry for key '99999'"
      """
      LOOKUP zip_codes_by_zip KEY '99999';
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Hash map lookup query over a remote owner returns a field-only record
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "zip_codes_dir" containing
      """
      {
        "lookup.jsonl": "{\"zip\":\"60601\",\"city\":\"Chicago\"}\n{\"zip\":\"10001\",\"city\":\"New York\"}\n"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE zip_codes;
      UPLOAD RESOURCE zip_codes VERSION '{{zip_codes_dir}}';
      """
    When these NSPL commands are executed on the leader node
      """

      CREATE SCHEMA zip_code_entry (
        zip STRING,
        city STRING
      );

      CREATE WIRE JSON SCHEMA zip_code_entry_wire MODE STRICT (
        zip string,
        city string
      );

      CREATE CODEC zip_code_entry_codec
        FROM WIRE JSON SCHEMA zip_code_entry_wire
        TO SCHEMA zip_code_entry;

      CREATE HASH MAP zip_codes_by_zip
        KEY zip
        FROM RESOURCE zip_codes VERSION 1
        PATH 'lookup.jsonl'
        DECODE USING zip_code_entry_codec;
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE HASH MAP zip_codes_by_zip;
      """
    Then the last command output contains
      """
      owner: node-
      """
    When these NSPL commands are executed on a node that is not a holder of the last described hash map
      """
      LOOKUP zip_codes_by_zip KEY '60601';
      """
    Then the last command output contains
      """
      "city":"Chicago"
      """
    And the last command output contains
      """
      "zip":"60601"
      """
    And the last command output does not contain
      """
      ingested_at_low_watermark
      """
    And the last command output does not contain
      """
      ingested_at_high_watermark
      """

    Examples:
      | cluster_size | replica_count |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: Hash map lookups survive a cluster restart
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "zip_codes_dir" containing
      """
      {
        "lookup.jsonl": "{\"zip\":\"60601\",\"city\":\"Chicago\"}\n{\"zip\":\"10001\",\"city\":\"New York\"}\n"
      }
      """
    And node "node-1" has resource directory "zip_codes_v2_dir" containing
      """
      {
        "lookup.jsonl": "{\"zip\":\"60601\",\"city\":\"Madison\"}\n{\"zip\":\"10001\",\"city\":\"New York\"}\n"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE zip_codes;
      UPLOAD RESOURCE zip_codes VERSION '{{zip_codes_dir}}';
      """
    When these NSPL commands are executed on the leader node
      """

      CREATE SCHEMA zip_code_entry (
        zip STRING,
        city STRING
      );

      CREATE WIRE JSON SCHEMA zip_code_entry_wire MODE STRICT (
        zip string,
        city string
      );

      CREATE CODEC zip_code_entry_codec
        FROM WIRE JSON SCHEMA zip_code_entry_wire
        TO SCHEMA zip_code_entry;

      CREATE HASH MAP zip_codes_by_zip
        KEY zip
        FROM RESOURCE zip_codes VERSION 1
        PATH 'lookup.jsonl'
        DECODE USING zip_code_entry_codec;
      """
    When these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE zip_codes VERSION '{{zip_codes_v2_dir}}';
      """
    When the cluster is restarted
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE HASH MAP zip_codes_by_zip;
      """
    Then the last command output contains
      """
      resource: zip_codes@1
      """
    When these NSPL commands are executed on the leader node
      """
      LOOKUP zip_codes_by_zip KEY '60601';
      """
    Then the last command output contains
      """
      "city":"Chicago"
      """
    And the last command output contains
      """
      "zip":"60601"
      """
    And the last command output does not contain
      """
      "city":"Madison"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |

  Scenario Outline: A hash map line decoded by a JAQ codec contributes one entry per unfolded message
    Given runtime replication is configured with replica count <replica_count> and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "zip_batches_dir" containing
      """
      {
        "lookup.jsonl": "[{\"zip\":\"60601\",\"city\":\"Chicago\"},{\"zip\":\"10001\",\"city\":\"New York\"}]\n"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE zip_batches;
      UPLOAD RESOURCE zip_batches VERSION '{{zip_batches_dir}}';
      """
    Then the last command output contains
      """
      uploaded resource version 1
      """
    When these NSPL commands are executed
      """
      CREATE SCHEMA zip_code_entry (
        zip STRING,
        city STRING
      );

      CREATE CODEC zip_batch_codec
        FROM JSON
        TO SCHEMA zip_code_entry
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';

      CREATE HASH MAP zip_codes_by_zip
        KEY zip
        FROM RESOURCE zip_batches VERSION 1
        PATH 'lookup.jsonl'
        DECODE USING zip_batch_codec;
      """
    When these NSPL commands are executed
      """
      DESCRIBE HASH MAP zip_codes_by_zip;
      """
    Then the last command output contains
      """
      entries: 2
      """
    When these NSPL commands are executed
      """
      LOOKUP zip_codes_by_zip KEY '60601';
      """
    Then the last command output contains
      """
      "city":"Chicago"
      """
    When these NSPL commands are executed
      """
      LOOKUP zip_codes_by_zip KEY '10001';
      """
    Then the last command output contains
      """
      "city":"New York"
      """

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
