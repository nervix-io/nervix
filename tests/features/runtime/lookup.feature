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

      CREATE JSON WIRE SCHEMA zip_code_entry_wire (
        zip string,
        city string
      );

      CREATE CODEC zip_code_entry_codec
        FROM WIRE JSON SCHEMA zip_code_entry_wire
        TO SCHEMA zip_code_entry;

      CREATE HASH MAP zip_codes_by_zip
        KEY zip
        FROM RESOURCE zip_codes
        PATH 'lookup.jsonl'
        DECODE USING zip_code_entry_codec;
      """
    Then node "node-1" eventually reports status containing "{{domain}} status=Stopped pace=UNPACED"
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

      CREATE SCHEMA zip_code_entry (
        zip STRING,
        city STRING
      );

      CREATE JSON WIRE SCHEMA zip_code_entry_wire (
        zip string,
        city string
      );

      CREATE CODEC zip_code_entry_codec
        FROM WIRE JSON SCHEMA zip_code_entry_wire
        TO SCHEMA zip_code_entry;

      CREATE HASH MAP zip_codes_by_zip
        KEY zip
        FROM RESOURCE zip_codes
        PATH 'lookup.jsonl'
        DECODE USING zip_code_entry_codec;
      """
    Then node "node-1" eventually reports status containing "{{domain}} status=Stopped pace=UNPACED"
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

      CREATE SCHEMA zip_code_entry (
        zip STRING,
        city STRING
      );

      CREATE JSON WIRE SCHEMA zip_code_entry_wire (
        zip string,
        city string
      );

      CREATE CODEC zip_code_entry_codec
        FROM WIRE JSON SCHEMA zip_code_entry_wire
        TO SCHEMA zip_code_entry;

      CREATE HASH MAP zip_codes_by_zip
        KEY zip
        FROM RESOURCE zip_codes
        PATH 'lookup.jsonl'
        DECODE USING zip_code_entry_codec;
      """
    Then node "node-1" eventually reports status containing "{{domain}} status=Stopped pace=UNPACED"
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
    And the leader node is configured with these NSPL commands
      """
      CREATE DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE RESOURCE zip_codes;
      UPLOAD RESOURCE zip_codes VERSION '{{zip_codes_dir}}';

      CREATE SCHEMA zip_code_entry (
        zip STRING,
        city STRING
      );

      CREATE JSON WIRE SCHEMA zip_code_entry_wire (
        zip string,
        city string
      );

      CREATE CODEC zip_code_entry_codec
        FROM WIRE JSON SCHEMA zip_code_entry_wire
        TO SCHEMA zip_code_entry;

      CREATE HASH MAP zip_codes_by_zip
        KEY zip
        FROM RESOURCE zip_codes
        PATH 'lookup.jsonl'
        DECODE USING zip_code_entry_codec;
      """
    When the cluster is restarted
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

    Examples:
      | cluster_size | replica_count |
      | 1            | 0             |
      | 3            | 0             |
      | 3            | 1             |
