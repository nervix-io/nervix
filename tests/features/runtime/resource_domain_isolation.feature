Feature: Domain-owned resources
  @resource_domain_isolation
  Scenario Outline: Resources with the same name are independent in every domain
    Given a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "alpha_dir" containing
      """
      {
        "payload.txt": "alpha-content\n"
      }
      """
    And node "node-1" has resource directory "beta_dir" containing
      """
      {
        "payload.txt": "beta-content\n",
        "extra.txt": "beta-only\n"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN {{domain}}_beta;
      CREATE UNPACED DOMAIN {{domain}}_empty;
      """
    Given client "alpha" is connected to the leader node
    And client "beta" is connected to the leader node
    And client "empty" is connected to the leader node
    When client "alpha" selects domain "{{domain}}"
    And client "beta" selects domain "{{domain}}_beta"
    And client "empty" selects domain "{{domain}}_empty"
    And client "alpha" executes these NSPL commands
      """
      CREATE RESOURCE shared_name;
      UPLOAD RESOURCE shared_name VERSION '{{alpha_dir}}';
      """
    Then the last command output contains
      """
      uploaded resource version 1
      """
    When client "beta" executes these NSPL commands
      """
      CREATE RESOURCE shared_name;
      UPLOAD RESOURCE shared_name VERSION '{{beta_dir}}';
      UPLOAD RESOURCE shared_name VERSION '{{beta_dir}}';
      """
    Then the last command output contains
      """
      uploaded resource version 2
      """
    When client "alpha" executes these NSPL commands
      """
      DESCRIBE RESOURCE shared_name;
      """
    Then the last command output contains
      """
      versions: 1
      """
    And the last command output does not contain
      """
      versions: 1,2
      """
    When client "beta" executes these NSPL commands
      """
      DESCRIBE RESOURCE shared_name;
      """
    Then the last command output contains
      """
      versions: 1,2
      """
    When client "alpha" executes these NSPL commands
      """
      DESCRIBE RESOURCE shared_name VERSION 1;
      """
    Then the last command output contains
      """
      file_count: 1
      """
    And the last command output does not contain
      """
      path=extra.txt
      """
    When client "beta" executes these NSPL commands
      """
      DESCRIBE RESOURCE shared_name VERSION 1;
      """
    Then the last command output contains
      """
      file_count: 2
      """
    And the last command output contains
      """
      path=extra.txt
      """
    When client "empty" fails to execute these NSPL commands
      """
      DESCRIBE RESOURCE shared_name;
      """
    Then the last command error contains
      """
      resource 'shared_name' does not exist
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  @resource_domain_isolation
  Scenario: Model validation resolves the resource stored in the model's own domain
    Given a 1 node nervix cluster is started
    And node "node-1" has resource directory "alpha_lookup_dir" containing
      """
      {
        "alpha.jsonl": "{\"key\":\"k1\",\"label\":\"alpha-label\"}\n"
      }
      """
    And node "node-1" has resource directory "beta_lookup_dir" containing
      """
      {
        "beta.jsonl": "{\"key\":\"k1\",\"label\":\"beta-label\"}\n"
      }
      """
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN {{domain}}_beta;
      """
    Given client "alpha" is connected to the leader node
    And client "beta" is connected to the leader node
    When client "alpha" selects domain "{{domain}}"
    And client "beta" selects domain "{{domain}}_beta"
    And client "alpha" executes these NSPL commands
      """
      CREATE RESOURCE lookup_bundle;
      UPLOAD RESOURCE lookup_bundle VERSION '{{alpha_lookup_dir}}';
      """
    And client "beta" executes these NSPL commands
      """
      CREATE RESOURCE lookup_bundle;
      UPLOAD RESOURCE lookup_bundle VERSION '{{beta_lookup_dir}}';
      """
    And client "alpha" executes these NSPL commands
      """
      CREATE SCHEMA lookup_entry (
        key STRING,
        label STRING
      );
      CREATE WIRE JSON SCHEMA lookup_entry_wire MODE STRICT (
        key string,
        label string
      );
      CREATE CODEC lookup_entry_codec
        FROM WIRE JSON SCHEMA lookup_entry_wire
        TO SCHEMA lookup_entry;
      CREATE HASH MAP entries_by_key
        KEY key
        FROM RESOURCE lookup_bundle
        PATH 'alpha.jsonl'
        DECODE USING lookup_entry_codec;
      """
    And client "beta" executes these NSPL commands
      """
      CREATE SCHEMA lookup_entry (
        key STRING,
        label STRING
      );
      CREATE WIRE JSON SCHEMA lookup_entry_wire MODE STRICT (
        key string,
        label string
      );
      CREATE CODEC lookup_entry_codec
        FROM WIRE JSON SCHEMA lookup_entry_wire
        TO SCHEMA lookup_entry;
      """
    And client "beta" fails to execute these NSPL commands
      """
      CREATE HASH MAP entries_by_key
        KEY key
        FROM RESOURCE lookup_bundle
        PATH 'alpha.jsonl'
        DECODE USING lookup_entry_codec;
      """
    Then the last command error contains
      """
      lookup file
      """
    And the last command error contains
      """
      alpha.jsonl' does not exist
      """
    When client "beta" executes these NSPL commands
      """
      CREATE HASH MAP entries_by_key
        KEY key
        FROM RESOURCE lookup_bundle
        PATH 'beta.jsonl'
        DECODE USING lookup_entry_codec;
      """
    And client "alpha" fails to execute these NSPL commands
      """
      CREATE HASH MAP entries_by_beta_key
        KEY key
        FROM RESOURCE lookup_bundle
        PATH 'beta.jsonl'
        DECODE USING lookup_entry_codec;
      """
    Then the last command error contains
      """
      lookup file
      """
    And the last command error contains
      """
      beta.jsonl' does not exist
      """
