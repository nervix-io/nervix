Feature: VM functions remain valid through public graph changes
  Scenario Outline: A schema mutation installs a newly compiled function plan before ingestion resumes
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA function_input (id STRING, value STRING);
      CREATE SCHEMA function_output (id STRING, result STRING);
      CREATE WIRE JSON SCHEMA function_wire MODE STRICT (id string, value string);
      CREATE CODEC function_codec FROM WIRE JSON SCHEMA function_wire TO SCHEMA function_input;
      CREATE RELAY function_inputs SCHEMA function_input UNBRANCHED;
      CREATE RELAY function_outputs SCHEMA function_output UNBRANCHED;
      CREATE VHOST edge vm-replan-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/functions' TYPE HTTP;
      CREATE INGESTOR source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING function_codec
        TO function_inputs
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION transform
        FROM function_inputs
        UNBRANCHED
        TO function_outputs
          SET id = input.id,
              result = upper(input.value)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION before_change TO function_outputs;
      START;
      """
    And http payload is posted to node "node-1" with host "vm-replan-{{test_id}}.example.com" path "/functions"
      """
      {"id":"before","value":"café"}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"before" | "result":"CAFÉ"
      """
    When this NSPL command request is executed on the leader node
      """
      BEGIN;
      ALTER WIRE JSON SCHEMA function_wire ALTER FIELD value SET TYPE integer;
      ALTER SCHEMA function_input ALTER FIELD value SET TYPE I64;
      ALTER SCHEMA function_output ALTER FIELD result SET TYPE I64;
      DROP CODEC function_codec;
      CREATE CODEC function_codec FROM WIRE JSON SCHEMA function_wire TO SCHEMA function_input;
      DROP JUNCTION transform;
      CREATE JUNCTION transform
        FROM function_inputs
        UNBRANCHED
        TO function_outputs
          SET id = input.id,
              result = abs(input.value)
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      COMMIT;
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SUBSCRIPTION after_change TO function_outputs;
      """
    And http payload is posted to node "node-1" with host "vm-replan-{{test_id}}.example.com" path "/functions"
      """
      {"id":"after","value":-42}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"after" | "result":42
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Replacing a route installs its newly prepared regular expression
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA pattern_input (id STRING, text STRING);
      CREATE SCHEMA pattern_output (id STRING, result STRING);
      CREATE WIRE JSON SCHEMA pattern_wire MODE STRICT (id string, text string);
      CREATE CODEC pattern_codec FROM WIRE JSON SCHEMA pattern_wire TO SCHEMA pattern_input;
      CREATE RELAY pattern_inputs SCHEMA pattern_input UNBRANCHED;
      CREATE RELAY pattern_outputs SCHEMA pattern_output UNBRANCHED;
      CREATE VHOST edge vm-pattern-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/patterns' TYPE HTTP;
      CREATE INGESTOR source
        FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING pattern_codec
        TO pattern_inputs
          INHERIT ALL
          UNBRANCHED
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE JUNCTION patternize
        FROM pattern_inputs
        UNBRANCHED
        TO pattern_outputs
          SET id = input.id,
              result = regexp_replace(input.text, '^a', 'A')
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      CREATE SUBSCRIPTION pattern_results TO pattern_outputs;
      START;
      """
    And http payload is posted to node "node-1" with host "vm-pattern-{{test_id}}.example.com" path "/patterns"
      """
      {"id":"before","text":"apple"}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"before" | "result":"Apple"
      """
    When this NSPL command request is executed on the leader node
      """
      BEGIN;
      DROP JUNCTION patternize;
      CREATE JUNCTION patternize
        FROM pattern_inputs
        UNBRANCHED
        TO pattern_outputs
          SET id = input.id,
              result = regexp_replace(input.text, '^p', 'B')
          FLUSH IMMEDIATE
          ON MESSAGE ERROR LOG;
      COMMIT;
      """
    And http payload is posted to node "node-1" with host "vm-pattern-{{test_id}}.example.com" path "/patterns"
      """
      {"id":"after","text":"pear"}
      """
    Then within "30s" the relay subscription receives payloads containing all fragments
      """
      "id":"after" | "result":"Bear"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
