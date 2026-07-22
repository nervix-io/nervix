Feature: List operations
  Scenario Outline: HTTP ingestor filter-map evaluates list operation builtins
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the ingestor logic fixture "http_endpoint" starts with output schema "list_operations" and program
      """
      INHERIT ALL EXCEPT values, fixed, labels
      SET total = sum(input.values),
          first_value = first(input.values),
          last_value = last(input.values),
          second_value = nth(input.values, 1),
          value_count = count(input.values),
          fixed_first = first(input.fixed),
          fixed_last = last(input.fixed),
          first_label = first(input.labels),
          last_label = last(input.labels)
      """
    And the ingestor logic transport "http_endpoint" delivers payload fixture "list_operations_message"
    Then the relay subscription receives a payload
      """
      "tenant":"acme"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    And the last relay subscription payload does not contain "values\""
    And the last relay subscription payload does not contain "fixed\""
    And the last relay subscription payload does not contain "labels\""
    And the last relay subscription payload contains
      """
      "total":6
      "first_value":1
      "last_value":3
      "second_value":2
      "value_count":3
      "fixed_first":10
      "fixed_last":20
      "first_label":"prod"
      "last_label":"edge"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
