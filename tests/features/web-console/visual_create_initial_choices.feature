Feature: Web console visual creation with a delayed initial domain publication

  Scenario Outline: An ingestor chooses its endpoint after the initial domain arrives
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE VHOST initial_host initial-{{test_id}}.example.com;
      CREATE ENDPOINT initial_endpoint ON initial_host PATH '/initial' TYPE HTTP;
      """
    And the web console is opened on the leader node with its initial session frames held
    Then selector ".domain-select" contains "loading domains"
    When the held initial web console session frames are released
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='ingestor']" is clicked
    And selector ".create-name" is filled with "initial_source"
    And selector ".create-ingestor-source [data-source='http-endpoint']" is clicked
    Then selector ".create-scope" contains "{{domain}}"
    When selector ".create-ingestor-source-ref .create-choice-search" is filled with "initial_endpoint"
    Then selector ".create-ingestor-source-ref [data-value='initial_endpoint']" exists

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A processor chooses its input after the initial domain arrives
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA initial_event (message STRING);
      CREATE RELAY initial_input SCHEMA initial_event UNBRANCHED;
      """
    And the web console is opened on the leader node with its initial session frames held
    Then selector ".domain-select" contains "loading domains"
    When the held initial web console session frames are released
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='reingestor']" is clicked
    And selector ".create-name" is filled with "initial_processor"
    Then selector ".create-scope" contains "{{domain}}"
    When selector ".create-processor-input .create-choice-search" is filled with "initial_input"
    Then selector ".create-processor-input [data-value='initial_input']" exists

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
