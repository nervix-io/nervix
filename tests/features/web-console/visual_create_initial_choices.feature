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

  Scenario Outline: Every other scoped form family chooses its prerequisite after the initial domain arrives
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA initial_key (tenant U32);
      CREATE RELAY initial_events SCHEMA initial_key UNBRANCHED;
      CREATE VHOST initial_host initial-{{test_id}}.example.com;
      CREATE RESOURCE initial_bundle;
      """
    And the web console is opened on the leader node with its initial session frames held
    Then selector ".domain-select" contains "loading domains"
    When the held initial web console session frames are released
    Then selector ".domain-menu [data-domain='{{domain}}'].active" exists
    When selector ".create-menu-button" is clicked
    And selector ".create-menu [data-create-kind='<kind>']" is clicked
    Then selector ".create-scope" contains "{{domain}}"
    When selector "<choice> .create-choice-search" is filled with "<value>"
    Then selector "<choice> [data-value='<value>']" exists

    Examples:
      | cluster_size | kind         | choice                 | value          |
      | 1            | branch       | .create-schema-ref     | initial_key    |
      | 3            | branch       | .create-schema-ref     | initial_key    |
      | 1            | subscription | .create-relay-ref      | initial_events |
      | 3            | subscription | .create-relay-ref      | initial_events |
      | 1            | codec        | .create-codec-schema   | initial_key    |
      | 3            | codec        | .create-codec-schema   | initial_key    |
      | 1            | endpoint     | .create-endpoint-vhost | initial_host   |
      | 3            | endpoint     | .create-endpoint-vhost | initial_host   |
      | 1            | hash-map     | .create-hash-resource  | initial_bundle |
      | 3            | hash-map     | .create-hash-resource  | initial_bundle |
