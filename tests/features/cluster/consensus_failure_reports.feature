Feature: Consensus failure reports at client mutation boundaries

  Scenario: Creating a domain reports a rejected durable proposal
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And consensus storage on the leader fails before committing operation "put-domain:{{domain}}"
    When these NSPL commands fail with "consensus storage"
      """
      CREATE UNPACED DOMAIN {{domain}};
      """

  Scenario Outline: Creating a user reports the storage failure that rejected its proposal
    Given a <cluster_size> node nervix cluster is started
    And consensus storage on the leader fails before committing operation "create-user:report_user"
    When these NSPL commands fail with "consensus storage"
      """
      CREATE USER report_user WITH PASSWORD 'created-password';
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Cordoning a node reports the storage failure that rejected its proposal
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And consensus storage on the leader fails before committing operation "cordon-node:node-2"
    When these NSPL commands fail with "consensus storage"
      """
      CORDON NODE node-2;
      """

  Scenario: Draining a node reports a rejected cordon proposal
    Given the production sticky scheduler is configured
    And a 3 node nervix cluster is started
    And consensus storage on the leader fails before committing operation "cordon-node:node-2"
    When these NSPL commands fail with "consensus storage"
      """
      DRAIN NODE node-2;
      """

  Scenario Outline: Queueing a transaction statement reports the storage failure that rejected its proposal
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And client "owner" is connected to the leader node
    When client "owner" executes these NSPL commands
      """
      BEGIN;
      """
    Then client "owner" transaction id is saved as placeholder "transaction_id"
    Given consensus storage on the leader fails before committing operation "queue-transaction-statement:{{transaction_id}}"
    When client "owner" fails to execute these NSPL commands
      """
      CREATE RESOURCE report_resource;
      """
    Then the last command error contains
      """
      consensus storage
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
