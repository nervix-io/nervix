@runtime
Feature: Nervix clients preserve restore identity after an invalid reply
  Scenario Outline: A <client> recovers a completed restore after <reply> on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event ( value STRING );
      CREATE RELAY events SCHEMA event UNBRANCHED;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "reply-validation.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the <client> restores archive "reply-validation.nvxb" through a peer answering with "<reply>" on node "{{leader}}"
    Then the completed restore is reported as uncertain under its original execution reference
    When the restore is repeated directly under its original execution reference
    Then the restore's completed outcome is recovered without another execution

    @client_restore_reply_validation @deloxide_stress @deloxide_stress_restore
    Examples: native client
      | cluster_size | client        | reply                   |
      | 1            | native-client | an undecodable reply    |
      | 3            | native-client | an undecodable reply    |
      | 1            | native-client | another request's reply |
      | 3            | native-client | another request's reply |

    Examples: CLI
      | cluster_size | client | reply                   |
      | 1            | CLI    | an undecodable reply    |
      | 3            | CLI    | an undecodable reply    |
      | 1            | CLI    | another request's reply |
      | 3            | CLI    | another request's reply |
