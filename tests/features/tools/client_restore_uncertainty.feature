@client_restore_uncertainty
Feature: A later archive failure cannot settle an earlier uncertain restore
  Scenario Outline: A later archive failure keeps the <client> restore uncertain on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA archived_event (key I64);
      CREATE RELAY archived_input SCHEMA archived_event UNBRANCHED;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "uncertain.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the "<client>" client loses the completed restore answer for archive "uncertain.nvxb" and cannot reopen it on node "{{leader}}"
    Then a later archive failure left its restore uncertain under the original execution reference
    And repeating the restore under that execution reference recovers its completed outcome

    @client_restore_uncertainty_native @deloxide_stress @deloxide_stress_restore
    Examples: Native client
      | cluster_size | client |
      | 1            | native |
      | 3            | native |

    Examples: CLI
      | cluster_size | client |
      | 1            | CLI    |
      | 3            | CLI    |
