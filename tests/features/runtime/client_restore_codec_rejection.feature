@client_restore_codec_rejection
Feature: A restore's unreadable gRPC reply leaves its original execution recoverable
  Scenario Outline: An unreadable restore reply keeps the <client> outcome uncertain on <cluster_size> nodes (<reply>)
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA archived_event (key I64);
      CREATE RELAY archived_input SCHEMA archived_event UNBRANCHED;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "codec.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the "<client>" client restores archive "codec.nvxb" through a peer returning a "<reply>" restore reply on node "{{leader}}"
    Then the unreadable restore reply left its outcome uncertain under its original execution reference
    And repeating the restore under that execution reference recovers its completed outcome

    @client_restore_codec_native @deloxide_stress @deloxide_stress_restore
    Examples: Native client
      | cluster_size | client | reply     |
      | 1            | native | truncated |
      | 3            | native | truncated |
      | 1            | native | oversized |
      | 3            | native | oversized |

    Examples: CLI
      | cluster_size | client | reply     |
      | 1            | CLI    | truncated |
      | 3            | CLI    | truncated |
      | 1            | CLI    | oversized |
      | 3            | CLI    | oversized |
