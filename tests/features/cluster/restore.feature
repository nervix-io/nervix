Feature: Restore configuration, users and resources from a backup archive

  Scenario Outline: A fresh cluster whose nodes are named differently restores a cluster archive
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "zip_codes_dir" containing
      """
      {
        "lookup.jsonl": "{\"zip\":\"60601\",\"city\":\"Chicago\"}\n{\"zip\":\"10001\",\"city\":\"New York\"}\n"
      }
      """
    And node "node-1" has TLS resource directory "tls_dir" for hosts "restored-{{test_id}}.example.com"
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE PACED DOMAIN {{domain}} WITH PERIOD 1s SKEW 100ms;
      CREATE USER restored_reader WITH PASSWORD 'reader-password';
      CREATE RESOURCE zip_codes;
      UPLOAD RESOURCE zip_codes VERSION '{{zip_codes_dir}}';
      CREATE RESOURCE tls_bundle;
      UPLOAD RESOURCE tls_bundle VERSION '{{tls_dir}}';
      """
    Then the current leader node is saved as placeholder "leader"
    Given resource installation on node "{{leader}}" fails before promotion
    And client "uploader" is connected to the leader node
    When client "uploader" selects domain "{{domain}}"
    And client "uploader" upload of resource "zip_codes" from "{{zip_codes_dir}}" with identity "failed-version" fails with "for version 2 failed"
    And these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE zip_codes VERSION '{{zip_codes_dir}}';
      """
    Then the last command output contains
      """
      uploaded resource version 3
      """
    When these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA zip_code_entry ( zip STRING, city STRING );
      CREATE WIRE JSON SCHEMA zip_code_entry_wire MODE STRICT ( zip string, city string );
      CREATE CODEC zip_code_entry_codec FROM WIRE JSON SCHEMA zip_code_entry_wire TO SCHEMA zip_code_entry;
      CREATE HASH MAP zip_codes_by_zip KEY zip FROM RESOURCE zip_codes VERSION 3 PATH 'lookup.jsonl' DECODE USING zip_code_entry_codec;
      CREATE VHOST edge restored-{{test_id}}.example.com WITH TLS tls_bundle VERSION 1;
      DESCRIBE RESOURCE zip_codes;
      """
    Then the last command output contains
      """
      latest: 3
      versions: 1,2,3
      """
    When the CLI backs up "cluster" from node "{{leader}}" into "cluster.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the SHOW CREATE output of every model backup archive "cluster.nvxb" holds for domain "{{domain}}" is saved as "source" from node "{{leader}}"
    And the details DESCRIBE RESOURCE "zip_codes" prints for versions "1,3" in domain "{{domain}}" on node "{{leader}}" are saved as "zip_codes"
    And the details DESCRIBE RESOURCE "tls_bundle" prints for versions "1" in domain "{{domain}}" on node "{{leader}}" are saved as "tls_bundle"
    Given resource directory "zip_codes_dir" is kept beside the scenario's archives
    And resource directory "tls_dir" is kept beside the scenario's archives
    And the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    When the CLI restores "cluster --on-existing-user skip" from "cluster.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}" with 3 resource versions and 5 models
    And the CLI restore report shows step "import users" as "applied"
    And the SHOW CREATE output of every model saved as "source" is the same in domain "{{domain}}" on node "{{leader}}"
    And DESCRIBE RESOURCE "zip_codes" in domain "{{domain}}" on node "{{leader}}" prints the details saved as "zip_codes"
    And DESCRIBE RESOURCE "tls_bundle" in domain "{{domain}}" on node "{{leader}}" prints the details saved as "tls_bundle"
    When these NSPL commands are executed on the leader node
      """
      LIST DOMAINS;
      """
    Then the last command output contains
      """
      {{domain}} pace=PACED status=STOPPED
      """
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE RESOURCE zip_codes;
      """
    Then the last command output contains
      """
      latest: 3
      versions: 1,3
      """
    When these NSPL commands are executed through the client on the leader node
      """
      UPLOAD RESOURCE zip_codes VERSION '{{zip_codes_dir}}';
      """
    Then the last command output contains
      """
      uploaded resource version 4
      """
    When the client connects to the leader node as user "restored_reader" with password "reader-password"
    Then the last command output contains
      """
      raft.current_leader:
      """
    When these NSPL commands are executed on the leader node
      """
      LOOKUP zip_codes_by_zip KEY '60601';
      """
    Then the last command output contains
      """
      "city":"Chicago"
      """
    And the HTTPS listener of every node for host "restored-{{test_id}}.example.com" presents the certificate from resource directory "tls_dir"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A domain restore plans a dry run, refuses an existing name, and restores beside its original under a new name
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "zip_codes_dir" containing
      """
      {
        "lookup.jsonl": "{\"zip\":\"60601\",\"city\":\"Chicago\"}\n"
      }
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE zip_codes;
      UPLOAD RESOURCE zip_codes VERSION '{{zip_codes_dir}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA zip_code_entry ( zip STRING, city STRING );
      CREATE WIRE JSON SCHEMA zip_code_entry_wire MODE STRICT ( zip string, city string );
      CREATE CODEC zip_code_entry_codec FROM WIRE JSON SCHEMA zip_code_entry_wire TO SCHEMA zip_code_entry;
      CREATE HASH MAP zip_codes_by_zip KEY zip FROM RESOURCE zip_codes VERSION 1 PATH 'lookup.jsonl' DECODE USING zip_code_entry_codec;
      CREATE RELAY zip_codes_seen SCHEMA zip_code_entry UNBRANCHED;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "domain.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the SHOW CREATE output of every model backup archive "domain.nvxb" holds for domain "{{domain}}" is saved as "original" from node "{{leader}}"
    And the CLI restores "domain {{domain}} --as {{domain}}_copy --dry-run" from "domain.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI dry run planned domain "{{domain}}_copy" with 1 resource versions and 5 models
    When the CLI restores "domain {{domain}}" from "domain.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_REFUSED" and a message containing "domain '{{domain}}' already exists; restore it AS another name"
    When these NSPL commands are executed on the leader node
      """
      LIST DOMAINS;
      """
    Then the last command output does not contain
      """
      {{domain}}_copy
      """
    When the CLI restores "domain {{domain}} --as {{domain}}_copy" from "domain.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}_copy" with 1 resource versions and 5 models
    And the SHOW CREATE output of every model saved as "original" is the same in domain "{{domain}}_copy" on node "{{leader}}"
    And the SHOW CREATE output of every model saved as "original" is the same in domain "{{domain}}" on node "{{leader}}"
    When these NSPL commands are executed on the leader node
      """
      LIST DOMAINS;
      """
    Then the last command output contains
      """
      {{domain}}_copy pace=UNPACED status=STOPPED
      """
    When the CLI restores "domain {{domain}} --as {{domain}}_copy" from "domain.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_REFUSED" and a message containing "domain '{{domain}}_copy' already exists"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A cluster restore decides by its user policy what becomes of a user the cluster has
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE USER alice WITH PASSWORD 'archived-password';
      CREATE USER bob WITH PASSWORD 'bob-password';
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "cluster" from node "{{leader}}" into "users.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming no domain
    Given the cluster is replaced by a fresh <cluster_size> node cluster whose nodes are named "restored"
    Then the current leader node is saved as placeholder "leader"
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE USER alice WITH PASSWORD 'current-password';
      """
    And the CLI restores "cluster" from "users.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_REFUSED" and a message containing "already exists; restore with ON EXISTING USER SKIP or ON EXISTING USER REPLACE"
    When the client connects to the leader node as user "bob" with password "bob-password"
    Then the last command error contains
      """
      authentication failed
      """
    When the CLI restores "cluster --on-existing-user <policy>" from "users.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded with users <created> created, <skipped> skipped and <replaced> replaced
    When the client connects to the leader node as user "alice" with password "<alice_password>"
    Then the last command output contains
      """
      raft.current_leader:
      """
    When the client connects to the leader node as user "alice" with password "<refused_password>"
    Then the last command error contains
      """
      authentication failed
      """
    When the client connects to the leader node as user "bob" with password "bob-password"
    Then the last command output contains
      """
      raft.current_leader:
      """

    Examples:
      | cluster_size | policy  | created | skipped | replaced | alice_password    | refused_password  |
      | 1            | skip    | 1       | 2       | 0        | current-password  | archived-password |
      | 1            | replace | 1       | 0       | 2        | archived-password | current-password  |
      | 3            | skip    | 1       | 2       | 0        | current-password  | archived-password |
      | 3            | replace | 1       | 0       | 2        | archived-password | current-password  |

  Scenario Outline: An archive that does not verify, or whose models do not parse, is refused before the restore changes anything
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "zip_codes_dir" containing
      """
      {
        "lookup.jsonl": "{\"zip\":\"60601\",\"city\":\"Chicago\"}\n"
      }
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE zip_codes;
      UPLOAD RESOURCE zip_codes VERSION '{{zip_codes_dir}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA zip_code_entry ( zip STRING, city STRING );
      CREATE RELAY zip_codes_seen SCHEMA zip_code_entry UNBRANCHED;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "domain.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given backup archive "domain.nvxb" is copied to "corrupted.nvxb" with one byte of version 1 of resource "zip_codes" in domain "{{domain}}" changed
    When the CLI restores "domain {{domain}} --as {{domain}}_copy" from "corrupted.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_REFUSED" and a message containing "the archive is not a valid backup archive"
    When restore "domain {{domain}} AS {{domain}}_copy" of backup archive "domain.nvxb" is streamed to node "{{leader}}" declaring the digest of "corrupted.nvxb"
    Then the restore stream is refused as "DigestMismatch"
    Given backup archive "domain.nvxb" is copied to "unparsed.nvxb" with "CREATE RELAY" replaced by "CREATE RELAYS" in the models of domain "{{domain}}"
    When the CLI restores "domain {{domain}} --as {{domain}}_copy" from "unparsed.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_REFUSED" and a message containing "the models of domain '{{domain}}' do not parse at line"
    When these NSPL commands are executed on the leader node
      """
      LIST DOMAINS;
      """
    Then the last command output does not contain
      """
      {{domain}}_copy
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A failed model step keeps the steps before it and reports the step
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "zip_codes_dir" containing
      """
      {
        "lookup.jsonl": "{\"zip\":\"60601\",\"city\":\"Chicago\"}\n"
      }
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE zip_codes;
      UPLOAD RESOURCE zip_codes VERSION '{{zip_codes_dir}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA zip_code_entry ( zip STRING, city STRING );
      CREATE WIRE JSON SCHEMA zip_code_entry_wire MODE STRICT ( zip string, city string );
      CREATE CODEC zip_code_entry_codec FROM WIRE JSON SCHEMA zip_code_entry_wire TO SCHEMA zip_code_entry;
      CREATE HASH MAP zip_codes_by_zip KEY zip FROM RESOURCE zip_codes VERSION 1 PATH 'lookup.jsonl' DECODE USING zip_code_entry_codec;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "domain.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given backup archive "domain.nvxb" is copied to "missing_path.nvxb" with "PATH 'lookup.jsonl'" replaced by "PATH 'missing.jsonl'" in the models of domain "{{domain}}"
    When the CLI restores "domain {{domain}} --as {{domain}}_copy" from "missing_path.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_INCOMPLETE" and a message containing "restore failed at step 'apply models of domain '{{domain}}_copy''"
    And the CLI restore report shows step "create domain '{{domain}}_copy'" as "applied"
    And the CLI restore report shows step "import resource versions of domain '{{domain}}_copy'" as "applied"
    And the CLI restore report shows step "apply models of domain '{{domain}}_copy'" as "failed"
    When these NSPL commands are executed on the leader node
      """
      LIST DOMAINS;
      """
    Then the last command output contains
      """
      {{domain}}_copy pace=UNPACED status=STOPPED
      """
    Given the active domain is "{{domain}}_copy"
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE RESOURCE zip_codes;
      """
    Then the last command output contains
      """
      latest: 1
      versions: 1
      """
    When these NSPL commands fail with "does not exist"
      """
      SHOW CREATE SCHEMA zip_code_entry;
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An interrupted restore upload is sent again under its execution reference
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "bulk_dir" holding a 2 MiB file
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE bulk;
      UPLOAD RESOURCE bulk VERSION '{{bulk_dir}}';
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "bulk.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When restore "domain {{domain}} AS {{domain}}_copy" of backup archive "bulk.nvxb" is streamed to node "{{leader}}" under execution reference "restore_reference" and abandoned after 2 chunks
    Then the restore stream was abandoned
    When these NSPL commands are executed on the leader node
      """
      LIST DOMAINS;
      """
    Then the last command output does not contain
      """
      {{domain}}_copy
      """
    When restore "domain {{domain}} AS {{domain}}_copy" of backup archive "bulk.nvxb" is streamed to node "{{leader}}" under execution reference "restore_reference"
    Then the restore stream's outcome is "completed" as "executed"
    Given the active domain is "{{domain}}_copy"
    When these NSPL commands are executed on the leader node
      """
      DESCRIBE RESOURCE bulk;
      """
    Then the last command output contains
      """
      latest: 1
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A restore repeated under its execution reference joins it or returns its recorded outcome
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "zip_codes_dir" containing
      """
      {
        "lookup.jsonl": "{\"zip\":\"60601\",\"city\":\"Chicago\"}\n"
      }
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE zip_codes;
      UPLOAD RESOURCE zip_codes VERSION '{{zip_codes_dir}}';
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "domain.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given restore step "import resources" of domain "{{domain}}_copy" on node "{{leader}}" pauses before it applies
    When restore "domain {{domain}} AS {{domain}}_copy" of backup archive "domain.nvxb" is streamed to node "{{leader}}" under execution reference "restore_reference" in the background
    Then the restore pauses at step "import resources" of domain "{{domain}}_copy" on node "{{leader}}"
    When restore "domain {{domain}} AS {{domain}}_copy" of backup archive "domain.nvxb" is streamed to node "{{leader}}" under execution reference "restore_reference"
    Then the restore stream's outcome is "still applying" as "recovered"
    When restore "domain {{domain}} AS {{domain}}_other" of backup archive "domain.nvxb" is streamed to node "{{leader}}" under execution reference "restore_reference"
    Then the restore stream's outcome is "reference conflict" as "executed"
    When the restore step pause on node "{{leader}}" is released
    Then the background restore stream's outcome is "completed" as "executed"
    When restore "domain {{domain}} AS {{domain}}_copy" of backup archive "domain.nvxb" is streamed to node "{{leader}}" under execution reference "restore_reference"
    Then the restore stream's outcome is "completed" as "recovered"
    And the restore stream's report shows every step applied

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A leader change while a restore applies resumes it on the new leader
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "zip_codes_dir" containing
      """
      {
        "lookup.jsonl": "{\"zip\":\"60601\",\"city\":\"Chicago\"}\n"
      }
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE zip_codes;
      UPLOAD RESOURCE zip_codes VERSION '{{zip_codes_dir}}';
      """
    And these NSPL commands are executed on the leader node
      """
      CREATE SCHEMA zip_code_entry ( zip STRING, city STRING );
      CREATE WIRE JSON SCHEMA zip_code_entry_wire MODE STRICT ( zip string, city string );
      CREATE CODEC zip_code_entry_codec FROM WIRE JSON SCHEMA zip_code_entry_wire TO SCHEMA zip_code_entry;
      CREATE HASH MAP zip_codes_by_zip KEY zip FROM RESOURCE zip_codes VERSION 1 PATH 'lookup.jsonl' DECODE USING zip_code_entry_codec;
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "follower"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "domain.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the SHOW CREATE output of every model backup archive "domain.nvxb" holds for domain "{{domain}}" is saved as "original" from node "{{leader}}"
    And restore "domain {{domain}} AS {{domain}}_copy" of backup archive "domain.nvxb" is streamed to node "{{follower}}" under a fresh execution reference
    Then the restore stream's outcome is "redirected" as "executed"
    Given the path of backup archive "domain.nvxb" is saved as placeholder "archive"
    And restore step "import resources" of domain "{{domain}}_copy" on node "{{leader}}" pauses before it applies
    And client "operator" is connected to the leader node
    When client "operator" begins executing these NSPL commands in the background
      """
      RESTORE DOMAIN {{domain}} AS {{domain}}_copy FROM '{{archive}}';
      """
    Then the restore pauses at step "import resources" of domain "{{domain}}_copy" on node "{{leader}}"
    When leadership is transferred to node "{{follower}}"
    And the restore step pause on node "{{leader}}" is released
    Then the background NSPL execution succeeds
    And the last command output contains
      """
      restored domain '{{domain}}' as '{{domain}}_copy'
      """
    And the SHOW CREATE output of every model saved as "original" is the same in domain "{{domain}}_copy" on node "{{follower}}"
    Given the active domain is "{{domain}}_copy"
    When these NSPL commands are executed on node "{{follower}}"
      """
      DESCRIBE RESOURCE zip_codes;
      LOOKUP zip_codes_by_zip KEY '60601';
      """
    Then the last command output contains
      """
      "city":"Chicago"
      """
