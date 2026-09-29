Feature: Configuration backup into a public archive

  Scenario Outline: A cluster backup archives every domain's models, users and resource versions
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "schema/root.proto": "syntax = \"proto3\";"
      }
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE proto;
      UPLOAD RESOURCE proto VERSION '{{proto_dir}}';
      CREATE SCHEMA order_event ( id I64, amount F64 );
      CREATE RELAY orders SCHEMA order_event UNBRANCHED;
      CREATE RELAY large_orders SCHEMA order_event UNBRANCHED;
      CREATE JUNCTION large_order_filter FROM orders UNBRANCHED TO large_orders INHERIT ALL WHERE output.amount > 100.0 FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "cluster" from node "{{leader}}" into "cluster.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup archive "cluster.nvxb" holds for domain "{{domain}}" exactly the models these NSPL commands create
      """
      CREATE SCHEMA order_event ( id I64, amount F64 );
      CREATE RELAY orders SCHEMA order_event UNBRANCHED;
      CREATE RELAY large_orders SCHEMA order_event UNBRANCHED;
      CREATE JUNCTION large_order_filter FROM orders UNBRANCHED TO large_orders INHERIT ALL WHERE output.amount > 100.0 FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
      """
    When the CLI describes backup archive "cluster.nvxb" as json
    Then the described backup lists the scenario user
    And the described backup lists version 1 of resource "proto" in domain "{{domain}}" with the checksums DESCRIBE RESOURCE reports on node "{{leader}}"
    When the CLI describes backup archive "cluster.nvxb" as text
    Then the CLI output contains "scope: cluster"
    And the CLI output contains "resource=proto version=1 state=completed"
    And the CLI output contains "archive=included"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A domain backup without resources records every version's checksums without its bytes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "proto_dir" containing
      """
      {
        "schema/root.proto": "syntax = \"proto3\";"
      }
      """
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE proto;
      UPLOAD RESOURCE proto VERSION '{{proto_dir}}';
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}} --without-resources" from node "{{leader}}" into "domain.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the CLI describes backup archive "domain.nvxb" as json
    Then the described backup holds no users
    And the described backup lists version 1 of resource "proto" in domain "{{domain}}" with the checksums DESCRIBE RESOURCE reports on node "{{leader}}" and without its archive
    When the CLI describes backup archive "domain.nvxb" as text
    Then the CLI output contains "users: not included"
    And the CLI output contains "archive=omitted"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" to standard output
    Then the archive the CLI wrote to standard output verifies and its report went to standard error

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An archive larger than 64 MiB streams to the client completely
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "bulk_dir" holding a 65 MiB file
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE bulk;
      UPLOAD RESOURCE bulk VERSION '{{bulk_dir}}';
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "cluster" from node "{{leader}}" into "bulk.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup archive "bulk.nvxb" is larger than 64 MiB

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A client that loses its download fetches the archive again until a download collects it
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And node "node-1" has resource directory "bulk_dir" holding a 4 MiB file
    When these NSPL commands are executed through the client on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE bulk;
      UPLOAD RESOURCE bulk VERSION '{{bulk_dir}}';
      """
    Then the current leader node is saved as placeholder "leader"
    When the session backs up the cluster on node "{{leader}}" without downloading its archive
    And the backup's archive download from node "{{leader}}" is abandoned after 2 chunks
    And the backup's archive is downloaded from node "{{leader}}"
    Then the downloaded archive matches the backup's summary
    When the backup's archive is downloaded from node "{{leader}}"
    Then the download is refused as "NotRetained"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An archive is refused once its execution reference's retry validity ends
    Given command retry identities are valid for "5s"
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the session backs up the cluster on node "{{leader}}" without downloading its archive
    And the backup's retry validity has ended
    And the backup's archive is downloaded from node "{{leader}}"
    Then the download is refused as "Expired"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A backup sent to a follower runs on the leader, and a follower sends its download there
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA order_event ( id I64 );
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "follower"
    When the CLI backs up "cluster" from node "{{follower}}" into "follower.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the session backs up the cluster on node "{{leader}}" without downloading its archive
    And the backup's archive is downloaded from node "{{follower}}"
    Then the download is redirected to node "{{leader}}"

  Scenario Outline: A backup taken while models change holds one coherent revision
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA burst_event ( value I64 );
      """
    Then the current leader node is saved as placeholder "leader"
    Given client "writer" is connected to the leader node
    When client "writer" begins executing these NSPL commands in the background
      """
      CREATE RELAY burst_1 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_2 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_3 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_4 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_5 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_6 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_7 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_8 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_9 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_10 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_11 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_12 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_13 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_14 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_15 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_16 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_17 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_18 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_19 SCHEMA burst_event UNBRANCHED;
      CREATE RELAY burst_20 SCHEMA burst_event UNBRANCHED;
      """
    And the CLI backs up "cluster" from node "{{leader}}" into "concurrent.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And backup archive "concurrent.nvxb" holds for domain "{{domain}}" schema "burst_event" and relays "burst_" numbered contiguously from 1
    And the background NSPL execution succeeds

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: An archive with a foreign record kind or format version is refused at its header
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "cluster" from node "{{leader}}" into "valid.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When backup archive "valid.nvxb" is copied to "foreign_kind.nvxb" with its manifest's record kind set to 9
    And the CLI describes backup archive "foreign_kind.nvxb" as text
    Then the CLI fails with "holds a record of kind 9 where a manifest record belongs"
    When backup archive "valid.nvxb" is copied to "foreign_version.nvxb" with its manifest's record version set to 2
    And the CLI describes backup archive "foreign_version.nvxb" as text
    Then the CLI fails with "holds a manifest record of format version 2; this reader supports version 1"

  Scenario: The CLI exits with a failure and a JSON error when a backup is refused
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain missing_{{test_id}}" from node "{{leader}}" into "missing.nvxb" reporting JSON
    Then the CLI backup failed with JSON error code "BACKUP_REFUSED"
    And backup archive "missing.nvxb" does not exist
    When the CLI backs up "cluster {{domain}}" from node "{{leader}}" into "named.nvxb" reporting JSON
    Then the CLI backup failed with JSON error code "INVALID_ARGUMENTS"
    When the CLI describes backup archive "absent.nvxb" as json
    Then the CLI backup failed with JSON error code "INVALID_ARCHIVE"

  Scenario: BACKUP runs alone, outside a transaction, and DESCRIBE BACKUP never reaches a server
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given client "operator" is connected to the leader node
    When client "operator" executes these NSPL commands
      """
      BEGIN;
      """
    And client "operator" fails to execute these NSPL commands
      """
      BACKUP CLUSTER TO 'in-transaction.nvxb';
      """
    Then the last command error contains
      """
      BACKUP cannot be queued in a transaction
      """
    When the session on node "{{leader}}" runs "DESCRIBE BACKUP 'cluster.nvxb';"
    Then the session's command failed with "DESCRIBE BACKUP reads an archive on the client's machine"

  Scenario Outline: Downloads of another user's backup, or under a reference without an archive, are refused
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE USER backup_reader WITH PASSWORD 'reader-password';
      """
    Then the current leader node is saved as placeholder "leader"
    When the session backs up the cluster on node "{{leader}}" without downloading its archive
    And the backup's archive is downloaded from node "{{leader}}" as user "backup_reader" with password "reader-password"
    Then the download is refused as "NotOwner"
    When an archive is downloaded from node "{{leader}}" under an execution reference no command used
    Then the download is refused as "NotRetained"
    When the session on node "{{leader}}" runs "CREATE SCHEMA plain_event ( id I64 );"
    And an archive is downloaded from node "{{leader}}" under the session command's execution reference
    Then the download is refused as "NotRetained"
    When the backup's archive is downloaded from node "{{leader}}"
    Then the downloaded archive matches the backup's summary

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: A download without credentials is refused
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these NSPL commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the session backs up the cluster on node "{{leader}}" without downloading its archive
    And the backup's archive is downloaded from node "{{leader}}" without credentials
    Then the download is refused as unauthenticated
