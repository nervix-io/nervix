@backup_cut_diagnostics @deloxide_stress_restore
Feature: Attributing a quiesced backup cut's timeout

  Scenario Outline: A capture timeout reports its active phase and completed drain
    Given a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      START;
      """
    Then the current leader node is saved as placeholder "leader"
    Given the backup cut for domain "{{domain}}" will pause before its capture
    When the CLI begins backing up "domain {{domain}} --timeout 20s --backup-wait-timeout 1m" from node "{{leader}}" into "capture-timeout.nvxb" in the background
    Then the backup cut for domain "{{domain}}" has reached its pause
    And the background CLI backup finishes
    And the CLI backup failed with JSON error code "BACKUP_REFUSED"
    And the CLI capture timeout reports the elapsed completed drain
    And backup archive "capture-timeout.nvxb" does not exist
    When the backup cut pause for domain "{{domain}}" is released
    Then node "{{leader}}" eventually reports status containing "{{domain}} status=Running pace=UNPACED"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  # A cut's TIMEOUT also ends the application of its pause. Every node's wait for the cluster to
  # ready a revision that pauses the domain is held for 45 seconds, the cut's own wait included, as
  # a cluster slow to apply the pause would hold it. The cut stops waiting at its 20-second deadline,
  # resumes the domain and is refused in its drain phase, which includes the pause's application.
  # The revision that resumes the domain is not held, so the refusal, which follows the resume,
  # reports less than 15 seconds beyond the budget; a cut that waited for the held application would
  # report the whole 45-second hold. The domain then runs and delivers a new record again.
  Scenario Outline: A pause the cluster applies slowly ends at its cut's deadline
    Given HTTP receiver "api" is running
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA event (event_id STRING);
      CREATE CODEC event_codec FROM JSON TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE VHOST edge backup-slow-application-{{test_id}}.example.com;
      CREATE ENDPOINT ingress ON edge PATH '/events' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR source FROM ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING event_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT api_client CONFIG {
        'endpoint' = '{{http_receiver.api}}', 'timeout_ms' = 60000
      };
      CREATE ATTACHED HTTP REQUEST EMITTER published FROM outgoing
        TO api_client METHOD 'POST' PATH concat('/events/', input.event_id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 100ms WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    And http payload is posted to host "backup-slow-application-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"1"}]
      """
    Then HTTP receiver "api" eventually receives at least 1 requests
    Then the current leader node is saved as placeholder "leader"
    Given every application of the backup pause for domain "{{domain}}" will be held for 45 seconds before the cluster reports it ready
    When the CLI backs up "domain {{domain}} --timeout 20s" from node "{{leader}}" into "slow-application.nvxb" reporting JSON
    Then the CLI backup failed with JSON error code "BACKUP_REFUSED"
    And the CLI drain timeout reports at least its 20 second budget elapsed and less than 15 seconds beyond it
    And the CLI backup failure mentions "timed out applying the backup pause of domain"
    And backup archive "slow-application.nvxb" does not exist
    And node "{{leader}}" eventually reports status containing "{{domain}} status=Running pace=UNPACED"
    When http payload is posted to host "backup-slow-application-{{test_id}}.example.com" path "/events"
      """
      [{"event_id":"2"}]
      """
    Then HTTP receiver "api" eventually receives at least 2 requests

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
