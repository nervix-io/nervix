Feature: Ephemeral activation expiry and the licensing stop gate
  At its fixed expiry an ephemeral cluster stops processing, keeps its data and its administrative
  access, and never falls back to the free policy. Only a new activation lifts the gate, and the
  domains licensing stopped stay stopped until an operator starts them. Licensing never limits base
  features, so the closed gate refuses only starts: it admits new domains, restores and members.

  Scenario Outline: Expiry stops every running domain with the licensing cause on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    And node "node-1" has resource directory "expiry_bundle" containing
      """
      {
        "payload.txt": "kept-through-expiry\n"
      }
      """
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      START;
      """
    Then the committed license decision is saved as placeholder "ephemeral"
    And the ephemeral expiry of license placeholder "ephemeral" is saved as placeholder "expired_at"
    And node "<observer>" eventually reports status containing "{{domain}} status=Running"
    When a licensing notice session is opened on the leader node
    And the cluster licensing clock passes the "ephemeral" deadline
    Then node "<observer>" eventually describes this license without a selected domain
      """
      mode: activated
      policy: stopped
      processing: stopped by licensing: ephemeral activation expired at {{expired_at}}
      """
    And node "<observer>" eventually reports status containing "{{domain}} status=Stopped"
    And a licensing Error notice saying "Processing stopped" is observed
    When these Plexus commands are executed on node "<observer>"
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      status: stopped
      stopped by licensing: ephemeral activation expired at {{expired_at}}
      """
    When these Plexus commands fail with "licensing stopped processing"
      """
      START;
      """
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}}_after_expiry;
      """
    Then node "<observer>" eventually reports status containing "{{domain}}_after_expiry status=Stopped"
    Given client "operator" is connected to the leader node
    When client "operator" selects domain "{{domain}}"
    And client "operator" executes these Plexus commands
      """
      CREATE RESOURCE expiry_bundle;
      UPLOAD RESOURCE expiry_bundle VERSION '{{expiry_bundle}}';
      """
    Then the last command output contains
      """
      uploaded resource version 1
      """
    And the current leader node is saved as placeholder "licensing_leader"
    When the CLI backs up "domain {{domain}}" from node "{{licensing_leader}}" into "expired.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    When the cluster is restarted
    Then node "<observer>" eventually describes this license without a selected domain
      """
      policy: stopped
      processing: stopped by licensing: ephemeral activation expired at {{expired_at}}
      """
    And the independent activation binding matches placeholder "ephemeral"
    And node "<observer>" eventually reports status containing "{{domain}} status=Stopped"
    When these Plexus commands are executed on node "<observer>"
      """
      DESCRIBE RESOURCE expiry_bundle;
      """
    Then the last command output contains
      """
      expiry_bundle
      """

    Examples:
      | cluster_size | observer |
      | 1            | node-1   |
      | 3            | node-3   |

  Scenario Outline: A new activation lifts the gate and the stopped domains wait for START on <cluster_size> nodes
    Given an HTTPS licensing test service allows one ephemeral activation
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      START;
      """
    Then the committed license decision is saved as placeholder "first_activation"
    And the ephemeral expiry of license placeholder "first_activation" is saved as placeholder "expired_at"
    And node "<observer>" eventually reports status containing "{{domain}} status=Running"
    When a licensing notice session is opened on the leader node
    And both licensing clocks pass the "ephemeral" deadline
    Then node "<observer>" eventually describes this license without a selected domain
      """
      policy: stopped
      """
    And node "<observer>" eventually reports status containing "{{domain}} status=Stopped"
    When these Plexus commands are executed on the leader node
      """
      REFRESH LICENSE;
      """
    Then node "<observer>" eventually describes this license without a selected domain
      """
      status: expired
      policy: stopped
      processing: stopped by licensing: ephemeral activation expired at {{expired_at}}
      """
    And the independent activation binding matches placeholder "first_activation"
    And the licensing service marks placeholder "first_activation" expired and frees its slot
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}}_while_stopped;
      """
    Then node "<observer>" eventually reports status containing "{{domain}}_while_stopped status=Stopped"
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then node "<observer>" eventually describes this license without a selected domain
      """
      policy: licensed fresh
      processing: running
      """
    And a licensing Info notice saying "Processing may resume" is observed
    And node "<observer>" eventually reports status containing "{{domain}} status=Stopped"
    When these Plexus commands are executed on node "<observer>"
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      stopped by licensing: ephemeral activation expired at {{expired_at}}
      """
    When these Plexus commands are executed on the leader node
      """
      START;
      """
    Then node "<observer>" eventually reports status containing "{{domain}} status=Running"

    Examples:
      | cluster_size | observer |
      | 1            | node-1   |
      | 3            | node-3   |

  Scenario: A leader transfer keeps the committed expiry, the closed gate and the stopped domains
    Given an HTTPS licensing test service is available
    And a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      START;
      """
    Then the committed license decision is saved as placeholder "ephemeral"
    And the ephemeral expiry of license placeholder "ephemeral" is saved as placeholder "expired_at"
    When the cluster licensing clock passes the "ephemeral" deadline
    Then node "node-1" eventually describes this license without a selected domain
      """
      policy: stopped
      """
    And node "node-1" eventually reports status containing "{{domain}} status=Stopped"
    When leadership is transferred to node "node-2"
    And leadership is transferred to node "node-3"
    Then node "node-1" eventually describes this license without a selected domain
      """
      policy: stopped
      processing: stopped by licensing: ephemeral activation expired at {{expired_at}}
      """
    And the independent activation binding matches placeholder "ephemeral"
    And node "node-2" eventually reports status containing "{{domain}} status=Stopped"
    When these Plexus commands fail with "licensing stopped processing"
      """
      START;
      """

  Scenario Outline: A backup whose cut the expiry interrupts completes and leaves its domain stopped by licensing on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      START;
      """
    Then the committed license decision is saved as placeholder "ephemeral"
    And the ephemeral expiry of license placeholder "ephemeral" is saved as placeholder "expired_at"
    And node "<observer>" eventually reports status containing "{{domain}} status=Running"
    Given the backup cut for domain "{{domain}}" will pause before its capture
    Then the current leader node is saved as placeholder "leader"
    When the CLI begins backing up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "interrupted.nvxb" in the background
    Then the backup cut for domain "{{domain}}" has reached its pause
    When the cluster licensing clock passes the "ephemeral" deadline
    Then node "<observer>" eventually describes this license without a selected domain
      """
      policy: stopped
      """
    When the backup cut pause for domain "{{domain}}" is released
    Then the background CLI backup finishes
    And the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And node "<observer>" eventually reports status containing "{{domain}} status=Stopped"
    When these Plexus commands are executed on node "<observer>"
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      status: stopped
      stopped by licensing: ephemeral activation expired at {{expired_at}}
      """
    When these Plexus commands fail with "licensing stopped processing"
      """
      START;
      """

    Examples:
      | cluster_size | observer |
      | 1            | node-1   |
      | 3            | node-3   |

  Scenario Outline: A RESUME restore the expiry interrupts completes with its domain stopped by licensing, a later RESUME is refused and a plain restore is admitted, on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the committed license decision is saved as placeholder "ephemeral"
    And the ephemeral expiry of license placeholder "ephemeral" is saved as placeholder "expired_at"
    And the current leader node is saved as placeholder "leader"
    When the CLI backs up "domain {{domain}}" from node "{{leader}}" into "resumable.nvxb" reporting JSON
    Then the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    Given restore step "apply models" of domain "{{domain}}_copy" on node "{{leader}}" pauses before it applies
    When restore "DOMAIN {{domain}} AS {{domain}}_copy RESUME" of backup archive "resumable.nvxb" is streamed to node "{{leader}}" under execution reference "interrupted-resume" in the background
    Then the restore pauses at step "apply models" of domain "{{domain}}_copy" on node "{{leader}}"
    When the cluster licensing clock passes the "ephemeral" deadline
    Then node "<observer>" eventually describes this license without a selected domain
      """
      policy: stopped
      """
    When the restore step pause on node "{{leader}}" is released
    Then the background restore stream's outcome is "completed" as "executed"
    And the restore stream's report shows every step applied
    And the restore stream's outcome message contains "restored domains are stopped by licensing: ephemeral activation expired at"
    When the CLI restores "domain {{domain}} --as {{domain}}_stopped_copy" from "resumable.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore succeeded, restoring domain "{{domain}}_stopped_copy" with 0 resource versions and 0 models
    And node "<observer>" eventually reports status containing "{{domain}}_stopped_copy status=Stopped"
    Given the active domain is "{{domain}}_copy"
    When these Plexus commands are executed on node "<observer>"
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      status: stopped
      stopped by licensing: ephemeral activation expired at {{expired_at}}
      """
    When the CLI restores "domain {{domain}} --as {{domain}}_again --resume" from "resumable.nvxb" on node "{{leader}}" reporting JSON
    Then the CLI restore failed with JSON error code "RESTORE_REFUSED" and a message containing "RESUME cannot run the restored domains: licensing stopped processing: ephemeral activation expired at {{expired_at}}"

    Examples:
      | cluster_size | observer |
      | 1            | node-1   |
      | 3            | node-3   |

  Scenario Outline: The licensing stop of a healthy domain ends gracefully well inside its fixed time on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      START;
      """
    Then the committed license decision is saved as placeholder "ephemeral"
    And the ephemeral expiry of license placeholder "ephemeral" is saved as placeholder "expired_at"
    And node "<observer>" eventually reports status containing "{{domain}} status=Running"
    When the cluster licensing clock passes the "ephemeral" deadline
    # Nothing is in motion, so the stop completes long before the licensing stop's fixed minute.
    Then within "30s" node "<observer>" eventually reports status containing "{{domain}} status=Stopped"
    When these Plexus commands are executed on node "<observer>"
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      status: stopped
      stopped by licensing: ephemeral activation expired at {{expired_at}}
      stop: every message in motion finished
      """

    Examples:
      | cluster_size | observer |
      | 1            | node-1   |
      | 3            | node-3   |

  # A STOP is not a pause: the licensing stop that ends a backup's pause ends the session of an
  # ingestor whose BUFFER mode retained payloads in that pause, and lists them in the stop's report.
  Scenario Outline: A licensing stop that ends a backup's pause discards what a paused BUFFER ingestor retained and lists it on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      CREATE SCHEMA event ( seq I64 );
      CREATE WIRE JSON SCHEMA event_wire MODE STRICT ( seq integer );
      CREATE WIRE JSON CODEC event_codec FROM WIRE SCHEMA event_wire TO SCHEMA event;
      CREATE RELAY events SCHEMA event UNBRANCHED;
      CREATE VHOST edge http-{{test_id}}-pause-buffer.example.com;
      CREATE HTTP ENDPOINT events_endpoint ON edge PATH '/events';
      CREATE HTTP ENDPOINT INGESTOR buffered
        FROM events_endpoint MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB RESPOND IMMEDIATE DECODE USING event_codec
        TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the committed license decision is saved as placeholder "ephemeral"
    And the ephemeral expiry of license placeholder "ephemeral" is saved as placeholder "expired_at"
    And node "<observer>" eventually reports status containing "{{domain}} status=Running"
    Given the backup cut for domain "{{domain}}" will pause before its capture
    Then the current leader node is saved as placeholder "leader"
    When the CLI begins backing up "domain {{domain}} --timeout 120s" from node "{{leader}}" into "paused.nvxb" in the background
    Then the backup cut for domain "{{domain}}" has reached its pause
    # The backup holds the domain paused, so the ingestor's BUFFER mode retains what arrives.
    When http payload is posted to node "node-1" with host "http-{{test_id}}-pause-buffer.example.com" path "/events"
      """
      {"seq":1}
      """
    And http payload is posted to node "node-1" with host "http-{{test_id}}-pause-buffer.example.com" path "/events"
      """
      {"seq":2}
      """
    And the cluster licensing clock passes the "ephemeral" deadline
    Then node "<observer>" eventually describes this license without a selected domain
      """
      policy: stopped
      """
    # The backup's pause ends in the licensing stop rather than a resume.
    When the backup cut pause for domain "{{domain}}" is released
    Then the background CLI backup finishes
    And the CLI backup succeeded with a JSON report naming domain "{{domain}}"
    And within "30s" node "<observer>" eventually reports status containing "{{domain}} status=Stopped"
    # The stop's report follows its status: it is committed once every node's teardown has ended.
    And within "30s" DESCRIBE DOMAIN on node "<observer>" contains
      """
      stopped by licensing: ephemeral activation expired at {{expired_at}}
      stop: every message in motion finished; discarded with the ingestor session: node-1: ingestor 'buffered': payloads an ingestor's pause buffer retained (2)
      """

    Examples:
      | cluster_size | observer |
      | 1            | node-1   |
      | 3            | node-3   |

  @long_running
  Scenario Outline: The licensing stop is forced at its fixed time while a publish is held on the wire, and no CANCEL calls it off, on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And Kafka is running
    And HTTP receiver "stuck" is running
    And HTTP receiver "stuck" answers unscripted requests with "hold response until released"
    And a <cluster_size> node nervix cluster is started
    And Kafka topic "licensed_stop_{{test_id}}" exists with 1 partitions
    And the active domain is "{{domain}}"
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      CREATE SCHEMA event ( id STRING );
      CREATE JAQ JSON CODEC ingest_codec TO SCHEMA event
        WITH JAQ TRANSFORMATIONS ON INGESTION '.[]';
      CREATE RELAY outgoing SCHEMA event UNBRANCHED;
      CREATE KAFKA CLIENT kafka_main CONFIG { 'bootstrap.servers' = '{{kafka_addr}}' };
      CREATE KAFKA TOPIC INGESTOR ingestor
        FROM kafka_main TOPIC 'licensed_stop_{{test_id}}'
          OFFSET BY CONSUMER GROUP 'licensed_stop_group_{{test_id}}' START AT EARLIEST
          MODE ACK SEQUENTIAL ACK TIMEOUT 5m RETRY POLICY BACKOFF 100ms MAX 1s
        ON QUIESCE SUSPEND DECODE USING ingest_codec
        TO outgoing INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      CREATE HTTP CLIENT stuck_api CONFIG {
        'endpoint' = '{{http_receiver.stuck}}', 'timeout_ms' = 300000
      };
      CREATE HTTP REQUEST EMITTER published FROM outgoing
        ATTACHED
        TO stuck_api METHOD 'POST' PATH concat('/events/', input.id)
          MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s WITHOUT BODY
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
      START;
      """
    Then the committed license decision is saved as placeholder "ephemeral"
    And the ephemeral expiry of license placeholder "ephemeral" is saved as placeholder "expired_at"
    And Kafka consumer group "licensed_stop_group_{{test_id}}" eventually has 1 consumers
    When Kafka message is published to topic "licensed_stop_{{test_id}}"
      """
      [{"id":"1"}]
      """
    Then HTTP receiver "stuck" eventually receives at least 1 request
    When the cluster licensing clock passes the "ephemeral" deadline
    Then node "<observer>" eventually reports status containing "{{domain}} status=Stopping"
    # An operator's request joins the licensing stop; its CANCEL withdraws only that request.
    Given client "operator" is connected to the leader node
    When client "operator" selects domain "{{domain}}"
    And client "operator" fails to execute these Plexus commands
      """
      STOP CANCEL AFTER 1s;
      """
    Then the last command error contains
      """
      cancelled
      """
    When these Plexus commands are executed on node "<observer>"
      """
      DESCRIBE DOMAIN;
      """
    Then the last command output contains
      """
      status: stopping
      """
    # The publish never finishes, so the licensing stop is forced at its fixed time.
    Then within "120s" node "<observer>" eventually reports status containing "{{domain}} status=Stopped"
    # The cut report follows the stopped status once the nodes finish tearing down the domain.
    And within "30s" DESCRIBE DOMAIN on node "<observer>" contains
      """
      emitter 'published': work in motion cut by enforcement
      """
    And the last command output contains
      """
      stopped by licensing: ephemeral activation expired at {{expired_at}}
      """
    And the last command output contains
      """
      the licensing stop enforced the stop
      """
    And HTTP receiver "stuck" eventually sees the client abandon at least 1 unfinished response

    Examples:
      | cluster_size | observer |
      | 1            | node-1   |
      | 3            | node-3   |

  Scenario Outline: A closed stop gate admits a joining node and keeps the members through a restart until a new activation lifts it on <cluster_size> nodes
    Given an HTTPS licensing test service allows one ephemeral activation
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the committed license decision is saved as placeholder "first_activation"
    And the ephemeral expiry of license placeholder "first_activation" is saved as placeholder "expired_at"
    When both licensing clocks pass the "ephemeral" deadline
    Then node "<observer>" eventually describes this license without a selected domain
      """
      policy: stopped
      processing: stopped by licensing: ephemeral activation expired at {{expired_at}}
      """
    When the cluster is restarted
    Then node "<observer>" eventually describes this license without a selected domain
      """
      policy: stopped
      processing: stopped by licensing: ephemeral activation expired at {{expired_at}}
      """
    And node "<observer>" eventually reports raft voters "<voters>"
    And the current leader node is saved as placeholder "licensing_leader"
    When node "<joining_node>" attempts to join
    Then node "<joining_node>" joined the cluster
    And node "<observer>" eventually reports raft voters "<voters_after>"
    When these Plexus commands are executed through the client on node "{{licensing_leader}}"
      """
      REFRESH LICENSE;
      """
    Then node "<observer>" eventually describes this license without a selected domain
      """
      status: expired
      policy: stopped
      """
    And the licensing service marks placeholder "first_activation" expired and frees its slot
    When these Plexus commands are executed through the client on node "{{licensing_leader}}"
      """
      ACTIVATE LICENSE EPHEMERAL WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then node "<observer>" eventually describes this license without a selected domain
      """
      policy: licensed fresh
      processing: running
      """
    And node "<observer>" eventually reports raft voters "<voters_after>"

    Examples:
      | cluster_size | observer | joining_node | voters               | voters_after                |
      | 1            | node-1   | node-2       | node-1               | node-1,node-2               |
      | 3            | node-3   | node-4       | node-1,node-2,node-3 | node-1,node-2,node-3,node-4 |
