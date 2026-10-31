Feature: Session protocol

  Scenario Outline: Semantic completion pages reach every domain once
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE UNPACED DOMAIN completion_alpha;
      CREATE UNPACED DOMAIN completion_bravo;
      CREATE UNPACED DOMAIN completion_charlie;
      CREATE UNPACED DOMAIN completion_delta;
      """
    When the active session collects completion pages for "USE " at byte 4 with page size 2
    Then the collected completion pages contain "completion_alpha" exactly once
    And the collected completion pages contain "completion_bravo" exactly once
    And the collected completion pages contain "completion_charlie" exactly once
    And the collected completion pages contain "completion_delta" exactly once
    And the completion search used at least 3 pages without duplicate candidates

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A long command leaves the session responsive and a waiter cancelled before admission admits nothing
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given command admission on node "{{leader}}" pauses before proposal
    When the active session sends request "slow" with this Plexus command
      """
      CREATE SCHEMA cancelled_record (
        value STRING
      );
      """
    Then the command admission pause on node "{{leader}}" is reached
    When the active session sends request "domains" listing domains
    Then request "domains" lists domain "{{domain}}"
    When the active session sends request "completion" completing "CREATE SCH" at byte 10
    Then request "completion" suggests "SCHEMA"
    When the active session sends request "inspection" inspecting its attached transaction
    Then request "inspection" is refused because no transaction is attached
    When the active session cancels request "slow"
    Then request "slow" is cancelled before admission
    When the command admission pause on node "{{leader}}" is released
    And this Plexus command request is executed on the leader node
      """
      SHOW CREATE SCHEMA cancelled_record;
      """
    Then the last command error contains
      """
      schema 'cancelled_record' does not exist in domain '{{domain}}'
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Cancelling a durably admitted command ends only the wait for it
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given command execution on node "{{leader}}" pauses after durable admission
    When the active session sends request "admitted" with execution reference "cancel-admitted-{{test_id}}" and this Plexus command
      """
      CREATE SCHEMA admitted_record (
        value STRING
      );
      """
    Then the durable command admission pause on node "{{leader}}" is reached
    When the active session cancels request "admitted"
    Then request "admitted" is cancelled after admission
    When the durable command admission pause on node "{{leader}}" is released
    And this Plexus command request with execution reference "cancel-admitted-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA admitted_record (
        value STRING
      );
      """
    Then the last command request succeeded
    When this Plexus command request is executed on the leader node
      """
      SHOW CREATE SCHEMA admitted_record;
      """
    Then the last command output contains
      """
      CREATE SCHEMA admitted_record (
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A malformed request is refused with a typed rejection and the session keeps serving
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the active session sends request "bad-reference" with execution reference text "not a reference"
    Then request "bad-reference" is rejected as an invalid request naming "CommandRequest.execution_reference"
    When the active session sends request "split-character" completing "SHOW é" at byte 6 inside a character
    Then request "split-character" is rejected as an invalid request naming "SuggestRequest.cursor"
    When the active session sends request "zero-page" completing with invalid page size 0
    Then request "zero-page" is rejected as an invalid request naming "SuggestRequest.page_size"
    When the active session sends request "oversized-page" completing with invalid page size 101
    Then request "oversized-page" is rejected as an invalid request naming "SuggestRequest.page_size"
    When the active session sends request "after-refusals" completing "SHOW é" at byte 5
    Then request "after-refusals" suggests "CLUSTER"
    When these Plexus commands are executed on the active session
      """
      CREATE SCHEMA served_after_refusals (
        value STRING
      );
      SHOW CREATE SCHEMA served_after_refusals;
      """
    Then the last command output contains
      """
      CREATE SCHEMA served_after_refusals (
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Authentication, framing and protocol failures end a native session with their status
    Given a 1 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then a session opened on the leader node with a wrong password is refused with status "Unauthenticated"
    When the active session sends a request whose identity is zero
    Then the active session is ended for violating the protocol
    Given the leader node is configured with these Plexus commands
      """
      SHOW CLUSTER STATUS;
      """
    When the active session sends 12 bytes that are not a frame
    Then the active session ends with status "Internal"
    Given the leader node is configured with these Plexus commands
      """
      SHOW CLUSTER STATUS;
      """
    When the active session sends a message larger than the frame limit
    Then the active session ends with status "OutOfRange"

  Scenario: Leadership lost after durable admission leaves an unknown outcome that a retry recovers
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "admitted_leader"
    And a node other than placeholder "admitted_leader" is saved as placeholder "successor"
    Given command execution on node "{{admitted_leader}}" pauses after durable admission
    When this Plexus command request with execution reference "leadership-lost-{{test_id}}" begins executing in the background on the leader node
      """
      CREATE SCHEMA uncertain_record (
        value STRING
      );
      """
    Then the durable command admission pause on node "{{admitted_leader}}" is reached
    When leadership is transferred from node "{{admitted_leader}}" to node "{{successor}}"
    Then node "{{successor}}" eventually reports leader "{{successor}}"
    And node "{{admitted_leader}}" eventually reports leader "{{successor}}"
    When the durable command admission pause on node "{{admitted_leader}}" is released
    Then the background command request reports an unknown outcome because leadership was lost
    When this Plexus command request with execution reference "leadership-lost-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA uncertain_record (
        value STRING
      );
      """
    Then the last command request succeeded
    When this Plexus command request is executed on the leader node
      """
      SHOW CREATE SCHEMA uncertain_record;
      """
    Then the last command output contains
      """
      CREATE SCHEMA uncertain_record (
      """

  Scenario Outline: A leader that cannot confirm its lease during <phase> reports that, not a move
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "lapsed_leader"
    Given the leader refuses the <phase> of execution reference "lapsed-lease-{{test_id}}" as a leader whose lease lapsed
    When this Plexus command request with execution reference "lapsed-lease-{{test_id}}" begins executing in the background on the leader node
      """
      CREATE SCHEMA unconfirmed_record (
        value STRING
      );
      """
    Then the background command request reports an unknown outcome because the leader could not confirm its lease
    And the leader refused the <phase> of execution reference "lapsed-lease-{{test_id}}" as a leader whose lease lapsed
    And node "{{lapsed_leader}}" eventually reports leader "{{lapsed_leader}}"
    When this Plexus command request with execution reference "lapsed-lease-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA unconfirmed_record (
        value STRING
      );
      """
    Then the last command request succeeded
    When this Plexus command request is executed on the leader node
      """
      SHOW CREATE SCHEMA unconfirmed_record;
      """
    Then the last command output contains
      """
      CREATE SCHEMA unconfirmed_record (
      """

    Examples:
      | phase        |
      | admission    |
      | finalization |

  Scenario: A leader that cannot confirm its lease while a command applies reports that, not a move
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE SCHEMA pause_event (key I64);
      CREATE RELAY pause_input SCHEMA pause_event UNBRANCHED;
      START;
      """
    Then the current leader node is saved as placeholder "lapsed_leader"
    Given the leader refuses the next model publication in domain "{{domain}}" and the pause release after it with "a lapsed leader lease"
    When this Plexus command request with execution reference "lapsed-applying-{{test_id}}" begins executing in the background on the leader node
      """
      CREATE RELAY lapsed_publication SCHEMA pause_event UNBRANCHED;
      """
    Then the background command request reports an unknown outcome because the leader could not confirm its lease
    And the refusal of the model publication in domain "{{domain}}" and of its pause release was delivered
    And node "{{lapsed_leader}}" eventually reports leader "{{lapsed_leader}}"
    When this Plexus command request with execution reference "lapsed-applying-{{test_id}}" is executed on the leader node
      """
      CREATE RELAY lapsed_publication SCHEMA pause_event UNBRANCHED;
      """
    Then the last command request succeeded

  Scenario: A leader setup step learns the outcome of a command its leader admitted before losing leadership
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "admitted_leader"
    And a node other than placeholder "admitted_leader" is saved as placeholder "successor"
    Given leadership moves from node "{{admitted_leader}}" to node "{{successor}}" under its next durably admitted command
    When these Plexus commands are executed on the leader node
      """
      CREATE SCHEMA recovered_record ( value STRING );
      CREATE RELAY recovered SCHEMA recovered_record UNBRANCHED;
      CREATE SUBSCRIPTION recovered_subscription TO recovered;
      """
    Then the leadership move under the durably admitted command completed
    When this Plexus command request is executed on the leader node
      """
      SHOW CREATE SCHEMA recovered_record;
      """
    Then the last command output contains
      """
      CREATE SCHEMA recovered_record (
      """

  Scenario: A leader configuration step learns the outcome of a command its leader admitted before losing leadership
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "admitted_leader"
    And a node other than placeholder "admitted_leader" is saved as placeholder "successor"
    Given leadership moves from node "{{admitted_leader}}" to node "{{successor}}" under its next durably admitted command
    And the leader node is configured with these Plexus statements
      """
      CREATE SCHEMA configured_record ( value STRING );
      CREATE RELAY configured SCHEMA configured_record UNBRANCHED;
      CREATE SUBSCRIPTION configured_subscription TO configured;
      """
    When this Plexus command request is executed on the leader node
      """
      SHOW CREATE SCHEMA configured_record;
      """
    Then the leadership move under the durably admitted command completed
    And the last command output contains
      """
      CREATE SCHEMA configured_record (
      """

  Scenario: A session that keeps its subscription learns the outcome of a command its leader admitted before losing leadership
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    And the leader node is configured with these Plexus statements
      """
      CREATE SCHEMA held_event ( seq I64 );
      CREATE WIRE JSON SCHEMA held_event_wire MODE STRICT ( seq integer );
      CREATE CODEC held_event_codec
        FROM WIRE JSON SCHEMA held_event_wire
        TO SCHEMA held_event;
      CREATE RELAY held_events SCHEMA held_event UNBRANCHED;
      CREATE VHOST held_edge http-{{test_id}}-held.example.com;
      CREATE ENDPOINT held_ingress ON held_edge PATH '/held' TYPE HTTP;
      CREATE HTTP ENDPOINT INGESTOR held_source
        FROM held_ingress MODE NO ACK
        ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING held_event_codec
        TO held_events
        INHERIT ALL
        UNBRANCHED
        FLUSH IMMEDIATE
        ON MESSAGE ERROR LOG
        ON GENERAL ERROR LOG;
      CREATE SUBSCRIPTION held_subscription TO held_events;
      """
    Then the current leader node is saved as placeholder "admitted_leader"
    And a node other than placeholder "admitted_leader" is saved as placeholder "successor"
    Given leadership moves from node "{{admitted_leader}}" to node "{{successor}}" under its next durably admitted command
    When these Plexus commands are executed
      """
      START;
      """
    Then the leadership move under the durably admitted command completed
    And node "{{successor}}" eventually accepts http traffic for host "http-{{test_id}}-held.example.com" path "/held"
      """
      {"seq":1}
      """
    And within "30s" the relay subscription receives a payload
      """
      "seq":1
      """

  Scenario: A command request under its own execution reference learns the outcome its leader admitted before losing leadership
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "admitted_leader"
    And a node other than placeholder "admitted_leader" is saved as placeholder "successor"
    Given leadership moves from node "{{admitted_leader}}" to node "{{successor}}" under its next durably admitted command
    When this Plexus command request with execution reference "moved-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA referenced_record ( value STRING );
      """
    Then the last command request succeeded
    And the leadership move under the durably admitted command completed
    When this Plexus command request is executed on the leader node
      """
      SHOW CREATE SCHEMA referenced_record;
      """
    Then the last command output contains
      """
      CREATE SCHEMA referenced_record (
      """

  Scenario: A background execution learns the outcome of a command its leader admitted before losing leadership
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus statements
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "admitted_leader"
    And a node other than placeholder "admitted_leader" is saved as placeholder "successor"
    Given leadership moves from node "{{admitted_leader}}" to node "{{successor}}" under its next durably admitted command
    When these Plexus commands begin executing in the background
      """
      CREATE SCHEMA background_record ( value STRING );
      """
    Then the background Plexus execution succeeds
    And the leadership move under the durably admitted command completed
    When this Plexus command request is executed on the leader node
      """
      SHOW CREATE SCHEMA background_record;
      """
    Then the last command output contains
      """
      CREATE SCHEMA background_record (
      """

  Scenario: A redirect names no endpoint for a leader that discovery cannot reach
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    And a node other than placeholder "leader" is saved as placeholder "survivor"
    And a node other than placeholders "leader" and "survivor" is saved as placeholder "third"
    When node "{{leader}}" is stopped
    And node "{{third}}" is stopped
    Then within "60s" node "{{survivor}}" redirects commands to leader "{{leader}}" without an endpoint

  Scenario: The console WebSocket closes a connection that sends something other than a session frame
    Given a 1 node nervix cluster is started
    Then a console WebSocket on the leader node that sends "text" is closed with code 1003
    And a console WebSocket on the leader node that sends "garbage" is closed with code 1007

  Scenario Outline: An upload stream the protocol does not allow is refused with a typed failure and admits nothing
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE RESOURCE blob;
      """
    When an upload of resource "blob" with identity "empty-stream" that is empty is sent to the leader node
    Then the last upload is refused as "InvalidStream" before it names an identity
    When an upload of resource "blob" with identity "chunk-first" that begins with a chunk is sent to the leader node
    Then the last upload is refused as "InvalidStream" before it names an identity
    When an upload of resource "blob" with identity "two-starts" that carries a second start is sent to the leader node
    Then the last upload is refused as "InvalidStream" for identity "two-starts"
    When an upload of resource "blob" with identity "oversized-body" that carries more bytes than it declares is sent to the leader node
    Then the last upload is refused as "SizeMismatch" for identity "oversized-body"
    And the last command error contains
      """
      the upload exceeds its declared size of 1 bytes
      """
    When an upload of resource "blob" with identity "oversized-declaration" that declares more bytes than an archive may hold is sent to the leader node
    Then the last upload is refused as "QuotaExceeded" for identity "oversized-declaration"
    When these Plexus commands are executed on the leader node
      """
      DESCRIBE RESOURCE blob;
      """
    Then the last command output contains
      """
      latest: (none)
      versions: (none)
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A domain clock attachment reply precedes its frames, a detach reply follows them, and a transaction refuses both
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      START;
      """
    And a clock session is opened on the leader node
    When the clock session sends request "attach" attaching the clock of domain "{{domain}}" and then request "stop" with this Plexus command
      """
      STOP;
      """
    Then request "attach" of the clock session attached the clock of domain "{{domain}}" as unpaced at generation 1
    And request "stop" of the clock session completed
    And within "10s" the clock session observes domain "{{domain}}" stopped at generation 1
    And the clock session received the reply to request "attach" before every frame about domain "{{domain}}"
    When the clock session executes this Plexus command
      """
      BEGIN;
      """
    And the clock session sends request "attach inside" attaching the clock of domain "{{domain}}"
    Then request "attach inside" of the clock session is refused because the session holds a transaction
    When the clock session sends request "detach inside" detaching the clock of domain "{{domain}}"
    Then request "detach inside" of the clock session is refused because the session holds a transaction
    When the clock session executes this Plexus command
      """
      REVERT;
      """
    And the clock session sends request "detach" detaching the clock of domain "{{domain}}" and then request "start" with this Plexus command
      """
      START;
      """
    Then request "detach" of the clock session detached the clock of domain "{{domain}}"
    And request "start" of the clock session completed
    And the clock session receives no frame about domain "{{domain}}" within "1s"
    And the clock session received no frame about domain "{{domain}}" after the reply to request "detach"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
