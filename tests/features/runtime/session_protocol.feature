Feature: Session protocol

  Scenario Outline: Semantic completion pages reach every domain once
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
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
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given command admission on node "{{leader}}" pauses before proposal
    When the active session sends request "slow" with this NSPL command
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
    And this NSPL command request is executed on the leader node
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
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    Given command execution on node "{{leader}}" pauses after durable admission
    When the active session sends request "admitted" with execution reference "cancel-admitted-{{test_id}}" and this NSPL command
      """
      CREATE SCHEMA admitted_record (
        value STRING
      );
      """
    Then the durable command admission pause on node "{{leader}}" is reached
    When the active session cancels request "admitted"
    Then request "admitted" is cancelled after admission
    When the durable command admission pause on node "{{leader}}" is released
    And this NSPL command request with execution reference "cancel-admitted-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA admitted_record (
        value STRING
      );
      """
    Then the last command request succeeded
    When this NSPL command request is executed on the leader node
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
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the active session sends request "bad-reference" with execution reference text "not a reference"
    Then request "bad-reference" is rejected as an invalid request naming "CommandRequest.execution_reference"
    When the active session sends request "split-character" completing "SHOW é" at byte 6 inside a character
    Then request "split-character" is rejected as an invalid request naming "SuggestRequest.cursor"
    When the active session sends request "after-refusals" completing "SHOW é" at byte 5
    Then request "after-refusals" suggests "CLUSTER"
    When these NSPL commands are executed on the active session
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
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then a session opened on the leader node with a wrong password is refused with status "Unauthenticated"
    When the active session sends a request whose identity is zero
    Then the active session is ended for violating the protocol
    Given the leader node is configured with these NSPL commands
      """
      SHOW CLUSTER STATUS;
      """
    When the active session sends 12 bytes that are not a frame
    Then the active session ends with status "Internal"
    Given the leader node is configured with these NSPL commands
      """
      SHOW CLUSTER STATUS;
      """
    When the active session sends a message larger than the frame limit
    Then the active session ends with status "OutOfRange"

  Scenario: Leadership lost after durable admission leaves an unknown outcome that a retry recovers
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "admitted_leader"
    And a node other than placeholder "admitted_leader" is saved as placeholder "successor"
    Given command execution on node "{{admitted_leader}}" pauses after durable admission
    When this NSPL command request with execution reference "leadership-lost-{{test_id}}" begins executing in the background on the leader node
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
    When this NSPL command request with execution reference "leadership-lost-{{test_id}}" is executed on the leader node
      """
      CREATE SCHEMA uncertain_record (
        value STRING
      );
      """
    Then the last command request succeeded
    When this NSPL command request is executed on the leader node
      """
      SHOW CREATE SCHEMA uncertain_record;
      """
    Then the last command output contains
      """
      CREATE SCHEMA uncertain_record (
      """

  Scenario: A redirect names no endpoint for a leader that discovery cannot reach
    Given a 3 node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these NSPL commands
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
    And the leader node is configured with these NSPL commands
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
    When these NSPL commands are executed on the leader node
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
