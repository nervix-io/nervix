Feature: Licensing change notifications
  Notification hints refresh verified decisions while polling remains independent.

  Scenario Outline: A notification received during rotation survives its completion on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      ALTER CLUSTER SET LICENSING_REQUEST_TIMEOUT = 10s, LICENSING_RETRY_BACKOFF_MAX = 10s;
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the licensing service holds exactly one authenticated notification stream
    Given the next licensing rotation response is delayed
    When this Plexus command request begins executing in the background on the leader node
      """
      ROTATE LICENSE CLUSTER KEY;
      """
    Then the licensing rotation request has reached the service
    When the licensing service changes the domain quota to 12
    Then the background command request succeeds
    And node "node-1" eventually describes this license without a selected domain
      """
      quota cluster.domains: 1 of 12
      verification: verified
      """
    And the licensing service holds exactly one authenticated notification stream

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: The licensed leader holds a notification stream on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the current leader node is saved as placeholder "licensing_leader"
    And within "10s" node "{{licensing_leader}}" observability metric "nervix_licensing_stream_connected" with labels eventually equals 1
      """
      """
    And the licensing service holds exactly one authenticated notification stream

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Duplicate licensing hints cause one verified refresh on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And the licensing service duplicates notification events
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the licensing service holds exactly one authenticated notification stream
    When the licensing service changes the domain quota to 12
    Then the notification commits one verified refresh before the polling deadline

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A notification wakes a foreground refresh retry on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      CREATE UNPACED DOMAIN {{domain}};
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the licensing service holds exactly one authenticated notification stream
    When the licensing service temporarily refuses refresh requests
    And this Plexus command request begins executing in the background on the leader node
      """
      REFRESH LICENSE;
      """
    Then the foreground license refresh has reached its retryable refusal
    When the licensing service resumes with a domain quota of 12
    Then node "node-1" eventually describes this license without a selected domain
      """
      quota cluster.domains: 1 of 12
      verification: verified
      """
    And the background command request succeeds

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Reconnect sync recovers a withheld change on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And the licensing notification stream is "withholding events"
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ALTER CLUSTER SET LICENSING_RETRY_BACKOFF_MAX = 2s;
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the licensing service holds exactly one authenticated notification stream
    And the committed license decision is saved as placeholder "before_notification_gap"
    When the licensing service changes the domain quota to 12
    And the licensing notification stream is disconnected
    Then the licensing notification reconnect uses a fresh ticket and its last event identifier
    And the notification commits one verified refresh before the polling deadline

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A blocked notification stream leaves automatic polling live on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And the licensing notification stream is "blocked"
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the licensing service holds exactly one authenticated notification stream
    And the committed license decision is saved as placeholder "before_blocked_stream"
    When both licensing clocks pass the refresh deadline
    Then the committed license decision is newer than placeholder "before_blocked_stream"
    And the licensing service holds exactly one authenticated notification stream

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: An idle licensing stream reconnects on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And the licensing notification stream is "blocked"
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ALTER CLUSTER SET LICENSING_STREAM_IDLE_TIMEOUT = 3s, LICENSING_RETRY_BACKOFF_MAX = 1s;
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the licensing service holds exactly one authenticated notification stream
    And the idle licensing stream reconnects without inventing an event identifier
    When the licensing notification stream is "normal"
    Then the licensing service holds exactly one authenticated notification stream

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Oversized notification input leaves polling live on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And the licensing notification stream is "oversized"
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ALTER CLUSTER SET LICENSING_RETRY_BACKOFF_MAX = 1s;
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the committed license decision is saved as placeholder "before_oversized_stream"
    And the oversized licensing stream reconnects without inventing an event identifier
    When both licensing clocks pass the refresh deadline
    Then the committed license decision is newer than placeholder "before_oversized_stream"

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A stale sync grants nothing and leaves later hints live on <cluster_size> nodes
    Given an HTTPS licensing test service is available
    And the licensing notification stream is "stale sync"
    And a <cluster_size> node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the committed license decision is saved as placeholder "before_stale_sync"
    And the licensing service holds exactly one authenticated notification stream
    And the committed license decision matches placeholder "before_stale_sync"
    When the licensing service changes the domain quota to 12
    Then the notification commits one verified refresh before the polling deadline

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario: Leadership transfer closes the departing leader's licensing stream
    Given an HTTPS licensing test service is available
    And a 3 node nervix cluster is started
    When these Plexus commands are executed on the leader node
      """
      ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';
      """
    Then the current leader node is saved as placeholder "departing_license_leader"
    And a node other than placeholder "departing_license_leader" is saved as placeholder "next_license_leader"
    And the licensing service holds exactly one authenticated notification stream
    When leadership is transferred from node "{{departing_license_leader}}" to node "{{next_license_leader}}"
    Then node "{{next_license_leader}}" eventually reports leader "{{next_license_leader}}"
    And node "{{next_license_leader}}" observability metric "nervix_licensing_stream_connected" with labels eventually equals 1
      """
      """
    And the licensing service holds exactly one authenticated notification stream
    When the licensing service changes the domain quota to 12
    Then the notification commits one verified refresh before the polling deadline
