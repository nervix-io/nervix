Feature: Client traffic

  Each node records the traffic its client listeners carry: the calls they authenticate or refuse,
  the sessions they serve, and the requests those sessions send and how the requests end. A name
  that names no user costs the node the credentials work a wrong password costs, and a public
  listener keeps serving after one of its accepts fails.

  Scenario Outline: A node counts the calls its client listeners authenticate and refuse
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    When the client authentication counts of node "{{leader}}" are remembered
    Then a native session opened on node "{{leader}}" with a wrong password is refused with status "Unauthenticated"
    And a native session opened on node "{{leader}}" as an unknown user is refused with status "Unauthenticated"
    And a native session opened on node "{{leader}}" without credentials is refused with status "Unauthenticated"
    And a console authentication check on node "{{leader}}" with a wrong password is refused with status 401
    And node "{{leader}}" counted 2 more rejected authentications on its grpc listener
    And node "{{leader}}" counted 1 more missing authentication on its grpc listener
    And node "{{leader}}" counted 1 more rejected authentication on its console listener
    And node "{{leader}}" counted 0 more unavailable authentications on its grpc listener
    And node "{{leader}}" observability metric "nervix_client_authentications_total" with labels eventually reaches at least 1
      """
      outcome="authenticated"
      transport="grpc"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: A name that names no user costs a node the credentials work of a wrong password
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    And a native session opened on node "{{leader}}" as an unknown user is refused after one credentials verification
    And a native session opened on node "{{leader}}" with a wrong password is refused after one credentials verification

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: Repeated refused credentials are paced for <username> on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      CREATE USER pacing_user WITH PASSWORD 'created-password';
      """
    Then the current leader node is saved as placeholder "leader"
    And a burst of 30 refused native sessions as "<username>" on node "{{leader}}" takes at least 2 seconds

    Examples:
      | cluster_size | username    |
      | 1            | pacing_user |
      | 3            | pacing_user |
      | 1            | nobody      |
      | 3            | nobody      |
      | 1            | not a user  |
      | 3            | not a user  |

  Scenario Outline: A node counts the sessions it serves, the requests they decode and how the requests end
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    Then the current leader node is saved as placeholder "leader"
    And the node serving the active session is saved as placeholder "served"
    When the active session sends request "domains" listing domains
    Then request "domains" lists domain "{{domain}}"
    When the active session sends request "completion" completing "CREATE SCH" at byte 10
    Then request "completion" suggests "SCHEMA"
    When the active session sends request "split-character" completing "SHOW é" at byte 6 inside a character
    Then request "split-character" is rejected as an invalid request naming "SuggestRequest.cursor"
    Given command admission on node "{{leader}}" pauses before proposal
    When the active session sends request "slow" with this Plexus command
      """
      CREATE SCHEMA cancelled_record (
        value STRING
      );
      """
    Then the command admission pause on node "{{leader}}" is reached
    When the active session cancels request "slow"
    Then request "slow" is cancelled before admission
    When the command admission pause on node "{{leader}}" is released
    Then node "{{served}}" observability metric "nervix_sessions" with labels eventually reaches at least 1
      """
      transport="grpc"
      """
    And node "{{served}}" observability metric "nervix_sessions_opened_total" with labels eventually reaches at least 1
      """
      transport="grpc"
      """
    And node "{{served}}" observability metric "nervix_session_requests_total" with labels eventually equals 1
      """
      request="list_domains"
      transport="grpc"
      """
    And node "{{served}}" observability metric "nervix_session_requests_total" with labels eventually equals 1
      """
      request="suggest"
      transport="grpc"
      """
    And node "{{served}}" observability metric "nervix_session_requests_total" with labels eventually equals 1
      """
      request="cancel"
      transport="grpc"
      """
    And node "{{served}}" observability metric "nervix_session_rejections_total" with labels eventually equals 1
      """
      rejection="invalid_request"
      transport="grpc"
      """
    And node "{{served}}" observability metric "nervix_session_cancellations_total" with labels eventually equals 1
      """
      stage="before_admission"
      transport="grpc"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: The <listener> listener keeps serving after one of its accepts fails
    Given a <cluster_size> node nervix cluster is started
    And the active domain is "{{domain}}"
    And the leader node is configured with these Plexus commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the next accept of the <listener> listener on node "node-1" fails
    Then the <listener> listener on node "node-1" serves a request
    And the grpc listener on node "node-1" serves a request

    Examples:
      | cluster_size | listener      |
      | 1            | console       |
      | 3            | console       |
      | 1            | observability |
      | 3            | observability |
      | 1            | http          |
      | 3            | http          |
      | 1            | grpc          |
      | 3            | grpc          |

  Scenario: A refused authentication is logged without the credentials it presented
    Given a nervix-server process is started
    Then a native session opened on the server process with a wrong password is refused with status "Unauthenticated"
    And the server process log eventually contains "client authentication failed"
    When the server process receives SIGTERM
    Then the server process exits with status 0
    And the server process log does not contain "not-the-password"
    And the server process log does not contain "ZGVmYXVsdDpub3QtdGhlLXBhc3N3b3Jk"
