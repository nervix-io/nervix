Feature: Postgres observation fixtures
  The shared database serves concurrent scenario observers without allocating a separate
  connection pool for every setup or observation. Verification uses the same SQLx driver and
  TLS policy as ordinary Postgres steps.

  Scenario Outline: Concurrent Postgres fixture observers retain usable clients over <transport>
    Given Postgres is running
    When 48 concurrent Postgres fixture observers retain 3 clients over <transport>
    Then every Postgres fixture observer received its complete query results

    Examples:
      | transport |
      | plaintext |
      | TLS       |

  Scenario Outline: Postgres observers keep capacity while two other fixtures hold locks over <transport>
    Given Postgres is running
    And two other Postgres fixtures hold table locks over <transport>
    When another Postgres fixture observer queries over <transport>
    Then the Postgres fixture observer queried over a connection established before both lock holders connected
    And every Postgres fixture observer received its complete query results

    Examples:
      | transport |
      | plaintext |
      | TLS       |

  # Each poll after the first follows a return of Pending, after which the task waits behind its
  # worker's run queue before it is polled again; on the parallel suite's busy runtime that wait,
  # repeated for each poll, decides how long establishing a connection takes.
  Scenario Outline: A new Postgres fixture connection is established in a few polls over <transport>
    Given Postgres is running
    When a new Postgres fixture connection is established over <transport>
    Then the Postgres fixture connection was established within 32 polls

    Examples:
      | transport |
      | plaintext |
      | TLS       |
