Feature: NSPL file formatting

  The formatter is an offline tool: it never starts a cluster or opens a session, so these
  scenarios exercise the executable directly rather than a one-node and three-node topology.

  Scenario: The formatter help lists its formatting flags
    When the nervix-nspl-format help is requested
    Then the last command output contains
      """
      Usage: nervix-nspl-format [OPTIONS] <PATH>...
      """
    And the last command output contains
      """
      --check
      """
    And the last command output contains
      """
      --stdout
      """

  Scenario: An unformatted NSPL file is rewritten in place
    Given an NSPL file "pipeline.nspl" containing
      """
      use    demo  ;begin;
      """
    When nervix-nspl-format formats the NSPL file "pipeline.nspl"
    Then the formatter exits with code 0
    And the NSPL file "pipeline.nspl" contains
      """
      USE demo;
      BEGIN;
      """

  Scenario: An already formatted file is left untouched
    Given an NSPL file "pipeline.nspl" containing
      """
      USE demo;
      BEGIN;
      """
    When nervix-nspl-format formats the NSPL file "pipeline.nspl"
    Then the formatter exits with code 0
    And the NSPL file "pipeline.nspl" is unchanged

  Scenario: Check mode reports an unformatted file without rewriting it
    Given an NSPL file "pipeline.nspl" containing
      """
      use    demo  ;
      """
    When nervix-nspl-format checks the NSPL file "pipeline.nspl"
    Then the formatter exits with code 1
    And the last command output contains
      """
      pipeline.nspl
      """
    And the NSPL file "pipeline.nspl" is unchanged

  Scenario: Check mode accepts an already formatted file
    Given an NSPL file "pipeline.nspl" containing
      """
      USE demo;
      """
    When nervix-nspl-format checks the NSPL file "pipeline.nspl"
    Then the formatter exits with code 0

  Scenario: Comments between statements survive formatting
    Given an NSPL file "pipeline.nspl" containing
      """
      // header

      use demo;

      // why we begin
      begin;

      // tail
      """
    When nervix-nspl-format formats the NSPL file "pipeline.nspl"
    Then the formatter exits with code 0
    And the NSPL file "pipeline.nspl" contains
      """
      // header

      USE demo;

      // why we begin
      BEGIN;

      // tail
      """

  Scenario: A statement holding a comment is left exactly as written
    Given an NSPL file "pipeline.nspl" containing
      """
      use    demo;

      CREATE RELAY orders // keep me
        SCHEMA order UNBRANCHED CAPACITY 1;
      """
    When nervix-nspl-format formats the NSPL file "pipeline.nspl"
    Then the formatter exits with code 0
    And the NSPL file "pipeline.nspl" contains
      """
      USE demo;

      CREATE RELAY orders // keep me
        SCHEMA order UNBRANCHED CAPACITY 1;
      """

  Scenario: A pooled client is rewritten with its pool clause between the type and the mount
    Given an NSPL file "pipeline.nspl" containing
      """
      create client postgres_main type postgres pool size min 2 max 8 mount dev_tls
        config { 'addr' = 'postgresql://HOST:5432/DATABASE?sslmode=verify-full' };
      """
    When nervix-nspl-format formats the NSPL file "pipeline.nspl"
    Then the formatter exits with code 0
    And the NSPL file "pipeline.nspl" contains
      """
      CREATE CLIENT postgres_main
        TYPE POSTGRES
        POOL SIZE MIN 2 MAX 8
        MOUNT dev_tls
        CONFIG {
          'addr' = 'postgresql://HOST:5432/DATABASE?sslmode=verify-full'
        };
      """

  Scenario: A directory is searched recursively for NSPL files
    Given an NSPL file "top.nspl" containing
      """
      use    demo  ;
      """
    And an NSPL file "nested/deep/inner.nspl" containing
      """
      begin;
      """
    And an NSPL file "nested/notes.txt" containing
      """
      use    demo  ;
      """
    When nervix-nspl-format formats the NSPL directory
    Then the formatter exits with code 0
    And the NSPL file "top.nspl" contains
      """
      USE demo;
      """
    And the NSPL file "nested/deep/inner.nspl" contains
      """
      BEGIN;
      """
    And the NSPL file "nested/notes.txt" is unchanged

  Scenario: Check mode reports unformatted files found by searching a directory
    Given an NSPL file "nested/deep/inner.nspl" containing
      """
      use    demo  ;
      """
    When nervix-nspl-format checks the NSPL directory
    Then the formatter exits with code 1
    And the last command output contains
      """
      inner.nspl
      """
    And the NSPL file "nested/deep/inner.nspl" is unchanged

  Scenario: A file that cannot be parsed is reported and left untouched
    Given an NSPL file "broken.nspl" containing
      """
      CREATE RELAY;
      """
    When nervix-nspl-format formats the NSPL file "broken.nspl"
    Then the formatter exits with code 3
    And the last command error contains
      """
      expected relay_name
      """
    And the NSPL file "broken.nspl" is unchanged

  Scenario: Standard input is formatted to standard output
    When nervix-nspl-format formats the standard input
      """
      use    demo  ;
      """
    Then the formatter exits with code 0
    And the last command output contains
      """
      USE demo;
      """
