@deadlock_reports
Feature: Engineers inspect and triage potential lock-order evidence locally
  Diagnostic evidence distinguishes an active deadlock from historical acquisition order.
  Reviewing potential order requires an explicit proof and retained regression evidence.

  Scenario: Export potential findings and retain an explicit non-overlap review
    Given diagnostic evidence containing a potential lock-order cycle
    When the engineer exports the potential findings
    Then the export names the lock instances, acquisition modes and source sites
    And the diagnostic evidence does not qualify before triage
    When the engineer records a non-overlap proof and retained regression
    Then the reviewed diagnostic evidence qualifies and retains the complete cycle
