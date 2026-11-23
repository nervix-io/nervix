@production_licensing
Feature: Production licensing smoke test
  A test cluster activates the production smoke-test account's license against the production
  licensing service, licensing.nervix.io, and verifies its decision with the production trust
  bundle, as a customer's cluster does: no scenario step starts the in-repo licensing test service.
  The account's plan grants nothing beyond the free policy except licensing.dummy, a capability that
  changes nothing, so DESCRIBE LICENSE showing it enabled proves that the cluster verified and
  applied a decision the production service signed. The license key comes from the
  NERVIX_LICENSING_SMOKE_KEY environment variable and never appears in a step or its output. The tag
  keeps these scenarios out of ordinary runs; just test-licensing-production-smoke runs them.

  Scenario Outline: A cluster activates the smoke-test license from the production service and shows licensing.dummy enabled on <cluster_size> nodes
    Given a <cluster_size> node nervix cluster is started
    When the leader node activates the production smoke-test license as ephemeral
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: activated
      verification: verified
      capability licensing.dummy: enabled
      """
    When these Plexus commands are executed on the leader node
      """
      RELEASE LICENSE;
      """
    Then node "node-1" eventually describes this license without a selected domain
      """
      mode: free
      capability licensing.dummy: disabled
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
