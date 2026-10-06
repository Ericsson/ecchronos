Feature: ecctool metrics

  Scenario: Get metrics with default output
    Given we have access to ecctool
    When we fetch metrics
    Then the output should contain metrics exposition text

  Scenario: Get metrics filtered by name
    Given we have access to ecctool
    When we fetch metrics filtered by name "jvm"
    Then the output should only contain metrics matching "jvm"

  Scenario: Get metrics with raw output
    Given we have access to ecctool
    When we fetch metrics with raw output
    Then the output should not contain metrics comments
