Feature: ecctool schedule-repair

  Scenario: Trigger repair for keyspace test2 and table table2 with v1 protocol
    Given we have access to ecctool
    When we trigger repair for keyspace test2 and table table2 with the v1 protocol
    Then the trigger repair output should contain a valid header for the v1 protocol
    And the trigger repair output should contain a row for test2.table2 for the v1 protocol
    And the repair output should not contain more rows

  Scenario: Trigger repair for keyspace test2 and table table2
    Given we have access to ecctool
    When we run repair for keyspace test2 and table table2
    Then the repair output should contain a valid header
    And the repair output should contain a valid repair row for test2.table2
    And the repair output should not contain more rows