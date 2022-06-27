Feature: ecctool run-repair

  Scenario: Run local repair for keyspace test2 and table table2
    Given we have access to ecctool
    When we run local repair for keyspace test2 and table table2
    Then the repair output should contain a valid header
    And the repair output should contain a valid repair row for test2.table2
    And the repair output should not contain more rows
    And the output should contain summary

  Scenario: Run local repair for keyspace test2
    Given we have access to ecctool
    When we run local repair for keyspace test2
    Then the repair output should contain a valid header
    And the repair output should contain a valid repair row for test2.table2
    And the repair output should contain a valid repair row for test2.table1
    And the repair output should not contain more rows
    And the output should contain summary

  Scenario: Run local repair for all keyspaces and tables
    Given we have access to ecctool
    When we run local repair
    Then the repair output should contain a valid header
    And the repair output should contain a valid repair row for test2.table2
    And the repair output should contain a valid repair row for test2.table1
    And the repair output should contain a valid repair row for test.table2
    And the repair output should contain a valid repair row for test.table1
    And the repair output should not contain more rows
    And the output should contain summary

  Scenario: Run cluster-wide repair for keyspace test2 and table table2
    Given we have access to ecctool
    When we run repair for keyspace test2 and table table2
    Then the repair output should contain a valid header
    And the repair output should contain a valid repair row for test2.table2
    And the repair output should contain a valid repair row for test2.table2
    And the repair output should not contain more rows
    And the output should contain summary

  Scenario: Run cluster-wide repair for keyspace test2
    Given we have access to ecctool
    When we run repair for keyspace test2
    Then the repair output should contain a valid header
    And the repair output should contain a valid repair row for test2.table2
    And the repair output should contain a valid repair row for test2.table2
    And the repair output should contain a valid repair row for test2.table1
    And the repair output should contain a valid repair row for test2.table1
    And the repair output should not contain more rows
    And the output should contain summary

  Scenario: Run cluster-wide repair for all keyspaces and tables
    Given we have access to ecctool
    When we run repair
    Then the repair output should contain a valid header
    And the repair output should contain a valid repair row for test.table2
    And the repair output should contain a valid repair row for test.table2
    And the repair output should contain a valid repair row for test.table1
    And the repair output should contain a valid repair row for test.table1
    And the repair output should contain a valid repair row for test2.table2
    And the repair output should contain a valid repair row for test2.table2
    And the repair output should contain a valid repair row for test2.table1
    And the repair output should contain a valid repair row for test2.table1
    And the repair output should not contain more rows
    And the output should contain summary