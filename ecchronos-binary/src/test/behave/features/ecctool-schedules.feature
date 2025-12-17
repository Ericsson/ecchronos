Feature: ecctool schedules

  Scenario: Get all schedules
    Given we have access to ecctool
    When we list all schedules
    Then the output should contain a valid snapshot header
    And the output should contain a valid schedule header
    And the output should contain a schedule row for keyspaceWithCamelCase.tableWithCamelCase with type VNODE
    And the output should contain a schedule row for test.table1 with type VNODE
    And the output should contain a schedule row for test.table2 with type VNODE
    And the output should contain a schedule row for test2.table1 with type INCREMENTAL
    And the output should contain a schedule row for test2.table2 with type PARALLEL_VNODE
#    And the output should not contain more rows
    And the output should contain a valid schedule summary

  Scenario: Get all schedules with a limit
    Given we have access to ecctool
    When we list all schedules with a limit of 1
    Then the output should contain a valid snapshot header
    And the output should contain a valid schedule header
    And the output should contain 1 row
    And the output should contain a valid schedule summary

  Scenario: Get schedules for keyspace test
    Given we have access to ecctool
    When we list all schedules for keyspace test
    Then the output should contain a valid snapshot header
    And the output should contain a valid schedule header
    And the output should contain a schedule row for test.table1 with type VNODE
    And the output should contain a schedule row for test.table2 with type VNODE
#    And the output should not contain more rows
    And the output should contain a valid schedule summary

  Scenario: Get schedules for keyspace test with a limit
    Given we have access to ecctool
    When we list all schedules for keyspace test with a limit of 1
    Then the output should contain a valid snapshot header
    And the output should contain a valid schedule header
    And the output should contain a schedule row for test..* with type VNODE
    And the output should not contain more rows
    And the output should contain a valid schedule summary

  Scenario: Get schedule for table test.table1
    Given we have access to ecctool
    When we list schedules for table test.table1
    Then the output should contain a valid snapshot header
    And the output should contain a valid schedule header
    And the output should contain a schedule row for test.table1 with type VNODE
#    And the output should not contain more rows
    And the output should contain a valid schedule summary

  Scenario: Get schedule for table test.table2 with a limit
    Given we have access to ecctool
    When we show schedule test.table2 with a limit of 5
#    Then the expected schedule header should be for test.table2 with type VNODE
#    And the token list should contain 5 rows

  Scenario: Get schedule for table test.table1 with a limit
    Given we have access to ecctool
    When we show schedule test.table2 with a limit of 15
#    Then the expected schedule header should be for test.table2 with type VNODE
#    And the token list should contain 15 rows

  Scenario: Get all schedules with columns output option
    Given we have access to ecctool
    When we list all schedules with columns 0,3,6
    Then the output should only contain column headers 0,3,6

  Scenario: Get all schedules with columns output option with only out of range values
    Given we have access to ecctool
    When we list all schedules with columns 100,200
    Then the output should contain all default column headers

  Scenario: Get all schedules with columns output option with some out of range values
    Given we have access to ecctool
    When we list all schedules with columns 0,1,5,6,98,100,102
    Then the output should only contain column headers 0,1,5,6

  Scenario: Get all schedules with json output option
    Given we have access to ecctool
    When we list all schedules with json output option
    Then the output should contain valid json data
