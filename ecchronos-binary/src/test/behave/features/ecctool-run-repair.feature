Feature: ecctool run-repair

  Scenario: Run local parallel_vnode repair for keyspace test2 and table table2
    Given we have access to ecctool
    And we have a nodeid
    When we run local repair for keyspace test2 and table table2 with type PARALLEL_VNODE
    Then the output should contain a valid repair header
    And the output should contain a repair row for test2.table2 with type PARALLEL_VNODE
    And the output should not contain more rows
    And the output should contain a valid repair summary

  Scenario: Run local repair for keyspace test2
    Given we have access to ecctool
    And we have a nodeid
    When we run local repair for keyspace test2
    Then the output should contain a valid repair header
    And the output should contain a repair row for test2.table2 with type VNODE
    And the output should contain a repair row for test2.table1 with type VNODE
    And the output should not contain more rows
    And the output should contain a valid repair summary

  Scenario: Run local repair for all keyspaces and tables
    Given we have access to ecctool
    And we have a nodeid
    When we run local repair
    Then the output should contain a valid repair header
    And the output should contain a repair row for keyspaceWithCamelCase.tableWithCamelCase with type VNODE
    And the output should contain a repair row for test2.table2 with type VNODE
    And the output should contain a repair row for test2.table1 with type VNODE
    And the output should contain a repair row for test.table2 with type VNODE
    And the output should contain a repair row for test.table1 with type VNODE
    And the output should contain a valid repair summary

  Scenario: Run cluster-wide repair for keyspace test2 and table table2
    Given we have access to ecctool
    When we run repair for keyspace test2 and table table2
    Then the output should contain a valid repair header
    And the output should contain 4 repair rows for test2.table2 with type VNODE
    And the output should not contain more rows
    And the output should contain a valid repair summary

  Scenario: Run invalid cluster-wide repair for keyspace test2 and table table2
    Given we have access to ecctool
    When we run invalid repair for keyspace test2 and table table2
    Then the output should contain a nodeid error message

  Scenario: Run cluster-wide repair for keyspace test2
    Given we have access to ecctool
    When we run repair for keyspace test2
    Then the output should contain a valid repair header
    And the output should contain 4 repair rows for test2.table2 with type VNODE
    And the output should contain 4 repair rows for test2.table1 with type VNODE
    And the output should not contain more rows
    And the output should contain a valid repair summary

  Scenario: Run cluster-wide repair for all keyspaces and tables
    Given we have access to ecctool
    When we run repair
    Then the output should contain a valid repair header
    And the output should contain 4 repair rows for keyspaceWithCamelCase.tableWithCamelCase with type VNODE
    And the output should contain 4 repair rows for test.table2 with type VNODE
    And the output should contain 4 repair rows for test.table1 with type VNODE
    And the output should contain 4 repair rows for test2.table2 with type VNODE
    And the output should contain 4 repair rows for test2.table1 with type VNODE
    And the output should contain a valid repair summary

  Scenario: Run local twcs repair for keyspace ecchronos and table repair_history
    Given we have access to ecctool
    And we have a nodeid
    When we run twcs repair for keyspace ecchronos and table repair_history with type VNODE
    Then the output should contain a valid repair header
    And the output should contain a repair row for ecchronos.repair_history with type VNODE
    And the output should not contain more rows
    And the output should contain a valid repair summary

  Scenario: Run local invalid twcs repair for keyspace ecchronos and table repair_history
    Given we have access to ecctool
    And we have a nodeid
    When we run local repair for keyspace ecchronos and table repair_history with type VNODE
    Then the output should contain a repair request failed message

  Scenario: Run local disabled repair for keyspace  test and table table3
    Given we have access to ecctool
    And we have a nodeid
    When we run enabled repair for keyspace test and table table3 with type VNODE
    Then the output should contain a valid repair header
    And the output should contain a repair row for test.table3 with type VNODE
    And the output should not contain more rows
    And the output should contain a valid repair summary

  Scenario: Run local invalid disabled repair for keyspace test and table table3
    Given we have access to ecctool
    And we have a nodeid
    When we run local repair for keyspace test and table test3 with type VNODE
    Then the output should contain a repair request failed message

