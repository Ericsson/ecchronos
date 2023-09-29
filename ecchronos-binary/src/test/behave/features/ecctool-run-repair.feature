Feature: ecctool run-repair

  Scenario: Run local parallel_vnode repair for keyspace test2 and table table2
    Given we have access to ecctool
    When we run local repair for keyspace test2 and table table2 with type PARALLEL_VNODE
    Then the output should contain a valid repair header
    And the output should contain a repair row for test2.table2 with type PARALLEL_VNODE
    And the output should not contain more rows
    And the output should contain a valid repair summary

  Scenario: Run local repair for keyspace test2
    Given we have access to ecctool
    When we run local repair for keyspace test2
    Then the output should contain a valid repair header
    And the output should contain a repair row for test2.table2 with type VNODE
    And the output should contain a repair row for test2.table1 with type VNODE
    And the output should not contain more rows
    And the output should contain a valid repair summary

  Scenario: Run local repair for all keyspaces and tables
    Given we have access to ecctool
    When we run local repair
    Then the output should contain a valid repair header
    And the output should contain a repair row for keyspaceWithCamelCase.tableWithCamelCase with type VNODE
    And the output should contain a repair row for test2.table2 with type VNODE
    And the output should contain a repair row for test2.table1 with type VNODE
    And the output should contain a repair row for test.table2 with type VNODE
    And the output should contain a repair row for test.table1 with type VNODE
    And the output should not contain more rows
    And the output should contain a valid repair summary

  Scenario: Run cluster-wide repair for keyspace test2 and table table2
    Given we have access to ecctool
    When we run repair for keyspace test2 and table table2
    Then the output should contain a valid repair header
    And the output should contain a repair row for test2.table2 with type VNODE
    And the output should contain a repair row for test2.table2 with type VNODE
    And the output should not contain more rows
    And the output should contain a valid repair summary

  Scenario: Run cluster-wide repair for keyspace test2
    Given we have access to ecctool
    When we run repair for keyspace test2
    Then the output should contain a valid repair header
    And the output should contain a repair row for test2.table2 with type VNODE
    And the output should contain a repair row for test2.table2 with type VNODE
    And the output should contain a repair row for test2.table1 with type VNODE
    And the output should contain a repair row for test2.table1 with type VNODE
    And the output should not contain more rows
    And the output should contain a valid repair summary

  Scenario: Run cluster-wide repair for all keyspaces and tables
    Given we have access to ecctool
    When we run repair
    Then the output should contain a valid repair header
    And the output should contain a repair row for keyspaceWithCamelCase.tableWithCamelCase with type VNODE
    And the output should contain a repair row for keyspaceWithCamelCase.tableWithCamelCase with type VNODE
    And the output should contain a repair row for test.table2 with type VNODE
    And the output should contain a repair row for test.table2 with type VNODE
    And the output should contain a repair row for test.table1 with type VNODE
    And the output should contain a repair row for test.table1 with type VNODE
    And the output should contain a repair row for test2.table2 with type VNODE
    And the output should contain a repair row for test2.table2 with type VNODE
    And the output should contain a repair row for test2.table1 with type VNODE
    And the output should contain a repair row for test2.table1 with type VNODE
    And the output should not contain more rows
    And the output should contain a valid repair summary