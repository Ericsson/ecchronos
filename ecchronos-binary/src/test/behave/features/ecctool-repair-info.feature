Feature: ecctool repair-info

  Scenario: Get repair-info for all tables with duration
    Given we have access to ecctool
    And we have a nodeid
    When we get repair-info with duration 5m
    Then the output should contain a valid repair-info header
    And the output should contain a repair-info row for keyspaceWithCamelCase.tableWithCamelCase
    And the output should contain a repair-info row for test.table1
    And the output should contain a repair-info row for test.table2
    And the output should contain a repair-info row for test2.table1
    And the output should contain a repair-info row for test2.table2
    And the output should contain a repair-info row for ecchronos.lock
    And the output should contain a repair-info row for ecchronos.lock_priority
    And the output should contain a repair-info row for ecchronos.on_demand_repair_status
    And the output should contain a repair-info row for ecchronos.reject_configuration
    And the output should contain a repair-info row for ecchronos.repair_history
    And the output should contain a repair-info row for system_auth.network_permissions
    And the output should contain a repair-info row for system_auth.resource_role_permissons_index
    And the output should contain a repair-info row for system_auth.role_members
    And the output should contain a repair-info row for system_auth.role_permissions
    And the output should contain a repair-info row for system_auth.roles

  Scenario: Get repair-info for all tables with duration and limit
    Given we have access to ecctool
    And we have a nodeid
    When we get repair-info with duration 5m and limit 15
    Then the output should contain a valid repair-info header
    And the output should contain a repair-info row for keyspaceWithCamelCase.tableWithCamelCase

  Scenario: Get repair-info for all tables with ISO8601 duration
    Given we have access to ecctool
    And we have a nodeid
    When we get repair-info with duration pt5m
    Then the output should contain a valid repair-info header
    And the output should contain a repair-info row for keyspaceWithCamelCase.tableWithCamelCase
    And the output should contain a repair-info row for test.table1
    And the output should contain a repair-info row for test.table2
    And the output should contain a repair-info row for test2.table1
    And the output should contain a repair-info row for test2.table2
    And the output should contain a repair-info row for ecchronos.lock
    And the output should contain a repair-info row for ecchronos.lock_priority
    And the output should contain a repair-info row for ecchronos.on_demand_repair_status
    And the output should contain a repair-info row for ecchronos.reject_configuration
    And the output should contain a repair-info row for ecchronos.repair_history
    And the output should contain a repair-info row for system_auth.network_permissions
    And the output should contain a repair-info row for system_auth.resource_role_permissons_index
    And the output should contain a repair-info row for system_auth.role_members
    And the output should contain a repair-info row for system_auth.role_permissions
    And the output should contain a repair-info row for system_auth.roles

  Scenario: Get repair-info for all tables with since
    Given we have access to ecctool
    And we have a nodeid
    When we get repair-info with since 0
    Then the output should contain a valid repair-info header
    And the output should contain a repair-info row for keyspaceWithCamelCase.tableWithCamelCase
    And the output should contain a repair-info row for test.table1
    And the output should contain a repair-info row for test.table2
    And the output should contain a repair-info row for test2.table1
    And the output should contain a repair-info row for test2.table2
    And the output should contain a repair-info row for ecchronos.lock
    And the output should contain a repair-info row for ecchronos.lock_priority
    And the output should contain a repair-info row for ecchronos.on_demand_repair_status
    And the output should contain a repair-info row for ecchronos.reject_configuration
    And the output should contain a repair-info row for ecchronos.repair_history
    And the output should contain a repair-info row for system_auth.network_permissions
    And the output should contain a repair-info row for system_auth.resource_role_permissons_index
    And the output should contain a repair-info row for system_auth.role_members
    And the output should contain a repair-info row for system_auth.role_permissions
    And the output should contain a repair-info row for system_auth.roles

  Scenario: Get repair-info for all tables with since and limit 15
    Given we have access to ecctool
    And we have a nodeid
    When we get repair-info with since 0 and limit 15
    Then the output should contain a valid repair-info header
    And the output should contain a repair-info row for keyspaceWithCamelCase.tableWithCamelCase

  Scenario: Get local repair-info for all tables with since
    Given we have access to ecctool
    And we have a nodeid
    When we get local repair-info with since 0
    Then the output should contain a valid repair-info header
    And the output should contain a repair-info row for keyspaceWithCamelCase.tableWithCamelCase
    And the output should contain a repair-info row for test.table1
    And the output should contain a repair-info row for test.table2
    And the output should contain a repair-info row for test2.table1
    And the output should contain a repair-info row for test2.table2
    And the output should contain a repair-info row for ecchronos.lock
    And the output should contain a repair-info row for ecchronos.lock_priority
    And the output should contain a repair-info row for ecchronos.on_demand_repair_status
    And the output should contain a repair-info row for ecchronos.reject_configuration
    And the output should contain a repair-info row for ecchronos.repair_history
    And the output should contain a repair-info row for system_auth.network_permissions
    And the output should contain a repair-info row for system_auth.resource_role_permissons_index
    And the output should contain a repair-info row for system_auth.role_members
    And the output should contain a repair-info row for system_auth.role_permissions
    And the output should contain a repair-info row for system_auth.roles

  Scenario: Get repair-info for all tables with since ISO8601 date
    Given we have access to ecctool
    And we have a nodeid
    When we get repair-info with since 2022-08-25T12:00:00.0+02:00
    Then the output should contain a valid repair-info header
    And the output should contain a repair-info row for keyspaceWithCamelCase.tableWithCamelCase
    And the output should contain a repair-info row for test.table1
    And the output should contain a repair-info row for test.table2
    And the output should contain a repair-info row for test2.table1
    And the output should contain a repair-info row for test2.table2
    And the output should contain a repair-info row for ecchronos.lock
    And the output should contain a repair-info row for ecchronos.lock_priority
    And the output should contain a repair-info row for ecchronos.on_demand_repair_status
    And the output should contain a repair-info row for ecchronos.reject_configuration
    And the output should contain a repair-info row for ecchronos.repair_history
    And the output should contain a repair-info row for system_auth.network_permissions
    And the output should contain a repair-info row for system_auth.resource_role_permissons_index
    And the output should contain a repair-info row for system_auth.role_members
    And the output should contain a repair-info row for system_auth.role_permissions
    And the output should contain a repair-info row for system_auth.roles

  Scenario: Get repair-info for all tables with since and duration
    Given we have access to ecctool
    And we have a nodeid
    When we get repair-info with since 0 and duration 0
    Then the output should contain a valid repair-info header
    And the output should contain a repair-info row for keyspaceWithCamelCase.tableWithCamelCase
    And the output should contain a repair-info row for test.table1
    And the output should contain a repair-info row for test.table2
    And the output should contain a repair-info row for test2.table1
    And the output should contain a repair-info row for test2.table2
    And the output should contain a repair-info row for ecchronos.lock
    And the output should contain a repair-info row for ecchronos.lock_priority
    And the output should contain a repair-info row for ecchronos.on_demand_repair_status
    And the output should contain a repair-info row for ecchronos.reject_configuration
    And the output should contain a repair-info row for ecchronos.repair_history
    And the output should contain a repair-info row for system_auth.network_permissions
    And the output should contain a repair-info row for system_auth.resource_role_permissons_index
    And the output should contain a repair-info row for system_auth.role_members
    And the output should contain a repair-info row for system_auth.role_permissions
    And the output should contain a repair-info row for system_auth.roles

  Scenario: Get repair-info for keyspace test with duration
    Given we have access to ecctool
    And we have a nodeid
    When we get repair-info for keyspace test with duration 5m
    Then the output should contain a valid repair-info header
    And the output should contain a repair-info row for test.table1
    And the output should contain a repair-info row for test.table2

  Scenario: Get repair-info for keyspace test with since
    Given we have access to ecctool
    And we have a nodeid
    When we get repair-info for keyspace test with since 0
    Then the output should contain a valid repair-info header
    And the output should contain a repair-info row for test.table1
    And the output should contain a repair-info row for test.table2
    And the output should not contain more rows

  Scenario: Get repair-info for keyspace test with since and duration
    Given we have access to ecctool
    And we have a nodeid
    When we get repair-info for keyspace test with since 0 and duration 5m
    Then the output should contain a valid repair-info header
    And the output should contain a repair-info row for test.table1
    And the output should contain a repair-info row for test.table2
    And the output should not contain more rows

  Scenario: Get repair-info for table test.table1 with duration
    Given we have access to ecctool
    And we have a nodeid
    When we get repair-info for table test.table1 with duration 5m
    Then the output should contain a valid repair-info header
    And the output should contain a repair-info row for test.table1
    And the output should not contain more rows

  Scenario: Get repair-info for table test.table1 with since
    Given we have access to ecctool
    And we have a nodeid
    When we get repair-info for table test.table1 with since 0
    Then the output should contain a valid repair-info header
    And the output should contain a repair-info row for test.table1
    And the output should not contain more rows

  Scenario: Get repair-info for table test.table1 with since and duration
    Given we have access to ecctool
    And we have a nodeid
    When we get repair-info for table test.table1 with since 0 and duration 5m
    Then the output should contain a valid repair-info header
    And the output should contain a repair-info row for test.table1
    And the output should not contain more rows

  Scenario: Get repair-info for table test.table1
    Given we have access to ecctool
    And we have a nodeid
    When we get repair-info for table test.table1
    Then the output should contain a valid repair-info header
    And the output should contain a repair-info row for test.table1
    And the output should not contain more rows