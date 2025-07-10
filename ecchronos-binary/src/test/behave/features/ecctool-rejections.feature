Feature: ecctool rejections

  Scenario: Create a new rejection
    Given we have access to ecctool
    When we create repair rejection for keyspace test1, table test1 with start hour 18, start minute 30, end hour 20, end minute 30, dc exclusions dc1
    When we create repair rejection for keyspace test1, table test2 with start hour 18, start minute 30, end hour 20, end minute 30, dc exclusions dc1
    When we create repair rejection for keyspace test2, table test1 with start hour 18, start minute 30, end hour 20, end minute 30, dc exclusions dc1
    Then the output should contain a valid rejection header
    And the output should contain a rejection row for test1.test1 with start hour 18, start minute 30, end hour 20, end minute 30, dc exclusions dc1
    And the output should contain a rejection row for test1.test2 with start hour 18, start minute 30, end hour 20, end minute 30, dc exclusions dc1
    And the output should contain a rejection row for test2.test1 with start hour 18, start minute 30, end hour 20, end minute 30, dc exclusions dc1
    And the output should not contain more rows

  Scenario: Get rejections for all keyspaces and tables
    Given we have access to ecctool
    When we get all rejections
    Then the output should contain a valid rejections header
    And the output should contain a rejection row for test1.test1 with start hour 18, start minute 30, end hour 20, end minute 30, dc exclusions dc1
    And the output should contain a rejection row for test1.test2 with start hour 18, start minute 30, end hour 20, end minute 30, dc exclusions dc1
    And the output should contain a rejection row for test2.test1 with start hour 18, start minute 30, end hour 20, end minute 30, dc exclusions dc1
    And the output should not contain more rows

  Scenario: Get rejections by keyspace
    Given we have access to ecctool
    When we get all rejections for test1
    Then the output should contain a valid rejections header
    And the output should contain a rejection row for test1.test1 with start hour 18, start minute 30, end hour 20, end minute 30, dc exclusions dc1
    And the output should contain a rejection row for test1.test2 with start hour 18, start minute 30, end hour 20, end minute 30, dc exclusions dc1
    And the output should not contain more rows

  Scenario: Get rejections by keyspace and table
    Given we have access to ecctool
    When we get all rejections for test1.test2
    Then the output should contain a valid rejections header
    And the output should contain a rejection row for test1.test2 with start hour 18, start minute 30, end hour 20, end minute 30, dc exclusions dc1
    And the output should not contain more rows

