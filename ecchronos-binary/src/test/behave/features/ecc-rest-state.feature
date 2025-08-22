Feature: API to get state

  Scenario: Get state for all nodes
    Given I have a json schema nodes
    And I use the url localhost:8080/state/nodes
    When I send a GET request
    Then the response is successful
    And the response matches the json schema nodes
    And the number of nodes is 3

  Scenario: Get state for all nodes in dc1
    Given I have a json schema nodes
    And I use the url localhost:8080/state/nodes?datacenter=dc1
    When I send a GET request
    Then the response is successful
    And the response matches the json schema nodes
    And the number of nodes is 3

  Scenario: Get state for all nodes in dc2
    Given I have a json schema nodes
    And I use the url localhost:8080/state/nodes?datacenter=dc2
    When I send a GET request
    Then the response is successful
    And the response matches the json schema nodes
    And the number of nodes is 0
