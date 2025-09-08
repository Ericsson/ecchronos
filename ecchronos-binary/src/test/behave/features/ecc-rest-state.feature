Feature: API to get state

  Scenario: Get state for all nodes
    Given I have a json schema nodes
    And I use the url localhost:8080/state/nodes
    When I send a GET request
    Then the response is successful
    And the response matches the json schema nodes
    And the number of nodes is 4

  Scenario: Get state for all nodes in dc1
    Given I have a json schema nodes
    And I use the url localhost:8080/state/nodes?datacenter=datacenter1
    When I send a GET request
    Then the response is successful
    And the response matches the json schema nodes
    And the number of nodes is 2

  Scenario: Get state for all nodes in dc2
    Given I have a json schema nodes
    And I use the url localhost:8080/state/nodes?datacenter=datacenter2
    When I send a GET request
    Then the response is successful
    And the response matches the json schema nodes
    And the number of nodes is 2
