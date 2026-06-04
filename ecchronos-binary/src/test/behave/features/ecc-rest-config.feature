Feature: API to manage runtime configuration

  Scenario: Get current configuration
    Given I use the url localhost:8080/repair-management/v2/config
    When I send a GET request
    Then the response is successful
    And the response contains session_window_ms
    And the response contains cooldown_ms
    And the response contains locks_per_resource

  Scenario: Update session window
    Given I use the url localhost:8080/repair-management/v2/config
    When I send a PATCH request with body {"session_window_ms": 120000}
    Then the response is successful
    And the response has session_window_ms equal to 120000

  Scenario: Update cooldown
    Given I use the url localhost:8080/repair-management/v2/config
    When I send a PATCH request with body {"cooldown_ms": 5000}
    Then the response is successful
    And the response has cooldown_ms equal to 5000

  Scenario: Update locks per resource
    Given I use the url localhost:8080/repair-management/v2/config
    When I send a PATCH request with body {"locks_per_resource": 5}
    Then the response is successful
    And the response has locks_per_resource equal to 5

  Scenario: Update multiple parameters
    Given I use the url localhost:8080/repair-management/v2/config
    When I send a PATCH request with body {"session_window_ms": 600000, "cooldown_ms": 10000}
    Then the response is successful
    And the response has session_window_ms equal to 600000
    And the response has cooldown_ms equal to 10000

  Scenario: Get config reflects previous update
    Given I use the url localhost:8080/repair-management/v2/config
    When I send a PATCH request with body {"cooldown_ms": 7777}
    Then the response is successful
    When I send a GET request
    Then the response is successful
    And the response has cooldown_ms equal to 7777
