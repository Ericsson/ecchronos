# Contributing

You are most welcome to create pull requests and issues to ecChronos.
Before creating pull requests it is recommended to first discuss your idea with at least one of the owners of the repository.
For keeping track of the history it is recommended that most communication is performed or summarized within pull requests or issues.

## Development

### Prerequisites

* Maven
* Java 17 / 21
* Docker (for test setup)
* Python 3

### Branches

Target the lowest applicable version of ecChronos when fixing an issue.
Bug fixes should be targeted to the lowest maintained version where the bug reside.
New features should, in general, be added to the master branch.

### Code Style

This project uses the cassandra code style which is based on Sun’s Java coding conventions.
Formatting rules for eclipse can be found [here](../code_style.xml).

Provided patches should be contained and should not modify code outside the scope of the patch.
This will make it quicker to perform reviews and merging the pull requests.

### Logging

The table below describe the main criteria(s) for each debugging level. The levels are in the order of (from most to least):
<br>
<pre>
  all > trace > debug > info > warn > error > off
</pre>

| Log&nbsp;level | Description                                                                                                                                                                                                                                                                                                                                          |
|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| all            | All levels will be logged <br> <b>Example:</b> -                                                                                                                                                                                                                                                                                                     |
| trace          | Detailed debugging (flows, request/response, details, etc). Will have a performance impact and is therefore not for production, unless it is a planned troubleshooting activity. Mainly used during development. <br>  <b>Example:</b> Every method call is logged in detail for a certain request and response flow. Used data is logged in detail. |
| debug          | Simple debug logging which can be turned on and used in production if necessary (should have no impact on performance). The logs at this level should be of the type a developer might need to spot a quick fix to a problem or to at least isolate the problem further. <br> <b>Example:</b> Specific events with contextual details.               |
| info           | For logging the normal flow and operation of the service(s). <br> <b>Example:</b> Service health, progress of requests/responses etc.                                                                                                                                                                                                                |
| warn           | Behaviors in the service(s) which are unexpected and potentially could lead to errors, but were handled for the moment. However, the service(s) as such are still working normally and as expected. <br> <b>Example:</b> A primary service switching to a secondary one, connection retries, reverting to defaults etc.                              |
| error          | A service or dependency have failed in the sense no requests can be served and/or data processed cannot be trusted. <br> <b>Example:</b> Connection attempts that ultimate fail. Crucial resources not available.                                                                                                                                    |
| off            | No levels will be logged at all. <br> <b>Example:</b> -                                                                                                                                                                                                                                                                                              |                                                                                                                                                                                                                                                |

If the log message may require lengthy calculations, method calls to collect data or concatenations, use an <i>is...Enabled</i> block to guard it. An example would be:
<pre>
  if (LOG.isDebugEnabled())
  {
    LOG.debug("Environment status: {}", <b>getSyncEnvironmentStatus()</b>);
  }
</pre>

### Builds

The builds required to merge a pull request are contained within the [Github configuration](../.github/workflows/actions.yml) and include tests, code coverage as well as PMD checks.

All checks need to pass before merging a pull request.
The current PMD rules are constantly evolving.
If you encounter a PMD rule that seems odd or non-relevant feel free to discuss it within an issue or pull request.

#### Built with

* [Maven](https://maven.apache.org) - Dependency and build management
* [docker-maven-plugin](https://github.com/fabric8io/docker-maven-plugin) - For integration tests

### Maintained versions

The following table state what versions of ecChronos is still under active maintenance.

| Version | First Release | Status |
|:-------:|:-------------:|:------:|
|  1.x.x  |  2019-Jan-25  |   -    |
|  2.x.x  |  2021-Feb-25  |   -    |
|  3.x.x  |  2022-Jun-30  |   -    |
|  4.x.x  |  2022-Dec-15  |   -    |
|  5.x.x  |  2023-Dec-06  |   X    |
|  6.x.x  |  2024-Nov-18  |   X    |

### REST API

Whenever there are any changes to the REST API, the OpenAPI specification must be generated.
Generation of the OpenAPI specification is done by running the `python-integration-tests` tests.
The generated specification is stored in `docs/autogenerated`.

### ecctool documentation

If changes have been made to ecctool, the documentation must be generated using `mvn clean install -P generate-ecctool-doc -DskipUTs`.
On top of that, if there's any change in the output of ecctool the [examples](ECCTOOL_EXAMPLES.md) must be updated manually.

### Tests

It is recommended to run all tests `mvn clean verify -Dprecommit.tests` before creating a PR.
If applicable, please add test cases to the appropriate test suites.
More information can be found on the [tests page](TESTS.md).

## Submit new Issues to ecChronos project

To void ask the author basic questions by providing more detailed information in the user story. The template creation took into consideration the "INVEST" pattern, which is a set of criteria or principles commonly used to evaluate and describe the quality of user stories or product backlog items in agile and Scrum methodologies, as described in the table below.

<div align="center">

   | Component    | Description                                                                                                                        |
   |--------------|------------------------------------------------------------------------------------------------------------------------------------|
   | Independent  | "Capable of being implemented independently, without relying on other tasks, allowing execution in any order."                     |
   | Negotiable   | "Flexible and subject to negotiation during development, allowing for adjustments according to the client's needs and priorities." |
   | Valuable     | "Provides significant value to the client, meeting their needs and expectations to achieve project objectives."                    |
   | Estimable    | "Possible to be estimated in terms of effort and time, enabling proper prioritization and planning of activities."                 |
   | Small        | "Small in scope and with a concise description, facilitating understanding and making it more manageable during development."      |
   | Testable     | "Allows the creation of tests to verify its functionality and quality, ensuring its correct implementation and integration."       |

  <figcaption>Figure 1: ecChronos and Cassandra Nodes.</figcaption>
</div>

So based on this ecChronos mainterners have decided to use the follow template to submit issues:

```markdown
Title: [Descriptive Title of the User Story Capitalized Using [Chicago style capitalization](https://capitalizemytitle.com/style/Chicago/).]

Story Description:

It is important to contextualize the objective, which is a task with a purpose. Therefore, it describes the current system's behavior and its proposed improvement/correction. Additionally, it should reference where the potential change should be made if the user story author has this information. This is to avoid asking the author basic questions that could have been avoided by providing more detailed information in the user story.

To determine if the description is good, you can use a golden rule, which is to ask yourself the following question: "If I was not aware of the content of this user story, could I start working just by reading the description and the code?"

Acceptance Criteria:

[Acceptance Criterion 1]: Describe a specific condition that must be met for the user story to be considered complete.

[Acceptance Criterion 2]: Add another condition or requirement that ensures the story's functionality.

[Acceptance Criterion 3]: Continue to list all relevant criteria that need to be satisfied.

Definition of Done:

[List any additional requirements or steps that need to be completed before considering the user story done.]

Notes:

[Include any additional information, constraints, or considerations related to the user story.]
```

## Creating a pull request

1. Ensure that the pull request is targeted at the minimum possible version based on the type of change.
2. If you intend to fix an issue, mention the issue number in the text body of the pull requests, i.e. `Closes #20`.
3. Add a changelog entry at the top of the current target version when handling an issue.
   Additionally, the related issue should be mentioned at the end with a `- Issue #<nr>`.
4. Before creating a pull request, make sure an issue exists (this does not apply to very small changes).
   The reason for this is to have the description of the change in the issue itself without needing to describe
   it in the commit body.
5. All checks must pass before merging a pull request.
6. In general at least one project admin should approve the pull request before merging.

## Merging

Merging is performed from minimum version towards master.
It is the responsibility of the person merging the pull request to make sure it gets merged to master.
