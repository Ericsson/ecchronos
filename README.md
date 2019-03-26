# ecChronos
[![build](https://travis-ci.org/Ericsson/ecchronos.svg?branch=master)](https://travis-ci.org/Ericsson/ecchronos)
[![coverage](https://coveralls.io/repos/github/Ericsson/ecchronos/badge.svg?branch=master)](https://coveralls.io/github/Ericsson/ecchronos?branch=master)
[![maven central](https://img.shields.io/maven-central/v/com.ericsson.bss.cassandra.ecchronos/ecchronos-binary.svg?label=maven%20central)](https://search.maven.org/search?q=g:%22com.ericsson.bss.cassandra.ecchronos%22%20AND%20a:%22ecchronos-binary%22)

ecChronos is a decentralized scheduling framework primarily focused on performing automatic repairs in Apache Cassandra.

## Getting started

See the [ARCHITECTURE.md](docs/ARCHITECTURE.md) for details on ecChronos architecture.

### Prerequisites

* Maven
* JDK8
* Docker (for test setup)
* Python

### Installing

For installation instructions see [SETUP.md](docs/SETUP.md).

## Running the tests

There are a few different tests that can be run:

* Unit tests
* Docker tests
  * Integration tests
  * Acceptance tests

The full test suite can be run by `mvn verify -P docker-integration-test,osgi-integration-tests,standalone-integration-tests,python-integration-tests`.
As Travis is used to verify pull requests the full test suite is shown in [.travis.yml](.travis.yml).

### Unit tests

The unit tests are run by `mvn clean test`.
They are running simple tests that sometimes utilize a single embedded Cassandra node.

### Docker tests

The acceptance tests and integration tests use docker instances by default.
The docker containers gets started when the `-P docker-integration-test` flag is used in maven.

#### Integration tests

The integration tests tries to start ecChronos with a cluster of nodes and verfiy that repairs are run.
They are activated by using `-P osgi-integration-tests` or `-P standalone-integration-tests`.
It is possible to run either OSGi integration tests or the standalone tests without the other.
This can be done by running either `mvn verify -P docker-integration-test,osgi-integration-tests` or `mvn verify -P docker-integration-test,standalone-integration-tests`.

##### Maven configuration properties

| Property                   | Default    | Description                                              |
|----------------------------|------------|----------------------------------------------------------|
| it.cassandra.memory        | 1073741824 | Memory limit for the docker instance                     |
| it.cassandra.heap          | 256M       | Amount of heap memory Cassandra should use at most       |

##### Running within IDEA/Eclipse

If you already have a three node cluster setup (through docker, ccm, etc) this can be used instead.

Make sure to run the following command (with correct data centers specified in the create_keyspaces.cql) to prepare the cluster.
```
cqlsh -f standalone-integration/src/test/resources/create_keyspaces.cql
```

The test cases needs to be run with the following system properties set:

| Variable                 |  Description                                  |
|--------------------------|-----------------------------------------------|
| it-cassandra.ip          | The host ip of one of the Cassandra instances |
| it-cassandra.native.port | The native port used                          |
| it-cassandra.jmx.port    | The JMX port used                             |

#### Acceptance tests

The acceptance test use behave to verify the python scripts as well as the REST server in ecChronos.
They are activated by using `-P python-integration-tests` in combination with the docker flag.

### Break down into end to end tests

The `standalone-integration` tests runs a setup similar to the standalone application to verify automated repairs.
The `osgi-integration` tests runs in a OSGi environment to verify that all services are available and verifies automated repairs.

## Deployment

For deployment on a running system read [SETUP.md](docs/SETUP.md) for details on how to setup your system to enable ecChronos to run.

## Built with

* [Maven](https://maven.apache.org) - Dependency and build management
* [docker-maven-plugin](https://github.com/fabric8io/docker-maven-plugin) - For integration tests

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We try to adhere to [SemVer](http://semver.org) for versioning.
* Anything requiring changes to configuration or plugin APIs should be released in a new major version.
* Anything extending configuration or plugins in a backwards compatible way should be released in a new minor version.
* Bug fixes should be made for the first known version and merged forward.

## Authors

* **Marcus Olsson** - *Initial work* - [emolsson](https://github.com/emolsson)

See also the list of [contributors](https://github.com/ericsson/ecchronos/contributors) who participated in this project.

## License

This project is licensed under the Apache License - see the [LICENSE.md](LICENSE.md) file for details
