# ecChronos
[![build](https://travis-ci.org/Ericsson/ecchronos.svg?branch=master)](https://travis-ci.org/Ericsson/ecchronos)
[![coverage](https://coveralls.io/repos/github/Ericsson/ecchronos/badge.svg?branch=master)](https://coveralls.io/github/Ericsson/ecchronos?branch=master)
[![maven central](https://img.shields.io/maven-central/v/com.ericsson.bss.cassandra.ecchronos/ecchronos-binary.svg?label=maven%20central)](https://search.maven.org/search?q=g:%22com.ericsson.bss.cassandra.ecchronos%22%20AND%20a:%22ecchronos-binary%22)

ecChronos is a decentralized scheduling framework primarily focused on performing automatic repairs in Apache Cassandra.

## Getting started

See the [STRUCTURE.md](docs/STRUCTURE.md) and [ARCHITECTURE.md](docs/ARCHITECTURE.md) for details on ecChronos project structure and architecture.

### Prerequisites

* JDK8
* Docker (for test setup)

### Installing

For installation instructions see [SETUP.md](docs/SETUP.md).

## Running the tests

There are two different test suites that can be run.
The first suite consist only of unit tests which can be run by `mvn clean install`.
These test will in some cases use an embedded Apache Cassandra.

The second suite consist of the unit tests as well as integration tests which can be run by `mvn clean install -P osgi-integration-tests,standalone-integration-tests`.
The integration tests start docker instances of Apache Cassandra to get a cluster environment where repair can run.
It is possible to run either OSGi integration tests or the standalone tests without the other.
This can be done by running either `mvn clean install -P osgi-integration-tests` or `mvn clean install-P standalone-integration-tests`.

### Maven configuration properties

| Property                   | Default    | Description                                              |
|----------------------------|------------|----------------------------------------------------------|
| it.cassandra.memory        | 1073741824 | Memory limit for the docker instance                     |
| it.cassandra.heap          | 256M       | Amount of heap memory Cassandra should use at most       |
| it.cassandra.ring.delay.ms | 2000       | Bootstrap delay in Cassandra (-Dcassandra.ring_delay_ms) |

### Running within IDEA/Eclipse

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
