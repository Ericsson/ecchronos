# Standalone

## Configuration

The standalone ecChronos can be configured through the file `conf/ecc.yml`.

## Custom connection providers

In order to use custom connection providers in the standalone version of ecChronos there are a few things that needs to be considered.

1. There must be a default constructor that takes `Config` as an argument.
2. The native connection provider must use `DataCenterAwarePolicy` which is located in the `connection` module.

The provided configuration argument is read from the file `ecc.yml` so that custom connection providers could use the same configuration file if needed.

Examples of implementations can be found in:

* [DefaultNativeConnectionProvider](../application/src/main/java/com/ericsson/bss/cassandra/ecchronos/application/DefaultNativeConnectionProvider.java)
  * Which uses [LocalNativeConnectionProvider](../connection.impl/src/main/java/com/ericsson/bss/cassandra/ecchronos/connection/impl/LocalNativeConnectionProvider.java)
* [DefaultJmxConnectionProvider](../application/src/main/java/com/ericsson/bss/cassandra/ecchronos/application/DefaultJmxConnectionProvider.java)
  * Which uses [LocalJmxConnectionProvider](../connection.impl/src/main/java/com/ericsson/bss/cassandra/ecchronos/connection/impl/LocalJmxConnectionProvider.java)
* [NoopStatementDecorator](../application/src/main/java/com/ericsson/bss/cassandra/ecchronos/application/NoopStatementDecorator.java)

In order to use the custom connection providers simply drop in the jar file in the `lib` directory of ecChronos and specify the full class name of the respective connection providers.