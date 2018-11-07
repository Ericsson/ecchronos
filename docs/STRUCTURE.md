# Project structure

## Modules

### application

The standalone application mode of ecChronos.

### karaf-feature

Utility module creating a feature file for OSGi.

### connection

Interfaces that ecChronos rely on to connect to Apache Cassandra.

### connection.impl

A configurable reference implementation of the connections.

### core

The core package handling scheduling and repairs.

### core.osgi

OSGi service wrappers around the different core classes.

### fm

Interfaces that ecChronos rely on for fault reporting.

### fm.impl

A reference implementation of the fault reporter utilizing loggers.

### osgi-integration

OSGi integration tests running with docker.

### standalone-integration

Standalone integration tests running with docker.