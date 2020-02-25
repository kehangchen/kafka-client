# NCR Streaming Framework Library

This library contains all the common modules and classes for developing either Kafka or Spark streaming applications.  Its goal is to reduce the common code that an application developer needs to write and let him/her to emphasize on writing business logic instead.

## Usage

If this library has not yet been pushed into an artifact repository, such as Nexus in NCR, you will need to install this library in your local environment with ``mvn clean install`` after cloning this project.  Then, you add the following section of the maven dependency into your project pom.xml,
```
    <dependency>
        <groupId>com.ncr.stream.frwk</groupId>
        <artifactId>streaming-utils</artifactId>
        <version>0.1</version>
    </dependency>
```

## Suggestion

* ``KafkaStreaming`` class that implements the common function for connecting to KaFka should be moved to this library;
* ``businessLogicProcess`` function should have its default implementation in the ``KafkaStreaming`` class.  If application developers want to have their own behaviours, they can override this default implementation in their sub class.
