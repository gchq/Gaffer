## Introduction 

These developer examples will assume you have already read the user walkthroughs.
They aim to provide developers with a bit more information about how to configure a Gaffer Graph.

## Running the Examples

The example can be run in a similar way to the user examples.

* The examples can be run directly from the IDE of your choice, or
* The examples JAR file can be compiled and the examples can be executed from the command line.

In order to run the examples from the command line, first the examples JAR must be compiled:

```bash
mvn clean install -Pquick -PdevDocJar
```

The examples can then be run from the JAR, for instance

```bash
java -cp doc/dev-doc/target/dev-doc-jar-with-dependencies.jar uk.gov.gchq.gaffer.doc.dev.walkthrough.Visibilities
```