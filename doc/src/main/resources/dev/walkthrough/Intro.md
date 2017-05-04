## Introduction 

These developer examples will assume you have already read the user walkthroughs.
They aim to provide developers with a bit more information about how to configure a Gaffer Graph.

### Running the Examples

The example can be run in a similar way to the user examples. 

You can download the doc-jar-with-dependencies.jar from [maven central](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22uk.gov.gchq.gaffer%22%20AND%20a%3A%22doc%22). Select the latest version and download the jar-with-dependencies.jar file.
Alternatively you can compile the code yourself by running:

```
mvn clean install -pl doc -am -Pquick
```

The doc-jar-with-dependencies.jar file will be located here: doc/target/doc-jar-with-dependencies.jar.

```bash
# Replace <Visibilities> with your example name.
java -cp doc-jar-with-dependencies.jar uk.gov.gchq.gaffer.doc.dev.walkthrough.Visibilities
```