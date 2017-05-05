Copyright 2016 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Documentation
======

This doc module is used to generate the documentation for the getting started wiki pages.

To build the full wiki pages run the following and store the output of each command in the relevant wiki page:

```bash
docJar=doc/target/doc-jar-with-dependencies.jar
java -cp $docJar uk.gov.gchq.gaffer.doc.user.walkthrough.UserWalkthroughRunner
java -cp $docJar uk.gov.gchq.gaffer.doc.dev.walkthrough.DevWalkthroughRunner
java -cp $docJar uk.gov.gchq.gaffer.doc.properties.walkthrough.PropertiesWalkthroughRunner

java -cp $docJar uk.gov.gchq.gaffer.doc.operation.OperationExamplesRunner
java -cp $docJar uk.gov.gchq.gaffer.doc.operation.accumulo.AccumuloOperationExamplesRunner
java -cp $docJar uk.gov.gchq.gaffer.doc.predicate.PredicateExamplesRunner
```