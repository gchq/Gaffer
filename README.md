<img align="right" width="300" height="auto" src="logos/logoWithText.png">

# Gaffer

![ci](https://github.com/gchq/Gaffer/actions/workflows/continuous-integration.yaml/badge.svg?branch=develop)
[![codecov](https://codecov.io/gh/gchq/Gaffer/branch/develop/graph/badge.svg?token=D7FRqMeurU)](https://codecov.io/gh/gchq/Gaffer)
[<img src="https://img.shields.io/badge/docs-passing-success.svg?logo=readthedocs">](https://gchq.github.io/gaffer-doc/latest/)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/uk.gov.gchq.gaffer/gaffer2/badge.svg)](https://maven-badges.herokuapp.com/maven-central/uk.gov.gchq.gaffer/gaffer2)

Gaffer is a graph database framework. It allows the storage of very large graphs containing rich properties on the nodes and edges. Several storage options are available, including Accumulo and an in-memory Java Map Store.

It is designed to be as flexible, scalable and extensible as possible, allowing for rapid prototyping and transition to production systems.

Gaffer offers:

 - Rapid query across very large numbers of nodes and edges
 - Continual ingest of data at very high data rates, and batch bulk ingest of data via MapReduce or Spark
 - Storage of arbitrary Java objects on the nodes and edges
 - Automatic, user-configurable in-database aggregation of rich statistical properties (e.g. counts, histograms, sketches) on the nodes and edges
 - Versatile query-time summarisation, filtering and transformation of data
 - Fine grained data access controls
 - Hooks to apply policy and compliance rules to queries
 - Automated, rule-based removal of data (typically used to age-off old data)
 - Retrieval of graph data into Apache Spark for fast and flexible analysis
 - A fully-featured REST API

To get going with Gaffer, visit our getting started pages ([1.x](https://gchq.github.io/gaffer-doc/v1docs/summaries/getting-started.html), [2.x](https://gchq.github.io/gaffer-doc/latest/getting-started/quickstart/)).

Gaffer is under active development. Version 1.0 of Gaffer was released in October 2017, version 2.0 was released in May 2023.

## Getting Started

### Try it out

We have a demo available to try that is based around a small uk road use dataset. See the example/road-traffic [README](https://github.com/gchq/Gaffer/blob/master/example/road-traffic/README.md) to try it out.

### Building and Deploying

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/gchq/Gaffer?quickstart=1)

To build Gaffer run `mvn clean install -Pquick` in the top-level directory. This will build all of Gaffer's core libraries and some examples of how to load and query data.

See our [Store](https://gchq.github.io/gaffer-doc/latest/reference/stores-guide/stores/) documentation page for a list of available Gaffer Stores to chose from and the relevant documentation for each.

### Inclusion in other projects

Gaffer is hosted on [Maven Central](https://mvnrepository.com/search?q=uk.gov.gchq.gaffer) and can easily be incorporated into your own maven projects.

To use Gaffer from the Java API the only required dependencies are the Gaffer graph module and a store module for the specific database technology used to store the data, e.g. for the Accumulo store:

```xml
<dependency>
    <groupId>uk.gov.gchq.gaffer</groupId>
    <artifactId>graph</artifactId>
    <version>${gaffer.version}</version>
</dependency>
<dependency>
    <groupId>uk.gov.gchq.gaffer</groupId>
    <artifactId>accumulo-store</artifactId>
    <version>${gaffer.version}</version>
</dependency>
```

This will include all other mandatory dependencies. Other (optional) components can be added to your project as required.

### Documentation

Our Javadoc can be found [here](http://gchq.github.io/Gaffer/). Gaffer's documentation is kept in the [gaffer-doc](https://github.com/gchq/gaffer-doc) repository and [published on GitHub pages (gchq.github.io)](https://gchq.github.io/gaffer-doc/latest/). 

We have some user guides in our documentation ([1.x](https://gchq.github.io/gaffer-doc/v1docs/getting-started/user-guide/contents.html), [2.x](https://gchq.github.io/gaffer-doc/latest/getting-started/guide/guide/)).

## Related repositories

The [gafferpy](https://github.com/gchq/gafferpy) repository contains a python shell that can execute operations.

The [gaffer-docker](https://github.com/gchq/gaffer-docker) repository contains the code needed to run Gaffer using Docker or Kubernetes.

The [koryphe](https://github.com/gchq/koryphe) repository contains an extensible functions library for filtering, aggregating and transforming data
based on the Java Function API. It is a dependency of Gaffer.

## License

Gaffer is licensed under the Apache 2 license and is covered by [Crown Copyright](https://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/uk-government-licensing-framework/crown-copyright/).  

```
Copyright 2016-2023 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

## Contributing

We welcome contributions to the project. Detailed information on our ways of working can be found [in our developer docs](https://gchq.github.io/gaffer-doc/latest/dev/ways-of-working/). In brief:

- Sign the [GCHQ Contributor Licence Agreement](https://cla-assistant.io/gchq/Gaffer)
- Push your changes to a fork
- Submit a pull request
