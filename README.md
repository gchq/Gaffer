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

<img src="logos/logoWithText.png" width="300">

Gaffer
======
[![Build Status](https://travis-ci.org/gchq/Gaffer.svg?branch=master)](https://travis-ci.org/gchq/Gaffer)
[![codecov](https://codecov.io/gh/gchq/Gaffer/branch/master/graph/badge.svg)](https://codecov.io/gh/gchq/Gaffer)

Gaffer is built for very large graphs.

It's designed to be as flexible, scalable and extensible as possible, allowing for rapid prototyping and transition to production systems.

Gaffer does 

 - rapid query across very large numbers of entities and relationships,
 - versatile query-time summarisation, filtering and transformation of data,
 - in-database aggregation of rich statistical properties describing entities and relationships,
 - scalable ingest at very high data rates and volumes,
 - automated, rule-based data purge,
 - fine grained data access and query execution controls.

Gaffer can be run on various databases, including Accumulo and HBase. It is also integrated with Spark for fast and flexible data analysis.

To get going with Gaffer, visit our [getting started pages](https://github.com/GovernmentCommunicationsHeadquarters/Gaffer/wiki/Getting-Started).

Gaffer is still under active development and isn't a finished product yet. There are still plenty of new features
to be added and additional documentation to write. Please contribute.

Getting Started
---------------

### Try it out

We have a demo available to try that is based around a small uk road use dataset. See the example/road-traffic [README](https://github.com/gchq/Gaffer/blob/master/example/road-traffic/README.md) to try it out.

### Building and Deploying

To build Gaffer run `mvn clean install -Pquick` in the top-level directory. This will build all of Gaffer's core libraries and some examples of how to load and query data.

See our [wiki](https://github.com/gchq/Gaffer/wiki) for a list of available Gaffer Stores to chose from and the relevant documentation for each.

### Inclusion in other projects

Gaffer is hosted on [Maven Central](https://mvnrepository.com/search?q=uk.gov.gchq.gaffer) and can easily be incorporated into your own projects.

To use Gaffer the only required dependencies are the Gaffer graph module and a store module which corresponds to the data storage framework to utilise (currently limited to Apache Accumulo):

```
<dependency>
    <groupId>uk.gov.gchq.gaffer.core</groupId>
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

Our Javadoc can be found [here](http://gchq.github.io/Gaffer/).

We have some user guides on our [wiki](https://github.com/gchq/Gaffer/wiki). 

### Contributing

We have some detailed information on our ways of working [pages](https://github.com/gchq/Gaffer/wiki/Ways-of-Working)

But in brief:

- Sign the [GCHQ Contributor Licence Agreement](https://github.com/gchq/Gaffer/wiki/GCHQ-OSS-Contributor-License-Agreement-V1.0)
 - Push your changes to your fork.
 - Submit a pull request.
