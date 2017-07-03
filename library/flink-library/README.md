Copyright 2017 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


Flink Library
============
This module contains various libraries for Apache Flink.

It is currently being developed and is experimental so should not yet be used.

In order to make use of the flink libraries you will need to include these libraries as a dependency:

```
 <dependency>
  <groupId>uk.gov.gchq.gaffer</groupId>
  <artifactId>flink-library</artifactId>
  <version>${gaffer.version}</version>
</dependency>
```

You will then need to register the flink operations and their handlers with your store.
You just need to add the following to your store properties file.
```
gaffer.store.operation.declarations=flinkOperationsDeclarations.json
```