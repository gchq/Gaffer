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


Spark Library
============
This module contains various spark libraries for Gaffer.

In order to make use of the spark libraries you will need to include these libraries as a dependency:

```
 <dependency>
  <groupId>uk.gov.gchq.gaffer</groupId>
  <artifactId>spark-library</artifactId>
  <version>${gaffer.version}</version>
</dependency>
```

You will then need to register the spark operations and their handlers with your store.
Currently only spark operation handlers for the Accumulo Store have been written.
To use spark with accumulo you will need to include this dependency:
```
 <dependency>
  <groupId>uk.gov.gchq.gaffer</groupId>
  <artifactId>spark-accumulo-library</artifactId>
  <version>${gaffer.version}</version>
</dependency>
```

Then to register the spark operations with the accumulo store you just need
to add the following to your store properties file.
```
gaffer.store.operation.declarations=sparkAccumuloOperationsDeclarations.json
```