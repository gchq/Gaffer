Copyright 2016-2017 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Named Operations JCS cache Implementation
============
This module contains JCS implementation of a cache used to store NamedOperations.

In order to make use of the Named Operations library you will need to include this Library as a dependency:

```
 <dependency>
  <groupId>uk.gov.gchq.gaffer</groupId>
  <artifactId>named-operation-jcs-cache</artifactId>
  <version>${gaffer.version}</version>
</dependency>
```

In order to use this particular implementation of NamedOperations, be sure to add this line to your store properties
file:

```
gaffer.store.operation.declarations=JCSNamedOperationDeclarations.json
```