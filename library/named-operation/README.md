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


Named Operations
================
This module contains Named Operation implementations.

By default Named Operations are disabled. To enable Named Operations you will need to add a dependency on the
named operation implementation you want e.g. named-operation-gaffer-cache:

```
<dependency>
    <groupId>uk.gov.gchq.gaffer</groupId>
    <artifactId>named-operation-gaffer-cache</artifactId>
    <version>${gaffer.version}</version>
</dependency>
```

As well as the base library, which contains all the shared code like the operations and their handlers:

```
<dependency>
   <groupId>uk.gov.gchq.gaffer</groupId>
   <artifactId>named-operation-library</artifactId>
   <version>${gaffer.version}</version>
</dependency>
```


You will then need to register the operations, relevant to the implementation, using the operation declarations file:
```
gaffer.store.operation.declarations=GafferCacheNamedOperationDeclarations.json
```

or by using an absolute path:
```
gaffer.store.operation.declarations=/path/to/GafferCacheNamedOperationDeclarations.json
```
