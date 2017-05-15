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

Named Operation Library
=======================
This module contains the Named Operations library for Gaffer.

In order to make use of the Named Operations library you will need
to include it as a dependency:

```
 <dependency>
  <groupId>uk.gov.gchq.gaffer</groupId>
  <artifactId>named-operation-library</artifactId>
  <version>${gaffer.version}</version>
</dependency>
```

Then you need to add all the named operations by referencing them in the
store properties file:
```
gaffer.store.operation.declarations=NamedOperationDeclarations.json
```

This will add all the Operations and their handlers. Each of the
handlers use a cache to store the Named Operations.

Named Operations depends on the Cache service being active at runtime.
In order for the cache service to run you must select your desired
implementation. You do this by adding another line to the store.properties
file:
```
gaffer.cache.service.class=uk.gov.gchq.gaffer.cache.impl.HashMapCacheService
```

To find out more about the different cache services on offer, see the
Cache Library README.