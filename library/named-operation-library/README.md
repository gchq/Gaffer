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

In order to make use of the Named Operations library you will need to include it as a dependency:

```
 <dependency>
  <groupId>uk.gov.gchq.gaffer</groupId>
  <artifactId>named-operation-library</artifactId>
  <version>${gaffer.version}</version>
</dependency>
```

and your chosen cache service implementation to use for the named operations (see library/cache-library/README.md), e.g for the JCS implementation:

```
<dependency>
   <groupId>uk.gov.gchq.gaffer</groupId>
   <artifactId>jcs-cache-service</artifactId>
   <version>${gaffer.version}</version>
</dependency>
```

This will add all the Operations and their handlers. Each of the handlers use a cache to store the Named Operations.