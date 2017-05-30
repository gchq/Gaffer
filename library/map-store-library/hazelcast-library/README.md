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

The master copy of this page is the README in the map-store module.

Hazelcast Library
===================

This module contains a HazelcastMapFactory class to be used with the MapStore.

If you want to use back your MapStore with Hazelcast then add a dependency on this module.

```xml
<groupId>uk.gov.gchq.gaffer</groupId>
<artifactId>hazelcast-library</artifactId>
<version>[gaffer.version]</version>
```

To configure the MapStore to use this hazelcast instance add the following to your store properties file.

```properties
gaffer.store.mapstore.map.factory=uk.gov.gchq.gaffer.mapstore.factory.HazelcastMapFactory
gaffer.store.mapstore.map.factory.config=[path to your hazelcast config]
```

Please note that this has not yet been tested at scale.