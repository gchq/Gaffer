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

Cache
=====
In the cache module you will find the `CacheServiceLoader` which is
started when the store is initialised. The cache service loader can be
called when a component needs access to short term data storage. To
get access to the cache service you need to call:
```java
CacheServiceLoader.getService();
```
You can change what service is returned by the service loader by adding
a line to the store.properties file:
```
gaffer.cache.service.class=uk.gov.gchq.gaffer.cache.impl.HashMapCacheService
```
If needs be you can add an additional configuration file which will
contain properties for the cache itself:
```
gaffer.cache.config.file=/path/to/file
```

By default there is no service loaded so if your using a component that
makes use of the `CacheServiceLoader`, be sure to specify the service class
in the store.properties file.

If using an external cache service (anything found in the cache library) be
sure to include the library as a dependency:
```
<dependency>
   <groupId>uk.gov.gchq.gaffer</groupId>
   <artifactId>jcs-cache-service</artifactId>
   <version>${gaffer.version}</version>
</dependency>
```