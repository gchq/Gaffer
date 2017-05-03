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

Cache Library
=============
The cache library contains the cache service that other components may use when storing data temporarily.
In order to make use of the cache service you must depend on the cache service:

```
<dependency>
    <groupId>uk.gov.gchq.gaffer</groupId>
    <artifactId>cache-service</artifactId>
    <version>${gaffer.version}</version>
</dependency>
```

Then when you want to use the cache service you simply call:

```
CacheServiceLoader.getService()
```

From this point you have access to all the basic CRUD operations (Create, Read, Update, Delete) as you would expect in a cache,
as well as a clear method. You could also use:

```
ICache cache = CacheServiceLoader.getService().getCache("my cache name");
```

However, unless you are always using the same object to call the cache, there is a chance of a disparity between the caches. Therefore it is always recommended
to use the CRUD operations that exist on the cache service.

The default cache service is backed by HashMaps and will boot if no configuration is specified. This will however offer no distribution or disk backup, should your system
go offline. If there is a requirement for these features then it is recommended to use a different cache implementation such as JCS or Hazelcast. In order to use these,
make sure to include them in your dependencies:

```
<dependency>
   <groupId>uk.gov.gchq.gaffer</groupId>
   <artifactId>jcs-cache-service</artifactId>
   <version>${gaffer.version}</version>
</dependency>
```

You will then need to tell the service loader to provide the JCS implementation of the cache service. You do this by adding the following line to gaffer.properties:
```
gaffer.cache.service.class=uk.gov.gchq.gaffer.cache.impl.JcsCacheService
```

These implementations will all have default configurations. If your application needed a specific configuration - you need to add a cache config file. You do this
by adding the following to gaffer.properties:
```
gaffer.cache.config=/path/to/config/file
```