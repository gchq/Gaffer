Copyright 2017-2018 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


Proxy Store
============

The `ProxyStore` implementation is simply a Gaffer store which delegates all
operations to a Gaffer REST API.

To create a `ProxyStore` you just need to provide a host, port and context
root. This can be done via the `ProxyStore.Builder`:

```java
Graph graph = new Graph.Builder()
    .store(new ProxyStore.Builder()
            .graphId(uniqueNameOfYourGraph)
            .host("localhost")
            .port(8080)
            .contextRoot("rest/v1")
            .build())
    .build();
```

You can then write your queries in Java and the `ProxyStore` will convert
them into JSON and execute them over the REST API.

These are the full set of configurable properties:

```properties
gaffer.host
gaffer.port
gaffer.context-root
gaffer.jsonserialiser.class

# Timeouts specified in milliseconds
gaffer.connect-timeout
gaffer.read-timeout
```
