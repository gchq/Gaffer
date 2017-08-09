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

This page has been copied from the Graph module README. To make any changes please update that README and this page will be automatically updated when the next release is done.


Graph
============

This module contains the Gaffer `Graph` object and related utilities. This
is the entry point (or proxy) for your chosen Gaffer store.

The `Graph` separates the user from the underlying store. It holds a connection
which acts as a proxy, delegating operations to the store.
It provides users with a single point of entry for executing operations
on a store. This allows the underlying store to be swapped and the same
operations can still be applied.

## Instantiating a Graph 
When you instantiate a `Graph`, this doesn't mean you are creating an entirely
new graph with its own data, you are simply creating a connection to a store
where some data is held.

To create an instance of `Graph`, we recommend you use the `Graph.Builder`
class. This has several helpful methods to create the graph from various
different sources. But, essentially a graph requires just 2 things: some
store properties and a valid schema.

The store properties tells the graph the type of store to connect to
along with any required connection details.

The schema is passed to the store to instruct the store how to store
and process the data.
 

## Graph Hooks
The `Graph` class is final and must be used when creating a new connection
to a store. We want to ensure that all users have a common point of entry
to Gaffer, so all users have to start by instantiating a `Graph`. Initially
this seems quite limiting, but to allow custom logic for different types
of graphs we have added graph hooks. These graph hooks allow custom code
to be run before and after an operation chain is executed.

You can use hooks to do things like custom logging or special operation
chain authorisation. To implement your own hook, just implement the `GraphHook`
interface and register it with the graph when you build a `Graph` instance.

## FAQs
Here are some frequently asked questions.

#### How can I export to another graph using User authorisations?

To use an example of the authorised Graph exporter within the road-traffic-demo using the proxy-store, follow these steps.

-Clone Gaffer twice, instance 1 and instance 2.

-On 1:

6.  Add store2 properties to the GraphLibrary
Add a file containing the properties below to road-traffic-demo/src/main/resources/graphLibrary/store2StoreProps.properties

```properties
    gaffer.store.properties.class=uk.gov.gchq.gaffer.proxystore.ProxyProperties
    gaffer.store.class=uk.gov.gchq.gaffer.proxystore.ProxyStore
    gaffer.host=localhost
    gaffer.context-root=/rest/v1
    gaffer.port=8081
```

2.  Start 1:
    `mvn clean install -Pquick -Proad-traffic-demo -pl :road-traffic-demo –am`


-On 2:

1.  Remove the following from road-traffic-demo/pom.xml to stop the demo data being added:
```xml
    <roadTraffic.dataLoader.dataPath>
        ${project.build.outputDirectory}/roadTrafficSampleData.csv
    </roadTraffic.dataLoader.dataPath>
```

2.  In the same pom (road-traffic-demo/pom.xml) update the port to 8081:
```xml
    <standalone-port>8081</standalone-port>
```

3.  Start 2:
    `mvn clean install -Pquick -Proad-traffic-demo -pl :road-traffic-demo –am`
    
-Navigate to localhost:8080/rest (Graph 1) and go to operations -> /graph/doOperation, then insert:
 ```json
 {
    "operations": [
        {
            "class": "uk.gov.gchq.gaffer.operation.impl.get.GetAllElements",
            "view": {
                "edges": {
                    "JunctionLocatedAt": {}
                }
            }
       },
       {
            "class": "uk.gov.gchq.gaffer.operation.impl.Limit",
            "resultLimit": 1
       },
       {
            "class" : "uk.gov.gchq.gaffer.operation.export.graph.ExportToOtherAuthorisedGraph",
            "graphId" : "roadTraffic2",
            "parentStorePropertiesId" : "store2",
            "parentSchemaIds" : ["roadTraffic"]
       }
   ]
 }
 ```
 
 This will check the opAuths of the User (the ones set when the User 
 was created) against the allowed opAuths specified in the OperationDeclarations 
 file that we modified.
 
 It will then export one element to Graph B, on localhost:8081/rest. 
 We can check by going to the same place, operations graph/DoOperation 
 and click example json.  That will getAllElements with a limit of 1. 
 This should show an element that has been successfully exported!
 