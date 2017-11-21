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
GraphHooks should be json serialisable and each hook should have a unit test 
that extends GraphHookTest.

## GraphConfig
When a `Graph` is created you must supply it with a `GraphConfig`. This
`GraphConfig` contains a full configuration for a `Graph`, and is used
alongside a `Schema` and `StoreProperties` to create the `Graph`. 
To create an instance of `GraphConfig` you can use the `GraphConfig.Builder`
class, or create it using a json file.
The `GraphConfig` class contains:
 
 - `graphId` - The `graphId` is a String field that uniquely identifies a `Graph`. 
   This is a required field. When backed by a Store like Accumulo, this `graphId`
   is used as the name of the Accumulo table for the `Graph`.
   
 - `View` - The `View` defines the Elements to be returned when an Operation is run
   on the `Graph`. For example if you want your `Graph` to only show data with a count
   more than 10. you can define a `View` that has a global filter like this:
   ${JSON_CODE}
   "postAggregationFilterFunctions" : [ {
         "predicate" : {
           "class" : "uk.gov.gchq.koryphe.impl.predicate.IsLessThan",
           "orEqualTo" : false,
           "value" : "10"
         },
         "selection" : [ "ExamplePropertyName" ]
       } ]
   ${END_CODE}
   This would be merged into to all queries so users only see this particular view of the data.
   
 - `GraphLibrary` - This contains information about the `Schema` and `StoreProperties` to be used.
   For more Information please see <insert GraphLibrary link>.
   
 - `Description` - Simple String describing the `Graph`.
 
 - `GraphHooks` - A list of `GraphHook` that will be triggered before and after operation chains on the `Graph`.
 
 Here is a full Java example of a `GraphConfig`, and below a json config example:
 
 ${START_JAVA_CODE}
  new GraphConfig.Builder()
        .graphId("exampleGraphId")
        .view(new View.Builder()
                .edge("ExampleEdgeGroup", new ViewElementDefinition.Builder()
                        .postAggregationFilter(new ElementFilter.Builder()
                            .select("ExamplePropertyName")
                            .execute(new IsLessThan("10"))
                            .build())
                        .build())
                .build())
        .library(new HashMapGraphLibrary())
        .description("Example Graph")
        .addHooks("path/to/hooks")
      .build();
 
 ${JSON_CODE}
 {
   "graphId": "exampleGraphId",
   "description": "Example Graph",
   "library": {
     "class": "uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary"
   },
   "view": {
     "edges" : {
       "ExampleEdgeGroup" : {
         "postAggregationFilterFunctions" : [ {
           "predicate" : {
             "class" : "uk.gov.gchq.koryphe.impl.predicate.IsLessThan",
             "orEqualTo" : false,
             "value" : "10"
           },
           "selection" : [ "ExamplePropertyName" ]
         } ]
       }
     },
     "entities" : { }
   },
   "hooks": [
     {
       "class": "uk.gov.gchq.gaffer.graph.hook.AddOperationsToChain",
       "before": {
         "uk.gov.gchq.gaffer.operation.impl.output.ToArray": [
           {
             "class": "uk.gov.gchq.gaffer.operation.impl.Limit",
             "resultLimit": 1000,
             "truncate": false
           }
         ]
       },
       "after": {
         "uk.gov.gchq.gaffer.operation.impl.get.GetAllElements": [
           {
             "class": "uk.gov.gchq.gaffer.operation.impl.Limit",
             "resultLimit": 1000,
             "truncate": false
           }
         ]
       }
     }
   ]
 }
 ${END_CODE}
 