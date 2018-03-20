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

When you instantiate a `Graph`, this doesn't mean you are creating an entirely
new graph with its own data, you are simply creating a connection to a store
where some data is held.

To create an instance of `Graph`, we recommend you use the `Graph.Builder`
class. This has several helpful methods to create the graph from various
different sources. But, essentially a graph requires just 3 things: some
store properties, a schema and some graph specific configuration.

## Store Properties
The store properties tells the graph the type of store to connect to
along with any required connection details. See [Stores](https://gchq.github.io/gaffer-doc/summaries/stores.html) for more information on the different Stores for Gaffer.

## Schema
The schema is passed to the store to instruct the store how to store
and process the data. See [Schemas](https://gchq.github.io/gaffer-doc/getting-started/developer-guide/schemas.html) for more information.

## Graph Configuration
The graph configuration allows you to apply special customisations to the Graph instance.
The only required field is the `graphId`.

To create an instance of `GraphConfig` you can use the `GraphConfig.Builder`
class, or create it using a json file.

The `GraphConfig` can be configured with the following:
 - `graphId` - The `graphId` is a String field that uniquely identifies a `Graph`. 
   When backed by a Store like Accumulo, this `graphId`
   is used as the name of the Accumulo table for the `Graph`.
 - `description` - a string describing the `Graph`.
 - `view` - The `Graph View` allows a graph to be configured to only returned a subset of Elements when any Operation is executed.
   For example if you want your `Graph` to only show data that has a count
   more than 10 you could add a View to every operation you execute, or you can
   use this `Graph View` to apply the filter once and it would be merged into to all
   Operation Views so users only ever see this particular view of the data.
 - `library` - This contains information about the `Schema` and `StoreProperties` to be used.
 - `hooks` - A list of `GraphHook`s that will be triggered before, after and on
   failure when operations are executed on the `Graph`.
   See [GraphHooks](#graph-hooks) for more information.
 
Here is an example of a `GraphConfig`:
 
```java
new GraphConfig.Builder()
    .graphId("exampleGraphId")
    .description("Example Graph description")
    .view(new View.Builder()
            .globalElements(new GlobalViewElementDefinition.Builder()
                    .postAggregationFilter(new ElementFilter.Builder()
                        .select("ExamplePropertyName")
                        .execute(new IsLessThan("10"))
                        .build())
                    .build())
            .build())
    .library(new FileGraphLibrary())
    .addHook(new Log4jLogger())
  .build();
```

and in json:

```json
{
  "graphId": "exampleGraphId",
  "description": "Example Graph description",
  "view": {
    "globalElements": [
      {
        "postAggregationFilterFunctions": [
          {
            "predicate": {
              "class": "uk.gov.gchq.koryphe.impl.predicate.IsLessThan",
              "orEqualTo": false,
              "value": "10"
            },
            "selection": ["ExamplePropertyName"]
          }
        ]
      }
    ]
  },
  "library": {
      "class": "uk.gov.gchq.gaffer.store.library.FileGraphLibrary"
  },
  "hooks": [
    {
      "class": "uk.gov.gchq.gaffer.graph.hook.Log4jLogger"
    }
  ]
}
```

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
