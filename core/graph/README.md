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


Graph
============

This module contains the Graph. The entry point or proxy for you chosen Gaffer Store.

The Graph separates the user from the underlying store. It holds a connection to the acts as a proxy, delegating Operations to the store.
It provides users with a single point of entry for executing operations on a store. 
This allows the underlying store to be swapped and the same operations can still be applied.

## Instantiating a Graph 
When you instantiate a Graph, this doesn't mean you are creating an entirely new
Graph, you are simply setting up a connection to your Store where you data is held.

To create an instance of Graph, we recommend you use the Graph Builder. This has
several helpful methods to create the Graph from various different sources. But,
essentially a Graph requires just 2 things: some store properties and a schema.

The store properties tell the Graph the type of Store to connect to and the
connection details for how to do that.

The Schema is passed to the Store to instruct the Store how to store and and process the data.
 

## Graph Hooks
The Graph class is final. We want to ensure that all users have a common point of entry
to Gaffer, so all users have to start by instantiating a Graph. Initially this seems
quite limiting, but to allow custom logic for different types of Graphs we have
added Graph Hooks. These Graph Hooks allow custom code to be run before and after
an Operation Chain is executed.

So you can use hooks to do things like custom logging or special operation chain
authorisation. To implement your own hook, just implement the GraphHook interface and
add register it with the Graph when you build a Graph instance.