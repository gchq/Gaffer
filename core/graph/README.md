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
GraphHooks should be json serialiseable and each hook should have a unit test 
that extends GraphHookTest.