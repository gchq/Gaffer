  Copyright 2016 Crown Copyright

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

GrafferQL
==================================

GrafferQL is a library that configures the GraphQL-java library using a Gaffer Schema.

This allows you to write GraphQL queries which will be converted to Gaffer operations.

The Gaffer Element properties returned are used to generate GraphQL compliant responses.

Currently the library simply configures an instance graphql.GraphQL with a gaffer.store.schema.Schema.
There is no REST interface for this yet so native Java access is the only way to use it.

Setup
------------------
An example can be seen in graphql.gaffer.GrafferQlTest.

Create a Gaffer graph in the normal way

    Graph graph = new Graph.Builder()
        .storeProperties(...stuff...)
        .addSchemas(...stuff...)
        .build();
        
Use the GafferQLSchemaBuilder class to build a graphql.GraphQL instance

    GraphQL graphQL = new GafferQLSchemaBuilder()
        .gafferSchema(graph.getSchema())
        .build();
        
For each request/operation chain you wish to run, do the following

Construct a User

    User user = new User.Builder()
        .userId("me")
        .dataAuth("see_everything")
                
Build a context for GrafferQL to use, this requires the gaffer.graph.Gaffer graph and the gaffer.user.User

    GrafferQLContext context = new GrafferQLContext.Builder()
        .graph(graph) // will be same value for all requests to this graph
        .user(user) // potentially different for each request, 
        .build();
                
Reset the GrafferQLContext if being reused across requests

    context.reset();
  
Compose your GraphQL query

    String query = "{person(vertex:\"user03\"){name{value} age{value}}}"
  
Run the query

    ExecutionResult result = graphQL.execute(query, context);
  
Interrogate the Execution Result for data, it will be a nest of HashMap<String, Object>.

Notes
------------------
The GraphQL queries must use escaped double quotes rather than single quotes, this is imposed by GraphQL-java
All properties are currently returned as Strings
The only filtering that currently exists is the initial seed.

The Shape of Queries
------------------
In a GrafferQL schema, Edges, Entities, Vertices and Properties are all treated as Object Types.

This allows you to hop from a property that happens to be the same data type as a vertex to another Entity.

E.g for the films example schema
  
I can write a query that returns the vertex value, certificate and name of a film

    {film(vertex:"filmC"){vertex{value} certificate{value} name{value}}}

This shows me jumping from Person (Entity) along Viewing (Edge) to a Film (Entity)
picking up the startTime, and film name.

    {person(vertex:"user01"){vertex{viewing{startTime{value} destination{film{name{value}}}}}}}

Why do the properties need to be referenced by {value}? Some properties are of types that are also
used as vertices. For example a film review has a property called userId. But because I used the GraphQLObject type
for the vertex as the property I can hop out from that property value to Entities attached to that vertex.

This query shows me jumping from review (entity) -> (vertex -> film -> name.value) and (userId -> person -> name.value)

    {review(vertex:"filmA"){vertex{film{name{value}}} userId{person{name{value}}}}}

Queries can start at any Element (Entity or Edge) and must be given the appropriate vertexes to work with.
For instance, to start a query at an Entity you must provide a value for 'vertex'.

    {film(vertex:"filmA"):{name{value}}}
  
To get Edges you must provide either
1. The source vertex as 'source'
2. The destination vertex as 'destination'
3. Both 'source' and 'destination'
4. Either source or destination as 'vertex'

    {viewing(source:"user01"){friendsSince}} // Anybody viewing by user01
    {viewing(destination:"filmB"){friendsSince}} // Any viewing of filmA
    {viewing(source:"user03", destination:"filmC"){friendsSince}} // search for a specific relationship
    {viewing(vertex:"user04"){startTime}} // effectively a bi-directional search on user02
  
Performance
------------------
The GrafferQL library will run a separate operation chain for each link in the traversal. This means that queries incur
a lot of round trips. The GraphQL query is not optimised in any way.

The extent to which this is a problem will be governed by the cost of round trips for the Store that backs your Gaffer Graph,
and how deep and broad the traversals of your queries are likely to be.

The GrafferQL library effectively trades performance for ease of use.
