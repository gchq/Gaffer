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

1. Create a Gaffer graph in the normal way

  Graph graph = new Graph.Builder()
        .storeProperties(...stuff...)
        .addSchemas(...stuff...)
        .build();
        
2. Use the GafferQLSchemaBuilder class to build a graphql.GraphQL instance

  GraphQL graphQL = new GafferQLSchemaBuilder()
        .gafferSchema(graph.getSchema())
        .build();
        
For each request/operation chain you wish to run, do the following
1. Construct a User to act as

  User user = new User.Builder()
        .userId("me")
        .dataAuth("see_everything")
                
2. Build a context for GrafferQL to use, this requires
  a. The gaffer.graph.Gaffer graph
  b. The gaffer.user.User

  GrafferQLContext context = new GrafferQLContext.Builder()
        .graph(graph) // will be same value for all requests to this graph
        .user(user) // potentially different for each request, 
        .build();
                
3. Reset the GrafferQLContext if being reused across requests

  context.reset();
  
4. Compose your GraphQL query

  String query = "{person(vertex:\"user03\"){name{value} age{value}}}"
  
5. Run the query

  ExecutionResult result = graphQL.execute(query, context);
  
6. Interrogate the Execution Result for data, it will be a nest of HashMap<String, Object>.

Notes
------------------
The GraphQL queries must use escaped double quotes rather than single quotes, this is imposed by GraphQL-java
All properties are currently returned as Strings
The only filtering that currently exists is the initial seed.

The Shape of Queries
------------------
In a GrafferQL schema, Edges, Entities, Vertices and Properties are all treated as Object Types.

This allows you to hop from a property that happens to be the same data type as a vertex to another Entity.

E.g for a schema that contains the following
Vertex Type: userId.string
Entity Type: User (with vertex of userId.string)
  property: name (name.string)
Entity: FilmReview (with vertex of filmId.string)
  Property: userId (userId.string)
  
I can write a query

  {FilmReview(vertex:"filmA"): {userId{User{name{value}}}}}

This shows me jumping from a property (Review.userId) to an Entity (User).

The flip side of this is that every property must be dereferenced with {value} to read the value.
So to see the name and certificate of a film you have to write

  {film(vertex:"filmA"){name{value} certificate{value}}}
  
Queries can start at any Element (Entity or Edge) and must be given the appropriate vertexes to work with.
For instance, to start a query at an Entity you must provide a value for 'vertex'.

  {Film(vertex:"filmA"):{name{value}}}
  
To get Edges you must provide either
1. The source vertex as 'source'
2. The destination vertex as 'destination'
3. Both 'source' and 'destination'
4. Either source or destination as 'vertex'

E.g (assuming FriendsWith is directional)

  {FriendsWith(source:"user01"){friendsSince}} // Anybody that user01 is friends with
  {FriendsWith(destination:"user03"){friendsSince}} // Anybody who is friends with user03
  {Viewing(source:"user01", destination:"user03"){friendsSince}} // search for a specific relationship
  {Viewing(vertex:"user02"){friendsSince}} // effectively a bi-directional search on user02
  
Performance
------------------
The GrafferQL library will run a separate operation chain for each link in the traversal. This means that queries incur
a lot of round trips. The GraphQL query is not optimised in any way.

The extent to which this is a problem will be governed by the cost of round trips for the Store that backs your Gaffer Graph,
and how deep and broad the traversals of your queries are likely to be.

The GrafferQL library effectively trades performance for ease of use.
