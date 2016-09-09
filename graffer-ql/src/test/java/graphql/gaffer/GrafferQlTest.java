/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package graphql.gaffer;

import gaffer.commonutil.StreamUtil;
import gaffer.example.films.analytic.LoadAndQuery;
import gaffer.example.films.data.Certificate;
import gaffer.graph.Graph;
import gaffer.graphql.GafferQLSchemaBuilder;
import gaffer.graphql.GrafferQLContext;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.OperationChain;
import gaffer.user.User;
import graphql.ExecutionResult;
import graphql.GraphQL;
import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * This is an integration test for the entire library.
 */
public class GrafferQlTest {
    private static final Logger LOGGER = Logger.getLogger(GrafferQlTest.class);

    @Test
    public void test() throws Exception {

        // Setup User
        final User user = new User.Builder()
                .userId("user02")
                .dataAuth(Certificate.U.name())
                .dataAuth(Certificate.PG.name())
                .dataAuth(Certificate._12A.name())
                .dataAuth(Certificate._15.name())
                .dataAuth(Certificate._18.name())
                .build();
        final JSONSerialiser json = new JSONSerialiser();

        // Setup graph
        final Graph graph = new Graph.Builder()
                .storeProperties(StreamUtil.openStream(LoadAndQuery.class, "/example/films/mockaccumulostore.properties"))
                .addSchemas(StreamUtil.openStreams(LoadAndQuery.class, "/example/films/schema"))
                .build();

        // Populate graph with some films data
        final OperationChain<Void> populateChain = json.deserialise(StreamUtil.openStream(LoadAndQuery.class, "/example/films/json/load.json"), OperationChain.class);
        graph.execute(populateChain, user); // Execute the populate operation chain on the graph

        // Build GraphQL based on Gaffer Graph
        final GraphQL graphQL = new GafferQLSchemaBuilder()
                .gafferSchema(graph.getSchema())
                .build();

        // Build a context for GrafferQL
        final GrafferQLContext context = new GrafferQLContext.Builder()
                .graph(graph)
                .user(user)
                .build();

        // Viewing Schema
        runGraphQL(graphQL, context, "{__schema{types{name}}}");

        // Look for a film filmC
        runGraphQL(graphQL, context, "{film(vertex:\"filmC\"){vertex{value} certificate{value} name{value}}}");

        // Look for a person user03
        runGraphQL(graphQL, context, "{person(vertex:\"user03\"){name{value} age{value}}}");

        // Look for reviews for filmA
        runGraphQL(graphQL, context, "{review(vertex:\"filmA\"){userId{value} rating{value}}}");

        // Look for reviews, any vertex of user01
        runGraphQL(graphQL, context, "{viewing(vertex:\"user01\"){source{value} destination{value} startTime{value}}}");

        // Look for reviews, source of user02
        runGraphQL(graphQL, context, "{viewing(source:\"user02\"){destination{value} startTime{value}}}");

        // Look for reviews, destination filmC
        runGraphQL(graphQL, context, "{viewing(destination:\"filmC\"){source_value destination{value} startTime{value}}}");

        // Look for name of the reviewers of for filmA, shows hopping between entities
        runGraphQL(graphQL, context, "{review(vertex:\"filmA\"){vertex{film{name{value}}}}}");

        // Look for all the ratings for a film reviewed filmA, shows hopping back and forth
        runGraphQL(graphQL, context, "{review(vertex:\"filmA\"){rating{value} vertex{film{vertex{review{rating{value}}}}}}}");

        // Attempt to traverse from person -> viewing (FORWARDS)
        runGraphQL(graphQL, context, "{person(vertex:\"user01\"){vertex{viewing{startTime{value}}}}}");

        // Attempt to traverse from film -> viewing (BACKWARDS)
        runGraphQL(graphQL, context, "{film(vertex:\"filmA\"){vertex{viewing{startTime{value}}}}}");

        // Attempt to traverse from person -> viewing -> film.name (FORWARDS)
        runGraphQL(graphQL, context, "{person(vertex:\"user01\"){vertex{viewing{startTime{value} destination{film{name{value}}}}}}}");

        // Attempt to traverse from film -> viewing -> person.name (BACKWARDS)
        runGraphQL(graphQL, context, "{film(vertex:\"filmA\"){name{value} vertex{viewing{startTime{value} source{person{name{value}}}}}}}");

        // Attempt to traverse from review -> film and user (hop out to vertex from userId property)
        runGraphQL(graphQL, context, "{review(vertex:\"filmA\"){vertex{film{name{value}}} userId{person{name{value}}}}}");
    }

    private final void runGraphQL(final GraphQL graphQL,
                                  final GrafferQLContext context,
                                  final String query) {
        LOGGER.info("Running Query");
        context.reset();
        final ExecutionResult result = graphQL
                .execute(query, context);
        LOGGER.info("Viewing Result: " + result.getData());
        LOGGER.info(String.format("Operations Run (%d, cache used %d) %s",
                context.getOperations().size(),
                context.getCacheUsed(),
                context.getOperations()));
        if (result.getErrors().size() > 0) {
            LOGGER.info("Viewing Errors: " + result.getErrors());
            fail("GraphQL Reported Errors " + result.getErrors());
        }

    }
}
