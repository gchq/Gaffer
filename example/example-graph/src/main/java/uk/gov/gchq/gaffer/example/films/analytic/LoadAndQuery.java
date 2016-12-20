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

package uk.gov.gchq.gaffer.example.films.analytic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.example.films.data.Certificate;
import uk.gov.gchq.gaffer.example.films.data.SampleData;
import uk.gov.gchq.gaffer.example.films.data.schema.Group;
import uk.gov.gchq.gaffer.example.films.data.schema.Property;
import uk.gov.gchq.gaffer.example.films.data.schema.TransientProperty;
import uk.gov.gchq.gaffer.example.films.function.transform.StarRatingTransform;
import uk.gov.gchq.gaffer.example.films.generator.DataGenerator;
import uk.gov.gchq.gaffer.function.filter.IsEqual;
import uk.gov.gchq.gaffer.function.filter.Not;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetEntities;
import uk.gov.gchq.gaffer.user.User;

/**
 * This example shows how to interact with a Gaffer graph using a Film example.
 */
public class LoadAndQuery {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadAndQuery.class);

    /**
     * The user for the user doing the query.
     * Here we are setting the authorisation to include all certificates so the user will be able to see all the data.
     */
    private static final User USER = new User.Builder()
            .userId("user02")
            .dataAuth(Certificate.U.name())
            .dataAuth(Certificate.PG.name())
            .dataAuth(Certificate._12A.name())
            .dataAuth(Certificate._15.name())
            .dataAuth(Certificate._18.name())
            .build();

    public static void main(final String[] args) throws OperationException {
        final CloseableIterable<Entity> results = new LoadAndQuery().run();
        final StringBuilder builder = new StringBuilder("Results from query:\n");
        for (final Entity result : results) {
            builder.append(result).append("\n");
        }
        results.close();
        LOGGER.info(builder.toString());
    }

    /**
     * Finds average reviews (from other users) of all films viewed by user02.
     * <ul>
     * <li>Starts from a seed of user02.</li>
     * <li>Finds all filmIds connected to user02 (adjacent entity seeds)</li>
     * <li>Then finds all reviews that have those filmIds.</li>
     * <li>Then filters out all reviews from user02.</li>
     * <li>Then aggregates the reviews together.</li>
     * <li>Then transforms the rating from a percent to a 5 star rating and stores the value in a transient property called starRating</li>
     * <li>Then returns the reviews (Entities)</li>
     * </ul>
     * This query can be written in JSON and executed over a rest service - see
     * resources/example/films/json/load.json and resources/example/films/json/query.json
     *
     * @return the review entities
     * @throws OperationException if operation chain fails to be executed on the graph
     */
    public CloseableIterable<Entity> run() throws OperationException {
        // Setup graph
        final Graph graph = new Graph.Builder()
                .storeProperties(StreamUtil.openStream(getClass(), "/example/films/mockaccumulostore.properties", true))
                .addSchemas(StreamUtil.openStreams(getClass(), "/example/films/schema", true))
                .build();

        // Populate the graph with some example data
        // Create an operation chain. The output from the first operation is passed in as the input the second operation.
        // So the chain operation will generate elements from the domain objects then add these elements to the graph.
        final OperationChain<Void> populateChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<>()
                        .objects(new SampleData().generate())
                        .generator(new DataGenerator())
                        .build())
                .then(new AddElements.Builder()
                        .build())
                .build();

        // Execute the populate operation chain on the graph
        graph.execute(populateChain, USER);


        // Run a query on the graph to fetch average star ratings for all films user02 has watched.
        // Create an operation chain.
        // So the chain operation will get the adjacent review entity seeds then get the review entities.
        final OperationChain<CloseableIterable<Entity>> queryChain = new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds.Builder()
                        .view(new View.Builder()
                                .edge(Group.VIEWING)
                                .build())
                        .addSeed(new EntitySeed("user02"))
                        .build())
                .then(new GetEntities.Builder()
                        .view(new View.Builder()
                                .entity(Group.REVIEW, new ViewElementDefinition.Builder()
                                        .transientProperty(TransientProperty.FIVE_STAR_RATING, Float.class)
                                        .preAggregationFilter(new ElementFilter.Builder()
                                                .select(Property.USER_ID)
                                                .execute(new Not(new IsEqual("user02")))
                                                .build())
                                        .groupBy() // grouping by nothing will cause all properties to be aggregated
                                        .transformer(new ElementTransformer.Builder()
                                                .select(Property.RATING, Property.COUNT)
                                                .project(TransientProperty.FIVE_STAR_RATING)
                                                .execute(new StarRatingTransform())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();

        // Execute the query operation chain on the graph.
        return graph.execute(queryChain, USER);
    }
}
